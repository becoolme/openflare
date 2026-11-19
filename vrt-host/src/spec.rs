//! `TenantLaunchSpec` — the typed contract between assembly and launch.

use crate::endpoint::WorkerEndpoint;
use std::collections::BTreeMap;
use std::path::PathBuf;
use vrt_config::{ExternalAddr, SecretValue};

/// Process-level resource bounds enforced by the supervisor's sampler.
/// `None` disables that axis; a sampler that cannot read the axis reports
/// `Unknown` — never treated as zero or as a violation.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ResourceLimits {
    /// Max process RSS in bytes.
    pub max_rss_bytes: Option<u64>,
    /// Max open file descriptors.
    pub max_fds: Option<u64>,
    /// PT1: dual-signal runaway watchdog (`None` disables). When set,
    /// the monitor probes `spec.heartbeat_endpoint` each tick and
    /// SIGKILLs the process group only when sustained CPU saturation
    /// AND a dead event loop coincide.
    pub runaway: Option<crate::runaway::RunawaySpec>,
}

/// Everything needed to start ONE generation of a tenant's workerd
/// process. Immutable once assembled — a config/version change produces a
/// new spec, never an in-place mutation.
///
/// Secret discipline: `env` entries are injected into the child
/// environment only. The launcher clears the child environment first, so
/// nothing is inherited from vrt-host; secrets never appear in argv, the
/// config text, diagnostics, or logs.
#[derive(Clone, Debug)]
pub struct TenantLaunchSpec {
    /// Tenant this process serves (identifier charset).
    pub tenant_id: String,
    /// Absolute path of the workerd (or stub) binary — no PATH lookup.
    pub binary: PathBuf,
    /// Path of the rendered `config.capnp` (or stub JSON config).
    pub config_path: PathBuf,
    /// sha256 of the config text — dedup/switch decisions and logs.
    pub config_digest: String,
    /// Per-generation working directory (`gen-<n>`) holding the config,
    /// embeds and worker sockets; removed after the generation dies.
    pub work_dir: PathBuf,
    /// `--external-addr=<service>=<addr>` argv fragments.
    pub external_addrs: Vec<ExternalAddr>,
    /// Secret environment entries (`fromEnvironment` bindings).
    /// Debug-redacted via `SecretValue`; `expose()` is called only at the
    /// child-env boundary.
    pub env: BTreeMap<String, SecretValue>,
    /// Non-secret environment entries (e.g. `TMPDIR`). Applied before
    /// `env`, which wins on conflict.
    pub base_env: BTreeMap<String, String>,
    /// `workerId -> ingress socket` the process must open. Socket names
    /// on the wire are `sock-<workerId>`.
    pub worker_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// T37b: `workerId -> queue-consumer dispatch socket` for workers
    /// carrying `queueConsumers` — wire name `sock-qconsumer-<workerId>`,
    /// file `<workerId>-qconsumer.sock`. Empty when no worker consumes.
    pub qconsumer_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// W3: `workerId -> wfops facade socket` for workers carrying
    /// `workflows` — wire name `sock-wfops-<workerId>`, file
    /// `<workerId>-wfops.sock`. Empty when no worker declares one.
    pub wfops_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// CR1: `workerId -> cron dispatcher socket` for workers carrying
    /// `crons` — wire name `sock-cron-<workerId>`, file
    /// `<workerId>-cron.sock`. Empty when no worker declares schedules.
    pub cron_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// W3-r2: `workerId -> workflowName -> facade token` for this
    /// generation — HMAC'd with the host's process pepper at render.
    /// The `/v1/wfops` hop injects the header from this map: a lookup
    /// miss IS the signed-desired undeclared check (names come from
    /// the rendered desired surface). In-process only.
    pub wfop_tokens: BTreeMap<String, BTreeMap<String, String>>,
    /// Process-level resource bounds for the monitor.
    pub limits: ResourceLimits,
    /// PT1: private `vrt-heartbeat` worker endpoint inside THIS
    /// process — `Some` only when the rendered config carries the
    /// watchdog socket (assemble emits it iff `limits.runaway` is
    /// set). Never mapped on ingress, never service-bindable.
    pub heartbeat_endpoint: Option<WorkerEndpoint>,
    /// MIG-12: private `vrt-wu-probe` aggregate-probe endpoint inside
    /// THIS generation's process — `Some` when the rendered config
    /// carries the `vrt-wu` socket (capnp assemble always emits
    /// it). Never mapped on ingress, never reachable by tenants; a
    /// draining generation is probed here before SIGTERM.
    pub waituntil_endpoint: Option<WorkerEndpoint>,
    /// Extra verbatim argv appended after the fixed arguments. Never
    /// carries spec-derived secrets (callers only pass constant flags).
    pub extra_args: Vec<String>,
    /// Generation number this spec was assembled under (the `gen-<n>`
    /// directory). The supervisor keeps retry generations strictly
    /// increasing through a shared counter so retry dirs never collide.
    pub generation: u64,
    /// T13/M2: per-tenant uid/gid process isolation — the launcher
    /// drops to these ids in `pre_exec` (setgroups → setgid → setuid)
    /// and sets `no_new_privs` + `dumpable=0`. `None` runs the child
    /// with the host's ids (dev mode only; production must set it).
    pub isolation: Option<TenantIsolation>,
    /// T13/H2: reassembly context for RETRY launches. `work_dir`/`sock`
    /// are per-ATTEMPT — a retry must never reuse the directory a
    /// draining predecessor still owns (its exit cleanup removes it).
    /// Set by `SupervisorHost::converge`; the actor re-assembles with a
    /// fresh generation number on every launch attempt when a shared
    /// `TenantAssembler` is available.
    pub relaunch: Option<RelaunchCtx>,
    /// P3c-1: Durable Object storage — `Some` when ANY worker in this
    /// spec declares `durableObjectClasses`. Its presence switches the
    /// supervisor to SERIALIZED generation transitions (old generation
    /// fully dead before the new one opens the same on-disk namespace
    /// dirs — see supervisor.rs "DO serialized switching").
    pub do_storage: Option<DoStorageSpec>,
}

/// P3c-1: the tenant's DO storage layout — everything the supervisor
/// needs for serialized switching and lifecycle cleanup. Worker dirs
/// are `<tenant_dir>/<workerId>`; class dirs are
/// `<tenant_dir>/<workerId>/<uniqueKey>` (workerd localDisk layout:
/// `workerd.capnp` DurableObjectNamespace.uniqueKey @1 — one directory
/// per class, actor objects inside).
#[derive(Clone, Debug)]
pub struct DoStorageSpec {
    /// Per-tenant DO root (`<doStorageRoot>/<seg(tenantId)>`) — created
    /// by assemble before the generation launches; removed only on
    /// tenant undeploy (supervisor `delete` lifecycle, idempotent).
    pub tenant_dir: PathBuf,
    /// `workerId → uniqueKey` class dirs THIS spec declares — the
    /// post-switch sweep keeps exactly these; anything else under a
    /// worker dir is a deleted/renamed class's leftover.
    pub worker_classes: std::collections::BTreeMap<String, Vec<String>>,
    /// DO1 `transferred_classes` lineage — `(workerId, uniqueKey) →
    /// fromWorkerId`: the data dir may still live under the SOURCE
    /// worker's segment. The launch handoff moves
    /// `<tenant>/<src>/<key>` → `<tenant>/<dst>/<key>` (atomic rename
    /// under the placement lease) or restores the committed snapshot;
    /// neither present → fail closed, never open an empty dir.
    pub transfer_sources: std::collections::BTreeMap<(String, String), String>,
}

/// Per-tenant process identity (T13/M2).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TenantIsolation {
    /// Real/effective uid of the child process.
    pub uid: u32,
    /// Real/effective gid (sole supplementary group cleared too).
    pub gid: u32,
}

/// Everything the supervisor needs to re-assemble this spec for a
/// retry: the tenant fragment + desired revision it was rendered from.
#[derive(Clone, Debug)]
pub struct RelaunchCtx {
    /// The `compute.desired` tenant fragment this spec came from.
    pub tenant: snapshot_protocol::compute_desired::TenantDesired,
    /// The desired revision it was rendered for.
    pub revision: u64,
}
