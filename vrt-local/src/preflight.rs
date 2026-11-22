//! Deploy-time config-load check (W1-r2/P0-2).
//!
//! Before `deploy` publishes a revision, render the post-deploy
//! tenant config exactly the way vrt-host will and ask the real
//! workerd whether it loads — a config that cannot load must fail
//! HERE, while the published state is still untouched, instead of
//! taking the whole tenant down at the next apply (the W1-r2
//! `miniflare:zod` outage).
//!
//! The check is driven by `config/vrt-host.toml` — the same document
//! `vrt init` writes for the host — so it can never drift from the
//! host's real inputs:
//!   - file absent          → not a standalone host root: skip
//!                            (library/test usage)
//!   - `[render].mode` stub → stub workerd never parses capnp: skip
//!   - capnp mode           → render + `workerd test`, fail closed.
//!
//! The subprocess verdict lives in [`vrt_config::check`] — the same
//! classifier the supervisor's apply-time preflight uses.

use crate::layout;
use crate::state::LocalState;
use crate::LocalError;
use std::path::{Path, PathBuf};

/// Fields the check needs out of `config/vrt-host.toml`.
struct HostCfg {
    workerd: PathBuf,
    facade_manifest: PathBuf,
    binding_backend_socket: String,
    system_compat_date: String,
    system_compat_flags: Vec<String>,
    /// `render.quotas` — the host's workerd enforces per-worker limits.
    quotas: bool,
}

/// Run the deploy-time check against the post-deploy state (`st`
/// already carries the new active version; nothing is persisted yet).
/// `pending` is the not-yet-stored artifact for the version being
/// deployed (`(digest, stored_document)`) — every other worker's
/// bundle is read from `objects/` as usual. `Err` must abort the
/// deploy before any publish.
pub(crate) fn deploy_preflight(
    root: &Path,
    st: &LocalState,
    pending: Option<(&str, &[u8])>,
) -> Result<(), LocalError> {
    let Some(cfg) = read_host_config(root)? else {
        return Ok(());
    };
    let desired = st.to_desired()?;
    let tenant = desired
        .tenants
        .first()
        .ok_or(LocalError::State("desired tenant"))?;

    // RuntimeParams mirrors the host's manifest-derived params — the
    // SAME seeded manifest plus vars/secrets from state. The DO root
    // is a throwaway under the check dir: the real DO root is never
    // touched by a config that may not even load.
    let work = std::fs::canonicalize(layout::host_work_dir(root)).map_err(LocalError::Io)?;
    // Sweep stale check dirs — a crash between materialize and
    // cleanup must not accumulate bounded-but-repeating garbage.
    if let Ok(rd) = std::fs::read_dir(&work) {
        for e in rd.flatten() {
            if e.file_name().to_string_lossy().starts_with("preflight-") {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    }
    let check_dir = work.join(format!("preflight-{}-{}", std::process::id(), seq()));
    let manifest_raw = std::fs::read_to_string(&cfg.facade_manifest)
        .map_err(|_| LocalError::Op("deploy check", "facade manifest unreadable".into()))?;
    let manifest = vrt_config::parse_facade_manifest(&manifest_raw)
        .map_err(|_| LocalError::Op("deploy check", "facade manifest invalid".into()))?;
    let secrets = crate::state::read_secrets(root)?
        .into_iter()
        .map(|(t, m)| {
            (
                t,
                m.into_iter()
                    .map(|(r, v)| (r, vrt_config::SecretValue::new(v)))
                    .collect(),
            )
        })
        .collect();
    let do_storage_dir = check_dir
        .join("do")
        .join(vrt_securefs::segment_encode(st.tenant_id()));
    let params = vrt_config::RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: cfg.binding_backend_socket.clone(),
        egress: manifest.egress,
        egress_policy: Default::default(),
        deny_outbound: manifest.deny_outbound,
        facades: manifest.facades,
        system_workers: manifest.system_workers,
        system_compat_date: cfg.system_compat_date.clone(),
        system_compat_flags: cfg.system_compat_flags.clone(),
        vars: st.vars_union(),
        secrets,
        do_storage_dir: do_storage_dir.display().to_string(),
        // W3-r2: per-process pepper — preflight renders wfops
        // facade tokens that are never verified locally (no
        // `/v1/wfops` hop exists in vrt-local); any value suffices,
        // but a fresh process pepper mirrors production semantics.
        wfop_pepper: crate::preflight::local_wfop_pepper(),
        // vrt-local has no control listener — the loopback shim
        // degrades to an explicit 501 (fail closed).
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        // Standalone has no AE ingest service — `analyticsEngine`
        // bindings fail the render closed (`aeAddr`).
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        // MIG-12: mirror the production probe-socket emit so the
        // workerd load check covers the wrapped-module surface.
        waituntil_socket: "sock/vrt-wu.sock".into(),
        log_tail: vrt_config::LogTailParams::default(),
    };

    // Bundles position-parallel to `tenant.workers` — same contract
    // as vrt-host assemble (`bundles[worker_idx]`).
    let mut bundles = Vec::with_capacity(tenant.workers.len());
    for w in &tenant.workers {
        let digest = &w.active_version.artifact_digest;
        let bytes = match pending {
            Some((d, doc)) if d == digest => doc.to_vec(),
            _ => std::fs::read(layout::objects_dir(root).join(digest))
                .map_err(|_| LocalError::Op("deploy check", "artifact object missing".into()))?,
        };
        bundles.push(
            vrt_config::VersionBundle::parse_stored(&bytes).map_err(|_| {
                LocalError::Op("deploy check", "artifact not a stored bundle".into())
            })?,
        );
    }
    // Per-worker quotas: the deploy must fail HERE when the host's
    // workerd cannot enforce a declared limit — otherwise the published
    // snapshot would render as a skipped worker (or run unlimited).
    vrt_config::ensure_quotas_enforceable(tenant, cfg.quotas)
        .map_err(|e| LocalError::Op("deploy check", format!("quotas: {e}")))?;
    let rendered = vrt_config::render_tenant_config(&vrt_config::TenantRenderInput {
        tenant,
        revision: st.revision,
        bundles: &bundles,
        params: &params,
    })
    .map_err(|e| LocalError::Op("deploy check", format!("render: {e}")))?;

    // Materialize the check gen dir — config + every declared embed,
    // digest-verified like assemble. The dir is fully removed after.
    let result =
        materialize_and_check(&check_dir, &rendered, tenant, &bundles, &params, root, &cfg);
    let _ = std::fs::remove_dir_all(&check_dir);
    result
}

/// Write config + embeds under `dir`, then run the shared workerd
/// load check with the same argv shape a serve launch would get.
/// W3-r2: pepper for the throwaway check render — fresh process
/// material per call (urandom; time+pid fallback). Preflight tokens
/// are never verified (vrt-local has no `/v1/wfops` hop), but the
/// render must not see an empty pepper (fail-closed emit rule).
pub fn local_wfop_pepper() -> String {
    let mut buf = [0u8; 32];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut buf))
        .is_err()
    {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id() as u128;
        for (i, b) in buf.iter_mut().enumerate() {
            *b = ((nanos ^ pid).wrapping_mul(0x9e3779b97f4a7c15) >> (i % 8)) as u8;
        }
    }
    hex::encode(buf)
}

fn materialize_and_check(
    dir: &Path,
    rendered: &vrt_config::RenderedConfig,
    tenant: &snapshot_protocol::compute_desired::TenantDesired,
    bundles: &[vrt_config::VersionBundle],
    params: &vrt_config::RuntimeParams,
    root: &Path,
    cfg: &HostCfg,
) -> Result<(), LocalError> {
    for embed in &rendered.embeds {
        let bytes = match &embed.kind {
            vrt_config::EmbedKind::BundleModule {
                worker_id,
                module_name,
            } => {
                let idx = tenant
                    .workers
                    .iter()
                    .position(|w| &w.worker_id == worker_id)
                    .ok_or(LocalError::Op(
                        "deploy check",
                        "embed worker missing".into(),
                    ))?;
                bundles[idx]
                    .manifest()
                    .modules
                    .iter()
                    .find(|m| m.name() == module_name.as_str())
                    .ok_or(LocalError::Op(
                        "deploy check",
                        "embed module missing".into(),
                    ))?
                    .content_bytes()
                    .map_err(|_| LocalError::Op("deploy check", "embed module decode".into()))?
            }
            vrt_config::EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                // System module digests come from the facade manifest —
                // bytes are the init-seeded `objects/<digest>`.
                let digest = params
                    .system_workers
                    .iter()
                    .find(|w| w.service_name == service_name.as_str())
                    .and_then(|w| w.modules.iter().find(|m| m.name == module_name.as_str()))
                    .map(|m| m.digest.as_str())
                    .ok_or(LocalError::Op("deploy check", "embed system module".into()))?;
                std::fs::read(layout::objects_dir(root).join(digest))
                    .map_err(|_| LocalError::Op("deploy check", "system module object".into()))?
            }
            vrt_config::EmbedKind::VendoredModule { name } => {
                vrt_config::workflows::vendored_bytes(name)
                    .ok_or(LocalError::Op(
                        "deploy check",
                        "embed vendored module".into(),
                    ))?
                    .to_vec()
            }
        };
        if hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes)) != embed.digest {
            return Err(LocalError::Op(
                "deploy check",
                "embed digest mismatch".into(),
            ));
        }
        let rel = Path::new(&embed.path);
        if rel.is_absolute()
            || rel
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(LocalError::Op(
                "deploy check",
                "embed path escapes dir".into(),
            ));
        }
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(LocalError::Io)?;
        }
        std::fs::write(&path, &bytes).map_err(LocalError::Io)?;
    }
    // W1-r3 P0: the checked config must behave as if it never ran —
    // `isolated_variant` rewrites unix sockets to loopback AND every
    // `disk = (path)` localDisk target to fresh scratch dirs (created
    // inside), so `workerd test` can neither collide with live
    // sockets nor touch the serving generation's DO storage.
    let scratch = dir.join("preflight-disks");
    std::fs::create_dir_all(&scratch).map_err(LocalError::Io)?;
    let config_path = dir.join("config.capnp");
    std::fs::write(
        &config_path,
        vrt_config::check::isolated_variant(&rendered.text, &scratch)
            .map_err(LocalError::Io)?
            .as_bytes(),
    )
    .map_err(LocalError::Io)?;

    // `--experimental` argv parity with assemble's derivation: the
    // predicate is shared (`vrt_config::launch_args`), so queue
    // consumers, DO/workflow namespaces, cron facades, workerLoader
    // bindings and non-"experimental" system experimental flags all
    // force the arg here exactly as they do at serve time. Deriving it
    // locally is how cron-basic deployments once failed the check.
    let extra = vrt_config::tenant_experimental_extra_args(&cfg.system_compat_flags, tenant);

    // Needles: every env value the child would see — redacted out of
    // the failure tail just like the launch path.
    let needles: Vec<String> = rendered
        .env
        .values()
        .map(|v| v.expose().to_string())
        .collect();
    vrt_config::check::check_workerd_loads(
        &cfg.workerd,
        &config_path,
        &rendered.external_addrs,
        &extra,
        &needles,
    )
    .map_err(|detail| LocalError::Op("deploy check", detail))
}

fn seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// `render.quotas` — does this root's host enforce per-worker limits?
/// Diagnostic only: an unreadable/malformed host config reads `false`
/// (fail closed: never claim enforcement we cannot see). A valid
/// `[render]` without the key reads `true` — that is the host's own
/// default (the pinned workerd carries the limits patch).
pub(crate) fn host_quotas_enforced(root: &Path) -> bool {
    std::fs::read_to_string(root.join("config/vrt-host.toml"))
        .ok()
        .and_then(|raw| raw.parse::<toml::Value>().ok())
        .and_then(|doc| {
            doc.get("render")
                .map(|r| r.get("quotas").and_then(|v| v.as_bool()).unwrap_or(true))
        })
        .unwrap_or(false)
}

/// Read `config/vrt-host.toml`; `None` when absent or stub mode.
fn read_host_config(root: &Path) -> Result<Option<HostCfg>, LocalError> {
    let path = root.join("config/vrt-host.toml");
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) => r,
        Err(_) => return Ok(None),
    };
    let doc: toml::Value = raw
        .parse()
        .map_err(|_| LocalError::Op("deploy check", "vrt-host.toml parse".into()))?;
    // Only a render-capable host root opts in: `vrt init` always
    // writes `[render]`; a toml without it (stub, serve-only, custom)
    // is not a capnp host config — skip rather than guess.
    let Some(render) = doc.get("render") else {
        return Ok(None);
    };
    let mode = render
        .get("mode")
        .and_then(|m| m.as_str())
        .unwrap_or("capnp");
    if mode != "capnp" {
        return Ok(None);
    }
    let get_str = |key: &str| -> Result<String, LocalError> {
        doc.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or(LocalError::Op(
                "deploy check",
                "vrt-host.toml missing key".into(),
            ))
    };
    let render_str = |key: &str| -> Result<String, LocalError> {
        render
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or(LocalError::Op(
                "deploy check",
                "vrt-host.toml render key".into(),
            ))
    };
    let flags = render
        .get("systemCompatFlags")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|f| f.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let workerd = PathBuf::from(get_str("workerdBinary")?);
    if !workerd.is_file() {
        return Err(LocalError::Op(
            "deploy check",
            "workerdBinary unreadable — init --workerd must point at a real workerd".into(),
        ));
    }
    Ok(Some(HostCfg {
        workerd,
        facade_manifest: PathBuf::from(render_str("facadeManifest")?),
        binding_backend_socket: render_str("bindingBackendSocket")?,
        system_compat_date: render_str("systemCompatDate")?,
        system_compat_flags: flags,
        quotas: render
            .get("quotas")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
    }))
}
