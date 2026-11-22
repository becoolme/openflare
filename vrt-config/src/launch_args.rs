//! workerd launch-argv derivation shared by every config-loading path.
//!
//! A rendered config may carry `$experimental`-gated surfaces — the
//! `service_binding_extra_handlers` flag on `facade-qconsumer-*` /
//! `facade-cron-*`, `workerLoader` bindings, `localDisk` DO storage,
//! and any system compat flag the vendored snapshot marks
//! `$experimental`. workerd refuses to load such a config unless
//! `--experimental` is on the command line, so every caller that
//! renders the same tenant must derive the same argv.
//!
//! Before this module existed the derivation was duplicated in
//! `vrt-host::assemble` (the serve path) and `vrt-local`'s deploy
//! preflight; the cron case was added to the former only, so the
//! preflight refused every cron deployment with
//! `The compatibility flag service_binding_extra_handlers is
//! experimental ... you must pass --experimental`. The predicate now
//! has exactly one implementation.

use snapshot_protocol::compute_desired::{BindingDesired, TenantDesired};

/// Tenant launch-shape facts that force `$experimental` workerd
/// surfaces. Derived from the desired snapshot, never from rendered
/// text, so callers cannot disagree about the same tenant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExperimentalNeeds {
    /// Any worker declares `[[queues.consumers]]` — emits
    /// `facade-qconsumer-*` carrying `service_binding_extra_handlers`.
    pub queue_consumers: bool,
    /// Any worker declares DO classes or workflows — `localDisk` DO
    /// storage is `$experimental` in workerd.
    pub durable_objects_or_workflows: bool,
    /// Any worker declares `triggers.crons` — emits `facade-cron-*`
    /// carrying `service_binding_extra_handlers` (`Fetcher.scheduled()`).
    pub crons: bool,
    /// Any worker binds a `workerLoader` — the binding is
    /// `$experimental`-gated upstream.
    pub worker_loaders: bool,
    /// Any worker binds an `analyticsEngine` — `experimental_feature(
    /// "AnalyticsEngine")` in workerd's bindings.
    pub analytics_engine: bool,
}

impl ExperimentalNeeds {
    /// Scan the tenant's desired state for `$experimental` surfaces.
    pub fn of_tenant(tenant: &TenantDesired) -> Self {
        Self {
            queue_consumers: tenant
                .workers
                .iter()
                .any(|w| !w.queue_consumers.is_empty()),
            durable_objects_or_workflows: tenant
                .workers
                .iter()
                .any(|w| !w.durable_object_classes.is_empty() || !w.workflows.is_empty()),
            crons: tenant.workers.iter().any(|w| !w.crons.is_empty()),
            worker_loaders: tenant.workers.iter().any(|w| {
                w.bindings
                    .iter()
                    .any(|b| matches!(b, BindingDesired::WorkerLoader(_)))
            }),
            analytics_engine: tenant.workers.iter().any(|w| {
                w.bindings
                    .iter()
                    .any(|b| matches!(b, BindingDesired::AnalyticsEngine(_)))
            }),
        }
    }
}

/// Extra workerd argv required by `needs` plus the host's system
/// compat flags. The literal `experimental` catch-all is deliberately
/// NOT derived — it stays an operator opt-in (the host config gate in
/// `vrt-host::assemble` refuses a config that carries it without the
/// arg), while every `$experimental` surface this module knows about
/// forces `--experimental` on its own.
pub fn experimental_extra_args(
    system_compat_flags: &[String],
    needs: ExperimentalNeeds,
) -> Vec<String> {
    let needs = needs.queue_consumers
        || needs.durable_objects_or_workflows
        || needs.crons
        || needs.worker_loaders
        || needs.analytics_engine
        || system_compat_flags
            .iter()
            .any(|f| f != "experimental" && crate::is_experimental_compat_flag(f));
    if needs {
        vec!["--experimental".to_string()]
    } else {
        Vec::new()
    }
}

/// [`experimental_extra_args`] for a whole tenant — the form every
/// production launch/deploy path uses.
pub fn tenant_experimental_extra_args(
    system_compat_flags: &[String],
    tenant: &TenantDesired,
) -> Vec<String> {
    experimental_extra_args(system_compat_flags, ExperimentalNeeds::of_tenant(tenant))
}

#[cfg(test)]
mod tests {
    use super::*;
    use snapshot_protocol::compute_desired::{
        CronDesired, QueueConsumerDesired, VersionDesired, WorkerDesired, WorkerLoaderBinding,
        WorkerQuotas,
    };

    fn tenant(workers: Vec<WorkerDesired>) -> TenantDesired {
        TenantDesired {
            tenant_id: "t1".into(),
            workers,
            flagship_apps: Vec::new(),
        }
    }

    /// Minimal worker carrying no `$experimental` surface.
    fn worker() -> WorkerDesired {
        WorkerDesired {
            worker_id: "w_0123456789abcdef".into(),
            name: "w1".into(),
            active_version: VersionDesired {
                version_id: "v1".into(),
                artifact_digest: "0".repeat(64),
                compatibility_date: "2024-09-25".into(),
                compatibility_flags: Vec::new(),
            },
            bindings: Vec::new(),
            routes: Vec::new(),
            crons: Vec::new(),
            queue_consumers: Vec::new(),
            durable_object_classes: Vec::new(),
            workflows: Vec::new(),
            tail_consumers: Vec::new(),
            observability: None,
            streaming_tail_consumers: Vec::new(),
            quotas: WorkerQuotas::platform_default(),
        }
    }

    #[test]
    fn plain_tenant_needs_no_experimental_arg() {
        assert!(tenant_experimental_extra_args(&[], &tenant(vec![worker()])).is_empty());
    }

    #[test]
    fn cron_only_tenant_derives_experimental_arg() {
        // The conformance `cron-basic` shape: one cron-carrying worker,
        // no DO / queues / loaders. Its rendered `facade-cron-*` service
        // carries `service_binding_extra_handlers`, so the deploy
        // preflight must launch workerd with `--experimental`.
        let mut w = worker();
        w.crons = vec![CronDesired {
            schedule_id: "c1".into(),
            expression: "* * * * *".into(),
        }];
        let args = tenant_experimental_extra_args(&[], &tenant(vec![w]));
        assert_eq!(args, vec!["--experimental".to_string()]);
    }

    #[test]
    fn worker_loader_only_tenant_derives_experimental_arg() {
        let mut w = worker();
        w.bindings = vec![BindingDesired::WorkerLoader(WorkerLoaderBinding {
            name: "LOADER".into(),
        })];
        let args = tenant_experimental_extra_args(&[], &tenant(vec![w]));
        assert_eq!(args, vec!["--experimental".to_string()]);
    }

    #[test]
    fn analytics_engine_tenant_derives_experimental_arg() {
        // `analyticsEngine` is `$experimental`-gated upstream too
        // (`experimental_feature("AnalyticsEngine")` in bindings) — a
        // tenant declaring `[[analytics_engine_datasets]]` must get
        // `--experimental` in argv or workerd refuses the config.
        let mut w = worker();
        w.bindings = vec![BindingDesired::AnalyticsEngine(
            snapshot_protocol::compute_desired::AnalyticsEngineBinding {
                name: "AE".into(),
                dataset: "weather".into(),
            },
        )];
        let args = tenant_experimental_extra_args(&[], &tenant(vec![w]));
        assert_eq!(args, vec!["--experimental".to_string()]);
    }

    #[test]
    fn queue_consumer_tenant_derives_experimental_arg() {
        let mut w = worker();
        w.queue_consumers = vec![QueueConsumerDesired {
            queue_id: "q0123456789abcdefghijklmn".into(),
            queue_name: "q1".into(),
            dead_letter_queue_id: None,
            max_batch_size: None,
            max_batch_timeout_secs: None,
            max_retries: None,
            retry_delay_secs: None,
            max_concurrency: None,
        }];
        let args = tenant_experimental_extra_args(&[], &tenant(vec![w]));
        assert_eq!(args, vec!["--experimental".to_string()]);
    }

    #[test]
    fn experimental_system_flag_derives_arg_but_literal_experimental_does_not() {
        let t = tenant(vec![worker()]);
        // A system flag the snapshot marks `$experimental` forces the
        // arg even for an otherwise plain tenant.
        let flags = vec!["service_binding_extra_handlers".to_string()];
        assert_eq!(tenant_experimental_extra_args(&flags, &t).len(), 1);
        // The `experimental` catch-all is the operator's opt-in — it is
        // never derived automatically (vrt-host refuses a config that
        // carries it without the arg).
        assert!(tenant_experimental_extra_args(&["experimental".to_string()], &t).is_empty());
    }
}
