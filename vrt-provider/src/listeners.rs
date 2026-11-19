//! T13/M5: per-tenant listener reconciliation — snapshot reload diffs
//! the wanted tenant set against live listeners: new tenants get a
//! bound+spawned listener, removed tenants get a graceful shutdown and
//! their socket file is unlinked. A failed bind for one tenant never
//! disturbs the others.

use std::collections::{HashMap, HashSet};
use std::io;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// One live tenant listener: shutdown channel + serve task.
struct Entry {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

/// Live listener set keyed by tenant id.
#[derive(Default)]
pub struct ListenerSet {
    live: HashMap<String, Entry>,
    /// Tasks of tenants whose shutdown was requested — reaped lazily.
    draining: Vec<JoinHandle<io::Result<()>>>,
}

impl ListenerSet {
    /// Empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Live tenant ids (diagnostics).
    pub fn tenants(&self) -> Vec<String> {
        self.live.keys().cloned().collect()
    }

    /// Reconcile `wanted` against live listeners.
    ///
    /// `spawn` is called once per NEW tenant and must bind the listener
    /// and start its serve task, returning the per-tenant shutdown
    /// sender plus the task. `cleanup(tenant)` runs after a removed
    /// tenant's shutdown fires (socket unlink).
    ///
    /// Returns `(added, removed, failed)` tenant ids — bind failures
    /// are isolated per tenant, never aborting the reconcile.
    pub fn reconcile<S, C>(
        &mut self,
        wanted: &[String],
        spawn: S,
        cleanup: C,
    ) -> (Vec<String>, Vec<String>, Vec<String>)
    where
        S: Fn(&str) -> io::Result<(watch::Sender<bool>, JoinHandle<io::Result<()>>)>,
        C: Fn(&str),
    {
        let want: HashSet<&str> = wanted.iter().map(String::as_str).collect();

        let removed: Vec<String> = self
            .live
            .keys()
            .filter(|t| !want.contains(t.as_str()))
            .cloned()
            .collect();
        for tenant in &removed {
            let Some(entry) = self.live.remove(tenant) else {
                continue;
            };
            // Graceful per-tenant shutdown — the task drains in-flight
            // requests and ends; socket cleanup runs immediately (the
            // kernel keeps the bound fd alive until the task exits).
            let _ = entry.shutdown.send(true);
            self.draining.push(entry.task);
            cleanup(tenant);
        }

        let mut added = Vec::new();
        let mut failed = Vec::new();
        for tenant in wanted {
            if self.live.contains_key(tenant) {
                continue;
            }
            match spawn(tenant) {
                Ok((shutdown, task)) => {
                    self.live.insert(tenant.clone(), Entry { shutdown, task });
                    added.push(tenant.clone());
                }
                Err(_) => failed.push(tenant.clone()),
            }
        }
        self.reap();
        (added, removed, failed)
    }

    /// Drop finished drain tasks; log nothing (caller logs spawns).
    fn reap(&mut self) {
        self.draining.retain(|t| !t.is_finished());
    }

    /// Tenants whose removal is still draining.
    pub fn draining_count(&self) -> usize {
        self.draining.iter().filter(|t| !t.is_finished()).count()
    }

    /// Signal every listener to shut down and await all tasks —
    /// used at provider shutdown.
    pub async fn shutdown_all(mut self) {
        for (_, entry) in self.live.drain() {
            let _ = entry.shutdown.send(true);
            self.draining.push(entry.task);
        }
        for task in self.draining {
            let _ = task.await;
        }
    }
}

/// Socket unlink for a removed tenant — idempotent.
pub fn cleanup_socket(dir: &std::path::Path, tenant_id: &str) {
    let _ = std::fs::remove_file(crate::unixsock::socket_path(dir, tenant_id));
}

/// H5-r2: split `wanted` into `(serveable, collided)` under an
/// isolation range `(base, span)`. Every tenant in a uid-colliding
/// group is withheld from binding — the group as a whole fails closed
/// on BOTH sides (the host refuses to launch the same tenants via the
/// same `vrt_securefs::colliding_tenants` over the same desired set).
///
/// Feeding `serveable` into [`ListenerSet::reconcile`] also unbinds a
/// previously-live tenant that just became colliding (it is absent
/// from wanted → drained + socket unlinked), and rebinds the survivor
/// automatically once the collision resolves — no special-casing in
/// the reconcile loop. When `isolation` is `None` every tenant is
/// serveable (no uid semantics exist to collide over).
pub fn partition_colliding(
    isolation: Option<(u32, u32)>,
    wanted: &[String],
) -> (Vec<String>, std::collections::BTreeSet<String>) {
    let Some((base, span)) = isolation else {
        return (wanted.to_vec(), Default::default());
    };
    let collided = vrt_securefs::colliding_tenants(base, span, wanted);
    (
        wanted
            .iter()
            .filter(|t| !collided.contains(*t))
            .cloned()
            .collect(),
        collided,
    )
}
