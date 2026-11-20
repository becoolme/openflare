//! Bridge from the async ObjectStore to synchronous callers (used inside the vrt-sql single-writer lock).
//!
//! A dedicated runtime (lazy global) drives the store future:
//! - calling thread is inside a runtime (async ctx) -> hand the future to a temporary
//!   std thread that runs `rt.block_on` (`Runtime::block_on` cannot nest inside a
//!   runtime thread); this thread joins and waits. Equivalent to offloading I/O to a dedicated
//!   reactor, and does not depend on `block_in_place` (which panics on a single-thread runtime).
//! - calling thread is not in a runtime -> call `rt.block_on` directly.

use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

static STORE_RT: OnceLock<Runtime> = OnceLock::new();

/// Dedicated object-store runtime (2 threads are enough: callers are serialized themselves).
pub fn store_runtime() -> &'static Runtime {
    STORE_RT.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("vrt-objstore")
            .build()
            .expect("vrt-objstore runtime")
    })
}

/// Synchronously wait for a store future to complete.
///
/// # Panics
/// A store future panic propagates (`join` unwrap) -- treated as a bug.
pub fn block_on_store<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let rt = store_runtime();
    if tokio::runtime::Handle::try_current().is_ok() {
        // Already on some runtime thread: must not nest block_on -> offload to a plain thread.
        std::thread::scope(|s| {
            s.spawn(|| rt.block_on(fut))
                .join()
                .expect("vrt-objstore driver thread panicked")
        })
    } else {
        rt.block_on(fut)
    }
}
