//! async ObjectStore → 同步调用方的桥接（vrt-sql 单写者锁内使用）。
//!
//! 专用 runtime（lazy 全局）驱动 store future：
//! - 调用线程在 runtime 内（async ctx）→ 把 future 交给一个临时
//!   std 线程跑 `rt.block_on`（`Runtime::block_on` 不能嵌套在
//!   runtime 线程里）；本线程 join 等待。等价于把 I/O 卸载到专用
//!   reactor，不依赖 `block_in_place`（单线程 runtime 会 panic）。
//! - 调用线程不在 runtime → 直接 `rt.block_on`。

use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

static STORE_RT: OnceLock<Runtime> = OnceLock::new();

/// 对象存储专用 runtime（2 线程足够：调用方本身串行化）。
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

/// 同步等待一个 store future 完成。
///
/// # Panics
/// store future panic 会传播（`join` unwrap）——视为 bug。
pub fn block_on_store<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let rt = store_runtime();
    if tokio::runtime::Handle::try_current().is_ok() {
        // 已在某 runtime 线程：绝不能 block_on 嵌套 → 卸载到普通线程。
        std::thread::scope(|s| {
            s.spawn(|| rt.block_on(fut))
                .join()
                .expect("vrt-objstore driver thread panicked")
        })
    } else {
        rt.block_on(fut)
    }
}
