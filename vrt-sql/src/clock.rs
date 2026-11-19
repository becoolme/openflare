//! 可注入时钟（T36）：lease 到期/续租/边界判定全部走 `Clock`，
//! 测试可确定性推进。

use std::sync::atomic::{AtomicU64, Ordering};

/// 毫秒级墙钟。
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// unix epoch 毫秒。
    fn now_ms(&self) -> u64;
}

/// 系统时钟。
#[derive(Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// 测试时钟：手动推进，确定性边界测试。
#[derive(Debug)]
pub struct TestClock {
    t: AtomicU64,
}

impl TestClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            t: AtomicU64::new(start_ms),
        }
    }
    pub fn advance(&self, ms: u64) {
        self.t.fetch_add(ms, Ordering::SeqCst);
    }
    pub fn set(&self, ms: u64) {
        self.t.store(ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.t.load(Ordering::SeqCst)
    }
}
