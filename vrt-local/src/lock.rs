//! Single-writer guard: `flock(LOCK_EX|LOCK_NB)` on `state/lock`.
//! Advisory — every mutating API in this crate takes it, so the
//! conflict it protects against is two vrt-local writers.

use crate::LocalError;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::Path;

pub struct StateLock {
    _file: File,
}

impl StateLock {
    pub fn acquire(root: &Path) -> Result<Self, LocalError> {
        let path = crate::layout::state_dir(root).join("lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|_| LocalError::Layout("lock file"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(LocalError::Locked);
        }
        Ok(Self { _file: file })
    }
}
