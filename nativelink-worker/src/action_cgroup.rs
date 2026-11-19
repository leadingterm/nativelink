// Copyright 2024 The NativeLink Authors. All rights reserved.
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct Source {
    pub(crate) metadata: PathBuf,
    pub(crate) pool: PathBuf,
    pub(crate) pid: u32,
    pub(crate) start: u64,
    pub(crate) scope: Option<PathBuf>,
}

impl Source {
    pub(crate) fn scope(&mut self) -> Option<&Path> {
        if self.scope.is_none() {
            let text = std::fs::read_to_string(&self.metadata).ok()?;
            self.scope = validate_scope(&text, &self.pool, self.pid, self.start);
        }
        self.scope.as_deref()
    }
}

/// The wrapper's start time is captured before its child handle can be reaped.
pub(crate) fn process_start(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
}

/// Accept only the exact direct child created by this wrapper, never a sibling.
pub(crate) fn validate_scope(value: &str, pool: &Path, pid: u32, start: u64) -> Option<PathBuf> {
    if !value.starts_with('/')
        || value
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return None;
    }
    let path = Path::new(value);
    if path.parent()? != pool {
        return None;
    }
    let mut parts = path.file_name()?.to_str()?.split('-');
    if parts.next()?.parse::<u32>().ok()? != pid
        || parts.next()?.parse::<u64>().ok()? != start
        || parts.next()?.parse::<u64>().is_err()
        || parts.next().is_some()
    {
        return None;
    }
    Some(path.to_path_buf())
}

pub(crate) fn read_kib(path: &Path, name: &str) -> Option<u64> {
    let bytes: u64 = std::fs::read_to_string(path.join(name))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(bytes / 1024 + u64::from(bytes % 1024 != 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_requires_normalized_direct_child_and_wrapper_identity() {
        let pool = Path::new("/sys/fs/cgroup/pool");
        assert_eq!(
            validate_scope("/sys/fs/cgroup/pool/71-983-17", pool, 71, 983),
            Some(pool.join("71-983-17"))
        );
        for path in [
            "/sys/fs/cgroup/pool/../pool/71-983-17",
            "/sys/fs/cgroup/other/71-983-17",
            "/sys/fs/cgroup/pool/72-983-17",
            "/sys/fs/cgroup/pool/71-984-17",
            "/sys/fs/cgroup/pool/71-983-x",
        ] {
            assert!(validate_scope(path, pool, 71, 983).is_none(), "{path}");
        }
    }

    #[test]
    fn kernel_readings_round_up_to_kib() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("memory.current"), "2051\n").unwrap();
        std::fs::write(dir.path().join("memory.peak"), "3073\n").unwrap();
        assert_eq!(read_kib(dir.path(), "memory.current"), Some(3));
        assert_eq!(read_kib(dir.path(), "memory.peak"), Some(4));
        assert_eq!(read_kib(dir.path(), "missing"), None);
    }
}
