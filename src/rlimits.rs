use std::ffi::CString;

pub const MAX_FILE_DESCRIPTORS: u64 = 8192;

/// Get the current maximum number of file descriptors.
///
/// It makes an underlying `getrlimit` syscall and returns the result.
/// If the syscall fails, it logs the error using `perror` and returns `None`.
///
/// See man 2 getrlimit for more details.
pub fn max_file_descriptors() -> Option<libc::rlimit> {
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };

    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            let message = CString::new("getrlimit failed").expect("CString::new succeds");
            // We log here so we can capture `errno` returned by the syscall.
            libc::perror(message.as_ptr());
            return None;
        }
    }

    tracing::info!(target: "limits", "Max file descriptor limits: soft={}, hard={}", limit.rlim_cur, limit.rlim_max);

    Some(limit)
}

/// Ensure the maximum number of file descriptors is at least the specified number.
///
/// It makes an underlying `gertlimit` and `setrlimit` syscall to set the soft limit to the specified number,
/// capped by the hard limit and `MAX_FILE_DESCRIPTORS`.
///
/// See man 2 setrlimit for more details.
pub fn ensure_max_file_descriptors(number: u64) -> Result<(), &'static str> {
    unsafe {
        let mut fd_limits = max_file_descriptors().ok_or("failed to get file descriptors limit")?;

        if fd_limits.rlim_cur >= number.min(MAX_FILE_DESCRIPTORS) {
            tracing::info!(target: "limits", "File descriptor limits already sufficient: soft={}, hard={}", fd_limits.rlim_cur, fd_limits.rlim_max);
            return Ok(());
        }

        // Raise the soft limit to the hard limit
        fd_limits.rlim_cur = number.min(fd_limits.rlim_max).min(MAX_FILE_DESCRIPTORS);
        if libc::setrlimit(libc::RLIMIT_NOFILE, &fd_limits) != 0 {
            let message = CString::new("setrlimit failed").expect("CString::new succeds");
            // We log here so we can capture `errno` returned by the syscall.
            libc::perror(message.as_ptr());
            return Err("failed to set file descriptors limit");
        }

        tracing::info!(target: "limits", "Updated file descriptor limits: soft={}, hard={}", fd_limits.rlim_cur, fd_limits.rlim_max);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_fd_limits() {
        let limits = max_file_descriptors().expect("should get fd limits");
        assert!(limits.rlim_cur > 0);
        assert!(limits.rlim_max > 0);
        assert!(limits.rlim_cur <= limits.rlim_max);
    }

    #[test]
    fn test_set_fd_limits() {
        let original_limits = max_file_descriptors().expect("should get fd limits");

        // Try to set to the max file descriptors limit, capped by MAX_FILE_DESCRIPTORS
        let new_soft_limit = (original_limits.rlim_max).min(MAX_FILE_DESCRIPTORS);
        ensure_max_file_descriptors(new_soft_limit).expect("should set fd limits");

        let updated_limits = max_file_descriptors().expect("should get fd limits");
        assert_eq!(updated_limits.rlim_cur, new_soft_limit);
        assert_eq!(updated_limits.rlim_max, original_limits.rlim_max);

        // Restore original limits
        ensure_max_file_descriptors(original_limits.rlim_cur).expect("should restore fd limits");
    }
}
