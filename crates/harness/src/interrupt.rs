//! SIGINT/SIGTERM as an orderly stop, for the modes that own outside
//! resources (`harness k8s-scenario`'s kind cluster, floci container and
//! namespaces). [`install`] makes the first signal set a flag instead of
//! killing the process; a second one exits at once (status 130).
//!
//! The flag is cooperative: the k8s mode's command runner kills its child
//! and fails, and [`eventually`](crate::scenarios::eventually) stops
//! retrying, while [`aborting`] is true. A scenario so fails fast and the
//! usual `Drop`s tear down what the run created. Teardown code runs under
//! a [`Teardown`] guard, which lets its own commands run to completion.
//!
//! Without [`install`] (every other mode) nothing changes: the flag is
//! never set and the signals keep their default action.

use std::sync::atomic::{AtomicUsize, Ordering};

static SIGNALS: AtomicUsize = AtomicUsize::new(0);
static TEARING_DOWN: AtomicUsize = AtomicUsize::new(0);

extern "C" fn on_signal(_: libc::c_int) {
    const FIRST: &str = "\n=== interrupted: tearing down what this run created \
                         (signal again to exit at once)\n";
    const SECOND: &str = "\n=== interrupted twice: exiting without teardown\n";
    if SIGNALS.fetch_add(1, Ordering::SeqCst) == 0 {
        // SAFETY: write(2) on stderr with a valid buffer.
        unsafe {
            libc::write(2, FIRST.as_ptr().cast(), FIRST.len());
        }
    } else {
        // SAFETY: write(2) and _exit(2) are async-signal-safe.
        unsafe {
            libc::write(2, SECOND.as_ptr().cast(), SECOND.len());
            libc::_exit(130);
        }
    }
}

/// Handle SIGINT and SIGTERM as described in the module docs.
pub fn install() -> std::io::Result<()> {
    for sig in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: a zeroed `sigaction` is a valid starting value; the
        // handler is an `extern "C" fn(c_int)`, as `sa_sigaction` expects
        // without `SA_SIGINFO`, and only touches an atomic and makes
        // async-signal-safe calls.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = (on_signal as extern "C" fn(libc::c_int)) as *const () as usize;
            sa.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut sa.sa_mask);
            if libc::sigaction(sig, &sa, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

/// A stop was asked for.
pub fn interrupted() -> bool {
    SIGNALS.load(Ordering::SeqCst) > 0
}

/// A stop was asked for and no [`Teardown`] is running: long waits and
/// commands give up now.
pub fn aborting() -> bool {
    interrupted() && TEARING_DOWN.load(Ordering::SeqCst) == 0
}

/// Fail if [`aborting`].
pub fn check() -> anyhow::Result<()> {
    if aborting() {
        anyhow::bail!("interrupted");
    }
    Ok(())
}

/// While one is alive, commands and waits run to completion even after a
/// signal: the teardown they belong to is what the signal asked for.
pub struct Teardown(());

impl Teardown {
    pub fn begin() -> Self {
        TEARING_DOWN.fetch_add(1, Ordering::SeqCst);
        Self(())
    }
}

impl Drop for Teardown {
    fn drop(&mut self) {
        TEARING_DOWN.fetch_sub(1, Ordering::SeqCst);
    }
}
