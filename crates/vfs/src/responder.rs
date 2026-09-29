//! Completion: [`Responder`], and the responders this crate provides.
//!
//! # Exactly once, and never left pending
//!
//! Every op completes exactly once through its responder:
//! [`Responder::done`] takes the responder by value, so it cannot be
//! called twice. The other half of the rule is the **drop fail-safe**: a
//! responder dropped without `done` — an early return, a panic unwinding
//! through the op, a deferred completion whose thread could not be
//! started — must still answer, with an error, never leave its op pending
//! (a FUSE request never answered hangs its caller for good,
//! uninterruptibly). Every implementation states how it guarantees that:
//!
//! - [`Blocking`]: the waiter holds the receiving end of a channel the
//!   responder is the only sender of; a dropped responder disconnects it,
//!   and [`BlockingWait::wait`] answers `Code::Io`.
//! - [`FnResponder`]: its `Drop` calls the function with `Code::Io` when
//!   `done` never ran.
//! - [`CollectDir`]: wraps a [`Blocking`], so the same.
//! - The FUSE frontend's responders are zero-sized-overhead wrappers
//!   around fuser's `Reply*` types, which answer `EIO` from their own
//!   `Drop` (`fuser::ReplyRaw`, with a warning in the log) — the wrapper
//!   adds nothing and loses nothing.
//!
//! A responder is `Send + 'static`: an op whose completion waits on
//! something unbounded (a lock grant) moves it to another thread and
//! returns the calling thread at once (plan 31 §6.3).

use crate::error::{VfsError, VfsResult};
use crate::name::NameBuf;
use crate::types::{FileKind, Ino};
use constellation_types::Code;
use std::marker::PhantomData;
use std::sync::mpsc;
use std::time::Duration;

/// Completes one op (see the module doc for the exactly-once and drop
/// rules every implementation follows).
pub trait Responder<T>: Send + 'static {
    fn done(self, result: VfsResult<T>);
}

/// A responder that hands the result to a thread parked on a
/// [`BlockingWait`]. For synchronous callers — tests, a frontend site not
/// yet converted to real deferral — not how a kernel frontend answers.
pub struct Blocking<T>(mpsc::SyncSender<VfsResult<T>>);

/// The parked side of a [`Blocking`].
pub struct BlockingWait<T>(mpsc::Receiver<VfsResult<T>>);

impl<T: Send + 'static> Blocking<T> {
    /// A responder and the handle to wait on its result.
    pub fn pair() -> (Blocking<T>, BlockingWait<T>) {
        let (tx, rx) = mpsc::sync_channel(1);
        (Blocking(tx), BlockingWait(rx))
    }

    /// Run `op` with a fresh responder and wait for its result (from this
    /// thread or any other).
    pub fn run(op: impl FnOnce(Blocking<T>)) -> VfsResult<T> {
        let (responder, wait) = Self::pair();
        op(responder);
        wait.wait()
    }
}

impl<T: Send + 'static> Responder<T> for Blocking<T> {
    fn done(self, result: VfsResult<T>) {
        // A buffer of one and a single send: never blocks. The waiter
        // having given up (dropped) is not the responder's concern.
        let _ = self.0.send(result);
    }
}

impl<T> BlockingWait<T> {
    /// The op's result; `Code::Io` if the responder was dropped unanswered.
    pub fn wait(self) -> VfsResult<T> {
        self.0.recv().unwrap_or(Err(VfsError::new(Code::Io)))
    }

    /// As [`Self::wait`], giving up with `Code::TimedOut` after `timeout`.
    pub fn wait_timeout(&self, timeout: Duration) -> VfsResult<T> {
        match self.0.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(VfsError::new(Code::TimedOut)),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(VfsError::new(Code::Io)),
        }
    }
}

/// A responder that calls a function with the result — on `done`, or
/// with `Code::Io` from `Drop` if `done` never ran.
pub struct FnResponder<T, F>
where
    F: FnOnce(VfsResult<T>) + Send + 'static,
{
    f: Option<F>,
    _result: PhantomData<fn(T)>,
}

impl<T, F> FnResponder<T, F>
where
    F: FnOnce(VfsResult<T>) + Send + 'static,
{
    pub fn new(f: F) -> Self {
        Self {
            f: Some(f),
            _result: PhantomData,
        }
    }
}

impl<T: 'static, F> Responder<T> for FnResponder<T, F>
where
    F: FnOnce(VfsResult<T>) + Send + 'static,
{
    fn done(mut self, result: VfsResult<T>) {
        if let Some(f) = self.f.take() {
            f(result);
        }
    }
}

impl<T, F> Drop for FnResponder<T, F>
where
    F: FnOnce(VfsResult<T>) + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f(Err(VfsError::new(Code::Io)));
        }
    }
}

/// Where `readdir` puts entries: the frontend's reply buffer. `readdir`'s
/// responder is also its sink (`R: DirSink + Responder<()>`), so the
/// entries and the completion travel together, as FUSE's
/// `ReplyDirectory` has them.
pub trait DirSink {
    /// Add one entry; `next` is the cookie to resume after it. Returns
    /// `true` when the buffer is full (the entry was not added: resume
    /// from it next time).
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool;
}

/// One directory entry, as [`CollectDir`] collects them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub ino: Ino,
    pub next: u64,
    pub kind: FileKind,
    pub name: NameBuf,
}

/// A blocking `readdir` responder that collects up to `limit` entries.
pub struct CollectDir {
    entries: Vec<DirEntry>,
    limit: usize,
    done: Blocking<Vec<DirEntry>>,
}

impl CollectDir {
    pub fn pair(limit: usize) -> (CollectDir, BlockingWait<Vec<DirEntry>>) {
        let (done, wait) = Blocking::pair();
        (
            CollectDir {
                entries: Vec::new(),
                limit,
                done,
            },
            wait,
        )
    }
}

impl DirSink for CollectDir {
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        if self.entries.len() >= self.limit {
            return true;
        }
        self.entries.push(DirEntry {
            ino,
            next,
            kind,
            name: NameBuf::new(name),
        });
        false
    }
}

impl Responder<()> for CollectDir {
    fn done(self, result: VfsResult<()>) {
        self.done.done(result.map(|()| self.entries));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn a_blocking_responder_delivers_across_threads() {
        let got = Blocking::run(|r: Blocking<u32>| {
            std::thread::spawn(move || r.done(Ok(7)));
        });
        assert_eq!(got, Ok(7));
        let err = Blocking::<()>::run(|r| r.done(Err(Code::NotFound.into())));
        assert_eq!(err, Err(VfsError::new(Code::NotFound)));
    }

    #[test]
    fn a_dropped_blocking_responder_answers_io_never_hangs() {
        // Dropped on this thread, and dropped on another one.
        assert_eq!(Blocking::<u32>::run(drop), Err(Code::Io.into()));
        let got = Blocking::<u32>::run(|r| {
            std::thread::spawn(move || drop(r)).join().unwrap();
        });
        assert_eq!(got, Err(Code::Io.into()));
        // An op still running is a timeout for a bounded waiter, not a hang.
        let (r, wait) = Blocking::<u32>::pair();
        assert_eq!(
            wait.wait_timeout(Duration::from_millis(10)),
            Err(Code::TimedOut.into())
        );
        r.done(Ok(1));
        assert_eq!(wait.wait(), Ok(1));
    }

    #[test]
    fn a_dropped_fn_responder_answers_io_exactly_once() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let record = |calls: &Arc<Mutex<Vec<VfsResult<u8>>>>| {
            let calls = calls.clone();
            FnResponder::new(move |r| calls.lock().unwrap().push(r))
        };
        record(&calls).done(Ok(3));
        drop(record(&calls));
        // A panic unwinding through the op drops it too.
        let r = record(&calls);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _r = r;
            panic!("op failed");
        }));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![Ok(3), Err(Code::Io.into()), Err(Code::Io.into())]
        );
    }

    #[test]
    fn collect_dir_stops_at_its_limit_and_answers_with_the_entries() {
        let (mut sink, wait) = CollectDir::pair(2);
        assert!(!sink.add(1, 1, FileKind::Dir, b"."));
        assert!(!sink.add(1, 2, FileKind::Dir, b".."));
        assert!(sink.add(9, 3, FileKind::File, b"f"), "full");
        sink.done(Ok(()));
        let entries = wait.wait().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].name, NameBuf::from(".."));
    }
}
