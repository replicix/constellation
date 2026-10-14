//! Children that die with the harness.
//!
//! A harness killed by an agent's `timeout` (or `kill -9`) used to leave
//! its `constellation mount` daemons, S3 gateways and proxies running for
//! days. [`TiedSpawn::spawn_tied`] starts a child with `PR_SET_PDEATHSIG`
//! = SIGTERM (Linux; other hosts have no such signal and do not tie their
//! children): when the harness process goes, the kernel TERMs the child,
//! and a daemon then runs its own orderly shutdown (its zombie reaper,
//! which has no death signal, lazily unmounts whatever the daemon left).
//!
//! The signal fires when the *thread* that forked the child exits, not the
//! process, and scenarios fork from short-lived worker threads. So every
//! child is forked by one process-long spawner thread: the caller's
//! command is handed over, forked there, and the `Child` handed back.

use std::process::{Child, Command};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Mutex, OnceLock};

type Job = Box<dyn FnOnce() + Send>;

fn spawner() -> &'static Mutex<Sender<Job>> {
    static S: OnceLock<Mutex<Sender<Job>>> = OnceLock::new();
    S.get_or_init(|| {
        let (tx, rx) = channel::<Job>();
        std::thread::Builder::new()
            .name("harness-spawner".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job();
                }
            })
            .expect("spawning the spawner thread");
        Mutex::new(tx)
    })
}

/// Make `cmd`'s child receive SIGTERM when its forking thread dies, where
/// the host can (Linux's `PR_SET_PDEATHSIG`, behind
/// [`constellation_platform::Process::die_with_parent`]). Elsewhere the
/// child is simply not tied, and a harness killed hard leaves it running
/// ([`crate::sweep`] removes mounts and containers, not processes).
fn tie(cmd: &mut Command) -> std::io::Result<()> {
    match constellation_platform::native()
        .process
        .die_with_parent(cmd)
    {
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => Ok(()),
        other => other,
    }
}

pub trait TiedSpawn {
    /// [`Command::spawn`], with the child dying (SIGTERM) with the harness.
    /// Unlike [`Command::spawn`], it consumes the command: `self` is left
    /// an empty `Command` and cannot spawn the same child again.
    fn spawn_tied(&mut self) -> std::io::Result<Child>;
}

impl TiedSpawn for Command {
    fn spawn_tied(&mut self) -> std::io::Result<Child> {
        tie(self)?;
        let mut owned = std::mem::replace(self, Command::new(""));
        let (tx, rx) = channel();
        let job: Job = Box::new(move || {
            let _ = tx.send(owned.spawn());
        });
        spawner()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(job)
            .map_err(|_| std::io::Error::other("spawner thread gone"))?;
        rx.recv()
            .map_err(|_| std::io::Error::other("spawner thread gone"))?
    }
}
