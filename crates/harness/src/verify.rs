//! Invariant checkers of the host-load campaign (phase 0): what must hold
//! whatever the load did, so that a run under load can only ever fail on
//! correctness, never on latency (that is [`crate::hostlag`]'s).
//!
//! - **Data** ([`payload`], [`check_payload`]): every file a campaign
//!   workload writes is self-describing — a header naming its path,
//!   writer and sequence, and a body derived from them — so any byte that
//!   is wrong, any file holding another's content, is found.
//! - **Acknowledged writes** ([`Ledger`]): every create, write, rename
//!   and unlink the application was told succeeded; at the end each one
//!   is in the tree or in a documented conflict copy, or it is a lost
//!   write; corrupt bytes at a path are corruption whatever copy exists.
//!   An op answered in doubt may still land once, late, on its own name
//!   ([`Ledger::failed_create`], [`Ledger::failed_rename`]); the check
//!   runs after quiescence ([`wait_quiescent`]). Acknowledged means `fsync` returned; the `close` is checked too (an
//!   error there is an error to the application). What the protocol
//!   counts on the way (stranded and replayed ops, refused replays,
//!   conflict copies, outwaited grants) is reported beside it
//!   ([`counters`]).
//! - **Lock exclusion**: a counter incremented under `flock` by every
//!   node is exact ([`counter_exact`]); `git-under-flock*`'s ref checks run
//!   under every profile as they are.
//! - **Fencing-token order** ([`check_token_order`]): from the daemons'
//!   `constellation::token_exec` lines — `tagged op executed` at the tag
//!   check, `tagged op landed` once the op's write committed (the host
//!   clock, read after the writer lock: what makes finding F1's delayed
//!   write visible) — and the owners' `granted a lock` lines (all at
//!   debug, which verification turns on): no op tagged with a grant passes
//!   its check or lands after a conflicting grant of the same inode to
//!   another node. A checker that reads nothing it should is a failure,
//!   not a pass ([`check_blind`]: marker lines that do not parse, tagged
//!   ops with no landed line; a missing kept log).
//! - **Errors to applications** ([`ErrnoLog`]): every errno a workload
//!   saw is classified as one the documented semantics allow at that
//!   point, or a bug, and counted per class ([`ErrnoReport`]); an `EIO`
//!   in doubt past the client deadline is a bug unless a triage run
//!   allows it.
//! - **No split-brain**: at most one node acquires the lease per epoch
//!   ([`check_lease_epochs`], from the logs), and the bucket's log has
//!   monotone epochs, one node per epoch, and every rid completed once
//!   ([`s3_log_check`]).
//! - **No hang** ([`start_watch`]): a watch on every daemon's in-flight
//!   FUSE requests; one older than `CONSTELLATION_HARNESS_HANG_S` (120 s)
//!   that is not a blocking lock wait (or an `fsync` across an injected
//!   S3 outage) fails the scenario: the daemon's backtraces are kept
//!   (gdb, else eu-stack, plus every thread's state and `wchan`) and its
//!   FUSE connection is aborted, so the run goes on instead of hanging. A
//!   control socket silent that long fails it too, with backtraces, but
//!   its connection is aborted only at twice the bound (a starved control
//!   service may sit beside FUSE threads that still move).
//!
//! `CONSTELLATION_HARNESS_VERIFY=1` turns on everything automatic: the
//! daemons' logs are kept under `CONSTELLATION_HARNESS_LOG_DIR` (default
//! a temp dir per run; one that cannot be made is an error), their `RUST_LOG` gains the two debug targets, the
//! FUSE stall watch (`CONSTELLATION_FUSE_REQUEST_STALL_S`, half the hang
//! bound) logs backtraces, the hang watch runs, and after every scenario
//! the log checks (token order, lease epochs) and, when its S3
//! environment goes, the bucket log check run over what the scenario
//! did. A violation fails the scenario whatever its own checks said.
//! The data, ledger, counter and errno checkers are for workloads to use
//! (`load-probe`, and the campaign's new scenarios).

use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub const VERIFY_ENV: &str = "CONSTELLATION_HARNESS_VERIFY";
pub const LOG_DIR_ENV: &str = "CONSTELLATION_HARNESS_LOG_DIR";
pub const HANG_ENV: &str = "CONSTELLATION_HARNESS_HANG_S";

/// The debug targets verification adds to every daemon's `RUST_LOG`.
pub const LOG_DIRECTIVES: &str =
    "constellation::token_exec=debug,constellation_authority::core::locks=debug";

/// Verification is on.
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var(VERIFY_ENV).is_ok_and(|v| !matches!(v.trim(), "" | "0" | "off" | "false"))
    })
}

/// The hang bound.
pub fn hang_bound() -> Duration {
    Duration::from_secs(
        std::env::var(HANG_ENV)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(120),
    )
}

/// Where the daemons' logs go (`None`: their scenario's temp dir).
pub fn log_dir() -> Option<&'static Path> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| {
        if let Some(d) = std::env::var_os(LOG_DIR_ENV).filter(|d| !d.is_empty()) {
            return Some(PathBuf::from(d));
        }
        enabled()
            .then(|| std::env::temp_dir().join(format!("harness-load-logs-{}", std::process::id())))
    })
    .as_deref()
}

/// Per scenario: the clients' log files (name, path) and backends.
#[derive(Default)]
struct ScenarioState {
    logs: Vec<(String, PathBuf)>,
    backends: Vec<String>,
    checked_backends: HashSet<String>,
    logs_checked: bool,
    violations: Vec<String>,
    hangs: Vec<String>,
    /// Backends of E2E filesystems (their segments do not decode).
    e2e: HashSet<String>,
    /// Logs a daemon was started on (a missing one of these was removed).
    opened: HashSet<PathBuf>,
}

/// A daemon was started writing to `log` (`Client`'s mount).
pub fn note_log_opened(log: &Path) {
    let scenario = crate::client::scenario();
    with_state(&scenario, |s| {
        s.opened.insert(log.to_path_buf());
    });
}

static STATE: Mutex<BTreeMap<String, ScenarioState>> = Mutex::new(BTreeMap::new());

fn with_state<T>(scenario: &str, f: impl FnOnce(&mut ScenarioState) -> T) -> T {
    let mut s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    f(s.entry(scenario.to_string()).or_default())
}

/// The log path of a client named `name` created now, under
/// [`log_dir`]: `<dir>/<scenario>/<name>[-<k>]/mount.log` (`k` counts a
/// scenario's clients of one name).
///
/// Under verification a log that cannot be placed there is an error, not
/// a silent fallback to the temp dir (its checks would never see it).
pub fn log_path_for(name: &str) -> Result<Option<PathBuf>> {
    let Some(dir) = log_dir() else {
        return Ok(None);
    };
    let scenario = crate::client::scenario();
    let scenario = if scenario.is_empty() {
        "unnamed".to_string()
    } else {
        scenario
    };
    let base = dir.join(&scenario);
    let mut k = 0;
    let path = loop {
        let leaf = if k == 0 {
            name.to_string()
        } else {
            format!("{name}-{k}")
        };
        if !base.join(&leaf).exists() {
            break base.join(leaf);
        }
        k += 1;
    };
    std::fs::create_dir_all(&path)
        .with_context(|| format!("creating the kept log dir {}", path.display()))?;
    let log = path.join("mount.log");
    with_state(&scenario, |s| s.logs.push((name.to_string(), log.clone())));
    Ok(Some(log))
}

/// A client of the current scenario runs an E2E filesystem at `backend`.
pub fn note_e2e(backend: &str) {
    let scenario = crate::client::scenario();
    with_state(&scenario, |s| {
        s.e2e.insert(backend.to_string());
    });
}

/// A client of the current scenario uses `backend`.
pub fn note_backend(backend: &str) {
    let scenario = crate::client::scenario();
    with_state(&scenario, |s| {
        if !s.backends.iter().any(|b| b == backend) {
            s.backends.push(backend.to_string());
        }
    });
}

/// Every log file (rotated incarnations included) of `name`'s log `log`.
fn incarnations(log: &Path) -> Vec<PathBuf> {
    let Some(dir) = log.parent() else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|r| {
            r.flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("mount.log"))
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

/// Verification's additions to a daemon's environment (`env`: the
/// client's own, which a scenario's `RUST_LOG` lives in).
pub fn daemon_env(env: &[(String, String)], cmd: &mut std::process::Command) {
    if !enabled() {
        return;
    }
    let own = |k: &str| {
        env.iter()
            .rev()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
    };
    let base = own("RUST_LOG")
        .filter(|v| !v.starts_with('\u{0}'))
        .or_else(|| std::env::var("RUST_LOG").ok())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "info".into());
    cmd.env("RUST_LOG", format!("{base},{LOG_DIRECTIVES}"));
    if own("CONSTELLATION_FUSE_REQUEST_STALL_S").is_none() {
        cmd.env(
            "CONSTELLATION_FUSE_REQUEST_STALL_S",
            (hang_bound().as_secs() / 2).max(1).to_string(),
        );
    }
    if own("CONSTELLATION_FUSE_STALL_BACKTRACE").is_none() {
        cmd.env("CONSTELLATION_FUSE_STALL_BACKTRACE", "1");
    }
}

// ------------------------------------------------------------------ data

/// Who wrote a file's content: a campaign payload names it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId {
    /// The path it was written at (relative to the mount).
    pub path: String,
    pub writer: String,
    pub seq: u64,
}

const MAGIC: &str = "CSTLOAD1";

/// A self-describing payload of `len` bytes (at least its header):
/// `CSTLOAD1 <writer> <seq> <len> <path>\n`, then a body derived from the
/// header (blake3 XOF), so the content proves its own integrity.
pub fn payload(id: &FileId, len: usize) -> Vec<u8> {
    let header = format!("{MAGIC} {} {} {len} {}\n", id.writer, id.seq, id.path);
    let mut out = header.clone().into_bytes();
    if out.len() < len {
        let mut body = vec![0u8; len - out.len()];
        blake3::Hasher::new()
            .update(header.as_bytes())
            .finalize_xof()
            .fill(&mut body);
        out.extend_from_slice(&body);
    }
    out
}

/// The identity a payload claims, if every byte of it is the one
/// [`payload`] makes for that identity.
pub fn check_payload(bytes: &[u8]) -> std::result::Result<FileId, String> {
    let nl = bytes
        .iter()
        .position(|b| *b == b'\n')
        .ok_or_else(|| format!("no header ({} bytes)", bytes.len()))?;
    let header = std::str::from_utf8(&bytes[..nl]).map_err(|_| "header not UTF-8".to_string())?;
    let mut f = header.splitn(5, ' ');
    let (magic, writer, seq, len, path) = (f.next(), f.next(), f.next(), f.next(), f.next());
    if magic != Some(MAGIC) {
        return Err(format!(
            "not a campaign payload: {:?}",
            truncate(header, 60)
        ));
    }
    let (Some(writer), Some(seq), Some(len), Some(path)) = (writer, seq, len, path) else {
        return Err(format!("short header {header:?}"));
    };
    let id = FileId {
        path: path.to_string(),
        writer: writer.to_string(),
        seq: seq.parse().map_err(|_| format!("bad seq in {header:?}"))?,
    };
    let len: usize = len.parse().map_err(|_| format!("bad len in {header:?}"))?;
    let want = payload(&id, len);
    if bytes.len() != want.len() {
        return Err(format!(
            "{id:?}: {} bytes, its header says {}",
            bytes.len(),
            want.len()
        ));
    }
    if let Some(at) = bytes.iter().zip(&want).position(|(a, b)| a != b) {
        return Err(format!("{id:?}: corrupt from byte {at}"));
    }
    Ok(id)
}

fn truncate(s: &str, n: usize) -> &str {
    &s[..s.len().min(n)]
}

// ---------------------------------------------------------------- ledger

#[derive(Debug, Default)]
struct LedgerInner {
    /// Path -> its acknowledged content (`None`: acknowledged unlinked).
    files: BTreeMap<String, Option<FileId>>,
    /// Content an op that failed (or was left in doubt) tried to write:
    /// it may have landed.
    attempted: HashSet<FileId>,
    /// Every acknowledged content ever.
    acked: HashSet<FileId>,
    /// Paths whose last op failed: any state of them is possible (a failed
    /// write may leave part of its content, a failed rename either name).
    doubtful: HashSet<String>,
    /// Ops answered in doubt ([`answered_in_doubt`]) that make a name:
    /// each may still land once, later, after the application's later
    /// ops on that name were acknowledged (a stranded execution replayed
    /// by rid on a node's drain: plan 30 §M9's unacknowledged ops; the
    /// rid executes once). Per path, what each leaves there: `None` an
    /// empty file (a create), `Some(id)` that content (a rename).
    late: BTreeMap<String, Vec<Option<FileId>>>,
    /// The daemons' client deadline (zero: no op is in doubt).
    client_deadline: Duration,
    ops: u64,
}

/// Whether an op that failed with `e` after `took` was answered in doubt:
/// `EIO` at the client deadline (`acquire_deadline_ms`), where the node
/// stops waiting for an outcome it does not have. The op may or may not
/// have happened, and may still happen (once). Zero `deadline`: never.
pub fn answered_in_doubt(e: &std::io::Error, took: Duration, deadline: Duration) -> bool {
    e.raw_os_error() == Some(libc::EIO) && !deadline.is_zero() && took >= deadline
}

/// The acknowledged-write ledger: a workload records each op the
/// application was told succeeded (and each that failed, which may have
/// landed all the same), and [`Ledger::check`] compares the tree with it.
/// Paths are relative to the mount; each path must have one writer at a
/// time (a workload's own directory), so the ledger's order is the
/// writer's.
#[derive(Debug, Clone, Default)]
pub struct Ledger(Arc<Mutex<LedgerInner>>);

/// What [`Ledger::check`] found.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct LedgerReport {
    pub acked_ops: u64,
    pub files: usize,
    pub intact: usize,
    /// Acknowledged content found only in a conflict copy (documented).
    pub in_conflict_copy: Vec<String>,
    /// Acknowledged content found nowhere: lost writes.
    pub lost: Vec<String>,
    /// Content that fails its own check.
    pub corrupt: Vec<String>,
    /// A path holding content nobody wrote there, or an older version of
    /// what was acknowledged.
    pub wrong: Vec<String>,
    /// Acknowledged unlinks (or renames away) whose path is back.
    pub resurrected: Vec<String>,
    /// Names back after an acknowledged unlink or rename away, holding
    /// what an op answered in doubt on that name would leave there: its
    /// late landing (allowed, counted).
    pub late_landed: Vec<String>,
    /// Files nobody acknowledged (nor tried to write) under the ledger's
    /// directories.
    pub phantoms: Vec<String>,
    /// Paths whose last op failed (any state allowed; counted).
    pub in_doubt: usize,
}

impl LedgerReport {
    /// The violations, as text (none: empty). Resurrected names are
    /// excused when `refused_replays` (a refused replay of an unlink or a
    /// rename is counted, not copied: `engine::recovery`).
    pub fn violations(&self, refused_replays: u64) -> Vec<String> {
        let mut v = Vec::new();
        let mut add = |what: &str, list: &[String]| {
            if !list.is_empty() {
                v.push(format!(
                    "{} {what}: {}",
                    list.len(),
                    list.iter().take(10).cloned().collect::<Vec<_>>().join("; ")
                ));
            }
        };
        add("lost acknowledged write(s)", &self.lost);
        add("corrupt file(s)", &self.corrupt);
        add("file(s) with the wrong content", &self.wrong);
        add("phantom file(s)", &self.phantoms);
        if refused_replays == 0 {
            add("acknowledged unlink(s) undone", &self.resurrected);
        }
        v
    }
}

impl Ledger {
    /// A ledger for which no failed op is in doubt: every op answered
    /// had its outcome.
    pub fn new() -> Ledger {
        Ledger::default()
    }

    /// A ledger of a workload on daemons with client deadline `d`: an op
    /// failing with `EIO` at `d` is in doubt ([`answered_in_doubt`]).
    pub fn with_client_deadline(d: Duration) -> Ledger {
        let l = Ledger::default();
        l.inner().client_deadline = d;
        l
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, LedgerInner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `rel` now holds `id` (acknowledged).
    pub fn acked_write(&self, rel: &str, id: FileId) {
        let mut l = self.inner();
        l.ops += 1;
        l.acked.insert(id.clone());
        l.doubtful.remove(rel);
        l.files.insert(rel.to_string(), Some(id));
    }

    /// A write of `id` at `rel` failed or is in doubt: it may be there, in
    /// part or whole, until `rel` is acknowledged again.
    pub fn failed_write(&self, rel: &str, id: FileId) {
        let mut l = self.inner();
        l.attempted.insert(id);
        l.doubtful.insert(rel.to_string());
    }

    /// Creating `rel` for `id` failed with `e` after `took`: as
    /// [`Ledger::failed_write`]; and when it was answered in doubt and
    /// `rel` was not a file the ledger knows (so the op would make the
    /// name, not truncate a file), `rel` may come back once, empty, after
    /// a later acknowledged unlink or rename of it.
    pub fn failed_create(&self, rel: &str, id: FileId, e: &std::io::Error, took: Duration) {
        let mut l = self.inner();
        let present = matches!(l.files.get(rel), Some(Some(_))) && !l.doubtful.contains(rel);
        if !present && answered_in_doubt(e, took, l.client_deadline) {
            l.late.entry(rel.to_string()).or_default().push(None);
        }
        drop(l);
        self.failed_write(rel, id);
    }

    /// A rename of `from` to `to` failed with `e` after `took`: as
    /// [`Ledger::failed_op`] on both; and when it was answered in doubt
    /// and `from` held acknowledged content, `to` may come back once with
    /// that content after a later acknowledged unlink or rename of it.
    pub fn failed_rename(&self, from: &str, to: &str, e: &std::io::Error, took: Duration) {
        let mut l = self.inner();
        let held = match l.files.get(from) {
            Some(Some(id)) if !l.doubtful.contains(from) => Some(id.clone()),
            _ => None,
        };
        if let Some(id) = held {
            if answered_in_doubt(e, took, l.client_deadline) {
                l.late.entry(to.to_string()).or_default().push(Some(id));
            }
        }
        drop(l);
        self.failed_op(&[from, to]);
    }

    /// An op on `rels` failed (a rename, an unlink): their state is
    /// unknown until acknowledged again.
    pub fn failed_op(&self, rels: &[&str]) {
        let mut l = self.inner();
        for r in rels {
            l.doubtful.insert(r.to_string());
            // What it held may now be anywhere among them.
            if let Some(Some(id)) = l.files.get(*r).cloned() {
                l.attempted.insert(id);
            }
        }
    }

    /// `from` was renamed to `to` (acknowledged): `to` holds what `from`
    /// held — in doubt if `from` was (a failed write's content may have
    /// landed, and moved with it) — and `from` is gone.
    pub fn acked_rename(&self, from: &str, to: &str) {
        let mut l = self.inner();
        l.ops += 1;
        let moved = l.files.insert(from.to_string(), None).flatten();
        let in_doubt = l.doubtful.remove(from);
        if in_doubt {
            l.doubtful.insert(to.to_string());
        } else {
            l.doubtful.remove(to);
        }
        l.files.insert(to.to_string(), moved);
    }

    /// `rel` was unlinked (acknowledged): whatever it held, in doubt or
    /// not, is gone.
    pub fn acked_unlink(&self, rel: &str) {
        let mut l = self.inner();
        l.ops += 1;
        l.doubtful.remove(rel);
        l.files.insert(rel.to_string(), None);
    }

    /// Write `id` as a payload of `len` bytes at `root/rel` (`fsync`ed),
    /// recording the outcome.
    ///
    /// The `close(2)` is explicit and its result counts: it is where a
    /// write under a lapsed lock grant is fenced (`EIO`). A failed
    /// write or `fsync` leaves the path in doubt (any content allowed);
    /// an `fsync` that succeeded acknowledged the content, so a close
    /// failing after it is an error to the application (the `Err`, for
    /// the workload's [`ErrnoLog`]), not a ledger finding. The error
    /// names the step: `create`, `write`, `fsync` or `close`.
    pub fn write(
        &self,
        root: &Path,
        rel: &str,
        id: FileId,
        len: usize,
    ) -> Result<(), (&'static str, std::io::Error)> {
        use std::io::Write;
        use std::os::fd::IntoRawFd;
        let t = Instant::now();
        let mut f = match std::fs::File::create(root.join(rel)) {
            Ok(f) => f,
            Err(e) => {
                self.failed_create(rel, id, &e, t.elapsed());
                return Err(("create", e));
            }
        };
        let synced = f
            .write_all(&payload(&id, len))
            .map_err(|e| ("write", e))
            .and_then(|()| f.sync_all().map_err(|e| ("fsync", e)));
        let fd = f.into_raw_fd();
        // SAFETY: `fd` is ours (just released from the `File`), closed once.
        let closed = if unsafe { libc::close(fd) } == 0 {
            Ok(())
        } else {
            Err(("close", std::io::Error::last_os_error()))
        };
        match synced {
            Ok(()) => {
                self.acked_write(rel, id);
                closed
            }
            Err(e) => {
                self.failed_write(rel, id);
                Err(e)
            }
        }
    }

    /// The paths the ledger knows, and their acknowledged content.
    pub fn expected(&self) -> BTreeMap<String, Option<FileId>> {
        self.inner().files.clone()
    }

    /// Compare the tree under `root` (a mount) with the ledger.
    pub fn check(&self, root: &Path) -> LedgerReport {
        let l = self.inner();
        let mut rep = LedgerReport {
            acked_ops: l.ops,
            files: l.files.len(),
            ..Default::default()
        };
        let copies = conflict_copies(root, l.files.keys().chain(&l.doubtful).map(String::as_str));
        let in_copy = |id: &FileId| copies.iter().any(|(_, c)| c == id);
        // A path only ever written in doubt is in doubt too, and its
        // directory is searched for phantoms like any other.
        let mut dirs: HashSet<PathBuf> = l
            .doubtful
            .iter()
            .filter_map(|rel| Path::new(rel).parent().map(Path::to_path_buf))
            .collect();
        rep.in_doubt = l
            .doubtful
            .iter()
            .filter(|r| !l.files.contains_key(*r))
            .count();
        for (rel, want) in &l.files {
            if let Some(parent) = Path::new(rel).parent() {
                dirs.insert(parent.to_path_buf());
            }
            if l.doubtful.contains(rel) {
                rep.in_doubt += 1;
                continue;
            }
            let got = std::fs::read(root.join(rel));
            match (want, got) {
                (Some(id), Ok(bytes)) => match check_payload(&bytes) {
                    Ok(have) if &have == id => rep.intact += 1,
                    // Corrupt bytes at a path are corruption, whatever
                    // copy exists elsewhere.
                    Err(e) => rep.corrupt.push(format!("{rel}: {e}")),
                    // Another version someone wrote, the acknowledged one
                    // in a conflict copy: the documented refusal.
                    Ok(have)
                        if (l.acked.contains(&have) || l.attempted.contains(&have))
                            && in_copy(id) =>
                    {
                        rep.in_conflict_copy
                            .push(format!("{rel}: {id:?} (the path holds {have:?})"))
                    }
                    Ok(have) => rep
                        .wrong
                        .push(format!("{rel}: holds {have:?}, acknowledged {id:?}")),
                },
                (Some(id), Err(_)) if in_copy(id) => {
                    rep.in_conflict_copy.push(format!("{rel}: {id:?}"))
                }
                (Some(id), Err(e)) => rep.lost.push(format!("{rel}: {id:?} ({e})")),
                (None, Ok(bytes)) => match l.late.get(rel).and_then(|late| {
                    late.iter().find(|left| match left {
                        None => bytes.is_empty(),
                        Some(id) => check_payload(&bytes).as_ref() == Ok(id),
                    })
                }) {
                    Some(None) => rep
                        .late_landed
                        .push(format!("{rel}: empty (a create in doubt)")),
                    Some(Some(id)) => rep
                        .late_landed
                        .push(format!("{rel}: {id:?} (a rename in doubt)")),
                    None => rep.resurrected.push(rel.clone()),
                },
                (None, Err(_)) => {}
            }
        }
        // Files under the ledger's directories it never heard of.
        for dir in dirs {
            let Ok(entries) = std::fs::read_dir(root.join(&dir)) else {
                continue;
            };
            for e in entries.flatten() {
                let rel = dir.join(e.file_name()).to_string_lossy().into_owned();
                if l.files.contains_key(&rel) || l.doubtful.contains(&rel) || !e.path().is_file() {
                    continue;
                }
                match std::fs::read(e.path())
                    .map_err(|e| e.to_string())
                    .and_then(|b| check_payload(&b))
                {
                    Ok(id) if l.attempted.contains(&id) || l.acked.contains(&id) => {}
                    Ok(id) => rep.phantoms.push(format!("{rel}: {id:?}")),
                    Err(err) => rep.phantoms.push(format!("{rel}: {err}")),
                }
            }
        }
        rep
    }
}

/// Every conflict copy that is a campaign payload, wherever the engine
/// may have put one for a file at one of `rels` (relative to the mount
/// `root`): (its copy's path, whose content). A copy lands in a
/// `.constellation-conflict` directory either beside its file (a refused
/// replay of an op without a lock tag, `repair drop-held`, an epoch's
/// reintegration) or at the root of a mounted view above the file, named
/// after the file's path below it (a refused commit or replay of an op
/// under a lock: the writing mount's root, or the deepest mounted view
/// above the file; the filesystem root when none is narrower). Seen from
/// `root`, that is the conflict directory of `root` itself or of any
/// directory between it and the file: all of them are searched
/// (cluster-locks.md, forwarded-mutations.md, write-path-hygiene.md).
pub fn conflict_copies<'a>(
    root: &Path,
    rels: impl IntoIterator<Item = &'a str>,
) -> Vec<(PathBuf, FileId)> {
    use constellation_meta::reintegrate::CONFLICT_DIR;
    let mut dirs = std::collections::BTreeSet::from([root.join(CONFLICT_DIR)]);
    for rel in rels {
        for above in Path::new(rel).ancestors().skip(1) {
            dirs.insert(root.join(above).join(CONFLICT_DIR));
        }
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut stack: Vec<PathBuf> = dirs.into_iter().collect();
    while let Some(d) = stack.pop() {
        if !seen.insert(d.clone()) {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(id) = std::fs::read(&p)
                .map_err(|e| e.to_string())
                .and_then(|b| check_payload(&b))
            {
                out.push((p, id));
            }
        }
    }
    out
}

/// Wait up to `within` for quiescence on every client's daemon: no
/// speculation outstanding and no replay pending, so no op answered in
/// doubt can still land and [`Ledger::check`]'s verdict is final.
pub fn wait_quiescent(clients: &[&crate::client::Client], within: Duration) -> Result<()> {
    let until = Instant::now() + within;
    for c in clients {
        loop {
            let spec = c.control_status()?["speculation"].clone();
            if spec["outstanding"].as_u64() == Some(0) && spec["pending_replay"].as_u64() == Some(0)
            {
                break;
            }
            anyhow::ensure!(
                Instant::now() < until,
                "{} never quiesced within {within:?}: speculation {spec}",
                c.name
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }
    Ok(())
}

/// What the protocol counted on `c` that the ledger's outcome depends on
/// (F9: stranded ops, replays, refused replays and conflict copies,
/// depositions, outwaited grants).
pub fn counters(c: &crate::client::Client) -> serde_json::Value {
    let Ok(s) = c.control_status() else {
        return serde_json::Value::Null;
    };
    let n = |v: &serde_json::Value| v.as_u64().unwrap_or(0);
    serde_json::json!({
        "node": c.name,
        "rolled_back": n(&s["speculation"]["rolled_back"]),
        "local_rolled_back": n(&s["speculation"]["local_rolled_back"]),
        "stranded_replayed": n(&s["speculation"]["stranded_replayed"]),
        "replay_conflicts": n(&s["speculation"]["replay_conflicts"]),
        "depositions": n(&s["speculation"]["depositions"]),
        "stranded_records": n(&s["reintegration"]["stranded_records"]),
        "conflicts_materialized": n(&s["reintegration"]["conflicts_materialized"]),
        "lock_recalls_expired": n(&s["locks"]["recalls_expired"]),
        "lock_fenced_io": n(&s["locks"]["fenced_io"]),
        "seals": n(&s["ack"]["seals"]),
        "backup_takeovers": n(&s["ack"]["backup_takeovers"]),
        "cto_degraded": n(&s["cto"]["degraded"]),
    })
}

/// The lock-exclusion check of a counter incremented under a lock.
pub fn counter_exact(what: &str, expected: u64, got: u64) -> Result<()> {
    anyhow::ensure!(
        got == expected,
        "lock exclusion: {what} is {got}, expected {expected} ({} increments {})",
        got.abs_diff(expected),
        if got < expected { "lost" } else { "doubled" }
    );
    Ok(())
}

// ----------------------------------------------------------------- errno

/// What an op was doing, for [`ErrnoLog`]'s classification.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpCtx {
    /// Under a lock the caller holds (an `EIO` is a fence, if the node
    /// fenced that many ops).
    pub under_lock: bool,
    /// A non-blocking lock request (`EAGAIN` is an answer).
    pub nonblocking_lock: bool,
    /// A new lock request (`ENOLCK` during an S3 outage is an answer).
    pub lock_request: bool,
    /// An `fsync` (`EINTR` when its process was killed; `EIO` under a
    /// soft `--fsync-timeout`).
    pub fsync: bool,
    pub fsync_soft_timeout: bool,
    pub killed: bool,
    /// The file may have been removed meanwhile (`ESTALE`).
    pub removable: bool,
}

#[derive(Debug, Clone)]
pub struct ErrnoRecord {
    pub node: String,
    pub op: String,
    pub errno: i32,
    pub started: Instant,
    pub took: Duration,
    pub ctx: OpCtx,
}

/// The errors a workload's ops saw.
#[derive(Debug, Clone, Default)]
pub struct ErrnoLog(Arc<Mutex<Vec<ErrnoRecord>>>);

/// What the classification needs from the daemons: per node, the ops
/// fenced with `EIO` ([`fenced_ops`]), and the in-doubt deadline.
#[derive(Debug, Clone, Default)]
pub struct Evidence {
    pub fenced_io: HashMap<String, u64>,
    pub acquire_deadline: Duration,
    /// Allow an `EIO` in doubt past the deadline
    /// (`CONSTELLATION_HARNESS_ALLOW_IN_DOUBT=1`, triage runs only).
    pub allow_in_doubt: bool,
}

/// Ops a node refused with `EIO` for a lapsed lock grant: I/O on a
/// fenced file (`fenced_io`) and ops of a fenced lock owner
/// (`owner_fenced_ops`).
pub fn fenced_ops(locks: &serde_json::Value) -> u64 {
    locks["fenced_io"].as_u64().unwrap_or(0) + locks["owner_fenced_ops"].as_u64().unwrap_or(0)
}

impl Evidence {
    pub fn from_clients(
        clients: &[&crate::client::Client],
        acquire_deadline: Duration,
    ) -> Evidence {
        let mut fenced_io = HashMap::new();
        for c in clients {
            let n = c
                .control_status()
                .ok()
                .map(|s| fenced_ops(&s["locks"]))
                .unwrap_or(0);
            *fenced_io.entry(c.name.clone()).or_insert(0) += n;
        }
        Evidence {
            fenced_io,
            acquire_deadline,
            allow_in_doubt: std::env::var("CONSTELLATION_HARNESS_ALLOW_IN_DOUBT")
                .is_ok_and(|v| v.trim() == "1"),
        }
    }
}

impl ErrnoLog {
    pub fn new() -> ErrnoLog {
        ErrnoLog::default()
    }

    /// Record `e` from `op` on `node` (no OS error: recorded as `EIO`'s
    /// absence, errno 0, which is always a bug).
    pub fn record(&self, node: &str, op: &str, e: &std::io::Error, started: Instant, ctx: OpCtx) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(ErrnoRecord {
                node: node.to_string(),
                op: op.to_string(),
                errno: e.raw_os_error().unwrap_or(0),
                started,
                took: started.elapsed(),
                ctx,
            });
    }

    pub fn records(&self) -> Vec<ErrnoRecord> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Classify every recorded error.
    pub fn judge(&self, ev: &Evidence) -> ErrnoReport {
        let recs = self.records();
        let mut rep = ErrnoReport::default();
        let mut eio_under_lock: HashMap<&str, u64> = HashMap::new();
        for r in &recs {
            let what = format!(
                "{} {} errno {} ({}) after {:?}",
                r.node,
                r.op,
                r.errno,
                std::io::Error::from_raw_os_error(r.errno),
                r.took
            );
            let (class, verdict) = classify(r, ev, &mut eio_under_lock);
            *rep.counts.entry(class).or_insert(0) += 1;
            match verdict {
                Ok(why) => rep.allowed.push(format!("{what}: [{class}] {why}")),
                Err(why) => rep.bugs.push(format!("{what}: [{class}] {why}")),
            }
        }
        rep
    }
}

/// [`ErrnoLog::judge`]'s verdict.
#[derive(Debug, Default, Clone)]
pub struct ErrnoReport {
    pub allowed: Vec<String>,
    pub bugs: Vec<String>,
    /// Per class (`fenced`, `eagain-nonblocking`, `in-doubt`, ...): how
    /// many — F6's spurious `EAGAIN` rate, for one, is read off these.
    pub counts: BTreeMap<&'static str, usize>,
}

/// Classify one error: its class, and whether the documented semantics
/// allow it there (the load campaign's §5.1 set). An `EIO` in doubt past
/// the client deadline is documented, but it is what load must never
/// reach: a bug unless [`Evidence::allow_in_doubt`] (triage runs).
/// `EAGAIN` on a non-blocking lock is allowed without proof that another
/// holder existed: counted apart, so a rate of spurious ones shows.
fn classify<'a>(
    r: &'a ErrnoRecord,
    ev: &Evidence,
    eio_under_lock: &mut HashMap<&'a str, u64>,
) -> (
    &'static str,
    std::result::Result<&'static str, &'static str>,
) {
    let e = r.errno;
    let c = r.ctx;
    if e == libc::EIO {
        if c.under_lock {
            let n = eio_under_lock.entry(&r.node).or_insert(0);
            *n += 1;
            return if *n <= ev.fenced_io.get(&r.node).copied().unwrap_or(0) {
                ("fenced", Ok("a fenced op under a lapsed lock grant"))
            } else {
                ("bug", Err("EIO under a lock beyond the node's fenced ops"))
            };
        }
        if c.fsync && c.fsync_soft_timeout {
            return ("fsync-soft-timeout", Ok("the soft fsync timeout"));
        }
        if !ev.acquire_deadline.is_zero() && r.took >= ev.acquire_deadline {
            return (
                "in-doubt",
                if ev.allow_in_doubt {
                    Ok("in doubt past the acquire deadline (allowed for triage)")
                } else {
                    Err("in doubt past the acquire deadline: load reached the client deadline")
                },
            );
        }
        return ("bug", Err("EIO the documented semantics do not allow here"));
    }
    if (e == libc::EAGAIN || e == libc::EWOULDBLOCK) && c.nonblocking_lock {
        return (
            "eagain-nonblocking",
            Ok("a non-blocking lock refused (another holder, or a just-closed one: F6)"),
        );
    }
    if e == libc::ENOLCK && c.lock_request {
        return if crate::toxiproxy::outage_at(r.started, r.started + r.took) {
            ("enolck-s3-outage", Ok("a new lock during an S3 outage"))
        } else {
            ("bug", Err("ENOLCK without an S3 outage"))
        };
    }
    if e == libc::ESTALE && c.removable {
        return ("estale", Ok("a removed file"));
    }
    if e == libc::EINTR && c.fsync && c.killed {
        return ("eintr-killed-fsync", Ok("a killed fsync"));
    }
    ("bug", Err("not in the documented set"))
}

// ------------------------------------------------------------------ logs

/// A daemon log line's timestamp (`2026-10-15T19:27:49.123456Z`, the
/// tracing default) as unix ms.
pub fn line_unix_ms(line: &str) -> Option<i64> {
    line_unix_us(line).map(|us| us.div_euclid(1_000))
}

/// The same, to the microsecond (the tracing default's six digits).
pub fn line_unix_us(line: &str) -> Option<i64> {
    let t = line.trim_start();
    let b = t.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| t.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (h, mi, s) = (n(11..13)?, n(14..16)?, n(17..19)?);
    let mut us = 0;
    if b.get(19) == Some(&b'.') {
        let frac: String = t[20..].chars().take_while(|c| c.is_ascii_digit()).collect();
        let f6: String = frac.chars().chain("000000".chars()).take(6).collect();
        us = f6.parse().ok()?;
    }
    // Days from civil (Howard Hinnant's algorithm).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some((((days * 24 + h) * 60 + mi) * 60 + s) * 1_000_000 + us)
}

/// `line` without ANSI escape sequences (the daemons' log styles field
/// names).
pub fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// The value of `key=` in a (de-styled) line: up to the next space.
pub fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("{key}=");
    let mut from = 0;
    while let Some(i) = line[from..].find(&pat) {
        let at = from + i;
        // A whole field name, not a suffix of another.
        let ok = at == 0
            || !line.as_bytes()[at - 1].is_ascii_alphanumeric() && line.as_bytes()[at - 1] != b'_';
        let rest = &line[at + pat.len()..];
        if ok {
            return Some(rest.split([' ', ',', '}']).next().unwrap_or(rest));
        }
        from = at + pat.len();
    }
    None
}

/// One `granted a lock` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRec {
    pub minter: u64,
    pub seq: u64,
    pub ino: u64,
    pub to: u64,
    pub exclusive: bool,
    /// The line's time (host clock, unix ms).
    /// When the owner logged it (host clock, unix µs).
    pub at_us: i64,
}

/// What the logs say about lock grants and tagged ops: the grants, and
/// per grant the first and last time an op tagged with it executed (on
/// the host's clock: the executor's protocol-clock offset removed).
#[derive(Debug, Clone, Default)]
pub struct LockEvidence {
    pub grants: Vec<GrantRec>,
    /// Per grant: the first and last time an op tagged with it passed the
    /// check or landed (whichever is later for the last).
    pub execs: HashMap<(u64, u64), (i64, i64)>,
    /// Grants with a `tagged op landed` line (the write-time evidence).
    pub landed: HashSet<(u64, u64)>,
    /// Lines that carried each marker text, and of them how many parsed:
    /// fewer parsed than carried is a checker gone blind (a format drift).
    pub raw: LineCounts,
    pub parsed: LineCounts,
}

/// Lines per marker text (see [`LockEvidence::raw`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LineCounts {
    pub granted: usize,
    pub executed: usize,
    pub landed: usize,
    pub acquired: usize,
}

/// One acquisition: who, which epoch, from which log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acquired {
    pub node: u64,
    pub epoch: u64,
    pub log: String,
}

/// Parse logs: `(log label, text, the protocol-clock offset its daemon
/// ran with)`.
pub fn parse_logs(logs: &[(String, String, i64)]) -> (LockEvidence, Vec<Acquired>) {
    let mut ev = LockEvidence::default();
    let mut acquired = Vec::new();
    for (label, text, offset) in logs {
        for raw in text.lines() {
            let kind = if raw.contains("tagged op executed") {
                1
            } else if raw.contains("tagged op landed") {
                2
            } else if raw.contains("granted a lock") {
                3
            } else if raw.contains("acquired the lease") {
                4
            } else {
                continue;
            };
            let line = strip_ansi(raw);
            let num = |k: &str| field(&line, k).and_then(|v| v.parse::<u64>().ok());
            let int = |k: &str| field(&line, k).and_then(|v| v.parse::<i64>().ok());
            match kind {
                1 | 2 => {
                    // In µs. `executed`: the protocol clock at the check, to
                    // the ms (the daemon's offset removed); `landed`: the
                    // host clock after the write committed, to the µs.
                    let at = if kind == 1 {
                        ev.raw.executed += 1;
                        int("at_ms").map(|a| (a - offset) * 1_000)
                    } else {
                        ev.raw.landed += 1;
                        int("host_us")
                    };
                    let (Some(minter), Some(seq), Some(at)) = (num("minter"), num("seq"), at)
                    else {
                        continue;
                    };
                    if kind == 1 {
                        ev.parsed.executed += 1;
                    } else {
                        ev.parsed.landed += 1;
                        ev.landed.insert((minter, seq));
                    }
                    let e = ev.execs.entry((minter, seq)).or_insert((at, at));
                    e.0 = e.0.min(at);
                    e.1 = e.1.max(at);
                }
                3 => {
                    ev.raw.granted += 1;
                    let (Some(minter), Some(seq), Some(ino), Some(to), Some(at_us), Some(mode)) = (
                        num("minter"),
                        num("seq"),
                        num("ino"),
                        num("to"),
                        line_unix_us(&line),
                        field(&line, "mode"),
                    ) else {
                        continue;
                    };
                    // A mode neither is unparsed (a renamed variant would
                    // otherwise read as shared and hide conflicts).
                    let exclusive = if mode.starts_with("Exclusive") {
                        true
                    } else if mode.starts_with("Shared") {
                        false
                    } else {
                        continue;
                    };
                    ev.parsed.granted += 1;
                    ev.grants.push(GrantRec {
                        minter,
                        seq,
                        ino,
                        to,
                        exclusive,
                        at_us,
                    });
                }
                _ => {
                    ev.raw.acquired += 1;
                    if let (Some(node), Some(epoch)) = (num("node"), num("epoch")) {
                        ev.parsed.acquired += 1;
                        acquired.push(Acquired {
                            node,
                            epoch,
                            log: label.clone(),
                        });
                    }
                }
            }
        }
    }
    ev.grants.sort_by_key(|g| g.at_us);
    (ev, acquired)
}

/// A checker that cannot read what the daemons wrote: lines that carry a
/// marker text but do not parse (a changed field name, a timestamp
/// format, JSON logs), or tagged ops with no write-time evidence at all
/// (a daemon without the `tagged op landed` line).
pub fn check_blind(ev: &LockEvidence) -> Vec<String> {
    let mut v = Vec::new();
    let (r, p) = (ev.raw, ev.parsed);
    for (what, raw, parsed) in [
        ("granted a lock", r.granted, p.granted),
        ("tagged op executed", r.executed, p.executed),
        ("tagged op landed", r.landed, p.landed),
        ("acquired the lease", r.acquired, p.acquired),
    ] {
        if parsed < raw {
            v.push(format!(
                "checker blind: {raw} `{what}` line(s) in the logs, {parsed} parsed (log format drift?)"
            ));
        }
    }
    if p.executed > 0 && p.landed == 0 {
        v.push(format!(
            "checker blind: {} `tagged op executed` line(s) but no `tagged op landed` one: \
             the token check cannot see a write delayed past its grant",
            p.executed
        ));
    }
    v
}

/// Fencing-token order: for every pair of conflicting grants of one inode
/// to two nodes, no op tagged with the earlier one was let through
/// (`tagged op executed`, the check) or landed (`tagged op landed`, its
/// write committed: the later of the two counts) at or after the later
/// one was granted. The landed line is what makes a write admitted before
/// its grant's end and committed after a successor's grant visible (F1:
/// the check runs before the op waits for the store's writer lock).
pub fn check_token_order(ev: &LockEvidence) -> Vec<String> {
    let mut out = Vec::new();
    let mut by_ino: BTreeMap<u64, Vec<&GrantRec>> = BTreeMap::new();
    for g in &ev.grants {
        by_ino.entry(g.ino).or_default().push(g);
    }
    for (ino, gs) in by_ino {
        for (i, a) in gs.iter().enumerate() {
            let Some(&(_, last)) = ev.execs.get(&(a.minter, a.seq)) else {
                continue;
            };
            for b in &gs[i + 1..] {
                if b.to == a.to || !(a.exclusive || b.exclusive) || b.at_us < a.at_us {
                    continue;
                }
                if (a.minter, a.seq) == (b.minter, b.seq) {
                    continue;
                }
                // Strictly later, in µs (a landed line and the grant line
                // are both stamped to the µs; a check line to the ms, taken
                // as its millisecond's start, so it never reads late).
                if last > b.at_us {
                    out.push(format!(
                        "fencing token order: ino {ino}: grant {}/{} to node {} let an op through at {} \
                         µs, {:.3} ms after grant {}/{} of it went to node {} (at {} µs)",
                        a.minter,
                        a.seq,
                        a.to,
                        last,
                        (last - b.at_us) as f64 / 1_000.0,
                        b.minter,
                        b.seq,
                        b.to,
                        b.at_us
                    ));
                    break;
                }
            }
        }
    }
    out
}

/// At most one node acquires the lease per epoch.
pub fn check_lease_epochs(acquired: &[Acquired]) -> Vec<String> {
    let mut by_epoch: BTreeMap<u64, Vec<&Acquired>> = BTreeMap::new();
    for a in acquired {
        by_epoch.entry(a.epoch).or_default().push(a);
    }
    by_epoch
        .into_iter()
        .filter_map(|(epoch, list)| {
            let nodes: HashSet<u64> = list.iter().map(|a| a.node).collect();
            (nodes.len() > 1).then(|| {
                format!(
                    "split brain: epoch {epoch} acquired by {} nodes: {}",
                    nodes.len(),
                    list.iter()
                        .map(|a| format!("node {} ({})", a.node, a.log))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
        })
        .collect()
}

/// The log checks over `logs` (name, path) of a scenario's clients, with
/// each daemon's clock offset.
fn log_violations(scenario: &str, logs: &[(String, PathBuf)]) -> Vec<String> {
    let offsets: HashMap<String, i64> = crate::load::daemons_of(scenario)
        .into_iter()
        .map(|d| (d.node, d.clock_offset))
        .collect();
    let mut texts = Vec::new();
    let mut v = Vec::new();
    for (name, log) in logs {
        if incarnations(log).is_empty() {
            v.push(format!(
                "log missing: {}'s kept log {}",
                name,
                log.display()
            ));
        }
        for f in incarnations(log) {
            if let Ok(t) = std::fs::read_to_string(&f) {
                texts.push((
                    format!(
                        "{name}:{}",
                        f.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    t,
                    offsets.get(name).copied().unwrap_or(0),
                ));
            }
        }
    }
    let (ev, acquired) = parse_logs(&texts);
    v.extend(check_token_order(&ev));
    v.extend(check_lease_epochs(&acquired));
    v.extend(check_blind(&ev));
    v
}

/// What [`check_logs`] read.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogCounts {
    pub grants: usize,
    pub tagged: usize,
    pub landed: usize,
    pub acquired: usize,
    pub logs: usize,
}

/// Run the log checks over `clients`' logs now (a workload's own call;
/// the automatic post-scenario pass then skips them).
pub fn check_logs(clients: &[&crate::client::Client]) -> Result<LogCounts> {
    let scenario = crate::client::scenario();
    let logs: Vec<(String, PathBuf)> = clients
        .iter()
        .flat_map(|c| {
            c.log_files()
                .into_iter()
                .map(|p| (c.name.clone(), p))
                .collect::<Vec<_>>()
        })
        .collect();
    // Every client's log must be there to be read: a missing one is a
    // check that did not run, not a clean one.
    let missing: Vec<String> = clients
        .iter()
        .filter(|c| c.log_files().is_empty())
        .map(|c| c.name.clone())
        .collect();
    anyhow::ensure!(
        missing.is_empty(),
        "log missing: no log of {missing:?} to check"
    );
    // `log_files` already lists every incarnation.
    let offsets: HashMap<String, i64> = crate::load::daemons_of(&scenario)
        .into_iter()
        .map(|d| (d.node, d.clock_offset))
        .collect();
    let texts: Vec<(String, String, i64)> = logs
        .iter()
        .filter_map(|(n, p)| {
            std::fs::read_to_string(p).ok().map(|t| {
                (
                    format!("{n}:{}", p.display()),
                    t,
                    offsets.get(n).copied().unwrap_or(0),
                )
            })
        })
        .collect();
    let (ev, acquired) = parse_logs(&texts);
    let mut v = check_token_order(&ev);
    v.extend(check_lease_epochs(&acquired));
    v.extend(check_blind(&ev));
    with_state(&scenario, |s| s.logs_checked = true);
    let counts = LogCounts {
        grants: ev.grants.len(),
        tagged: ev.execs.len(),
        landed: ev.landed.len(),
        acquired: acquired.len(),
        logs: texts.len(),
    };
    eprintln!(
        "    verify: {} grant(s), {} tagged grant(s) ({} with a landed write), {} lease \
         acquisition(s) in {} log(s)",
        counts.grants, counts.tagged, counts.landed, counts.acquired, counts.logs
    );
    anyhow::ensure!(v.is_empty(), "{}", v.join("; "));
    Ok(counts)
}

// -------------------------------------------------------------------- S3

/// The bucket log of the filesystem at `prefix`: epochs monotone in
/// segment order, one node per epoch, every rid completed once. `Ok`
/// with the segments listed and decoded.
pub fn s3_log_check(endpoint: &str, prefix: &str) -> Result<SegmentCounts> {
    let (listed, tenures) = crate::scenarios::segment_tenures(endpoint, prefix)?;
    let mut v = Vec::new();
    let mut last: Option<(u64, u64, u64)> = None;
    let mut holder: HashMap<u64, u64> = HashMap::new();
    for &(seq, node, epoch) in &tenures {
        if let Some((pseq, _, pepoch)) = last {
            if epoch < pepoch {
                v.push(format!(
                    "segment {seq:x} has epoch {epoch} after segment {pseq:x}'s {pepoch}"
                ));
            }
        }
        if let Some(n) = holder.insert(epoch, node) {
            if n != node && epoch != 0 {
                v.push(format!(
                    "epoch {epoch} has segments from node {n} and node {node} (segment {seq:x})"
                ));
            }
        }
        last = Some((seq, node, epoch));
    }
    let completions = crate::scenarios::logged_completions(endpoint, prefix)?;
    if let Err(e) = constellation_chaos::check_log_completions(&completions) {
        v.push(format!("exactly-once in the log: {e}"));
    }
    anyhow::ensure!(v.is_empty(), "bucket log of {prefix}: {}", v.join("; "));
    Ok(SegmentCounts {
        listed,
        decoded: tenures.len(),
    })
}

/// What [`s3_log_check`] read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentCounts {
    pub listed: usize,
    pub decoded: usize,
}

/// An S3 environment of the current scenario goes away: check the bucket
/// log of every backend the scenario's clients used that it holds.
pub fn on_s3_env_drop(direct_endpoint: &str) {
    if !enabled() {
        return;
    }
    let scenario = crate::client::scenario();
    let (backends, e2e) = with_state(&scenario, |s| {
        (
            s.backends
                .iter()
                .filter(|b| !s.checked_backends.contains(*b))
                .cloned()
                .collect::<Vec<_>>(),
            s.e2e.clone(),
        )
    });
    for b in backends {
        let Some(prefix) = b.strip_prefix(&format!("s3://{}/", crate::s3env::BUCKET)) else {
            continue;
        };
        match s3_log_check(direct_endpoint, prefix) {
            // Not this environment's bucket (or nothing shipped yet):
            // another environment of the scenario may hold it.
            Ok(c) if c.listed == 0 => {
                eprintln!("    verify: bucket log of {prefix}: no segments in this S3 environment")
            }
            Ok(c) if c.decoded == 0 && e2e.contains(&b) => {
                eprintln!(
                    "    verify: bucket log of {prefix}: {} E2E segment(s), not decodable here",
                    c.listed
                );
                with_state(&scenario, |s| s.checked_backends.insert(b.clone()));
            }
            Ok(c) if c.decoded == 0 => with_state(&scenario, |s| {
                s.checked_backends.insert(b.clone());
                s.violations.push(format!(
                    "checker blind: bucket log of {prefix}: {} segment(s) listed, none decoded \
                     (envelope drift?)",
                    c.listed
                ));
            }),
            Ok(c) => {
                eprintln!(
                    "    verify: bucket log of {prefix}: {} of {} segment(s) decoded, clean",
                    c.decoded, c.listed
                );
                with_state(&scenario, |s| s.checked_backends.insert(b.clone()));
            }
            Err(e) => with_state(&scenario, |s| {
                s.checked_backends.insert(b.clone());
                s.violations.push(format!("{e:#}"));
            }),
        }
    }
}

// ------------------------------------------------------------------ hang

/// Start the hang watch (once; verification only).
pub fn start_watch() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if !enabled() || STARTED.set(()).is_err() {
        return;
    }
    let _ = std::thread::Builder::new()
        .name("hang-watch".into())
        .spawn(watch_loop);
}

fn watch_loop() {
    let bound = hang_bound();
    // pid -> since when its control socket has not answered.
    let mut silent: HashMap<u32, Instant> = HashMap::new();
    // Reported (and its connection aborted): done with it.
    let mut reported: HashSet<u32> = HashSet::new();
    // Its control silence reported (not aborted: FUSE may be moving).
    let mut silence_reported: HashSet<u32> = HashSet::new();
    // Its status schema drift reported.
    let mut blind_reported: HashSet<u32> = HashSet::new();
    // Often enough to catch a hang within a third of the bound, rarely
    // enough not to load the daemons it watches.
    let every = (bound / 3).clamp(Duration::from_secs(1), Duration::from_secs(5));
    loop {
        std::thread::sleep(every);
        for d in crate::load::daemons() {
            if reported.contains(&d.pid)
                || crate::load::scenario_paused(d.pid)
                || crate::load::proc_state(d.pid) == Some('T')
            {
                silent.remove(&d.pid);
                continue;
            }
            let status = crate::client::control_call_at(
                &d.state,
                "node.status",
                serde_json::json!({}),
                Duration::from_secs(5),
            );
            match status {
                Ok(s) => {
                    silent.remove(&d.pid);
                    if let Some(blind) = watch_blind(&s) {
                        if blind_reported.insert(d.pid) {
                            let msg = format!("{} (pid {}): {blind}", d.node, d.pid);
                            eprintln!("!!! {msg}");
                            with_state(&d.scenario, |st| st.violations.push(msg));
                        }
                    }
                    if let Some(why) = stalled_request(&s, bound) {
                        reported.insert(d.pid);
                        report_hang(&d, &why, bound, true);
                    }
                }
                Err(e) => {
                    // A starved control service (tokio) with FUSE (OS
                    // threads) still moving is no hang of an application:
                    // a finding (it fails the scenario, with backtraces),
                    // but the connection is aborted only at twice the
                    // bound, when nothing at all is left to wait for.
                    let since = *silent.entry(d.pid).or_insert_with(Instant::now);
                    let quiet = since.elapsed();
                    if quiet >= 2 * bound {
                        reported.insert(d.pid);
                        let why =
                            format!("its control socket has not answered for {quiet:?} ({e:#})");
                        report_hang(&d, &why, bound, true);
                    } else if quiet >= bound && silence_reported.insert(d.pid) {
                        let why = format!(
                            "control unresponsive: no answer for {quiet:?} ({e:#}); its FUSE \
                             connection is left alone until {:?}",
                            2 * bound
                        );
                        report_hang(&d, &why, bound, false);
                    }
                }
            }
        }
    }
}

/// A daemon whose status has `fuse_requests` but not its
/// `stalled_requests` list: the schema changed under the watch, which can
/// then see no hang — a checker gone blind, so a violation.
fn watch_blind(status: &serde_json::Value) -> Option<String> {
    (status.get("fuse_requests").is_some()
        && !status["fuse_requests"]["stalled_requests"].is_array())
    .then(|| {
        "checker blind: status.fuse_requests has no stalled_requests list (schema drift): the \
         hang watch cannot see stalled requests"
            .to_string()
    })
}

/// The first request in `status` stalled past `bound` that is a hang (not
/// a blocking lock wait, nor an `fsync` across an injected S3 outage).
fn stalled_request(status: &serde_json::Value, bound: Duration) -> Option<String> {
    // A status without the list ([`watch_blind`] reports that).
    let reqs = status["fuse_requests"]["stalled_requests"].as_array()?;
    for r in reqs {
        let age = Duration::from_secs(r["age_s"].as_u64().unwrap_or(0));
        if age < bound || r["blocking"] == true {
            continue;
        }
        let op = r["op"].as_str().unwrap_or("?");
        if op.contains("fsync") && crate::toxiproxy::outage_at(Instant::now() - age, Instant::now())
        {
            continue;
        }
        return Some(format!(
            "FUSE {op} on ino {} in flight for {age:?} (stage {:?}, thread {})",
            r["ino"],
            r["stage"].as_str().unwrap_or("?"),
            r["tid"]
        ));
    }
    None
}

fn report_hang(d: &crate::load::Daemon, why: &str, bound: Duration, abort: bool) {
    let dir = log_dir()
        .map(|l| l.join(&d.scenario))
        .unwrap_or_else(std::env::temp_dir);
    let _ = std::fs::create_dir_all(&dir);
    let bt = dir.join(format!(
        "{}-{}-{}.txt",
        d.node,
        d.pid,
        if abort { "hang" } else { "silent" }
    ));
    let trace = backtraces(d.pid);
    let _ = std::fs::write(&bt, format!("{why}\n\n{trace}"));
    let aborted = if abort {
        abort_connection(&d.mnt)
    } else {
        "not aborted".to_string()
    };
    let msg = format!(
        "hang: {} (pid {}): {why} (bound {bound:?}); backtraces in {}; FUSE connection {}",
        d.node,
        d.pid,
        bt.display(),
        aborted
    );
    eprintln!("!!! {msg}");
    with_state(&d.scenario, |s| s.hangs.push(msg));
}

/// Every thread's backtrace (gdb, else eu-stack; directly, then through
/// `sudo -n` when ptrace is refused), and each thread's state and wchan.
pub fn backtraces(pid: u32) -> String {
    let p = pid.to_string();
    let gdb: Vec<&str> = vec![
        "gdb",
        "-p",
        &p,
        "-batch",
        "-nx",
        "-ex",
        "set pagination off",
        "-ex",
        "thread apply all bt",
    ];
    let eu: Vec<&str> = vec!["eu-stack", "-p", &p];
    let mut out = String::new();
    'tools: for tool in [&gdb, &eu] {
        if !crate::suites::have(tool[0]) {
            continue;
        }
        for sudo in [false, true] {
            let mut argv: Vec<&str> = Vec::new();
            if sudo {
                argv.extend(["sudo", "-n"]);
            }
            argv.extend(tool.iter());
            if let Some(text) = run_bounded(&argv, Duration::from_secs(90)) {
                if text.contains("Thread") || text.contains("TID") || text.contains("#0") {
                    out.push_str(&format!("$ {}\n{text}\n", argv.join(" ")));
                    break 'tools;
                }
            }
        }
    }
    if out.is_empty() {
        out.push_str("(no gdb or eu-stack backtrace could be taken)\n");
    }
    out.push_str("\nthreads (tid state wchan comm):\n");
    if let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) {
        for t in tasks.flatten() {
            let tid = t.file_name().to_string_lossy().into_owned();
            let rd = |f: &str| std::fs::read_to_string(t.path().join(f)).unwrap_or_default();
            let state = crate::load::proc_state(tid.parse().unwrap_or(0)).unwrap_or('?');
            out.push_str(&format!(
                "{tid} {state} {} {}\n",
                rd("wchan").trim(),
                rd("comm").trim()
            ));
        }
    }
    out
}

fn run_bounded(argv: &[&str], bound: Duration) -> Option<String> {
    let mut child = std::process::Command::new(argv[0])
        .args(&argv[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = std::io::Read::read_to_string(&mut stdout, &mut s);
        s
    });
    let t = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if t.elapsed() < bound => std::thread::sleep(Duration::from_millis(100)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
        }
    }
    reader.join().ok()
}

/// Abort the FUSE connection of the mount at `mnt` (its blocked callers
/// get `ENOTCONN`): what happened.
fn abort_connection(mnt: &Path) -> String {
    let Some(n) = crate::client::fuse_connection_of(mnt) else {
        return "not found (not aborted)".into();
    };
    let path = format!("/sys/fs/fuse/connections/{n}/abort");
    if std::fs::write(&path, b"1").is_ok() {
        return format!("{n} aborted");
    }
    match crate::load::root_sh(&format!("echo 1 > {path}")) {
        Ok(_) => format!("{n} aborted (as root)"),
        Err(e) => format!("{n} NOT aborted: {e:#}"),
    }
}

/// A scenario ran past `limit` (`harness run`'s scenario timeout): keep
/// every live daemon's backtraces and abort their FUSE connections, so
/// what blocked on them returns. What was done, as text.
pub fn abort_scenario(scenario: &str, limit: Duration) -> String {
    let daemons: Vec<_> = crate::load::daemons()
        .into_iter()
        .filter(|d| d.scenario == scenario)
        .collect();
    let mut done = Vec::new();
    for d in &daemons {
        let dir = log_dir()
            .map(|l| l.join(scenario))
            .unwrap_or_else(std::env::temp_dir);
        let _ = std::fs::create_dir_all(&dir);
        let bt = dir.join(format!("{}-{}-timeout.txt", d.node, d.pid));
        let _ = std::fs::write(&bt, backtraces(d.pid));
        if crate::load::proc_state(d.pid) == Some('T') {
            // A frozen daemon answers nothing: thaw it first.
            // SAFETY: kill(2) of a pid this harness spawned.
            unsafe { libc::kill(d.pid as i32, libc::SIGCONT) };
        }
        done.push(format!(
            "{} (pid {}): backtraces in {}, connection {}",
            d.node,
            d.pid,
            bt.display(),
            abort_connection(&d.mnt)
        ));
    }
    format!(
        "still running after {limit:?}; {}",
        if done.is_empty() {
            "no live daemon to abort".to_string()
        } else {
            done.join("; ")
        }
    )
}

// ------------------------------------------------------------- scenario

/// What verification found about `scenario` once it ended: hangs, the log
/// checks (unless the scenario ran them itself), bucket-log violations.
pub fn after_scenario(scenario: &str) -> Vec<String> {
    if !enabled() {
        return Vec::new();
    }
    let (logs, checked, mut out) = with_state(scenario, |s| {
        let mut out = std::mem::take(&mut s.hangs);
        out.extend(std::mem::take(&mut s.violations));
        // Only the logs a daemon wrote to (a client that never mounted
        // has none).
        let logs: Vec<(String, PathBuf)> = s
            .logs
            .iter()
            .filter(|(_, l)| s.opened.contains(l))
            .cloned()
            .collect();
        (logs, s.logs_checked, out)
    });
    if !checked {
        out.extend(log_violations(scenario, &logs));
    }
    out
}

/// Context for a failed run: every S3 proxy change of the scenario.
pub fn toxic_timeline(since: Instant) -> Vec<String> {
    crate::toxiproxy::timeline(Some(since))
        .into_iter()
        .map(|e| format!("{} {} {}", e.unix_ms, e.proxy, e.what))
        .collect()
}

/// Pick a conflicting pair of grants to two nodes from `logs` (for a
/// workload that proves the token check catches a violation): the later
/// grant, and the earlier one's id.
pub fn conflicting_grants(texts: &[(String, String, i64)]) -> Option<(GrantRec, GrantRec)> {
    let (ev, _) = parse_logs(texts);
    for (i, a) in ev.grants.iter().enumerate() {
        if let Some(b) = ev.grants[i + 1..]
            .iter()
            .find(|b| b.ino == a.ino && b.to != a.to && (a.exclusive || b.exclusive))
        {
            return Some((a.clone(), b.clone()));
        }
    }
    None
}

/// Read a log for [`conflicting_grants`].
pub fn read_log(name: &str, path: &Path) -> Result<(String, String, i64)> {
    Ok((
        name.to_string(),
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?,
        0,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_prove_themselves() {
        let id = FileId {
            path: "w-a/f1".into(),
            writer: "a".into(),
            seq: 7,
        };
        let p = payload(&id, 4096);
        assert_eq!(p.len(), 4096);
        assert_eq!(check_payload(&p), Ok(id.clone()));
        // A flipped byte, a truncation, another's content.
        let mut bad = p.clone();
        bad[2000] ^= 1;
        assert!(check_payload(&bad)
            .unwrap_err()
            .contains("corrupt from byte 2000"));
        assert!(check_payload(&p[..4000]).unwrap_err().contains("bytes"));
        assert!(check_payload(b"hello\n").is_err());
        // A short payload is its header alone.
        let short = payload(&id, 1);
        assert_eq!(check_payload(&short), Ok(id));
    }

    #[test]
    fn the_ledger_finds_lost_corrupt_wrong_and_phantom_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir(root.join("w")).unwrap();
        let l = Ledger::new();
        let id = |seq| FileId {
            path: format!("w/f{seq}"),
            writer: "a".into(),
            seq,
        };
        for seq in 0..6 {
            l.write(root, &format!("w/f{seq}"), id(seq), 1000).unwrap();
        }
        l.acked_rename("w/f5", "w/g5");
        std::fs::rename(root.join("w/f5"), root.join("w/g5")).unwrap();
        l.acked_unlink("w/f4");
        std::fs::remove_file(root.join("w/f4")).unwrap();
        let clean = l.check(root);
        assert!(clean.violations(0).is_empty(), "{clean:?}");
        assert_eq!(clean.intact, 5);
        // Lost, corrupt, wrong, phantom, resurrected.
        std::fs::remove_file(root.join("w/f0")).unwrap();
        let mut b = std::fs::read(root.join("w/f1")).unwrap();
        b[500] ^= 0xff;
        std::fs::write(root.join("w/f1"), b).unwrap();
        std::fs::write(root.join("w/f2"), payload(&id(3), 1000)).unwrap();
        std::fs::write(root.join("w/stray"), payload(&id(99), 10)).unwrap();
        std::fs::write(root.join("w/f4"), payload(&id(4), 1000)).unwrap();
        let rep = l.check(root);
        assert_eq!(rep.lost.len(), 1, "{rep:?}");
        assert_eq!(rep.corrupt.len(), 1, "{rep:?}");
        assert_eq!(rep.wrong.len(), 1, "{rep:?}");
        assert_eq!(rep.phantoms.len(), 1, "{rep:?}");
        assert_eq!(rep.resurrected, vec!["w/f4".to_string()]);
        assert_eq!(rep.violations(0).len(), 5);
        // A refused replay explains the undone unlink.
        assert_eq!(rep.violations(1).len(), 4);
        // A lost write found in a conflict copy is documented, not lost.
        let cc = root.join(".constellation-conflict");
        std::fs::create_dir(&cc).unwrap();
        std::fs::write(cc.join("w%2Ff0@1-2-3"), payload(&id(0), 1000)).unwrap();
        let rep = l.check(root);
        assert!(
            rep.lost.is_empty() && rep.in_conflict_copy.len() == 1,
            "{rep:?}"
        );
    }

    #[test]
    fn a_rename_carries_its_sources_doubt() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir(root.join("w")).unwrap();
        let l = Ledger::new();
        let id = |seq| FileId {
            path: format!("w/f{seq}"),
            writer: "a".into(),
            seq,
        };
        // f0 acknowledged, then rewritten in doubt (the write landed all
        // the same), then renamed: g0 holds the doubtful content.
        l.write(root, "w/f0", id(0), 1000).unwrap();
        std::fs::write(root.join("w/f0"), payload(&id(10), 1000)).unwrap();
        l.failed_write("w/f0", id(10));
        std::fs::rename(root.join("w/f0"), root.join("w/g0")).unwrap();
        l.acked_rename("w/f0", "w/g0");
        // f1 only ever written in doubt (it landed), then renamed.
        std::fs::write(root.join("w/f1"), payload(&id(11), 1000)).unwrap();
        l.failed_write("w/f1", id(11));
        std::fs::rename(root.join("w/f1"), root.join("w/g1")).unwrap();
        l.acked_rename("w/f1", "w/g1");
        let rep = l.check(root);
        assert!(rep.violations(0).is_empty(), "{rep:?}");
        assert_eq!(rep.in_doubt, 2, "{rep:?}");
        // A certain source clears the target's doubt: what it moved there
        // must be there.
        l.failed_write("w/g2", id(12));
        l.write(root, "w/f2", id(2), 1000).unwrap();
        std::fs::rename(root.join("w/f2"), root.join("w/g2")).unwrap();
        l.acked_rename("w/f2", "w/g2");
        std::fs::remove_file(root.join("w/g2")).unwrap();
        let rep = l.check(root);
        assert_eq!(rep.lost.len(), 1, "{rep:?}");
        assert!(rep.lost[0].starts_with("w/g2"), "{rep:?}");
        // An acknowledged unlink ends a doubt: a name back is resurrected.
        l.acked_unlink("w/g0");
        std::fs::write(root.join("w/g0"), payload(&id(10), 1000)).unwrap();
        let rep = l.check(root);
        assert_eq!(rep.resurrected, vec!["w/g0".to_string()], "{rep:?}");
    }

    #[test]
    fn an_op_answered_in_doubt_may_land_once_late() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir(root.join("w")).unwrap();
        let deadline = Duration::from_secs(40);
        let l = Ledger::with_client_deadline(deadline);
        let id = |seq| FileId {
            path: format!("w/f{seq}"),
            writer: "a".into(),
            seq,
        };
        let eio = std::io::Error::from_raw_os_error(libc::EIO);
        let enospc = std::io::Error::from_raw_os_error(libc::ENOSPC);
        assert!(answered_in_doubt(&eio, deadline, deadline));
        assert!(!answered_in_doubt(&eio, deadline / 2, deadline));
        assert!(!answered_in_doubt(&enospc, deadline, deadline));
        assert!(!answered_in_doubt(&eio, deadline, Duration::ZERO));
        // The drain's interleaving: create f0 answered in doubt; the
        // retry lands and is renamed away (both acknowledged); the stranded
        // create is replayed by rid after them: f0 is back, empty.
        l.failed_create("w/f0", id(0), &eio, deadline);
        l.write(root, "w/f0", id(1), 1000).unwrap();
        std::fs::rename(root.join("w/f0"), root.join("w/g0")).unwrap();
        l.acked_rename("w/f0", "w/g0");
        std::fs::write(root.join("w/f0"), b"").unwrap();
        let rep = l.check(root);
        assert!(rep.violations(0).is_empty(), "{rep:?}");
        assert_eq!(rep.late_landed.len(), 1, "{rep:?}");
        assert_eq!(rep.intact, 1, "{rep:?}");
        // What the create could not have left there is resurrection: a
        // create writes no content.
        std::fs::write(root.join("w/f0"), payload(&id(0), 1000)).unwrap();
        assert_eq!(l.check(root).resurrected, vec!["w/f0".to_string()]);
        std::fs::remove_file(root.join("w/f0")).unwrap();
        // Failed before the deadline, or with another errno: an outcome,
        // not doubt; the name stays gone.
        l.failed_create("w/f1", id(2), &eio, deadline / 2);
        l.failed_create("w/f2", id(3), &enospc, deadline);
        for f in ["w/f1", "w/f2"] {
            l.acked_unlink(f);
            std::fs::write(root.join(f), b"").unwrap();
        }
        let rep = l.check(root);
        assert_eq!(
            rep.resurrected,
            vec!["w/f1".to_string(), "w/f2".to_string()]
        );
        assert!(rep.late_landed.is_empty(), "{rep:?}");
        for f in ["w/f1", "w/f2"] {
            std::fs::remove_file(root.join(f)).unwrap();
        }
        // A create in doubt of a file that existed truncates it, makes no
        // name: unlinked later, it stays gone.
        l.write(root, "w/f3", id(4), 1000).unwrap();
        l.failed_create("w/f3", id(5), &eio, deadline);
        l.write(root, "w/f3", id(6), 1000).unwrap();
        std::fs::remove_file(root.join("w/f3")).unwrap();
        l.acked_unlink("w/f3");
        std::fs::write(root.join("w/f3"), b"").unwrap();
        assert_eq!(l.check(root).resurrected, vec!["w/f3".to_string()]);
        std::fs::remove_file(root.join("w/f3")).unwrap();
        // Only the name the op was on: another name back is resurrected.
        l.failed_create("w/f4", id(7), &eio, deadline);
        l.write(root, "w/f5", id(8), 1000).unwrap();
        std::fs::remove_file(root.join("w/f5")).unwrap();
        l.acked_unlink("w/f5");
        std::fs::write(root.join("w/f5"), b"").unwrap();
        assert_eq!(l.check(root).resurrected, vec!["w/f5".to_string()]);
        std::fs::remove_file(root.join("w/f5")).unwrap();
        // A rename in doubt may leave its source's content at its target
        // once, after the target was acknowledged gone; nothing else.
        l.write(root, "w/f6", id(9), 1000).unwrap();
        l.failed_rename("w/f6", "w/g6", &eio, deadline);
        l.acked_unlink("w/g6");
        std::fs::rename(root.join("w/f6"), root.join("w/g6")).unwrap();
        let rep = l.check(root);
        assert!(rep.violations(0).is_empty(), "{rep:?}");
        assert_eq!(rep.late_landed.len(), 1, "{rep:?}");
        assert!(rep.late_landed[0].starts_with("w/g6"), "{rep:?}");
        std::fs::write(root.join("w/g6"), payload(&id(10), 1000)).unwrap();
        assert_eq!(l.check(root).resurrected, vec!["w/g6".to_string()]);
        // A ledger without a deadline has no op in doubt.
        let strict = Ledger::new();
        strict.failed_create("w/h", id(11), &eio, deadline);
        strict.acked_unlink("w/h");
        std::fs::write(root.join("w/h"), b"").unwrap();
        assert_eq!(strict.check(root).resurrected, vec!["w/h".to_string()]);
    }

    #[test]
    fn conflict_copies_are_found_wherever_the_engine_puts_them() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("v/w")).unwrap();
        let l = Ledger::new();
        let id = |seq| FileId {
            path: format!("v/w/f{seq}"),
            writer: "a".into(),
            seq,
        };
        for seq in 0..3 {
            l.write(root, &format!("v/w/f{seq}"), id(seq), 1000)
                .unwrap();
            std::fs::remove_file(root.join(format!("v/w/f{seq}"))).unwrap();
        }
        let rep = l.check(root);
        assert_eq!(rep.lost.len(), 3, "{rep:?}");
        // Under the mount's root, named after the path (a refused commit
        // under a lock).
        let at_root = root.join(".constellation-conflict");
        std::fs::create_dir(&at_root).unwrap();
        std::fs::write(at_root.join("v%2Fw%2Ff0@1-2-3"), payload(&id(0), 1000)).unwrap();
        // Under the root of a mounted view above the file (a subtree
        // mount at v).
        let at_view = root.join("v/.constellation-conflict");
        std::fs::create_dir(&at_view).unwrap();
        std::fs::write(at_view.join("w%2Ff1@1-2-4"), payload(&id(1), 1000)).unwrap();
        // Beside the file (a refused replay of an op without a lock tag).
        let beside = root.join("v/w/.constellation-conflict");
        std::fs::create_dir(&beside).unwrap();
        std::fs::write(beside.join("f2@1-2-5"), payload(&id(2), 1000)).unwrap();
        let rep = l.check(root);
        assert!(rep.lost.is_empty(), "{rep:?}");
        assert_eq!(rep.in_conflict_copy.len(), 3, "{rep:?}");
        // A conflict directory beside the file is no phantom.
        assert!(rep.phantoms.is_empty(), "{rep:?}");
        let found = conflict_copies(root, ["v/w/f0"]);
        assert_eq!(found.len(), 3, "{found:?}");
        // Nothing outside the file's ancestry: a sibling's directory.
        std::fs::create_dir_all(root.join("x/.constellation-conflict")).unwrap();
        std::fs::write(
            root.join("x/.constellation-conflict/f9@1-2-6"),
            payload(&id(9), 10),
        )
        .unwrap();
        assert_eq!(conflict_copies(root, ["v/w/f0"]).len(), 3);
    }

    #[test]
    fn timestamps_and_fields_parse() {
        assert_eq!(
            line_unix_ms("1970-01-01T00:00:01.500000Z  INFO x"),
            Some(1_500)
        );
        assert_eq!(
            line_unix_ms("2026-10-15T19:27:49.123456Z DEBUG y"),
            Some(1_792_092_469_123)
        );
        assert_eq!(line_unix_ms("garbage"), None);
        assert_eq!(
            line_unix_us("2026-10-15T19:27:49.123456Z DEBUG y"),
            Some(1_792_092_469_123_456)
        );
        assert_eq!(line_unix_us("1970-01-01T00:00:01.5Z x"), Some(1_500_000));
        let styled = "2026-10-15T19:27:49.1Z \u{1b}[34mDEBUG\u{1b}[0m \u{1b}[3mminter\u{1b}[0m\u{1b}[2m=\u{1b}[0m3 seq=17 to=4";
        let line = strip_ansi(styled);
        assert_eq!(field(&line, "minter"), Some("3"));
        assert_eq!(field(&line, "seq"), Some("17"));
        assert_eq!(field(&line, "to"), Some("4"));
        assert_eq!(field("xto=1 to=2", "to"), Some("2"));
        assert_eq!(field("a=1", "b"), None);
    }

    fn grant_line(t: &str, minter: u64, seq: u64, to: u64, mode: &str) -> String {
        format!(
            "{t} DEBUG constellation_authority::core::locks: granted a lock node={minter} to={to} ino=42 mode={mode} ttl=5000 upgraded=false minter={minter} seq={seq}"
        )
    }

    #[test]
    fn token_order_catches_an_op_after_a_later_grant() {
        let owner = [
            grant_line("2026-10-15T00:00:01.000Z", 1, 1, 2, "Exclusive"),
            grant_line("2026-10-15T00:00:05.000Z", 1, 2, 3, "Exclusive"),
            // Shared grants to two nodes never conflict.
            grant_line("2026-10-15T00:00:09.000Z", 1, 3, 2, "Shared"),
            grant_line("2026-10-15T00:00:09.500Z", 1, 4, 3, "Shared"),
        ]
        .join("\n");
        let base = line_unix_ms("2026-10-15T00:00:00.000Z").unwrap();
        let exec = |seq: u64, at: i64| {
            format!(
                "2026-10-15T00:00:00.000Z DEBUG constellation::token_exec: tagged op executed minter=1 seq={seq} at_ms={}",
                base + at
            )
        };
        let good = [
            exec(1, 2_000),
            exec(1, 4_000),
            exec(2, 6_000),
            exec(3, 9_800),
            exec(4, 9_700),
        ]
        .join("\n");
        let logs = vec![
            ("owner".to_string(), owner.clone(), 0),
            ("exec".to_string(), good, 0),
        ];
        let (ev, _) = parse_logs(&logs);
        assert_eq!(ev.grants.len(), 4);
        assert!(
            check_token_order(&ev).is_empty(),
            "{:?}",
            check_token_order(&ev)
        );
        // Grant 1/1's op lands after 1/2 went to node 3.
        let bad = [exec(1, 2_000), exec(1, 5_200)].join("\n");
        let (ev, _) = parse_logs(&[("owner".into(), owner.clone(), 0), ("exec".into(), bad, 0)]);
        let v = check_token_order(&ev);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("200.000 ms after grant 1/2"), "{v:?}");
        // The grant line's own millisecond is no evidence of an overlap
        // (both clocks read whole ms).
        let tie = exec(1, 5_000);
        let (ev, _) = parse_logs(&[("owner".into(), owner.clone(), 0), ("exec".into(), tie, 0)]);
        assert!(
            check_token_order(&ev).is_empty(),
            "{:?}",
            check_token_order(&ev)
        );
        // The same op on an executor whose protocol clock ran 400 ms ahead
        // happened at 4.8 s: in order.
        let ahead = exec(1, 5_200);
        let (ev, _) = parse_logs(&[
            ("owner".into(), owner.clone(), 0),
            ("exec".into(), ahead, 400),
        ]);
        assert!(check_token_order(&ev).is_empty());
        assert!(conflicting_grants(&[("owner".into(), owner, 0)]).is_some());
    }

    #[test]
    fn a_write_that_lands_after_a_later_grant_is_caught_and_blindness_is_loud() {
        let owner = [
            grant_line("2026-10-15T00:00:01.000Z", 1, 1, 2, "Exclusive"),
            grant_line("2026-10-15T00:00:05.000Z", 1, 2, 3, "Exclusive"),
        ]
        .join("\n");
        let base = line_unix_ms("2026-10-15T00:00:00.000Z").unwrap();
        let line = |what: &str, field: &str, seq: u64, at: i64| {
            let at = if field == "host_us" {
                base * 1_000 + at
            } else {
                base + at
            };
            format!(
                "2026-10-15T00:00:00.000Z DEBUG constellation::token_exec: tagged op {what} minter=1 seq={seq} {field}={at}"
            )
        };
        // F1's shape: checked at 4 s (before the grant ended), landed at
        // 5.3 s, after grant 1/2 went to node 3 at 5 s.
        let f1 = [
            line("executed", "at_ms", 1, 4_000),
            line("landed", "host_us", 1, 5_300_000),
            line("executed", "at_ms", 2, 6_000),
            line("landed", "host_us", 2, 6_001_000),
        ]
        .join("\n");
        let (ev, _) = parse_logs(&[("owner".into(), owner.clone(), 0), ("x".into(), f1, 400)]);
        let v = check_token_order(&ev);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("300.000 ms after grant 1/2"), "{v:?}");
        assert!(check_blind(&ev).is_empty(), "{:?}", check_blind(&ev));
        // To the µs: a landing 300 µs after the grant line, in its very
        // millisecond, is caught; one 300 µs before it is not.
        let owner_us = grant_line("2026-10-15T00:00:05.000500Z", 1, 2, 3, "Exclusive");
        let first = grant_line("2026-10-15T00:00:01.000Z", 1, 1, 2, "Exclusive");
        for (landed, caught) in [(5_000_800, true), (5_000_200, false)] {
            let (ev, _) = parse_logs(&[
                ("owner".into(), format!("{first}\n{owner_us}"), 0),
                ("x".into(), line("landed", "host_us", 1, landed), 0),
            ]);
            assert_eq!(
                !check_token_order(&ev).is_empty(),
                caught,
                "landed at {landed} µs"
            );
        }
        // Executed lines without any landed one: blind to F1.
        let old = line("executed", "at_ms", 1, 4_000);
        let (ev, _) = parse_logs(&[("owner".into(), owner.clone(), 0), ("x".into(), old, 0)]);
        assert!(check_blind(&ev)[0].contains("no `tagged op landed`"));
        // A grant line whose fields drifted is counted, not parsed: blind.
        let drifted = "2026-10-15T00:00:07.000Z DEBUG x: granted a lock node=1 to=2 inode=42 mode=Exclusive id=1/9";
        let (ev, _) = parse_logs(&[("owner".into(), drifted.into(), 0)]);
        let b = check_blind(&ev);
        assert!(
            b[0].contains("1 `granted a lock` line(s) in the logs, 0 parsed"),
            "{b:?}"
        );
    }

    #[test]
    fn two_nodes_in_one_epoch_is_split_brain() {
        let a = "2026-10-15T00:00:01Z INFO x: acquired the lease node=1 epoch=3 takeover=false";
        let b = "2026-10-15T00:00:02Z INFO x: acquired the lease node=2 epoch=4 takeover=true";
        let c = "2026-10-15T00:00:03Z INFO x: acquired the lease node=3 epoch=4 takeover=true";
        let (_, acq) = parse_logs(&[("a".into(), a.into(), 0), ("b".into(), b.into(), 0)]);
        assert!(check_lease_epochs(&acq).is_empty());
        let (_, acq) = parse_logs(&[
            ("a".into(), a.into(), 0),
            ("b".into(), b.into(), 0),
            ("c".into(), c.into(), 0),
        ]);
        let v = check_lease_epochs(&acq);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("epoch 4"), "{v:?}");
    }

    #[test]
    fn errno_classification() {
        let rec = |errno, ctx, took| ErrnoRecord {
            node: "a".into(),
            op: "write".into(),
            errno,
            started: Instant::now(),
            took,
            ctx,
        };
        let log = ErrnoLog::new();
        let under_lock = OpCtx {
            under_lock: true,
            ..Default::default()
        };
        {
            let mut v = log.0.lock().unwrap();
            v.push(rec(libc::EIO, under_lock, Duration::ZERO));
            v.push(rec(libc::EIO, under_lock, Duration::ZERO));
            v.push(rec(
                libc::EAGAIN,
                OpCtx {
                    nonblocking_lock: true,
                    ..Default::default()
                },
                Duration::ZERO,
            ));
            v.push(rec(libc::EIO, OpCtx::default(), Duration::from_secs(130)));
            v.push(rec(libc::EBADF, OpCtx::default(), Duration::ZERO));
            v.push(rec(
                libc::ENOLCK,
                OpCtx {
                    lock_request: true,
                    ..Default::default()
                },
                Duration::ZERO,
            ));
        }
        let mut ev = Evidence {
            fenced_io: [("a".to_string(), 1)].into(),
            acquire_deadline: Duration::from_secs(120),
            allow_in_doubt: false,
        };
        let rep = log.judge(&ev);
        // One fenced EIO and EAGAIN; the second EIO under the lock (the
        // node fenced one op), the in-doubt EIO, EBADF and the ENOLCK
        // without an outage are bugs.
        assert_eq!(rep.allowed.len(), 2, "{:?}", rep.allowed);
        assert_eq!(rep.bugs.len(), 4, "{:?}", rep.bugs);
        assert_eq!(rep.counts["fenced"], 1);
        assert_eq!(rep.counts["in-doubt"], 1);
        assert_eq!(rep.counts["eagain-nonblocking"], 1);
        assert_eq!(rep.counts["bug"], 3);
        // Triage runs may allow the in-doubt one.
        ev.allow_in_doubt = true;
        assert_eq!(log.judge(&ev).allowed.len(), 3);
    }

    #[test]
    fn blind_spots_are_violations() {
        // A grant whose mode is neither variant is counted, not parsed.
        let odd = grant_line("2026-10-15T00:00:01.000Z", 1, 1, 2, "Upgradable");
        let ok = grant_line("2026-10-15T00:00:02.000Z", 1, 2, 3, "Shared");
        let (ev, _) = parse_logs(&[("owner".into(), format!("{odd}\n{ok}"), 0)]);
        assert_eq!(ev.grants.len(), 1);
        let b = check_blind(&ev);
        assert!(
            b.iter()
                .any(|v| v.contains("2 `granted a lock` line(s) in the logs, 1 parsed")),
            "{b:?}"
        );
        // A status with the section but no list: blind; with it, or without
        // the section (another kind of status), not.
        let drifted = serde_json::json!({"fuse_requests": {"in_flight": 3}});
        assert!(watch_blind(&drifted).is_some_and(|v| v.contains("checker blind")));
        assert_eq!(stalled_request(&drifted, Duration::ZERO), None);
        let fine = serde_json::json!({"fuse_requests": {"stalled_requests": []}});
        assert_eq!(watch_blind(&fine), None);
        assert_eq!(watch_blind(&serde_json::json!({})), None);
    }

    #[test]
    fn a_stalled_request_is_a_hang_unless_it_is_a_lock_wait() {
        let s = serde_json::json!({"fuse_requests": {"stalled_requests": [
            {"op": "setlkw", "ino": 3, "age_s": 500, "stage": "lock-wait", "tid": 9, "blocking": true},
            {"op": "flush", "ino": 7, "age_s": 130, "stage": "running", "tid": 10, "blocking": false},
        ]}});
        let h = stalled_request(&s, Duration::from_secs(120)).unwrap();
        assert!(h.contains("flush") && h.contains("ino 7"), "{h}");
        assert_eq!(stalled_request(&s, Duration::from_secs(200)), None);
    }
}
