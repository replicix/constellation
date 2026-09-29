//! Constellation entry point: CLI, daemon, and FUSE mount in one binary.

mod control;
mod daemon_lock;
mod daemonize;
mod handover;
mod node_runtime;
mod parallelism;
mod startup;

// Plan 31 C3: the engine modules live in `constellation-engine`; importing
// them here keeps every `crate::<module>::…` path in this crate as it was.
use constellation_engine::{
    atime, authority_driver, backend, cto, designation, doctor, e2e_pin, fsck, gc, lease, leave,
    locks, log_buffer, registry, shipper, target, writeback,
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use constellation_control::methods as cm;
use constellation_control::proto::types as api;
use constellation_meta::Meta;
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta};
use constellation_types::Code;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

#[derive(Parser)]
#[command(
    name = "constellation",
    version = env!("CONSTELLATION_VERSION"),
    about = "Distributed POSIX filesystem on S3"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Filesystem lifecycle.
    Fs {
        #[command(subcommand)]
        command: FsCommand,
    },
    /// Mount a filesystem by name — see below for `TARGET`/`MOUNTPOINT`.
    Mount {
        /// A registered name ("myfs"), a name with a subtree/snapshot
        /// selector ("myfs:/data"), or (with `--s3`/`--state-dir`) a
        /// literal path/selector for an ad-hoc, unregistered mount.
        target: String,
        /// Where to mount. Omit to reuse the target's stored mountpoint,
        /// or (for a bare name with no subtree) to mount every view
        /// already registered for that name.
        mountpoint: Option<PathBuf>,
        /// Backend: s3://bucket/prefix, file:///path, or absolute path.
        /// Refused if it contradicts an already-populated state dir
        /// (identity is pinned to the name); fills in an empty one.
        #[arg(long)]
        s3: Option<String>,
        /// Local state directory (metadata DB + chunk cache). Explicit
        /// escape hatch for an ad-hoc, unregistered mount.
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Run in the foreground instead of backgrounding.
        #[arg(long, short = 'f')]
        foreground: bool,
        /// Chunk cache budget (e.g. 10G, 512MiB). Suffixes are binary.
        #[arg(long, value_parser = parse_byte_size)]
        cache_size: Option<u64>,
        /// Allow other users to access the mount.
        #[arg(long)]
        allow_other: bool,
        /// Filesystem source name reported by mount tools.
        #[arg(long)]
        fs_name: Option<String>,
        /// What fsync() waits for: "local" (journal on disk; background
        /// ship) or "s3" (record durable in the shared log).
        #[arg(long)]
        fsync_mode: Option<String>,
        /// Close-to-open consistency (plan 30 §M8): "bounded" (the
        /// default: an open reads the local replica, which follows the
        /// log within the visibility bound) or "strict" (an open or lookup
        /// sees every close another node completed before it started: one
        /// ReadIndex round trip to the sequencer, then local under a read
        /// delegation). `CONSTELLATION_CTO` supplies the default.
        #[arg(long)]
        cto: Option<String>,
        /// File locks (plan 30 §M14): "cluster" (the default with P2P:
        /// `flock`/`fcntl` locks exclude each other across nodes, leased
        /// from the file's sequencer) or "local" (each node's kernel keeps
        /// its own locks, as before). Cluster locks need P2P; without it
        /// the mode is local, and an explicit "cluster" fails the mount.
        /// `CONSTELLATION_LOCKS` supplies the default.
        #[arg(long)]
        locks: Option<String>,
        /// Chunk close policy: "through" waits for S3; "back" returns
        /// after the local durable queue is journaled.
        #[arg(long)]
        write_mode: Option<String>,
        /// Enrol this state directory as a read-only cluster member.
        /// Only valid on its first mount; RO members do not count toward
        /// a continuation epoch's write-eligible roster.
        #[arg(long)]
        read_only_member: bool,
        /// Read-time access-time updates: off (default), relatime, or
        /// lazy (plan 20). Best-effort and eventually consistent;
        /// overridable by CONSTELLATION_ATIME.
        #[arg(long)]
        atime: Option<String>,
        /// Mount a snapshot selector through an automatically created clone.
        #[arg(long)]
        rw: bool,
        /// Destination path/name for `--rw`.
        #[arg(long, requires = "rw")]
        clone_name: Option<String>,
        /// Create a temporary clone and remove it on clean unmount.
        #[arg(long, requires = "rw", conflicts_with = "clone_name")]
        ephemeral: bool,
        /// Confine hard links to the view (plan 31 §6.12): `link()` of a
        /// file none of whose names lies in the destination's link domain
        /// (the view's root, or the nearest directory marked
        /// `trusted.constellation.link_domain`) fails with EXDEV, as does
        /// moving one of several names of a file across domains.
        #[arg(long)]
        confine_links: bool,
        /// Serve the embedded control UI on localhost. Zero disables it.
        /// Bare `--web-ui` listens on 8080; `--web-ui <port>` picks a port.
        #[arg(
            long,
            env = "CONSTELLATION_WEB_UI_PORT",
            num_args = 0..=1,
            default_missing_value = "8080"
        )]
        web_ui: Option<u16>,
    },
    /// Detach a view (`umount myfs:/sub`) or every currently-mounted
    /// view (`umount myfs`). The daemon exits once its last view is
    /// gone.
    Umount {
        /// A registered name ("myfs") or a name with a subtree selector
        /// ("myfs:/data") identifying the view(s) to detach.
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Permanently remove a registered filesystem: leave the cluster (if
    /// ever mounted), detach every view, delete the state dir, and drop
    /// the registry row. The only way to un-register a name.
    Export {
        name: String,
        /// Proceed even if the daemon is unreachable or slow to
        /// close — the local state may be left mid-shutdown; safe
        /// because lease expiry and replica recovery on the next mount
        /// handle it, not a silent data-loss shortcut.
        #[arg(long)]
        force: bool,
    },
    /// Verify backend capabilities (conditional writes, filesystem state).
    Doctor {
        target: String,
        #[arg(long)]
        s3: Option<String>,
    },
    /// The running daemon of a filesystem. `--upgrade` replaces its binary
    /// in place while every view stays mounted (plan 31 C4b): the daemon
    /// detaches its FUSE sessions (queued requests wait in the kernel),
    /// shuts its node down cleanly, and `exec`s the new binary, which
    /// resumes the same connections under the same pid — no unmount, no
    /// `ENOTCONN` for a process with a file open on the mount. Refused
    /// (nothing changes) while a cluster lock is held or waited for.
    Daemon {
        /// A registered name, or (with `--state-dir`) anything.
        target: Option<String>,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Upgrade the running daemon in place.
        #[arg(long)]
        upgrade: bool,
        /// The binary to upgrade to (default: the path the daemon was
        /// started from, as it is on disk now).
        #[arg(long, requires = "upgrade")]
        binary: Option<PathBuf>,
        /// How long to wait for the new image to serve.
        #[arg(long, default_value_t = 300)]
        timeout_s: u64,
        /// (internal) Print this binary's handover version.
        #[arg(long, hide = true)]
        handover_abi: bool,
        /// (internal) Resume a handed-over daemon from this memfd.
        #[arg(long, hide = true)]
        resume_from: Option<i32>,
    },
    /// (internal) The daemon's zombie reaper: watches the daemon that
    /// spawned it and, once the kernel has killed it but a thread wedged
    /// inside a FUSE notification keeps it a zombie holding its mounts,
    /// lock and socket (campaign 6 B-1), aborts those mounts' FUSE
    /// connections so the zombie can go.
    #[command(hide = true)]
    ZombieReaper {
        #[arg(long)]
        parent: u32,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Show filesystem information from the backend, or live daemon
    /// status (spool backlog, cache, every mounted view) from a
    /// registered name / mount's state dir.
    Status {
        target: Option<String>,
        #[arg(long, conflicts_with = "state_dir")]
        s3: Option<String>,
        /// State dir of a running mount: query its control socket.
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Keep a subtree fully cached on this node and follow its changes.
    Pin {
        /// `myfs`, `myfs:/data`, or (with `--state-dir`) a literal path.
        /// A bare name means the whole filesystem (root).
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Stop keeping a subtree resident; its chunks become evictable.
    Unpin {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// List this node's pinned subtrees.
    Pins {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Designate this node for exclusive local-speed writes under a
    /// subtree while it stays reachable.
    Offline {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Grant a read guarantee (pin) without write authority; other
        /// nodes' writes under `path` remain unrestricted.
        #[arg(long)]
        ro: bool,
    },
    /// Release this node's designation for a subtree.
    Online {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// List active offline designations visible to this node.
    Designations {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Plan 30 §M11: delegate a directory's subtree to a node (this
    /// node must hold the lease).
    Delegate {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// The delegate's node id.
        #[arg(long)]
        to: u64,
        /// Plan 30 §M12: one name-hash range of the directory,
        /// `<idx>/<count>` (count 2, 4, 8 or 16), instead of the whole.
        #[arg(long)]
        range: Option<String>,
    },
    /// Plan 30 §M11: recall the delegation on a directory.
    Undelegate {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Plan 30 §M11: list the live delegation table.
    Delegations {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Run this node's deposition recovery now: roll back the journal a
    /// deposition stranded and queue its ops for replay by rid through the
    /// current holder (plan 30 M3b; it also runs by itself on the next sync
    /// round).
    Reintegrate {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Permanently leave the cluster (tombstone the registry record).
    /// Omit `--node-id` to leave this mount; pass `--node-id` to retire
    /// a different (unreachable) member via a still-mounted peer.
    Leave {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Admin form: retire a different node id.
        #[arg(long)]
        node_id: Option<u64>,
        /// Skip courtesy refusals (live designation / live foreign lease).
        /// Never skips an open epoch or a stranded deposed journal.
        #[arg(long)]
        force: bool,
    },
    /// Change a running mount's write policy. Switching to `through`
    /// drains the durable pending-upload queue before it takes effect.
    WriteMode {
        target: String,
        mode: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Get or set the cluster-wide logical size cap.
    Quota {
        #[command(subcommand)]
        command: QuotaCommand,
    },
    /// Manage retention prune policies (plan 22).
    Prune {
        #[command(subcommand)]
        command: PruneCommand,
    },
    /// Inspect one file or directory through the running daemon.
    Inspect {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Inspect local cache state.
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
    /// Read recent daemon logs.
    Log {
        #[command(subcommand)]
        command: LogCommand,
    },
    /// Create, list, and delete immutable subtree snapshots.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    /// Create an ordinary writable subtree from a snapshot.
    Clone {
        /// `myfs:/path@name` or (with `--state-dir`) a literal selector.
        target: String,
        /// Plain path, same filesystem as `target` implied (clone
        /// cannot cross filesystems).
        destination: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Low-level inspection commands.
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
    /// Coordinated bucket garbage collection.
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },
    /// Check bucket, replica, and cache consistency.
    Fsck {
        target: String,
        #[arg(long)]
        s3: Option<String>,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        #[arg(long)]
        repair: bool,
        /// Explicitly release this expired partition lease while repairing.
        #[arg(long, requires = "repair")]
        force_release: Option<String>,
    },
    /// Repair verbs for a running mount (plan 30 §M4).
    Repair {
        #[command(subcommand)]
        command: RepairCommand,
    },
}

/// `constellation repair ...`.
#[derive(Subcommand)]
enum RepairCommand {
    /// Discard the journal records held back behind an inode's
    /// unrecoverable pending chunk(s) (`status`'s `held` section) into a
    /// `.constellation-conflict/` copy whose lost chunks read as holes.
    /// Records that only depended on them are rolled back and replayed.
    DropHeld {
        target: String,
        /// The inode `status` lists under `held.inodes` (or, with
        /// `--remote`, under `held.remote`).
        ino: u64,
        /// The inode's records wait for chunks another node forwarded as
        /// pending (`held.remote`) and that node is gone for good: declare
        /// those chunks unrecoverable and drop the records the same way.
        /// Never use it for a node that will come back: its chunks would
        /// upload and the write would ship by itself.
        #[arg(long)]
        remote: bool,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum GcCommand {
    /// Mark, condemn, wait, and sweep eligible objects.
    Run {
        target: String,
        #[arg(long)]
        s3: Option<String>,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Run the mark phase only and print deletion evidence.
    Verify {
        target: String,
        #[arg(long)]
        s3: Option<String>,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    /// `myfs:/path@name` — a selector always names a path; `@name` is
    /// required.
    Create {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// `myfs[:/path]` — a bare name lists from the root.
    Ls {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    Delete {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum QuotaCommand {
    /// Show the current cap and used logical bytes.
    Get {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Set the cap (`10G`, `unlimited`, or `0` to clear).
    Set {
        target: String,
        /// Byte size (`10G`), or `unlimited`/`0` to clear the cap.
        size: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum PruneCommand {
    /// Validate a policy expression without writing it. Exit 2 on error.
    Check {
        /// The policy, e.g. 'age(90d)' or 'lru(high=85%, low=70%)'.
        expr: String,
    },
    /// Write a policy onto a directory (validated first).
    Set {
        /// A directory inside a mount.
        path: PathBuf,
        /// The policy expression.
        expr: String,
        /// Arm the policy for real deletion (otherwise dry-run).
        #[arg(long)]
        arm: bool,
    },
    /// Remove the arming token, leaving the policy in dry-run.
    Disarm { path: PathBuf },
    /// Delete the policy from a directory.
    Rm { path: PathBuf },
    /// Show the effective policy for a path and where it is inherited from.
    Show { path: PathBuf },
    /// List every marked prune root (via the daemon).
    Ls {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Run one prune pass now (via the daemon).
    Run {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Restrict to the marked root governing this path.
        #[arg(long)]
        path: Option<String>,
        /// Evaluate and report without deleting.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show the prune counters (via the daemon).
    Status {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum CacheCommand {
    Ls {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    Stat {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Drop clean LRU chunks from the local cache (pinned/dirty kept).
    #[command(visible_alias = "evict")]
    Prune {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Keep at most this many bytes used (e.g. `1G`, `512MiB`).
        /// Default `0` frees every clean chunk.
        #[arg(long, default_value = "0", value_parser = parse_byte_size)]
        target_bytes: u64,
    },
}

#[derive(Subcommand)]
enum LogCommand {
    Tail {
        target: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 100)]
        lines: usize,
    },
}

#[derive(Subcommand)]
enum DebugCommand {
    /// `debug snap-refs myfs <id>` — the id is a separate positional
    /// since a snapshot id is never path-shaped.
    SnapRefs {
        target: String,
        id: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum FsCommand {
    /// Register (and create at the backend, if `--s3` is new) a named
    /// filesystem. No views/mounts yet — `mount NAME` adds those.
    Create {
        name: String,
        #[arg(long)]
        s3: String,
        /// Chunk size in bytes (power of two, 1-64 MiB).
        #[arg(long, default_value_t = constellation_fs_core::DEFAULT_CHUNK_SIZE)]
        chunk_size: u32,
        /// Root compression setting (raw, zstd, zstd:LEVEL).
        #[arg(long, default_value = "zstd:3")]
        compression: String,
        /// Protect content and filename-bearing metadata with a passphrase.
        #[arg(long)]
        e2e: bool,
        /// Optional logical size cap (e.g. 10G). Unbounded when omitted.
        #[arg(long, value_parser = parse_byte_size)]
        max_size: Option<u64>,
        /// Acknowledgement policy of the filesystem (plan 30 §M9), for
        /// every mount and every lease tenure: "local" (default; a
        /// mutation is acknowledged once journaled by its sequencer, and
        /// held by a backup peer within the RTT budget when there is one)
        /// or "s3" (every acknowledgement waits for the record to land in
        /// the shared log; a silent holder is taken over fast). Fixed at
        /// creation. `CONSTELLATION_ACK` supplies the default.
        #[arg(long)]
        ack_policy: Option<String>,
        /// Plan 30 §M10: how many write-eligible nodes a continuation
        /// epoch may form without (`f`, default 0: every node must be a
        /// member). With f > 0 an S3 takeover needs f other nodes'
        /// heartbeat promises.
        #[arg(long, default_value_t = 0)]
        epoch_slack: u32,
    },
    /// Change a filesystem setting stored in `meta.json`.
    Set {
        #[command(subcommand)]
        setting: FsSetting,
    },
    /// Change an E2E filesystem's passphrase without re-encrypting data.
    Passwd {
        target: String,
        #[arg(long)]
        s3: Option<String>,
    },
    /// List every registered filesystem and its views.
    List,
}

#[derive(Subcommand)]
enum FsSetting {
    /// Plan 30 §M10: set `epoch_slack` (`f`). Refused when `f` is at
    /// least the write-eligible roster size; warns when `f > N − 2` (a
    /// single crashed holder then blocks TTL failover until it returns).
    /// Mounted nodes pick it up within a minute; a node still on the old
    /// value keeps advertising it in its heartbeat, and a taker honours
    /// the largest advertised.
    EpochSlack {
        target: String,
        slack: u32,
        #[arg(long)]
        s3: Option<String>,
    },
}

/// Shared boilerplate for the many control commands that take
/// `TARGET [--state-dir DIR]`: load the registry, resolve `target`
/// against it, then resolve the effective state dir (explicit wins,
/// else the registry, else an error naming what's missing).
fn resolve_target(target: &str, state_dir: Option<PathBuf>) -> Result<(target::Target, PathBuf)> {
    let reg = registry::Registry::load()?;
    let t = target::resolve(target, &reg);
    let dir = target::state_dir(state_dir, &t)?;
    Ok((t, dir))
}

fn passphrase(env: &str, prompt: &str) -> Result<Zeroizing<String>> {
    if let Ok(value) = std::env::var(env) {
        if value.is_empty() {
            bail!("{env} must not be empty");
        }
        return Ok(Zeroizing::new(value));
    }
    let value = rpassword::prompt_password(prompt)?;
    if value.is_empty() {
        bail!("passphrase must not be empty");
    }
    Ok(Zeroizing::new(value))
}

/// `allow_other` needs `user_allow_other` in `/etc/fuse.conf` for a
/// non-root user; without it the FUSE mount fails deep in the daemon with
/// no useful message. Check it up front so the error is actionable and
/// reaches the caller's terminal. Root is exempt (the kernel allows it).
fn ensure_allow_other_supported(enabled: bool) -> Result<()> {
    if !enabled {
        return Ok(());
    }
    if constellation_platform::native().process.effective_ids().0 == 0 {
        return Ok(());
    }
    let enabled_in_conf = std::fs::read_to_string("/etc/fuse.conf")
        .unwrap_or_default()
        .lines()
        .any(|line| {
            let code = line.split('#').next().unwrap_or("").trim();
            code == "user_allow_other"
        });
    if !enabled_in_conf {
        bail!(
            "--allow-other requires `user_allow_other` in /etc/fuse.conf. \
             Add or uncomment that line (as root) in /etc/fuse.conf, or run the mount as root."
        );
    }
    Ok(())
}

/// Is a daemon already serving this state dir? A `Ping` answered within
/// the control timeout means yes — a new `mount` will attach to it rather
/// than unlock the keyring itself, so it needs no passphrase. A bare
/// `connect` succeeding is not enough (campaign 6 B-1: a daemon the
/// kernel has killed keeps its listener while one thread is stuck on its
/// way out, and accepts connections it will never answer). Runs through
/// a throwaway runtime that is fully dropped before the caller forks.
fn daemon_socket_is_live(state_dir: &Path) -> bool {
    if constellation_control::transport::locate_socket(state_dir).is_none() {
        return false;
    }
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return false;
    };
    let live = rt.block_on(control::ping(state_dir, daemon_lock::control_timeout()));
    drop(rt);
    match live {
        Ok(live) => live,
        Err(e) => {
            tracing::warn!(
                state_dir = %state_dir.display(),
                error = %e,
                "a daemon holds this state dir's control socket but does not answer; \
                 not attaching to it"
            );
            false
        }
    }
}

/// For an interactive E2E mount, prompt for the passphrase *before* the
/// daemon fork (the child has no terminal). Returns `None` — deferring to
/// the env var or an in-daemon prompt — when the env var is set (it
/// survives the fork) or the filesystem is not E2E. Reads `meta.json`
/// through a throwaway runtime that is fully dropped before the caller
/// forks, so no runtime threads leak into the daemon child.
/// Run the backend preflight for `fs create`, print the per-operation
/// report, and bail with a concise message if any required operation is
/// unsupported (e.g. Backblaze B2 rejecting the conditional-write headers).
async fn preflight_backend(store: &ChunkStore, s3: &str) -> Result<()> {
    let checks = store.preflight().await;
    let mut failed = Vec::new();
    for c in &checks {
        let status = match &c.outcome {
            Ok(()) => "ok".to_string(),
            Err(reason) => {
                if c.required {
                    failed.push((c.name, reason.clone()));
                    format!("FAILED: {reason}")
                } else {
                    format!("unavailable: {reason}")
                }
            }
        };
        eprintln!("  {:.<40} {}", format!("{} ", c.name), status);
    }
    if !failed.is_empty() {
        let details = failed
            .iter()
            .map(|(name, reason)| format!("  - {name}: {reason}"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "backend at {s3} is not usable as a constellation filesystem; \
             the following required operations failed:\n{details}"
        );
    }
    Ok(())
}

/// What [`check_fs_before_mount`] learned from `meta.json`.
struct MountCheck {
    /// Whether to prompt for a passphrase: `meta.json`'s `e2e`, already
    /// held against this machine's pin (see [`e2e_pin`]).
    e2e: bool,
    /// The S3 endpoint `meta.json` was read from (`None`: local backend).
    endpoint: Option<String>,
}

/// Read `meta.json` before the daemon forks, so a missing filesystem
/// fails here — on this terminal, explained by
/// [`backend::load_fs_explained`] (where the command looked, and whether
/// it reached the store the name was created on) — and so an interactive
/// E2E mount knows to prompt for the passphrase before the child loses
/// the terminal. A downgrade from, or change of, the E2E state this
/// machine pinned fails here too, before any prompt. The pin itself is
/// written by the daemon once the passphrase has opened the keyring. Runs
/// through a throwaway runtime that is fully dropped before the caller
/// forks, so no runtime threads leak into the daemon child.
fn check_fs_before_mount(
    s3: &str,
    registered_endpoint: Option<&str>,
    pin: &e2e_pin::PinTarget,
) -> Result<MountCheck> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let check = rt.block_on(async {
        let (backend, info) = backend::open_backend_described(s3).await?;
        let meta = backend::load_fs_explained(&backend, &info, registered_endpoint).await?;
        e2e_pin::check(pin, &meta)?;
        anyhow::Ok(MountCheck {
            e2e: meta.e2e,
            endpoint: info.endpoint().map(str::to_string),
        })
    })?;
    drop(rt);
    Ok(check)
}

/// Remember the endpoint a name's `meta.json` was read from (best effort:
/// diagnostics only, see `FsEntry::endpoint`).
fn remember_endpoint(name: &str, endpoint: &str) {
    let saved = registry::Registry::load_locked().and_then(|mut reg| {
        reg.merge_and_save(
            name,
            registry::FsOverrides {
                endpoint: Some(endpoint.to_string()),
                ..Default::default()
            },
        )
    });
    if let Err(e) = saved {
        tracing::debug!(name, error = %e, "recording the name's S3 endpoint failed");
    }
}

fn main() -> Result<()> {
    let log_buffer = log_buffer::LogBuffer::default();
    let log_writer = log_buffer.clone();
    // aws_config logs the loaded credentials (access key id included) at
    // INFO on every command: keep it at WARN even under an explicit
    // `RUST_LOG=info` or `debug`, unless RUST_LOG names aws_config itself.
    let rust_log = std::env::var("RUST_LOG").unwrap_or_default();
    let mut filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if !rust_log.contains("aws_config") {
        filter = filter.add_directive("aws_config=warn".parse().expect("static directive"));
    }
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(move || log_writer.writer())
        .init();
    let cli = Cli::parse();
    let threads = parallelism::thread_plan();
    tracing::info!(
        cpus = threads.cpus,
        fuse_threads = threads.fuse,
        tokio_threads = threads.tokio,
        blocking_threads = threads.blocking,
        "selected host-aware thread plan"
    );
    // `mount` builds its own tokio runtime *after* `daemonize::fork_if_needed`
    // decides whether to fork (plan 21, step 5): forking after a
    // multi-thread runtime's worker threads exist would hand the
    // daemonized child a corpse runtime (the calling thread survives
    // `fork()`, but its other worker threads do not), so every
    // `block_on`/`spawn` in the child would hang forever waiting for
    // work no thread is left to poll. Every other command is unaffected
    // by that constraint and shares one runtime built below as before.
    if let Command::Mount {
        target,
        mountpoint,
        s3,
        state_dir,
        foreground,
        cache_size,
        allow_other,
        fs_name,
        fsync_mode,
        cto,
        locks,
        write_mode,
        read_only_member,
        atime,
        rw,
        clone_name,
        ephemeral,
        confine_links,
        web_ui,
    } = cli.command
    {
        return cmd_mount(
            threads,
            MountArgs {
                target,
                mountpoint,
                s3,
                state_dir,
                foreground,
                cache_size,
                allow_other,
                fs_name,
                fsync_mode,
                cto,
                locks,
                write_mode,
                read_only_member,
                atime,
                rw,
                clone_name,
                ephemeral,
                confine_links,
                web_ui,
            },
            log_buffer,
        );
    }

    // Plan 31 C4b: the new image of an upgraded daemon (builds its own
    // runtime, like `mount`), and the preflight's probe.
    if let Command::Daemon {
        resume_from,
        handover_abi,
        ..
    } = &cli.command
    {
        if *handover_abi {
            println!("{}", handover::handover_abi());
            return Ok(());
        }
        if let Some(fd) = *resume_from {
            return handover::resume_main(fd, threads, log_buffer);
        }
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.tokio)
        .max_blocking_threads(threads.blocking)
        .enable_all()
        .build()?;

    match cli.command {
        Command::Fs {
            command:
                FsCommand::Create {
                    name,
                    s3,
                    chunk_size,
                    compression,
                    e2e,
                    max_size,
                    ack_policy,
                    epoch_slack,
                },
        } => {
            constellation_fs_core::validate_chunk_size(chunk_size)?;
            let ack_policy = crate::authority_driver::ack_policy_flag(ack_policy.as_deref())?;
            let setting: CompressionSetting =
                compression.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
            let (backend, backend_info) = rt
                .block_on(backend::open_backend_described(&s3))
                .context("opening backend")?;
            let store = ChunkStore::new(backend.clone());
            // Verify the backend supports every operation a filesystem
            // needs before writing (or prompting for) anything, so an
            // unusable backend fails fast with a clean report instead of
            // a raw protocol error mid-create.
            rt.block_on(preflight_backend(&store, &s3))?;
            let mut meta = FsMeta::new(chunk_size, &setting.to_string());
            meta.e2e = e2e;
            // E2E filesystems seed the gossip topic from the passphrase-
            // protected keyring, not plaintext `meta.json`; leaving it here
            // would let a bucket reader join the topic. Non-E2E keeps it in
            // `meta.json` (S3 is the trust boundary there anyway).
            if e2e {
                meta.gossip_secret = None;
            }
            meta.max_logical_bytes = max_size.filter(|&n| n > 0);
            meta.ack_policy = ack_policy;
            meta.epoch_slack = (epoch_slack > 0).then_some(epoch_slack);
            if epoch_slack > 0 {
                let ttl = crate::lease::lease_ttl_ms();
                constellation_store_s3::PromiseConfig::from_env(ttl)
                    .validate(ttl)
                    .map_err(|why| anyhow::anyhow!("--epoch-slack {epoch_slack}: {why}"))?;
            }
            // Collect the passphrase before writing anything: an abort at
            // the prompt (Ctrl-C, empty input) must leave no orphan
            // `meta.json` behind. The keyring block is built into `meta`,
            // so the whole E2E filesystem — identity and keys — is created
            // by a single conditional PUT with no orphan window.
            if e2e {
                let secret = passphrase("CONSTELLATION_PASSPHRASE", "New filesystem passphrase: ")?;
                meta.keyring = Some(
                    constellation_store_s3::create_keyring_block(&secret)
                        .context("creating E2E keyring")?,
                );
            }
            rt.block_on(store.create_fs(&meta))
                .context("creating filesystem")?;
            registry::Registry::load_locked()?
                .merge_and_save(
                    &name,
                    registry::FsOverrides {
                        s3: Some(s3.clone()),
                        endpoint: backend_info.endpoint().map(str::to_string),
                        ..Default::default()
                    },
                )
                .context("registering the new filesystem")?;
            e2e_pin::record(&e2e_pin::PinTarget::named(&name, &s3), &meta, None);
            println!("created filesystem {} at {s3}", meta.uuid);
            println!("  name:        {name}");
            println!("  chunk_size:  {chunk_size}");
            println!("  compression: {setting}");
            println!("  e2e:         {e2e}");
            if let Some(cap) = meta.max_logical_bytes {
                println!("  max_size:    {cap}");
            }
            if let Some(p) = &meta.ack_policy {
                println!("  ack_policy:  {p}");
            }
            if epoch_slack > 0 {
                println!("  epoch_slack: {epoch_slack}");
            }
            Ok(())
        }
        Command::Fs {
            command:
                FsCommand::Set {
                    setting: FsSetting::EpochSlack { target, slack, s3 },
                },
        } => {
            let reg = registry::Registry::load()?;
            let t = target::resolve(&target, &reg);
            let s3 = target::s3_url(s3, &t)?;
            let backend = rt
                .block_on(backend::open_backend(&s3))
                .context("opening backend")?;
            let store = ChunkStore::new(backend.clone());
            if slack > 0 {
                let ttl = crate::lease::lease_ttl_ms();
                constellation_store_s3::PromiseConfig::from_env(ttl)
                    .validate(ttl)
                    .map_err(|why| anyhow::anyhow!("epoch-slack {slack}: {why}"))?;
            }
            let roster = rt
                .block_on(constellation_store_s3::write_eligible_roster(
                    backend.clone(),
                ))
                .context("reading the write-eligible roster")?;
            use constellation_store_s3::heartbeat::{check_epoch_slack, SlackFit};
            match check_epoch_slack(slack, roster.len()) {
                // No node enrolled yet (set before the first mount): the
                // roster decides nothing (formation applies the quorum).
                _ if roster.is_empty() => {}
                SlackFit::Invalid => bail!(
                    "epoch-slack {slack} leaves no member out of a {}-node write-eligible roster",
                    roster.len()
                ),
                SlackFit::NoTtlFailover => eprintln!(
                    "warning: epoch-slack {slack} > N - 2 (N = {}): an S3 takeover needs {slack} \
                     other live nodes' promises, so one crashed holder blocks TTL failover \
                     until it returns (seal-based and ack=s3 failover are unaffected)",
                    roster.len()
                ),
                SlackFit::Ok => {}
            }
            let previous = rt
                .block_on(store.set_epoch_slack(slack))
                .context("updating meta.json")?;
            println!("epoch_slack: {previous} -> {slack}");
            if slack < previous {
                println!(
                    "  nodes still on {previous} keep advertising it until they adopt {slack}; \
                     takeovers honour the largest advertised meanwhile"
                );
            }
            Ok(())
        }
        Command::Fs {
            command: FsCommand::Passwd { target, s3 },
        } => {
            let reg = registry::Registry::load()?;
            let t = target::resolve(&target, &reg);
            let s3 = target::s3_url(s3, &t)?;
            let backend = rt
                .block_on(backend::open_backend(&s3))
                .context("opening backend")?;
            let store = ChunkStore::new(backend.clone());
            let meta = rt.block_on(store.load_fs())?;
            let pin = e2e_pin::PinTarget::for_target(&t, &s3);
            e2e_pin::check(&pin, &meta)?;
            if !meta.e2e {
                bail!("filesystem is not in E2E mode");
            }
            let old = passphrase("CONSTELLATION_PASSPHRASE", "Current passphrase: ")?;
            let new = passphrase(
                "CONSTELLATION_NEW_PASSPHRASE",
                "New filesystem passphrase: ",
            )?;
            rt.block_on(store.change_passphrase(&old, &new))
                .context("changing E2E passphrase")?;
            // The rewrap kept the master, so the pin's fingerprint stands;
            // record it anyway once the new passphrase has opened the block
            // (a `fs create` pin has none yet).
            let rewrapped = rt.block_on(store.load_fs())?;
            match rewrapped.unlock(&new) {
                Ok(keys) => e2e_pin::record(&pin, &rewrapped, Some(&keys)),
                Err(e) => eprintln!(
                    "warning: meta.json read back after the change does not open with the new \
                     passphrase ({e}); the local E2E pin was not updated"
                ),
            }
            println!(
                "passphrase changed; data-encryption keys were not rotated \
                 and mounted nodes need no remount"
            );
            Ok(())
        }
        Command::Fs {
            command: FsCommand::List,
        } => cmd_fs_list(&rt),
        Command::ZombieReaper { parent, state_dir } => daemon_lock::reaper_main(parent, &state_dir),
        Command::Daemon {
            target,
            state_dir,
            upgrade,
            binary,
            timeout_s,
            ..
        } => {
            if !upgrade {
                bail!("nothing to do: `constellation daemon --upgrade <TARGET>`");
            }
            let dir = match (target, state_dir) {
                (Some(target), state_dir) => resolve_target(&target, state_dir)?.1,
                (None, Some(dir)) => dir,
                (None, None) => bail!("TARGET (a registered filesystem name) or --state-dir"),
            };
            rt.block_on(cmd_daemon_upgrade(
                &dir,
                binary,
                std::time::Duration::from_secs(timeout_s),
            ))
        }
        Command::Doctor { target, s3 } => {
            let reg = registry::Registry::load()?;
            let t = target::resolve(&target, &reg);
            let s3 = target::s3_url(s3, &t)?;
            let (backend, info) = rt
                .block_on(backend::open_backend_described(&s3))
                .context("opening backend")?;
            println!("backend ........................... {info}");
            let store = ChunkStore::new(backend);
            let caps = rt.block_on(store.probe_conditional_writes())?;
            let yn = |b: bool| if b { "ok" } else { "MISSING" };
            println!(
                "create-if-absent (If-None-Match) ... {}",
                yn(caps.create_if_absent)
            );
            println!("etag CAS (If-Match) ............... {}", yn(caps.etag_cas));
            if !caps.create_if_absent {
                bail!("backend lacks create-if-absent; unusable as a constellation backend");
            }
            if !caps.etag_cas {
                println!(
                    "note: etag CAS unavailable; single-node mounts work (leases degrade to \
                     create-only, single-writer assumed), but lease renew/takeover — and \
                     therefore multi-node mounts — need If-Match"
                );
            }
            // Plan 30 §M4 items 1 and 4: what each CAS edge answers, and
            // bucket versioning.
            let report = rt.block_on(constellation_store_s3::probe_cas_semantics(store.inner()))?;
            doctor::print_cas_report(&report)?;
            print!("filesystem at prefix .............. ");
            // `HEAD` first: a `GET` of a `meta.json` that does not exist
            // yet is the answer a store caching negative reads would keep
            // serving to the `mount` after `fs create`.
            match rt.block_on(store.fs_exists()) {
                Ok(false) => println!(
                    "none — {} (run `constellation fs create`)",
                    rt.block_on(store.locate_missing_fs())
                ),
                Ok(true) => match rt.block_on(store.load_fs()) {
                    Ok(meta) => println!("ok ({}, format v{})", meta.uuid, meta.format_version),
                    Err(e) => bail!(e),
                },
                Err(e) => bail!(e),
            }
            Ok(())
        }
        Command::Status {
            target,
            s3,
            state_dir,
        } => {
            if let Some(s3) = s3 {
                let (backend, info) = rt
                    .block_on(backend::open_backend_described(&s3))
                    .context("opening backend")?;
                let meta = rt.block_on(backend::load_fs_explained(&backend, &info, None))?;
                println!("{}", serde_json::to_string_pretty(&meta)?);
                return Ok(());
            }
            let target =
                target.context("TARGET (a registered filesystem name) or --s3 is required")?;
            let (_, dir) = resolve_target(&target, state_dir)?;
            // Bounded: a daemon the kernel has killed but that still holds
            // its listener (campaign 6 B-1) must not park `status` forever.
            let s = rt.block_on(control::call_bounded::<cm::NodeStatus>(
                &dir,
                Default::default(),
                daemon_lock::control_timeout(),
            ))?;
            print_json(&s)
        }
        Command::Pin { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::PinAdd>(&rt, &dir, api::PathParams { path }, print_ack)
        }
        Command::Unpin { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::PinRemove>(&rt, &dir, api::PathParams { path }, print_ack)
        }
        Command::Pins { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::PinList>(&rt, &dir, Default::default(), |l| {
                print_list(&l.pins, "no pinned subtrees")
            })
        }
        Command::Offline {
            target,
            state_dir,
            ro,
        } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::DesignationOffline>(
                &rt,
                &dir,
                api::OfflineParams {
                    path,
                    read_only: ro,
                },
                print_ack,
            )
        }
        Command::Online { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::DesignationOnline>(&rt, &dir, api::PathParams { path }, print_ack)
        }
        Command::Designations { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::DesignationList>(&rt, &dir, Default::default(), |l| {
                print_list(&l.designations, "no active designations")
            })
        }
        Command::Delegate {
            target,
            state_dir,
            to,
            range,
        } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::DesignationDelegate>(
                &rt,
                &dir,
                api::DelegateParams {
                    path,
                    node: to,
                    range,
                },
                print_ack,
            )
        }
        Command::Undelegate { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::DesignationUndelegate>(&rt, &dir, api::PathParams { path }, print_ack)
        }
        Command::Delegations { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::DesignationListDelegations>(&rt, &dir, Default::default(), |l| {
                print_json(&l.delegations)
            })
        }
        Command::Reintegrate { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::NodeReintegrate>(&rt, &dir, Default::default(), print_ack)
        }
        Command::Repair {
            command:
                RepairCommand::DropHeld {
                    target,
                    ino,
                    remote,
                    state_dir,
                },
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::LocksDropHeld>(&rt, &dir, api::DropHeldParams { ino, remote }, print_ack)
        }
        Command::Leave {
            target,
            state_dir,
            node_id,
            force,
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::NodeLeave>(&rt, &dir, api::LeaveParams { node_id, force }, print_ack)
        }
        Command::WriteMode {
            target,
            mode,
            state_dir,
        } => {
            let mode: writeback::WriteMode = mode.parse().map_err(anyhow::Error::msg)?;
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::NodeSetWriteMode>(
                &rt,
                &dir,
                api::SetWriteModeParams {
                    mode: mode.as_str().into(),
                },
                print_ack,
            )
        }
        Command::Quota { command } => match command {
            QuotaCommand::Get { target, state_dir } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                ctl::<cm::QuotaGet>(&rt, &dir, Default::default(), print_quota)
            }
            QuotaCommand::Set {
                target,
                size,
                state_dir,
            } => {
                let max_bytes = parse_quota_arg(&size)?;
                let (_, dir) = resolve_target(&target, state_dir)?;
                ctl::<cm::QuotaSet>(&rt, &dir, api::SetQuotaParams { max_bytes }, |_| {
                    match max_bytes {
                        Some(cap) => println!("quota set to {cap} bytes"),
                        None => println!("quota cleared (unlimited)"),
                    }
                    Ok(())
                })
            }
        },
        Command::Prune { command } => run_prune_command(&rt, command),
        Command::Inspect { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            ctl::<cm::BrowseInspect>(&rt, &dir, api::PathParams { path }, |e| print_json(&e))
        }
        Command::Cache { command } => match command {
            CacheCommand::Ls { target, state_dir } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                ctl::<cm::CacheList>(&rt, &dir, Default::default(), |l| print_json(&l.entries))
            }
            CacheCommand::Stat { target, state_dir } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                ctl::<cm::NodeStatus>(&rt, &dir, Default::default(), |s| print_json(&s))
            }
            CacheCommand::Prune {
                target,
                state_dir,
                target_bytes,
            } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                ctl::<cm::CachePrune>(&rt, &dir, api::CachePruneParams { target_bytes }, |r| {
                    println!("{}", r.detail);
                    Ok(())
                })
            }
        },
        Command::Log {
            command:
                LogCommand::Tail {
                    target,
                    state_dir,
                    lines,
                },
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(async {
                use std::io::Write;
                let client = control::connect(&dir).await?;
                let mut chunks = client
                    .call_chunks::<cm::NodeLogsTail>(api::LogTailParams {
                        lines,
                        follow: false,
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e.message))?;
                let mut out = std::io::stdout().lock();
                while let Some(chunk) = futures::StreamExt::next(&mut chunks).await {
                    out.write_all(&chunk.map_err(|e| anyhow::anyhow!("{}", e.message))?)?;
                }
                out.flush()?;
                Ok(())
            })
        }
        Command::Snapshot { command } => match command {
            SnapshotCommand::Create { target, state_dir } => {
                let (t, dir) = resolve_target(&target, state_dir)?;
                let selector = match &t {
                    target::Target::Named { path: Some(p), .. } => p.clone(),
                    target::Target::Named { path: None, .. } => {
                        bail!("snapshot create needs a path/selector: myfs:/path@name")
                    }
                    target::Target::Raw(raw) => raw.clone(),
                };
                ctl::<cm::SnapshotCreate>(
                    &rt,
                    &dir,
                    api::SnapshotCreateParams {
                        selector,
                        hold: None,
                    },
                    |c| {
                        println!("{}", c.detail);
                        Ok(())
                    },
                )
            }
            SnapshotCommand::Ls { target, state_dir } => {
                let (t, dir) = resolve_target(&target, state_dir)?;
                let path = match &t {
                    target::Target::Named { path, .. } => path.clone(),
                    target::Target::Raw(raw) => Some(raw.clone()),
                };
                ctl::<cm::SnapshotList>(&rt, &dir, api::SnapshotListParams { path }, |l| {
                    print_json(&l.snapshots)
                })
            }
            SnapshotCommand::Delete { target, state_dir } => {
                let (t, dir) = resolve_target(&target, state_dir)?;
                let selector = match &t {
                    target::Target::Named { path: Some(p), .. } => p.clone(),
                    target::Target::Named { path: None, .. } => {
                        bail!("snapshot delete needs a path/selector: myfs:/path@name")
                    }
                    target::Target::Raw(raw) => raw.clone(),
                };
                ctl::<cm::SnapshotDelete>(
                    &rt,
                    &dir,
                    api::SnapshotDeleteParams { selector },
                    print_ack,
                )
            }
        },
        Command::Clone {
            target,
            destination,
            state_dir,
        } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let selector = match &t {
                target::Target::Named { path: Some(p), .. } => p.clone(),
                target::Target::Named { path: None, .. } => {
                    bail!("clone needs a path/selector: myfs:/path@name")
                }
                target::Target::Raw(raw) => raw.clone(),
            };
            ctl::<cm::CloneCreate>(
                &rt,
                &dir,
                api::CloneParams {
                    selector,
                    destination,
                },
                print_ack,
            )
        }
        Command::Debug {
            command:
                DebugCommand::SnapRefs {
                    target,
                    id,
                    state_dir,
                },
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::SnapshotRefs>(&rt, &dir, api::SnapRefsParams { id }, |r| {
                for hash in r.hashes {
                    println!("{hash}");
                }
                Ok(())
            })
        }
        Command::Gc { command } => {
            let (target, s3, state_dir, verify_only) = match command {
                GcCommand::Run {
                    target,
                    s3,
                    state_dir,
                } => (target, s3, state_dir, false),
                GcCommand::Verify {
                    target,
                    s3,
                    state_dir,
                } => (target, s3, state_dir, true),
            };
            let reg = registry::Registry::load()?;
            let t = target::resolve(&target, &reg);
            let s3 = target::s3_url(s3, &t)?;
            let dir = target::state_dir_opt(state_dir, &t);
            let pin = e2e_pin::PinTarget::for_target(&t, &s3);
            let report = rt.block_on(run_gc_cli(&s3, &pin, dir, verify_only))?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::Fsck {
            target,
            s3,
            state_dir,
            repair,
            force_release,
        } => {
            let reg = registry::Registry::load()?;
            let t = target::resolve(&target, &reg);
            let s3 = target::s3_url(s3, &t)?;
            let dir = target::state_dir_opt(state_dir, &t);
            let pin = e2e_pin::PinTarget::for_target(&t, &s3);
            let report = rt.block_on(run_fsck_cli(
                &s3,
                &pin,
                dir,
                repair,
                force_release.as_deref(),
            ))?;
            let code = report.exit_code();
            println!("{}", serde_json::to_string_pretty(&report)?);
            if code != 0 {
                std::process::exit(code);
            }
            Ok(())
        }
        Command::Mount { .. } => unreachable!(
            "Command::Mount is handled earlier in main(), before the shared runtime is built"
        ),
        Command::Umount { target, state_dir } => rt.block_on(cmd_umount(target, state_dir)),
        Command::Export { name, force } => rt.block_on(cmd_export(name, force)),
    }
}

/// Raw `Command::Mount` fields, bundled so `cmd_mount` stays under
/// clippy's too-many-arguments threshold. Every `Option`-typed field
/// (besides `mountpoint`/`state_dir`, which are inherently optional) is
/// `None` when the flag was not given on this invocation — that is what
/// lets `merge_and_save` implement "any explicit argument overwrites the
/// stored value, an absent one leaves it alone."
struct MountArgs {
    target: String,
    mountpoint: Option<PathBuf>,
    s3: Option<String>,
    state_dir: Option<PathBuf>,
    foreground: bool,
    cache_size: Option<u64>,
    allow_other: bool,
    fs_name: Option<String>,
    fsync_mode: Option<String>,
    cto: Option<String>,
    locks: Option<String>,
    write_mode: Option<String>,
    read_only_member: bool,
    atime: Option<String>,
    rw: bool,
    clone_name: Option<String>,
    ephemeral: bool,
    confine_links: bool,
    web_ui: Option<u16>,
}

/// One view this invocation of `mount` needs to bring up.
struct ViewSpec {
    /// Registry convention: `""` is root.
    subtree: String,
    mountpoint: PathBuf,
    allow_other: bool,
    fs_name: String,
    rw: bool,
    clone_name: Option<String>,
    ephemeral: bool,
    confine_links: bool,
}

impl ViewSpec {
    fn from_entry(e: &registry::MountEntry) -> Self {
        Self {
            subtree: e.subtree.clone(),
            mountpoint: e.mountpoint.clone(),
            allow_other: e.allow_other,
            fs_name: if e.fs_name.is_empty() {
                "constellation".to_string()
            } else {
                e.fs_name.clone()
            },
            rw: e.rw,
            clone_name: e.clone_name.clone(),
            ephemeral: e.ephemeral,
            confine_links: e.confine_links,
        }
    }

    /// `""` (root) in registry convention is `"/"` in `ViewConfig`'s
    /// `inner_path` convention — the one place these two spellings meet.
    fn inner_path(&self) -> String {
        if self.subtree.is_empty() {
            "/".to_string()
        } else {
            self.subtree.clone()
        }
    }

    fn view_config(&self, fuse_threads: usize) -> node_runtime::ViewConfig {
        node_runtime::ViewConfig {
            inner_path: self.inner_path(),
            mountpoint: self.mountpoint.clone(),
            allow_other: self.allow_other,
            fs_name: self.fs_name.clone(),
            fuse_threads,
            rw_snapshot: self.rw,
            clone_name: self.clone_name.clone(),
            ephemeral: self.ephemeral,
            confine_links: self.confine_links,
            labels: Default::default(),
            qos: Default::default(),
        }
    }

    /// `view.mount`'s parameters for attaching this view to a running
    /// daemon.
    fn mount_params(&self) -> api::ViewMountParams {
        api::ViewMountParams {
            subtree: self.inner_path(),
            source: api::MountSource::Path {
                mountpoint: self.mountpoint.clone(),
                opts: api::MountViewOpts {
                    allow_other: self.allow_other,
                    fs_name: Some(self.fs_name.clone()),
                    fuse_threads: None,
                    rw: self.rw,
                    clone_name: self.clone_name.clone(),
                    ephemeral: self.ephemeral,
                },
            },
            labels: Default::default(),
            qos: Default::default(),
            confine_links: self.confine_links,
        }
    }
}

/// `Command::Mount`'s dispatch (plan 21, step 4): resolve the target and
/// merge any explicit flags into the registry, fork unless
/// `--foreground`, take the per-name `daemon.lock`, then either attach
/// to an already-running daemon (`MountAdd`) or become the daemon
/// (`NodeRuntime::start` + `add_mount`).
fn cmd_mount(
    threads: parallelism::ThreadPlan,
    args: MountArgs,
    log_buffer: log_buffer::LogBuffer,
) -> Result<()> {
    let MountArgs {
        target,
        mountpoint,
        s3,
        state_dir,
        foreground,
        cache_size,
        allow_other,
        fs_name,
        fsync_mode,
        cto,
        locks,
        write_mode,
        read_only_member,
        atime,
        rw,
        clone_name,
        ephemeral,
        confine_links,
        web_ui,
    } = args;
    let cto_strict = crate::cto::strict_from(cto.as_deref())?;
    // Plan 30 §M14: refuse an explicit `--locks cluster` with P2P turned
    // off here, before forking (the daemon re-checks against the endpoint
    // it actually started).
    let locks = crate::locks::cluster_flag(locks.as_deref())?;
    crate::locks::cluster_effective(locks, constellation_net::enabled())?;
    // Resolve once: env CONSTELLATION_ATIME overrides the --atime flag.
    let atime_mode =
        crate::atime::AtimeMode::resolve(atime.as_deref().and_then(crate::atime::AtimeMode::parse));
    if let Some(raw) = &atime {
        if crate::atime::AtimeMode::parse(raw).is_none() {
            bail!("invalid --atime {raw:?} (expected off, relatime, or lazy)");
        }
    }
    // Fail fast and clearly on an unusable `--allow-other`, before we
    // touch the registry (so a doomed attempt never persists the option)
    // and before the daemon forks (so the error reaches this terminal
    // instead of dying as "daemon exited before reporting status").
    ensure_allow_other_supported(allow_other)?;

    let mut reg = registry::Registry::load_locked()?;
    let mut resolved = target::resolve(&target, &reg);
    // `mount` is the one command allowed to *create* a name: a target
    // that is syntactically name-shaped but not yet registered (first
    // `mount myfs --s3 ...` for that name) must not fall into the
    // ad-hoc/`--state-dir`-required path just because `resolve` only
    // returns `Named` for names that already exist. An explicit
    // `--state-dir` still takes the ad-hoc escape hatch, matching
    // `target::state_dir`'s own precedence (explicit wins).
    if state_dir.is_none() {
        if let target::Target::Raw(raw) = &resolved {
            if let Some((name, path)) = target::split_name(raw) {
                resolved = target::Target::Named {
                    name,
                    path,
                    entry: registry::FsEntry::default(),
                };
            }
        }
    }

    // Build the plan: which views to bring up, the node-level config,
    // and (for a named target) the registry writes to make first.
    let mut registered_endpoint: Option<String> = None;
    let (name, state_dir, node_s3, node_cache_size, node_fsync_mode, node_write_mode, views): (
        Option<String>,
        PathBuf,
        String,
        u64,
        String,
        String,
        Vec<ViewSpec>,
    ) = match &resolved {
        target::Target::Named {
            name,
            path,
            entry: current,
        } => {
            // Identity is pinned to the name: refuse an --s3 that
            // contradicts an already-populated state dir.
            if let Some(new_s3) = &s3 {
                if !current.s3.is_empty()
                    && current.s3 != *new_s3
                    && current.state_dir.join("meta.db").exists()
                {
                    bail!(
                        "{name} already points at {:?}; `--s3 {new_s3:?}` would repoint an \
                         existing state dir. Run `constellation export {name}` first if you \
                         really want to re-point it.",
                        current.s3
                    );
                }
            }
            let mount_override = (mountpoint.is_some() || path.is_some()).then(|| {
                let subtree = path.clone().unwrap_or_default();
                let stored = current.mount(&subtree);
                registry::MountEntry {
                    subtree: subtree.clone(),
                    mountpoint: mountpoint.clone().unwrap_or_else(|| {
                        stored.map(|m| m.mountpoint.clone()).unwrap_or_default()
                    }),
                    // Authoritative from this command line, not sticky:
                    // an explicit `mount NAME MOUNTPOINT` redefines the
                    // view, so omitting `--allow-other` turns it back off.
                    // (A bare `mount NAME` keeps the stored views as-is.)
                    allow_other,
                    fs_name: fs_name
                        .clone()
                        .or_else(|| stored.map(|m| m.fs_name.clone()))
                        .unwrap_or_else(|| name.clone()),
                    rw: rw || stored.is_some_and(|m| m.rw),
                    clone_name: clone_name
                        .clone()
                        .or_else(|| stored.and_then(|m| m.clone_name.clone())),
                    ephemeral: ephemeral || stored.is_some_and(|m| m.ephemeral),
                    // Like `allow_other`: an access policy this command
                    // line states (a bare `mount NAME` keeps it as stored).
                    confine_links,
                }
            });
            if let Some(view) = &mount_override {
                if view.mountpoint.as_os_str().is_empty() {
                    bail!(
                        "{name}{}: no stored mountpoint on record; pass MOUNTPOINT",
                        path.as_deref().map(|p| format!(":{p}")).unwrap_or_default()
                    );
                }
            }
            let entry = reg.merge_and_save(
                name,
                registry::FsOverrides {
                    s3: s3.clone(),
                    cache_size: cache_size.map(|b| format!("{b}")),
                    cache_dir: None,
                    fsync_mode: fsync_mode.clone(),
                    write_mode: write_mode.clone(),
                    read_only_member: read_only_member.then_some(true),
                    web_ui,
                    // A repointed name forgets the old URL's endpoint.
                    endpoint: s3
                        .as_ref()
                        .filter(|new| **new != current.s3)
                        .map(|_| String::new()),
                    mount: mount_override,
                },
            )?;
            registered_endpoint = (!entry.endpoint.is_empty()).then(|| entry.endpoint.clone());
            let views = if path.is_none() && mountpoint.is_none() {
                // Bare `mount NAME`: every registered view.
                if entry.mounts.is_empty() {
                    bail!(
                        "no views registered for {name}; run `mount {name} MOUNTPOINT` to add one"
                    );
                }
                entry.mounts.iter().map(ViewSpec::from_entry).collect()
            } else {
                let subtree = path.clone().unwrap_or_default();
                let m = entry
                    .mount(&subtree)
                    .unwrap_or_else(|| panic!("just-saved view {subtree:?} missing"));
                vec![ViewSpec::from_entry(m)]
            };
            (
                Some(name.clone()),
                entry.state_dir.clone(),
                entry.s3.clone(),
                parse_byte_size(&entry.cache_size).unwrap_or(10 * 1024 * 1024 * 1024),
                if entry.fsync_mode.is_empty() {
                    "local".to_string()
                } else {
                    entry.fsync_mode.clone()
                },
                if entry.write_mode.is_empty() {
                    "through".to_string()
                } else {
                    entry.write_mode.clone()
                },
                views,
            )
        }
        target::Target::Raw(raw) => {
            // Ad-hoc, unregistered mount: never touches the registry.
            let s3 = s3.context("--s3 is required for an unregistered mount")?;
            let mountpoint = mountpoint.context("MOUNTPOINT is required")?;
            let dir = state_dir
                .clone()
                .context("--state-dir is required for an unregistered mount")?;
            let view = ViewSpec {
                subtree: raw.clone(),
                mountpoint,
                allow_other,
                fs_name: fs_name.unwrap_or_else(|| "constellation".to_string()),
                rw,
                clone_name,
                ephemeral,
                confine_links,
            };
            (
                None,
                dir,
                s3,
                cache_size.unwrap_or(10 * 1024 * 1024 * 1024),
                fsync_mode.unwrap_or_else(|| "local".to_string()),
                write_mode.unwrap_or_else(|| "through".to_string()),
                vec![view],
            )
        }
    };
    drop(reg); // release the registry's own advisory lock before forking

    // A view may enable allow_other from the registry rather than this
    // command line; validate the effective value too.
    for view in &views {
        ensure_allow_other_supported(view.allow_other)?;
    }

    let fsync_s3 = match node_fsync_mode.as_str() {
        "local" => false,
        "s3" => true,
        other => bail!("invalid fsync_mode {other:?} (expected local or s3)"),
    };
    let initial_write_mode: writeback::WriteMode =
        node_write_mode.parse().map_err(anyhow::Error::msg)?;
    let pin_target = match &name {
        Some(name) => e2e_pin::PinTarget::named(name, &node_s3),
        None => e2e_pin::PinTarget::unnamed(&node_s3),
    };

    // Read meta.json before the fork (a missing filesystem fails on this
    // terminal, explained), and collect the E2E passphrase in the
    // FOREGROUND: the daemon child is `setsid()`'d away from its
    // controlling terminal and cannot prompt. `fork()` inherits the secret
    // in memory. The prompt is skipped in `--foreground` (the body keeps
    // the terminal); both are skipped when a daemon already serves this
    // state dir (we will attach, not unlock).
    let mount_passphrase = if daemon_socket_is_live(&state_dir) {
        None
    } else {
        let check = check_fs_before_mount(&node_s3, registered_endpoint.as_deref(), &pin_target)?;
        if let (Some(name), Some(endpoint)) = (&name, &check.endpoint) {
            if registered_endpoint.as_deref() != Some(endpoint.as_str()) {
                remember_endpoint(name, endpoint);
            }
        }
        if foreground || !check.e2e || std::env::var_os("CONSTELLATION_PASSPHRASE").is_some() {
            None
        } else {
            Some(passphrase(
                "CONSTELLATION_PASSPHRASE",
                "Filesystem passphrase: ",
            )?)
        }
    };

    match daemonize::fork_if_needed(foreground, &state_dir)? {
        daemonize::Outcome::Foreground => cmd_mount_body(
            threads,
            &state_dir,
            node_s3,
            node_cache_size,
            fsync_s3,
            cto_strict,
            locks,
            initial_write_mode,
            read_only_member,
            atime_mode,
            web_ui.unwrap_or(0),
            log_buffer,
            views,
            mount_passphrase,
            pin_target,
            None,
        ),
        daemonize::Outcome::Daemon(verdict) => {
            let result = cmd_mount_body(
                threads,
                &state_dir,
                node_s3,
                node_cache_size,
                fsync_s3,
                cto_strict,
                locks,
                initial_write_mode,
                read_only_member,
                atime_mode,
                web_ui.unwrap_or(0),
                log_buffer,
                views,
                mount_passphrase,
                pin_target,
                Some(verdict),
            );
            // `cmd_mount_body` already reported success/failure through
            // the verdict before it could reach this point in the
            // "became the daemon" case (it blocks serving after that);
            // reaching here means either the attach path (verdict
            // already sent) or an error before the verdict was sent.
            result
        }
    }
}

/// Take the exclusive `daemon.lock` for `state_dir` and decide which of
/// the two step-4 paths applies: attach to a live daemon, or become one.
enum LockOutcome {
    /// No daemon is running for this state dir; the lock is now held for
    /// this process's lifetime (leaked intentionally — released only by
    /// process exit, which is what "for the daemon's whole lifetime"
    /// means. Any `control.sock`/`daemon.pid` found here was stale.
    BecomeDaemon,
    /// A daemon already owns this state dir; talk to it.
    Attach,
}

fn take_state_dir_lock(state_dir: &Path) -> Result<LockOutcome> {
    std::fs::create_dir_all(state_dir)
        .with_context(|| format!("creating {}", state_dir.display()))?;
    let lock_file =
        constellation_platform::lock::open_lock_file(&state_dir.join(daemon_lock::LOCK_NAME))
            .context("opening daemon.lock")?;
    match constellation_platform::native()
        .file_lock
        .try_lock(lock_file)
        .context("locking daemon.lock")?
    {
        Some(guard) => {
            // We hold it now. Leak the guard so the lock survives for the
            // rest of this process's life (released automatically on
            // exit, by the kernel closing every fd) rather than dropping
            // here.
            // Plan 31 C4b: an in-place upgrade hands this very lock on.
            handover::set_lock_fd(std::os::fd::AsRawFd::as_raw_fd(guard.file()));
            std::mem::forget(guard);
            constellation_control::transport::forget_socket(state_dir);
            let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
            Ok(LockOutcome::BecomeDaemon)
        }
        None => Ok(LockOutcome::Attach),
    }
}

/// Everything after daemonization has been decided: take the state-dir
/// lock, then either attach to a live daemon (`MountAdd` per view, all
/// or nothing) or become the daemon (`NodeRuntime::start` + `add_mount`
/// per view, rolling back on any failure) and report the verdict.
#[allow(clippy::too_many_arguments)]
fn cmd_mount_body(
    threads: parallelism::ThreadPlan,
    state_dir: &Path,
    s3: String,
    cache_size: u64,
    fsync_s3: bool,
    cto_strict: bool,
    locks: Option<bool>,
    initial_write_mode: writeback::WriteMode,
    read_only_member: bool,
    atime_mode: crate::atime::AtimeMode,
    web_ui: u16,
    log_buffer: log_buffer::LogBuffer,
    views: Vec<ViewSpec>,
    passphrase: Option<Zeroizing<String>>,
    pin_target: e2e_pin::PinTarget,
    verdict: Option<daemonize::Verdict>,
) -> Result<()> {
    let fuse_threads = threads.fuse;
    // Built here, strictly after `daemonize::fork_if_needed` has already
    // decided whether (and forked if so) — never before it. See the
    // comment at this function's only two call sites in `cmd_mount`.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.tokio)
        .max_blocking_threads(threads.blocking)
        .enable_all()
        .build()?;
    // A daemon that is draining before exit still holds `daemon.lock` and
    // still serves `control.sock`, so we would otherwise `Attach` and add
    // a view onto a process about to exit (which orphans the mount). If
    // the attach is refused for that reason, wait for the old daemon to
    // release the lock, then loop: the next `take_state_dir_lock` will
    // return `BecomeDaemon`.
    //
    // Campaign 6 B-1: the lock holder may be a daemon the kernel has
    // already killed whose last thread is stuck in the kernel; it keeps
    // its lock and its listener, and never answers. Every wait here is
    // bounded (`daemon_lock::attach_timeout`), the holder is pinged
    // before anything is asked of it, and a holder the kernel says can
    // never run again is taken over (`daemon_lock::take_over`); a holder
    // that is alive but mute makes this mount fail, explained, within
    // the bound.
    startup::phase("taking daemon.lock");
    let attach_started = std::time::Instant::now();
    let attach_deadline = daemon_lock::attach_timeout();
    let mut took_over = false;
    let fail = |verdict: Option<daemonize::Verdict>, msg: String| -> Result<()> {
        if let Some(v) = verdict {
            v.failure(&msg)?;
            std::process::exit(1);
        }
        bail!(msg)
    };
    let lock_outcome = loop {
        match take_state_dir_lock(state_dir)? {
            LockOutcome::Attach => {
                startup::phase("probing the daemon that holds daemon.lock");
                let probe = rt.block_on(control::ping(state_dir, daemon_lock::control_timeout()));
                match probe {
                    Ok(true) => {}
                    other => {
                        let holder = daemon_lock::holder_for_takeover(state_dir);
                        match (&other, &holder) {
                            (_, daemon_lock::Holder::Wedged { pid, why, .. }) if !took_over => {
                                tracing::warn!(
                                    pid,
                                    why,
                                    "daemon.lock is held by a daemon the kernel has killed; \
                                     taking the state dir over"
                                );
                                eprintln!(
                                    "previous daemon (pid {pid}) is dead but still holds this \
                                     state dir: {why}; taking over"
                                );
                                daemon_lock::take_over(state_dir, *pid)?;
                                took_over = true;
                                continue;
                            }
                            _ => {}
                        }
                        let detail = match &other {
                            Ok(_) => "its control socket is not up".to_string(),
                            Err(e) => format!("{e:#}"),
                        };
                        if attach_started.elapsed() >= attach_deadline {
                            return fail(
                                verdict,
                                format!(
                                    "another daemon holds {}/daemon.lock but could not be attached \
                                     to within {attach_deadline:?}: {detail}; {holder}. Not taking \
                                     its state dir over while it can still run: if it is stuck, \
                                     kill it and retry; if it then lingers as a zombie, its last \
                                     thread is stuck in the kernel (abort its FUSE connection under \
                                     /sys/fs/fuse/connections/*/abort) and the next mount takes over",
                                    state_dir.display()
                                ),
                            );
                        }
                        tracing::info!(
                            %holder,
                            detail,
                            waited_s = attach_started.elapsed().as_secs(),
                            "daemon.lock is held; retrying the attach"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        continue;
                    }
                }
                startup::phase("attaching views to the running daemon");
                let remaining = attach_deadline.saturating_sub(attach_started.elapsed());
                match rt.block_on(attach_views(state_dir, &views, remaining))? {
                    AttachOutcome::Attached => {
                        for view in &views {
                            println!("mounted {} at {}", view.subtree, view.mountpoint.display());
                        }
                        startup::done("attached");
                        if let Some(v) = verdict {
                            v.success_attached()?;
                        }
                        return Ok(());
                    }
                    AttachOutcome::DaemonUpgrading => {
                        // Views already attached stay; `MountAdd` of one
                        // already mounted is refused below the next time,
                        // so retry only while the deadline allows.
                        if attach_started.elapsed() >= attach_deadline {
                            return fail(
                                verdict,
                                "the daemon was being upgraded for the whole attach timeout"
                                    .to_string(),
                            );
                        }
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        continue;
                    }
                    AttachOutcome::DaemonShuttingDown => {
                        eprintln!(
                            "existing daemon for this mount is shutting down (draining); \
                         waiting for it to exit before taking over…"
                        );
                        // Wait for the old daemon to release the lock / remove
                        // its socket. Bounded so a genuinely wedged daemon
                        // surfaces an error rather than spinning forever.
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(600);
                        while control::socket_exists(state_dir)
                            && std::time::Instant::now() < deadline
                        {
                            std::thread::sleep(std::time::Duration::from_millis(200));
                        }
                        if control::socket_exists(state_dir) {
                            return fail(
                                verdict,
                                "existing daemon is still shutting down after 600s; \
                             not taking over (retry once it has exited)"
                                    .to_string(),
                            );
                        }
                        // Old daemon is gone: loop and re-take the lock, which
                        // now yields `BecomeDaemon`.
                        continue;
                    }
                }
            }
            LockOutcome::BecomeDaemon => break LockOutcome::BecomeDaemon,
        }
    };
    match lock_outcome {
        LockOutcome::Attach => unreachable!("attach handled in the loop above"),
        LockOutcome::BecomeDaemon => {
            tracing::info!(
                state_dir = %state_dir.display(),
                took_over,
                "daemon.lock taken: this process becomes the daemon"
            );
            // Whatever the previous daemon of this state dir left
            // mounted serves nothing any more; a mount it left wedged
            // (campaign 6 B-1) would hang the stale-mountpoint check
            // below and keep its zombie alive until aborted.
            for stale in daemon_lock::abort_stale_mounts(state_dir) {
                tracing::warn!(
                    %stale,
                    "a mount of the previous daemon of this state dir was left behind"
                );
                if stale.connection.is_some() {
                    eprintln!("previous daemon's mount left behind: {stale}");
                }
            }
            // And should this daemon end the same way, its reaper does
            // the abort within seconds instead of at the next mount.
            match daemon_lock::spawn_reaper(state_dir) {
                Ok(pid) => {
                    handover::set_reaper_pid(pid);
                    tracing::info!(reaper_pid = pid, "zombie reaper started")
                }
                Err(e) => tracing::warn!(error = %e, "the zombie reaper could not be started"),
            }
            startup::phase("starting the node runtime");
            let handle = rt.handle().clone();
            let node = match node_runtime::NodeRuntime::start(
                node_runtime::NodeConfig {
                    fs_id: constellation_engine::FsId::new(state_dir.display().to_string()),
                    engine: constellation_engine::EngineConfig {
                        state_dir: Some(state_dir.to_path_buf()),
                        cache_size,
                        fsync_s3,
                        cto_strict,
                        locks,
                        initial_write_mode,
                        read_only_member,
                        atime_mode,
                        // Collected in the foreground before the fork, or
                        // (in `--foreground`) the env var / a prompt, only
                        // if the filesystem turns out to be encrypted.
                        passphrase: match passphrase {
                            Some(secret) => constellation_engine::PassphraseSource::Given(secret),
                            None => constellation_engine::PassphraseSource::Ask(Box::new(|| {
                                crate::passphrase(
                                    "CONSTELLATION_PASSPHRASE",
                                    "Filesystem passphrase: ",
                                )
                            })),
                        },
                        pin_target: Some(pin_target),
                        version: env!("CONSTELLATION_VERSION").to_string(),
                        ..constellation_engine::EngineConfig::new(s3)
                    },
                    web_ui,
                    log_buffer,
                    resumed: None,
                },
                handle,
            ) {
                Ok(node) => node,
                Err(e) => {
                    if let Some(v) = verdict {
                        v.failure(&format!("{e:#}"))?;
                        std::process::exit(1);
                    }
                    return Err(e);
                }
            };
            // All-or-nothing: collect every failure, then roll back only
            // what this invocation added.
            let mut added = Vec::new();
            let mut errors = Vec::new();
            startup::phase("mounting views");
            for view in &views {
                match node.add_mount(view.view_config(fuse_threads)) {
                    Ok(id) => added.push((id, view)),
                    Err(e) => errors.push(format!("{}: {e:#}", view.mountpoint.display())),
                }
            }
            if !errors.is_empty() {
                for (id, _) in &added {
                    let _ = node.remove_mount(*id);
                }
                let message = errors.join("; ");
                if !added.is_empty() {
                    // Rollback emptied the node; it already ran its own
                    // shutdown when the last view was removed.
                } else {
                    // Never got a single view up: nothing will trigger
                    // shutdown on its own, so run it explicitly.
                    let _ = node.shutdown();
                }
                for e in &errors {
                    eprintln!("mount failed: {e}");
                }
                if let Some(v) = verdict {
                    v.failure(&message)?;
                    std::process::exit(1);
                }
                bail!(message);
            }
            startup::done("daemon serving");
            if let Some(v) = verdict {
                v.success_daemon()?;
            }
            // Block on every view; the process exits once the last one's
            // thread has completed its own teardown (see
            // `NodeRuntime::add_mount`'s thread body) — and not while an
            // in-place upgrade is handing the views over.
            drop(added);
            node.wait_all();
            let failed = node.shutdown_error();
            drop(node);
            // Never wait indefinitely for a blocking task a failed drain
            // left behind: the exit is what the caller is waiting for.
            rt.shutdown_timeout(std::time::Duration::from_secs(10));
            match failed {
                // Already logged in full by `NodeRuntime::shutdown`.
                Some(message) => bail!(message),
                None => Ok(()),
            }
        }
    }
}

/// Outcome of trying to attach views to an already-running daemon.
enum AttachOutcome {
    /// Every view attached.
    Attached,
    /// The daemon is mid-shutdown (draining before exit) and refused the
    /// attach. The caller should wait for it to exit and retry the whole
    /// lock/become-daemon flow rather than orphaning a mount on a dying
    /// process.
    DaemonShuttingDown,
    /// Plan 31 C4b: the daemon is handing its views over to a new image
    /// (`daemon --upgrade`); the same daemon answers again shortly.
    DaemonUpgrading,
}

/// Attach every view through the running daemon's control socket. Each
/// `MountAdd` is bounded by what is left of `within` (campaign 6 B-1: a
/// listener nobody serves must not park this process forever).
async fn attach_views(
    state_dir: &Path,
    views: &[ViewSpec],
    within: std::time::Duration,
) -> Result<AttachOutcome> {
    let started = std::time::Instant::now();
    for view in views {
        let remaining = within
            .saturating_sub(started.elapsed())
            .max(std::time::Duration::from_secs(1));
        let resp =
            control::try_call::<cm::ViewMount>(state_dir, view.mount_params(), Some(remaining))
                .await
                .with_context(|| format!("attaching {}", view.mountpoint.display()))?;
        match resp {
            Ok(_) => {}
            Err(e) if e.message.contains("shutting down") => {
                return Ok(AttachOutcome::DaemonShuttingDown);
            }
            Err(e) if e.message.contains(node_runtime::UPGRADING) => {
                return Ok(AttachOutcome::DaemonUpgrading);
            }
            Err(e) => bail!("{}: {}", view.mountpoint.display(), e.message),
        }
    }
    Ok(AttachOutcome::Attached)
}

/// `constellation umount`: resolve the target, then send `MountRemove`
/// for one view or (bare name) every currently-mounted view. The daemon
/// exits on its own once its last view is gone; this just waits for the
/// socket to close (or a bounded timeout).
async fn cmd_umount(target: String, state_dir: Option<PathBuf>) -> Result<()> {
    let reg = registry::Registry::load()?;
    let resolved = target::resolve(&target, &reg);
    let dir = target::state_dir(state_dir, &resolved)?;
    let list = || async {
        control::call::<cm::ViewList>(&dir, Default::default())
            .await
            .map(|l| l.views)
    };
    let mountpoints: Vec<PathBuf> = match &resolved {
        target::Target::Named {
            path: Some(subtree),
            ..
        } => {
            let want = subtree.as_str();
            list()
                .await?
                .into_iter()
                .filter(|m| m.subtree == want)
                .map(|m| PathBuf::from(m.mountpoint))
                .collect()
        }
        _ => list()
            .await?
            .into_iter()
            .map(|m| PathBuf::from(m.mountpoint))
            .collect(),
    };
    if mountpoints.is_empty() {
        bail!("no matching mounted view for {target:?}");
    }
    for mountpoint in &mountpoints {
        let ack = control::call::<cm::ViewUnmount>(
            &dir,
            api::ViewUnmountParams {
                mountpoint: mountpoint.clone(),
            },
        )
        .await?;
        println!("{}", ack.detail);
    }
    // Removing a view does not by itself mean the daemon is going away: it
    // only runs its clean-shutdown sequence (and removes its socket) once
    // its *last* view is gone (`NodeRuntime::remove_mount`). A
    // `NAME:/sub` umount that leaves sibling views mounted must not wait on
    // the socket at all — it would never disappear while the daemon keeps
    // serving them, which is how `umount myfs:/sub` used to hang forever
    // even though the daemon's own log showed the view cleanly detached
    // ("FUSE detached") and moved on. Ask the daemon (if still reachable)
    // whether any view is left before deciding to wait for it to exit.
    let other_views_remain = matches!(list().await, Ok(views) if !views.is_empty());
    // Wait for the daemon to actually exit if this removed its last view.
    // The daemon removes its socket only at the very end of its clean
    // shutdown (drain uploads + ship journal), so the socket's presence is
    // the honest "still shutting down" signal. Rather than a fixed 10s cap
    // — which silently returned "done" while a large drain was still in
    // flight, tempting a remount that orphaned a mount — poll the live
    // status and report drain progress until the socket is gone.
    if !other_views_remain && control::socket_exists(&dir) {
        wait_for_daemon_exit(&dir).await;
    }
    Ok(())
}

/// Poll the daemon's control socket until it disappears (clean shutdown
/// complete), printing periodic drain progress so `umount` does not look
/// finished while uploads/journal are still shipping. Purely informational
/// — a daemon that never finishes draining will keep this waiting, which
/// is the honest state; the user can Ctrl-C to stop watching (the drain
/// continues in the daemon regardless).
async fn wait_for_daemon_exit(state_dir: &Path) {
    let mut last_report = std::time::Instant::now();
    let started = last_report;
    while control::socket_exists(state_dir) {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if last_report.elapsed() >= std::time::Duration::from_secs(2)
            && control::socket_exists(state_dir)
        {
            last_report = std::time::Instant::now();
            if let Ok(report) = control::call::<cm::NodeStatus>(state_dir, Default::default()).await
            {
                let pending = report.writeback.pending_uploads;
                let backlog = report.spool.journal_backlog;
                if pending > 0 || backlog > 0 {
                    eprintln!(
                        "shutting down: draining {pending} uploads, {backlog} journal entries \
                         ({}s elapsed)…",
                        started.elapsed().as_secs()
                    );
                }
            }
        }
    }
}

/// `constellation export NAME` (plan 21, step 6): the only way to
/// un-register a name. Leaves the cluster (if ever mounted) *while the
/// daemon is still up* — self-leave drains the journal and releases
/// leases through the running sync task — then deletes the state dir and
/// the registry row.
async fn cmd_export(name: String, force: bool) -> Result<()> {
    let mut reg = registry::Registry::load_locked()?;
    let entry = reg
        .entry(&name)
        .cloned()
        .with_context(|| format!("{name:?} is not a registered filesystem"))?;
    let db_path = entry.state_dir.join("meta.db");
    let dir = entry.state_dir.clone();
    // A running daemon holds `fjall`'s single-process lock on `meta.db`
    // (unlike the old SQLite/WAL engine, which tolerated this direct
    // open concurrently), so check aliveness first: if it answers at
    // all it has necessarily already claimed a node id, and opening the
    // store directly here would otherwise fail with `Locked` and be
    // swallowed by `.ok()`, silently skipping the self-leave below.
    let daemon_alive = matches!(
        control::ping(&dir, daemon_lock::control_timeout()).await,
        Ok(true)
    );
    let node_id_claimed = daemon_alive
        || (db_path.exists() && {
            Meta::open(&db_path)
                .ok()
                .and_then(|m| m.kv_get("node_id").ok().flatten())
                .is_some()
        });
    if node_id_claimed {
        match control::try_call::<cm::NodeLeave>(
            &dir,
            api::LeaveParams {
                node_id: None,
                force,
            },
            None,
        )
        .await
        {
            Ok(Ok(ack)) => {
                println!("{}", ack.detail);
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                while control::socket_exists(&dir) && std::time::Instant::now() < deadline {
                    if force {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
            Ok(Err(e)) => {
                let message = e.message;
                if !force {
                    bail!("leave failed: {message}");
                }
                eprintln!("leave failed (continuing: --force): {message}");
            }
            Err(_) => {
                // Daemon unreachable: it left ungracefully or was never
                // cleanly stopped. Refuse a live-but-wedged daemon
                // (lock still held) unless forced; otherwise retire the
                // node with the admin path directly against the store —
                // there is no journal to drain from a dead process.
                let lock_path = entry.state_dir.join("daemon.lock");
                if lock_path.exists() && !force {
                    let probe = std::fs::OpenOptions::new().write(true).open(&lock_path);
                    if let Ok(f) = probe {
                        // Anything but getting the lock (and releasing it
                        // again as the guard drops) counts as held.
                        let held = !matches!(
                            constellation_platform::native().file_lock.try_lock(f),
                            Ok(Some(_))
                        );
                        if held {
                            bail!(
                                "daemon.lock for {name:?} is held by a live process (starting up \
                                 or wedged, not gone); pass --force to retire it anyway"
                            );
                        }
                    }
                }
                let meta = std::sync::Arc::new(Meta::open(&db_path)?);
                let node_id: u64 = meta
                    .kv_get("node_id")?
                    .context("no node_id on record")?
                    .parse()?;
                // `cmd_export` is already running on a tokio runtime
                // (`main` drives it via `rt.block_on`), so this awaits
                // directly rather than nesting a second runtime — doing
                // that panics ("Cannot start a runtime from within a
                // runtime").
                let backend = backend::open_backend(&entry.s3).await?;
                let store = ChunkStore::new(backend);
                let designations = std::sync::Arc::new(designation::DesignationManager::new(
                    constellation_store_s3::designation::DesignationStore::new(
                        store.inner().clone(),
                        constellation_store_s3::designation::DesignationMode::Cas,
                    ),
                    meta.clone(),
                    constellation_net::Peers::disabled(),
                    node_id,
                ));
                // `self_id` only guards "you can't admin-leave yourself
                // via the admin form" (`target == self_id`); there is no
                // real "self" here (this CLI process is not a mounted
                // node), so `0` — never a claimed node id — just needs
                // to differ from `target`.
                leave::admin_leave(store.inner().clone(), &designations, 0, node_id, true)
                    .await
                    .context("admin-retiring the dead daemon's node")?;
                println!("retired node {node_id} in the registry (daemon was unreachable)");
            }
        }
    }
    if entry.state_dir.exists() {
        std::fs::remove_dir_all(&entry.state_dir)
            .with_context(|| format!("removing {}", entry.state_dir.display()))?;
    }
    reg.remove(&name)?;
    println!("exported {name}: registry entry and state dir removed");
    Ok(())
}

/// `constellation fs list` (plan 21, step 7): every registered
/// filesystem and its views, analogous to `zfs list`/`zpool list`.
fn cmd_fs_list(rt: &tokio::runtime::Runtime) -> Result<()> {
    let reg = registry::Registry::load()?;
    println!(
        "{:<16} {:<32} {:<40} {:<16} {:<24} MOUNTED?",
        "NAME", "S3", "STATE-DIR", "SUBTREE", "MOUNTPOINT"
    );
    for (name, entry) in reg.iter() {
        let live: Vec<api::ViewInfo> = rt
            .block_on(control::call::<cm::ViewList>(
                &entry.state_dir,
                Default::default(),
            ))
            .map(|l| l.views)
            .unwrap_or_default();
        if entry.mounts.is_empty() {
            println!(
                "{:<16} {:<32} {:<40} {:<16} {:<24} no",
                name,
                entry.s3,
                entry.state_dir.display(),
                "-",
                "-"
            );
            continue;
        }
        for m in &entry.mounts {
            let subtree = if m.subtree.is_empty() {
                "/"
            } else {
                &m.subtree
            };
            let mounted = live
                .iter()
                .any(|lm| lm.mountpoint == m.mountpoint.display().to_string());
            println!(
                "{:<16} {:<32} {:<40} {:<16} {:<24} {}",
                name,
                entry.s3,
                entry.state_dir.display(),
                subtree,
                m.mountpoint.display(),
                if mounted { "yes" } else { "no" }
            );
        }
    }
    Ok(())
}

async fn run_gc_cli(
    s3: &str,
    pin: &e2e_pin::PinTarget,
    state_dir: Option<PathBuf>,
    verify_only: bool,
) -> Result<gc::GcReport> {
    let backend = backend::open_backend(s3).await?;
    let plain = ChunkStore::new(backend.clone());
    let fsmeta = plain.load_fs().await?;
    let pin = e2e_pin::check(pin, &fsmeta)?;
    let keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(fsmeta.unlock(&secret)?)
    } else {
        None
    };
    pin.confirm(keys.as_deref())?;
    let chunks = std::sync::Arc::new(match &keys {
        Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
        None => plain,
    });
    let logs = match keys {
        Some(keys) => constellation_store_s3::LogStore::new_e2e(backend.clone(), keys),
        None => constellation_store_s3::LogStore::new(backend.clone()),
    };
    let dir = state_dir.unwrap_or_else(|| default_state_dir(&fsmeta));
    std::fs::create_dir_all(&dir)?;

    // `fjall` (unlike the old SQLite/WAL engine) refuses a second
    // process's open of the same metadata store directory while a mount
    // daemon holds it. If one is up for this state dir, run GC inside it
    // over the control socket instead of racing it for the lock; a
    // failed/absent connection means nothing is mounted here, so fall
    // through to opening the store directly exactly as before.
    match control::try_call::<cm::GcRun>(&dir, api::GcRunParams { verify_only }, None).await {
        Ok(Ok(report)) => return Ok(serde_json::from_value(report.report.0)?),
        Ok(Err(e)) => bail!("gc failed in the running daemon: {}", e.message),
        Err(_) => {}
    }

    let db = dir.join("meta.db");
    if !db.exists() {
        shipper::bootstrap(&db, &logs).await?;
    }
    let meta = std::sync::Arc::new(Meta::open(db)?);
    let caps = chunks.probe_conditional_writes().await?;
    let mode = if caps.etag_cas {
        constellation_store_s3::LeaseMode::Cas
    } else {
        constellation_store_s3::LeaseMode::SingleWriter
    };
    let tail = gc::GcTail::standalone(logs, &meta)?;
    gc::run(backend, chunks, meta, mode, verify_only, None, &tail).await
}

async fn run_fsck_cli(
    s3: &str,
    pin: &e2e_pin::PinTarget,
    state_dir: Option<PathBuf>,
    repair: bool,
    force_release: Option<&str>,
) -> Result<fsck::FsckReport> {
    let backend = backend::open_backend(s3).await?;
    let plain = ChunkStore::new(backend.clone());
    let fsmeta = plain.load_fs().await?;
    let pin = e2e_pin::check(pin, &fsmeta)?;
    let keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(fsmeta.unlock(&secret)?)
    } else {
        None
    };
    pin.confirm(keys.as_deref())?;
    let chunks = std::sync::Arc::new(match &keys {
        Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
        None => plain,
    });
    let logs = match keys {
        Some(keys) => constellation_store_s3::LogStore::new_e2e(backend.clone(), keys),
        None => constellation_store_s3::LogStore::new(backend.clone()),
    };
    let dir = state_dir.unwrap_or_else(|| default_state_dir(&fsmeta));
    std::fs::create_dir_all(&dir)?;

    // Same lock-avoidance routing as `constellation gc` (plan 29 M3a):
    // `fjall` refuses a second process's open of the metadata store
    // while a mount daemon holds it, so run `fsck` inside the daemon
    // over the control socket when one is up for this state dir; a
    // failed/absent connection means nothing is mounted here, so fall
    // through to opening the store directly exactly as before.
    match control::try_call::<cm::FsckRun>(
        &dir,
        api::FsckRunParams {
            repair,
            force_release: force_release.map(str::to_string),
        },
        None,
    )
    .await
    {
        Ok(Ok(report)) => return Ok(serde_json::from_value(report.report.0)?),
        Ok(Err(e)) => bail!("fsck failed in the running daemon: {}", e.message),
        Err(_) => {}
    }

    let db = dir.join("meta.db");
    if !db.exists() {
        shipper::bootstrap(&db, &logs).await?;
    }
    let meta = std::sync::Arc::new(Meta::open(db)?);
    let caps = chunks.probe_conditional_writes().await?;
    let mode = if caps.etag_cas {
        constellation_store_s3::LeaseMode::Cas
    } else {
        constellation_store_s3::LeaseMode::SingleWriter
    };
    let compression: CompressionSetting = fsmeta
        .compression
        .parse()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    fsck::run(
        backend,
        chunks,
        &logs,
        meta,
        Some(&dir),
        compression,
        mode,
        repair,
        force_release,
    )
    .await
}

///
/// A daemon-side refusal (over budget, no such path) comes back as
/// `Response::Error` and must exit non-zero: `pin` failing silently would
/// leave the user believing a subtree is resident when it is not.
/// The `constellation prune ...` subcommands. The tree-editing ones
/// (`check`/`set`/`disarm`/`rm`/`show`) act directly on the mounted path
/// via xattr syscalls — `set` still goes through the daemon's validation
/// gate, this just gives a good client-side error first. The rest are
/// daemon round-trips.
fn run_prune_command(rt: &tokio::runtime::Runtime, command: PruneCommand) -> Result<()> {
    use constellation_meta::prune::Policy;
    use constellation_meta::prune::PRUNE_XATTR;
    match command {
        PruneCommand::Check { expr } => match Policy::parse(&expr) {
            Ok(policy) => {
                println!("ok: {policy}");
                Ok(())
            }
            Err(e) => {
                eprintln!("{}", e.render(&expr));
                std::process::exit(2);
            }
        },
        PruneCommand::Set { path, expr, arm } => {
            let mut policy = match Policy::parse(&expr) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("{}", e.render(&expr));
                    std::process::exit(2);
                }
            };
            if arm && !policy.off {
                policy.armed = true;
            }
            let value = policy.to_string();
            xattr_set(&path, PRUNE_XATTR, value.as_bytes())
                .with_context(|| format!("setting prune policy on {}", path.display()))?;
            println!("{}: {value}", path.display());
            Ok(())
        }
        PruneCommand::Disarm { path } => {
            let raw = xattr_get(&path, PRUNE_XATTR)?
                .ok_or_else(|| anyhow::anyhow!("no prune policy on {}", path.display()))?;
            let expr = String::from_utf8_lossy(&raw);
            let mut policy = Policy::parse(&expr).map_err(|e| anyhow::anyhow!("{}", e))?;
            policy.armed = false;
            let value = policy.to_string();
            xattr_set(&path, PRUNE_XATTR, value.as_bytes())?;
            println!("{}: {value}", path.display());
            Ok(())
        }
        PruneCommand::Rm { path } => {
            xattr_remove(&path, PRUNE_XATTR)?;
            println!("{}: prune policy removed", path.display());
            Ok(())
        }
        PruneCommand::Show { path } => {
            let mut cur = path.canonicalize().unwrap_or(path.clone());
            loop {
                if let Some(raw) = xattr_get(&cur, PRUNE_XATTR)? {
                    let expr = String::from_utf8_lossy(&raw);
                    let armed = Policy::parse(&expr).map(|p| p.armed).unwrap_or(false);
                    println!(
                        "{}\n  policy: {}\n  from:   {}\n  armed:  {}",
                        path.display(),
                        expr,
                        cur.display(),
                        armed
                    );
                    return Ok(());
                }
                match cur.parent() {
                    Some(parent) if parent != cur => cur = parent.to_path_buf(),
                    _ => break,
                }
            }
            println!("{}: no prune policy", path.display());
            Ok(())
        }
        PruneCommand::Ls { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::PruneList>(rt, &dir, Default::default(), |l| {
                print_list(&l.roots, "no prune policies")
            })
        }
        PruneCommand::Run {
            target,
            state_dir,
            path,
            dry_run,
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            ctl::<cm::PruneRun>(rt, &dir, api::PruneRunParams { path, dry_run }, print_ack)
        }
        PruneCommand::Status { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            let report = rt.block_on(control::call_bounded::<cm::NodeStatus>(
                &dir,
                Default::default(),
                daemon_lock::control_timeout(),
            ))?;
            print_json(&report.prune)
        }
    }
}

/// Set an extended attribute on `path` via libc (no `xattr` crate dep).
fn xattr_set(path: &std::path::Path, name: &str, value: &[u8]) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let cname = std::ffi::CString::new(name)?;
    let rc = unsafe {
        libc::setxattr(
            cpath.as_ptr(),
            cname.as_ptr(),
            value.as_ptr() as *const libc::c_void,
            value.len(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Read an extended attribute; `Ok(None)` when it is absent.
fn xattr_get(path: &std::path::Path, name: &str) -> Result<Option<Vec<u8>>> {
    use std::os::unix::ffi::OsStrExt;
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let cname = std::ffi::CString::new(name)?;
    let size = unsafe { libc::getxattr(cpath.as_ptr(), cname.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        let err = std::io::Error::last_os_error();
        return match Code::from_io_error(&err) {
            Code::NoData => Ok(None),
            _ => Err(err.into()),
        };
    }
    let mut buf = vec![0u8; size as usize];
    let got = unsafe {
        libc::getxattr(
            cpath.as_ptr(),
            cname.as_ptr(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
        )
    };
    if got < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    buf.truncate(got as usize);
    Ok(Some(buf))
}

/// Remove an extended attribute.
fn xattr_remove(path: &std::path::Path, name: &str) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let cname = std::ffi::CString::new(name)?;
    let rc = unsafe { libc::removexattr(cpath.as_ptr(), cname.as_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Run one control call against the daemon of `dir` and print its answer
/// the way the CLI always has (plan 31 C5 kept every command's output).
fn ctl<M: constellation_control::Method>(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    params: M::Params,
    print: impl FnOnce(M::Result) -> Result<()>,
) -> Result<()> {
    print(rt.block_on(control::call::<M>(dir, params))?)
}

fn print_ack(ack: api::Ack) -> Result<()> {
    println!("{}", ack.detail);
    Ok(())
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// A listing, or `empty` when there is nothing in it.
fn print_list<T: serde::Serialize>(rows: &[T], empty: &str) -> Result<()> {
    if rows.is_empty() {
        println!("{empty}");
        Ok(())
    } else {
        print_json(&rows)
    }
}

fn print_quota(q: api::QuotaStatus) -> Result<()> {
    match q.max_bytes {
        Some(cap) => println!("quota: {} / {cap} bytes used", q.used_bytes),
        None => println!("quota: {} bytes used (unlimited)", q.used_bytes),
    }
    Ok(())
}

/// `constellation daemon --upgrade`: ask the daemon to hand its views
/// over (`handover`), then wait until the new image answers `status`
/// with a higher generation.
async fn cmd_daemon_upgrade(
    state_dir: &Path,
    binary: Option<PathBuf>,
    within: std::time::Duration,
) -> Result<()> {
    let status =
        |timeout| control::call_bounded::<cm::NodeStatus>(state_dir, Default::default(), timeout);
    let before = status(daemon_lock::control_timeout())
        .await
        .context("no daemon answers for this state dir")?;
    let binary = binary
        .map(|b| std::fs::canonicalize(&b).with_context(|| format!("{}", b.display())))
        .transpose()?;
    match control::try_call::<cm::NodeHandoff>(
        state_dir,
        api::HandoffParams {
            target: api::HandoffTarget::Exec { binary },
            ..Default::default()
        },
        Some(within),
    )
    .await?
    {
        Ok(report) => println!("{}", report.detail),
        Err(e) => bail!("upgrade refused: {}", e.message),
    }
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() > within {
            bail!("the upgraded daemon did not report serving within {within:?}");
        }
        // The new image answers once it serves; during the handover the
        // connection waits in the listener's backlog.
        if let Ok(now) = status(std::time::Duration::from_secs(5)).await {
            if now.handover.generation > before.handover.generation && !now.handover.upgrading {
                println!(
                    "upgraded: pid {} generation {} version {} serving {} view(s)",
                    now.handover.pid,
                    now.handover.generation,
                    now.version,
                    now.mounts.len()
                );
                return Ok(());
            }
            if !now.handover.upgrading {
                if let Some(error) = now.handover.last_error {
                    bail!("the upgrade failed: {error}");
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// `<data dir>/<uuid>` (`constellation_engine::default_state_dir`, for
/// the one-shot commands that open a state dir without an engine).
fn default_state_dir(meta: &FsMeta) -> PathBuf {
    constellation_engine::default_state_dir(constellation_platform::native(), meta)
}

/// Parse a human-readable byte size for CLI flags (`10G`, `512MiB`, bare
/// integer). Suffixes K/M/G/T (and KiB/MiB/…, KB/MB/…) are binary
/// (1024-based): `10G` means 10 GiB, matching the common cache-budget
/// convention rather than SI decimal.
fn parse_byte_size(input: &str) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty byte size".into());
    }
    let digits = s
        .char_indices()
        .take_while(|(_, c)| c.is_ascii_digit())
        .last()
        .map(|(i, _)| i + 1)
        .unwrap_or(0);
    if digits == 0 {
        return Err(format!("invalid byte size: {input}"));
    }
    let num: u64 = s[..digits]
        .parse()
        .map_err(|e| format!("invalid byte size: {e}"))?;
    let unit = s[digits..].trim().to_ascii_lowercase();
    let mult: u64 = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "ki" | "kib" => 1 << 10,
        "m" | "mb" | "mi" | "mib" => 1 << 20,
        "g" | "gb" | "gi" | "gib" => 1 << 30,
        "t" | "tb" | "ti" | "tib" => 1 << 40,
        _ => return Err(format!("unknown size unit in {input:?}")),
    };
    num.checked_mul(mult)
        .ok_or_else(|| format!("byte size overflow: {input}"))
}

/// Parse `constellation quota set` argument: `unlimited`/`0` → clear,
/// otherwise a byte size via [`parse_byte_size`].
fn parse_quota_arg(input: &str) -> Result<Option<u64>> {
    let s = input.trim();
    if s.eq_ignore_ascii_case("unlimited") || s == "0" {
        return Ok(None);
    }
    let n = parse_byte_size(s).map_err(anyhow::Error::msg)?;
    if n == 0 {
        Ok(None)
    } else {
        Ok(Some(n))
    }
}

#[cfg(test)]
mod parse_byte_size_tests {
    use super::*;

    #[test]
    fn binary_suffixes() {
        assert_eq!(parse_byte_size("10G").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size("10GiB").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size("10g").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size("64M").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("64MiB").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("512KiB").unwrap(), 512 << 10);
        assert_eq!(parse_byte_size("1T").unwrap(), 1 << 40);
    }

    #[test]
    fn bare_bytes_and_whitespace() {
        assert_eq!(parse_byte_size("10737418240").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size(" 64M ").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("0").unwrap(), 0);
    }

    #[test]
    fn rejects_junk() {
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("G").is_err());
        assert!(parse_byte_size("10X").is_err());
        assert!(parse_byte_size("10.5G").is_err());
    }

    #[test]
    fn mount_default_is_unset_and_falls_back_at_mount_time() {
        // Plan 21, step 4: cache_size is now `Option<u64>` so
        // `merge_and_save` can tell "not given" from "given as zero";
        // the 10 GiB default is applied when actually mounting, not by
        // clap. See `cmd_mount`.
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "/",
            "/mnt",
            "--s3",
            "file:///tmp/x",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { cache_size, .. } => assert_eq!(cache_size, None),
            _ => panic!("expected Mount"),
        }
    }

    #[test]
    fn mount_accepts_human_cache_size() {
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "/",
            "/mnt",
            "--s3",
            "file:///tmp/x",
            "--cache-size",
            "64M",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { cache_size, .. } => assert_eq!(cache_size, Some(64 << 20)),
            _ => panic!("expected Mount"),
        }
    }

    #[test]
    fn bare_web_ui_defaults_to_8080() {
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "/",
            "/mnt",
            "--s3",
            "file:///tmp/x",
            "--web-ui",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { web_ui, .. } => assert_eq!(web_ui, Some(8080)),
            _ => panic!("expected Mount"),
        }
    }

    #[test]
    fn web_ui_port_override() {
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "/",
            "/mnt",
            "--s3",
            "file:///tmp/x",
            "--web-ui",
            "9090",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { web_ui, .. } => assert_eq!(web_ui, Some(9090)),
            _ => panic!("expected Mount"),
        }
    }
}

/// Regression for plan 29 M3b: `constellation umount myfs:/sub` used to
/// hang forever on a shared daemon that still had sibling views mounted.
/// `cmd_umount` unconditionally waited for `control.sock` to disappear,
/// but `NodeRuntime::remove_mount` only ever deletes it when the removed
/// view was the *last* one (`NodeRuntime::shutdown`) — a daemon that keeps
/// serving another view never deletes it, so the wait never ended even
/// though the view being unmounted had cleanly detached
/// (the view's session thread logs "FUSE detached" and moves on).
#[cfg(test)]
mod umount_tests {
    use super::*;
    use constellation_store_s3::{ChunkStore, FsMeta};

    /// One `NodeRuntime` with two real FUSE views (root + `/sub`), driven
    /// through the shared registry/control-socket path exactly like the
    /// `named-shared-daemon` harness scenario, but in-process so it runs
    /// under `cargo test`. Removing the non-last view must return quickly
    /// (well under the harness's minutes-long hang) and must leave the
    /// daemon and the sibling view alive.
    #[test]
    fn umount_of_a_non_last_view_returns_and_leaves_the_daemon_serving_the_rest() {
        unsafe {
            std::env::set_var("CONSTELLATION_P2P", "off");
        }
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        {
            let store = ChunkStore::new(
                rt.block_on(backend::open_backend(&backend))
                    .expect("open backend"),
            );
            let meta = FsMeta::new(1024 * 1024, "raw");
            rt.block_on(store.create_fs(&meta)).expect("create_fs");
        }

        let root_mnt = root.path().join("root-mnt");
        let sub_mnt = root.path().join("sub-mnt");
        std::fs::create_dir_all(&root_mnt).unwrap();
        std::fs::create_dir_all(&sub_mnt).unwrap();
        let state_dir = root.path().join("state");

        let node = node_runtime::NodeRuntime::start(
            node_runtime::NodeConfig {
                fs_id: constellation_engine::FsId::new("myfs"),
                engine: constellation_engine::EngineConfig {
                    state_dir: Some(state_dir.clone()),
                    cache_size: 16 * 1024 * 1024,
                    ..constellation_engine::EngineConfig::new(backend.clone())
                },
                web_ui: 0,
                log_buffer: log_buffer::LogBuffer::default(),
                resumed: None,
            },
            rt.handle().clone(),
        )
        .expect("NodeRuntime::start");
        node.add_mount(node_runtime::ViewConfig {
            inner_path: "/".to_string(),
            mountpoint: root_mnt.clone(),
            allow_other: false,
            fs_name: "constellation-test".to_string(),
            fuse_threads: 1,
            rw_snapshot: false,
            clone_name: None,
            ephemeral: false,
            confine_links: false,
            labels: Default::default(),
            qos: Default::default(),
        })
        .expect("mount root");
        std::fs::create_dir(root_mnt.join("sub")).expect("mkdir sub via root view");
        node.add_mount(node_runtime::ViewConfig {
            inner_path: "/sub".to_string(),
            mountpoint: sub_mnt.clone(),
            allow_other: false,
            fs_name: "constellation-test".to_string(),
            fuse_threads: 1,
            rw_snapshot: false,
            clone_name: None,
            ephemeral: false,
            confine_links: false,
            labels: Default::default(),
            qos: Default::default(),
        })
        .expect("mount sub");

        // Register the name so `target::resolve("myfs:/sub", ..)` finds it
        // (only its *presence*, and this test's explicit `--state-dir`
        // equivalent, matter — `cmd_umount` never consults the registry's
        // own stored state dir once one is passed explicitly).
        let registry_path = root.path().join("registry.toml");
        unsafe {
            std::env::set_var("CONSTELLATION_REGISTRY", &registry_path);
        }
        let mut reg = registry::Registry::load_locked_at(registry_path.clone()).unwrap();
        reg.merge_and_save(
            "myfs",
            registry::FsOverrides {
                s3: Some(backend.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        drop(reg);

        let list = |rt: &tokio::runtime::Runtime| {
            rt.block_on(control::call::<cm::ViewList>(
                &state_dir,
                Default::default(),
            ))
            .map(|l| l.views)
        };
        let before = list(&rt).unwrap();
        assert_eq!(
            before.len(),
            2,
            "expected both views mounted before umount: {before:?}"
        );

        // The actual regression check: this must return promptly. The
        // pre-fix code waited on `control.sock` disappearing, which never
        // happens while the root view is still mounted — the harness
        // caught this at minutes-long hangs, so a generous-but-bounded 10s
        // here is already 10x-plus the margin needed once fixed.
        let start = std::time::Instant::now();
        rt.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                cmd_umount("myfs:/sub".to_string(), Some(state_dir.clone())),
            )
            .await
            .expect("cmd_umount(\"myfs:/sub\") hung")
            .expect("cmd_umount(\"myfs:/sub\") failed");
        });
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "umount of a non-last view took {:?}, should return almost immediately",
            start.elapsed()
        );

        // The daemon must still be up, still serving the root view, and no
        // longer serving `/sub`.
        let mounts = list(&rt).expect("daemon should still be reachable (root view still mounted)");
        assert_eq!(
            mounts.len(),
            1,
            "expected exactly the root view left: {mounts:?}"
        );
        assert_eq!(mounts[0].subtree, "/");
        assert!(
            std::fs::metadata(root_mnt.join("sub")).is_ok(),
            "root view must still be serving reads after the sub view was unmounted"
        );

        // Clean up: unmount the remaining root view so the daemon exits.
        rt.block_on(cmd_umount("myfs".to_string(), Some(state_dir.clone())))
            .expect("final umount");
    }
}
