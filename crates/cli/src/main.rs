//! Constellation entry point: CLI, daemon, and FUSE mount in one binary.

mod atime;
mod authority_driver;
mod backend;
mod coop;
mod cto;
mod daemon_lock;
mod daemonize;
mod designation;
mod doctor;
mod epoch;
mod existence;
mod fault;
mod forward;
mod fsck;
mod fusefs;
mod gc;
mod held;
mod inbox;
mod kernel_inval;
mod lease;
mod leave;
mod locks;
mod log_buffer;
mod mtree_gc;
mod mtree_publish;
mod mtree_read;
mod node_runtime;
mod parallelism;
mod paths;
mod pin;
mod placement;
mod prefetch;
mod prune;
mod recovery;
mod registry;
mod reintegrate;
mod scan;
mod shipper;
mod singleton;
mod snapshot;
mod sources;
mod staging;
mod startup;
mod target;
mod writeback;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::Meta;
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta};
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
        /// The inode `status` lists under `held.inodes`.
        ino: u64,
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
    // SAFETY: geteuid is always safe and never fails.
    if unsafe { libc::geteuid() } == 0 {
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
    let sock = state_dir.join(constellation_api::SOCKET_NAME);
    if !sock.exists() {
        return false;
    }
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return false;
    };
    let live = rt.block_on(constellation_api::ping(
        &sock,
        daemon_lock::control_timeout(),
    ));
    drop(rt);
    match live {
        Ok(live) => live,
        Err(e) => {
            tracing::warn!(
                sock = %sock.display(),
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
    e2e: bool,
    /// The S3 endpoint `meta.json` was read from (`None`: local backend).
    endpoint: Option<String>,
}

/// Read `meta.json` before the daemon forks, so a missing filesystem
/// fails here — on this terminal, explained by
/// [`backend::load_fs_explained`] (where the command looked, and whether
/// it reached the store the name was created on) — and so an interactive
/// E2E mount knows to prompt for the passphrase before the child loses
/// the terminal. Runs through a throwaway runtime that is fully dropped
/// before the caller forks, so no runtime threads leak into the daemon
/// child.
fn check_fs_before_mount(s3: &str, registered_endpoint: Option<&str>) -> Result<MountCheck> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let check = rt.block_on(async {
        let (backend, info) = backend::open_backend_described(s3).await?;
        let meta = backend::load_fs_explained(&backend, &info, registered_endpoint).await?;
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
                web_ui,
            },
            log_buffer,
        );
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
            println!(
                "passphrase changed; data-encryption keys were not rotated \
                 and mounted nodes need no remount"
            );
            Ok(())
        }
        Command::Fs {
            command: FsCommand::List,
        } => cmd_fs_list(&rt),
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
            let sock = dir.join(constellation_api::SOCKET_NAME);
            // Bounded: a daemon the kernel has killed but that still holds
            // its listener (campaign 6 B-1) must not park `status` forever.
            let resp = rt.block_on(constellation_api::call_bounded(
                &sock,
                &constellation_api::Request::Status,
                daemon_lock::control_timeout(),
            ))?;
            match resp {
                constellation_api::Response::Status(s) => {
                    println!("{}", serde_json::to_string_pretty(&s)?)
                }
                other => bail!("unexpected response: {other:?}"),
            }
            Ok(())
        }
        Command::Pin { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(&dir, constellation_api::Request::Pin { path }))
        }
        Command::Unpin { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Unpin { path },
            ))
        }
        Command::Pins { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(&dir, constellation_api::Request::ListPins))
        }
        Command::Offline {
            target,
            state_dir,
            ro,
        } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Offline {
                    path,
                    read_only: ro,
                },
            ))
        }
        Command::Online { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Online { path },
            ))
        }
        Command::Designations { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::ListDesignations,
            ))
        }
        Command::Delegate {
            target,
            state_dir,
            to,
            range,
        } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Delegate {
                    path,
                    node: to,
                    range,
                },
            ))
        }
        Command::Undelegate { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Undelegate { path },
            ))
        }
        Command::Delegations { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::ListDelegations,
            ))
        }
        Command::Reintegrate { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(&dir, constellation_api::Request::Reintegrate))
        }
        Command::Repair {
            command:
                RepairCommand::DropHeld {
                    target,
                    ino,
                    state_dir,
                },
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::DropHeld { ino },
            ))
        }
        Command::Leave {
            target,
            state_dir,
            node_id,
            force,
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Leave { node_id, force },
            ))
        }
        Command::WriteMode {
            target,
            mode,
            state_dir,
        } => {
            let mode: writeback::WriteMode = mode.parse().map_err(anyhow::Error::msg)?;
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::SetWriteMode {
                    mode: mode.as_str().into(),
                },
            ))
        }
        Command::Quota { command } => match command {
            QuotaCommand::Get { target, state_dir } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                rt.block_on(control_call(&dir, constellation_api::Request::GetQuota))
            }
            QuotaCommand::Set {
                target,
                size,
                state_dir,
            } => {
                let max_bytes = parse_quota_arg(&size)?;
                let (_, dir) = resolve_target(&target, state_dir)?;
                rt.block_on(control_call(
                    &dir,
                    constellation_api::Request::SetQuota { max_bytes },
                ))
            }
        },
        Command::Prune { command } => run_prune_command(&rt, command),
        Command::Inspect { target, state_dir } => {
            let (t, dir) = resolve_target(&target, state_dir)?;
            let path = target::effective_path(&t);
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Inspect { path },
            ))
        }
        Command::Cache { command } => match command {
            CacheCommand::Ls { target, state_dir } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                rt.block_on(control_call(&dir, constellation_api::Request::CacheList))
            }
            CacheCommand::Stat { target, state_dir } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                rt.block_on(control_call(&dir, constellation_api::Request::Status))
            }
            CacheCommand::Prune {
                target,
                state_dir,
                target_bytes,
            } => {
                let (_, dir) = resolve_target(&target, state_dir)?;
                rt.block_on(control_call(
                    &dir,
                    constellation_api::Request::CachePrune { target_bytes },
                ))
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
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::LogTail { lines },
            ))
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
                rt.block_on(control_call(
                    &dir,
                    constellation_api::Request::SnapshotCreate { selector },
                ))
            }
            SnapshotCommand::Ls { target, state_dir } => {
                let (t, dir) = resolve_target(&target, state_dir)?;
                let path = match &t {
                    target::Target::Named { path, .. } => path.clone(),
                    target::Target::Raw(raw) => Some(raw.clone()),
                };
                rt.block_on(control_call(
                    &dir,
                    constellation_api::Request::SnapshotList { path },
                ))
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
                rt.block_on(control_call(
                    &dir,
                    constellation_api::Request::SnapshotDelete { selector },
                ))
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
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::Clone {
                    selector,
                    destination,
                },
            ))
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
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::SnapRefs { id },
            ))
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
            let report = rt.block_on(run_gc_cli(&s3, dir, verify_only))?;
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
            let report = rt.block_on(run_fsck_cli(&s3, dir, repair, force_release.as_deref()))?;
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
        }
    }

    fn mount_opts(&self) -> constellation_api::MountViewOpts {
        constellation_api::MountViewOpts {
            allow_other: self.allow_other,
            fs_name: Some(self.fs_name.clone()),
            fuse_threads: None,
            rw: self.rw,
            clone_name: self.clone_name.clone(),
            ephemeral: self.ephemeral,
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
        let check = check_fs_before_mount(&node_s3, registered_endpoint.as_deref())?;
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
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(state_dir.join("daemon.lock"))
        .context("opening daemon.lock")?;
    use std::os::fd::AsRawFd;
    let fd = lock_file.as_raw_fd();
    // SAFETY: `fd` is a valid, open fd owned by `lock_file` for the
    // duration of this call.
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        // We hold it now. Leak the `File` so the lock survives for the
        // rest of this process's life (released automatically on exit,
        // by the kernel closing every fd) rather than dropping here.
        std::mem::forget(lock_file);
        let _ = std::fs::remove_file(state_dir.join(constellation_api::SOCKET_NAME));
        let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
        Ok(LockOutcome::BecomeDaemon)
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(LockOutcome::Attach)
        } else {
            Err(err).context("locking daemon.lock")
        }
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
    let sock = state_dir.join(constellation_api::SOCKET_NAME);
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
                let probe = rt.block_on(constellation_api::ping(
                    &sock,
                    daemon_lock::control_timeout(),
                ));
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
                match rt.block_on(attach_views(&sock, &views, remaining))? {
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
                        while sock.exists() && std::time::Instant::now() < deadline {
                            std::thread::sleep(std::time::Duration::from_millis(200));
                        }
                        if sock.exists() {
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
            startup::phase("starting the node runtime");
            let handle = rt.handle().clone();
            let node = match node_runtime::NodeRuntime::start(
                node_runtime::NodeConfig {
                    s3,
                    state_dir: Some(state_dir.to_path_buf()),
                    cache_size,
                    fsync_s3,
                    cto_strict,
                    locks,
                    initial_write_mode,
                    read_only_member,
                    web_ui,
                    log_buffer,
                    atime_mode,
                    passphrase,
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
            // `NodeRuntime::add_mount`'s thread body).
            for (id, _) in added {
                node.join_mount(id)?;
            }
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
}

/// Attach every view through the running daemon's control socket. Each
/// `MountAdd` is bounded by what is left of `within` (campaign 6 B-1: a
/// listener nobody serves must not park this process forever).
async fn attach_views(
    sock: &Path,
    views: &[ViewSpec],
    within: std::time::Duration,
) -> Result<AttachOutcome> {
    let started = std::time::Instant::now();
    for view in views {
        let remaining = within
            .saturating_sub(started.elapsed())
            .max(std::time::Duration::from_secs(1));
        let resp = constellation_api::call_bounded(
            sock,
            &constellation_api::Request::MountAdd {
                subtree: view.inner_path(),
                mountpoint: view.mountpoint.clone(),
                opts: view.mount_opts(),
            },
            remaining,
        )
        .await
        .with_context(|| format!("attaching {}", view.mountpoint.display()))?;
        match resp {
            constellation_api::Response::Ok { .. } => {}
            constellation_api::Response::Error { message } if message.contains("shutting down") => {
                return Ok(AttachOutcome::DaemonShuttingDown);
            }
            constellation_api::Response::Error { message } => {
                bail!("{}: {message}", view.mountpoint.display())
            }
            other => bail!("unexpected response: {other:?}"),
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
    let sock = dir.join(constellation_api::SOCKET_NAME);
    let mountpoints: Vec<PathBuf> = match &resolved {
        target::Target::Named {
            path: Some(subtree),
            ..
        } => {
            let resp =
                constellation_api::call(&sock, &constellation_api::Request::MountList).await?;
            let mounts = match resp {
                constellation_api::Response::Mounts { mounts } => mounts,
                other => bail!("unexpected response: {other:?}"),
            };
            let want = subtree.as_str();
            mounts
                .into_iter()
                .filter(|m| m.subtree == want)
                .map(|m| PathBuf::from(m.mountpoint))
                .collect()
        }
        _ => {
            let resp =
                constellation_api::call(&sock, &constellation_api::Request::MountList).await?;
            match resp {
                constellation_api::Response::Mounts { mounts } => mounts
                    .into_iter()
                    .map(|m| PathBuf::from(m.mountpoint))
                    .collect(),
                other => bail!("unexpected response: {other:?}"),
            }
        }
    };
    if mountpoints.is_empty() {
        bail!("no matching mounted view for {target:?}");
    }
    for mountpoint in &mountpoints {
        let resp = constellation_api::call(
            &sock,
            &constellation_api::Request::MountRemove {
                mountpoint: mountpoint.clone(),
            },
        )
        .await?;
        match resp {
            constellation_api::Response::Ok { detail } => println!("{detail}"),
            constellation_api::Response::Error { message } => bail!("{message}"),
            other => bail!("unexpected response: {other:?}"),
        }
    }
    // Removing a view does not by itself mean the daemon is going away: it
    // only runs its clean-shutdown sequence (and deletes `control.sock`)
    // once its *last* view is gone (`NodeRuntime::remove_mount`). A
    // `NAME:/sub` umount that leaves sibling views mounted must not wait on
    // the socket at all — it would never disappear while the daemon keeps
    // serving them, which is how `umount myfs:/sub` used to hang forever
    // even though the daemon's own log showed the view cleanly detached
    // ("FUSE detached") and moved on. Ask the daemon (if still reachable)
    // whether any view is left before deciding to wait for it to exit.
    let other_views_remain = matches!(
        constellation_api::call(&sock, &constellation_api::Request::MountList).await,
        Ok(constellation_api::Response::Mounts { mounts }) if !mounts.is_empty()
    );
    // Wait for the daemon to actually exit if this removed its last view.
    // The daemon deletes `control.sock` only at the very end of its clean
    // shutdown (drain uploads + ship journal), so the socket's presence is
    // the honest "still shutting down" signal. Rather than a fixed 10s cap
    // — which silently returned "done" while a large drain was still in
    // flight, tempting a remount that orphaned a mount — poll the live
    // status and report drain progress until the socket is gone.
    if !other_views_remain && sock.exists() {
        wait_for_daemon_exit(&sock).await;
    }
    Ok(())
}

/// Poll the daemon's control socket until it disappears (clean shutdown
/// complete), printing periodic drain progress so `umount` does not look
/// finished while uploads/journal are still shipping. Purely informational
/// — a daemon that never finishes draining will keep this waiting, which
/// is the honest state; the user can Ctrl-C to stop watching (the drain
/// continues in the daemon regardless).
async fn wait_for_daemon_exit(sock: &Path) {
    let mut last_report = std::time::Instant::now();
    let started = last_report;
    while sock.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if last_report.elapsed() >= std::time::Duration::from_secs(2) && sock.exists() {
            last_report = std::time::Instant::now();
            if let Ok(constellation_api::Response::Status(report)) =
                constellation_api::call(sock, &constellation_api::Request::Status).await
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
    let sock = entry.state_dir.join(constellation_api::SOCKET_NAME);
    // A running daemon holds `fjall`'s single-process lock on `meta.db`
    // (unlike the old SQLite/WAL engine, which tolerated this direct
    // open concurrently), so check aliveness first: if it answers at
    // all it has necessarily already claimed a node id, and opening the
    // store directly here would otherwise fail with `Locked` and be
    // swallowed by `.ok()`, silently skipping the self-leave below.
    let daemon_alive = matches!(
        constellation_api::call(&sock, &constellation_api::Request::Ping).await,
        Ok(constellation_api::Response::Pong)
    );
    let node_id_claimed = daemon_alive
        || (db_path.exists() && {
            Meta::open(&db_path)
                .ok()
                .and_then(|m| m.kv_get("node_id").ok().flatten())
                .is_some()
        });
    if node_id_claimed {
        match constellation_api::call(
            &sock,
            &constellation_api::Request::Leave {
                node_id: None,
                force,
            },
        )
        .await
        {
            Ok(constellation_api::Response::Ok { detail }) => {
                println!("{detail}");
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                while sock.exists() && std::time::Instant::now() < deadline {
                    if force {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
            Ok(constellation_api::Response::Error { message }) => {
                if !force {
                    bail!("leave failed: {message}");
                }
                eprintln!("leave failed (continuing: --force): {message}");
            }
            Ok(other) => bail!("unexpected response: {other:?}"),
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
                        use std::os::fd::AsRawFd;
                        let held = unsafe {
                            libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0
                        };
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
        let sock = entry.state_dir.join(constellation_api::SOCKET_NAME);
        let live: Vec<constellation_api::MountInfo> = rt
            .block_on(constellation_api::call(
                &sock,
                &constellation_api::Request::MountList,
            ))
            .ok()
            .and_then(|resp| match resp {
                constellation_api::Response::Mounts { mounts } => Some(mounts),
                _ => None,
            })
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
    state_dir: Option<PathBuf>,
    verify_only: bool,
) -> Result<gc::GcReport> {
    let backend = backend::open_backend(s3).await?;
    let plain = ChunkStore::new(backend.clone());
    let fsmeta = plain.load_fs().await?;
    let keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(fsmeta.unlock(&secret)?)
    } else {
        None
    };
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
    let sock = dir.join(constellation_api::SOCKET_NAME);
    match constellation_api::call(&sock, &constellation_api::Request::GcRun { verify_only }).await {
        Ok(constellation_api::Response::GcReport { report }) => {
            return Ok(serde_json::from_value(report)?);
        }
        Ok(constellation_api::Response::Error { message }) => {
            bail!("gc failed in the running daemon: {message}");
        }
        Ok(other) => bail!("unexpected response from running daemon: {other:?}"),
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
    state_dir: Option<PathBuf>,
    repair: bool,
    force_release: Option<&str>,
) -> Result<fsck::FsckReport> {
    let backend = backend::open_backend(s3).await?;
    let plain = ChunkStore::new(backend.clone());
    let fsmeta = plain.load_fs().await?;
    let keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(fsmeta.unlock(&secret)?)
    } else {
        None
    };
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
    let sock = dir.join(constellation_api::SOCKET_NAME);
    match constellation_api::call(
        &sock,
        &constellation_api::Request::FsckRun {
            repair,
            force_release: force_release.map(str::to_string),
        },
    )
    .await
    {
        Ok(constellation_api::Response::FsckReport { report }) => {
            return Ok(serde_json::from_value(report)?);
        }
        Ok(constellation_api::Response::Error { message }) => {
            bail!("fsck failed in the running daemon: {message}");
        }
        Ok(other) => bail!("unexpected response from running daemon: {other:?}"),
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

fn remove_live_subtree(meta: &Meta, path: &str) -> Result<()> {
    let ino = meta
        .resolve_path(path)?
        .with_context(|| format!("clone path {path} disappeared"))?;
    fn clear(meta: &Meta, ino: u64) -> Result<()> {
        for entry in constellation_meta::MetaStore::readdir(meta, ino)? {
            if entry.kind == constellation_fs_core::InodeKind::Dir {
                clear(meta, entry.ino)?;
                constellation_meta::MetaStore::rmdir(meta, ino, &entry.name)?;
            } else {
                constellation_meta::MetaStore::unlink(meta, ino, &entry.name)?;
            }
        }
        Ok(())
    }
    clear(meta, ino)?;
    let parent = meta
        .parent_of(ino)?
        .context("ephemeral clone cannot be the filesystem root")?;
    let name = path
        .rsplit('/')
        .find(|part| !part.is_empty())
        .context("ephemeral clone has no basename")?;
    constellation_meta::MetaStore::rmdir(meta, parent, name)?;
    Ok(())
}

/// Bridges the P2P layer to the daemon's sync task.
///
/// Both directions are latency-only. A `SegmentPublished` hint just
/// nudges the syncer, which would have polled anyway; a `LeaseRequest`
/// asks the sync task to flush and release, and S3's CAS remains the
/// authority for who actually holds the lease.
/// How long an epoch proposal's member-side S3 probe may take: a member
/// whose probe has not succeeded by then is treated as cut from S3 (and
/// may join, today's behaviour). Well inside the proposer's wait for the
/// answer (`epoch::PROPOSE_REQUEST_TIMEOUT`, 2 s), so a probe that hangs
/// in a real outage never turns the ack into a timeout — which would stop
/// the epoch from forming in exactly the outage it is for.
const EPOCH_MEMBER_S3_PROBE: std::time::Duration = std::time::Duration::from_millis(300);

struct P2pBridge {
    node_id: u64,
    nudge: tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    epochs: std::sync::Arc<epoch::EpochManager>,
    /// The bucket, for an epoch proposal's member-side S3 probe.
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    coop: std::sync::Arc<crate::coop::Coop>,
    placement: std::sync::Arc<placement::Placement>,
}

impl P2pBridge {
    /// Plan 30 §M10's member-side S3 probe: one lease GET, bounded by
    /// [`EPOCH_MEMBER_S3_PROBE`]. A member whose own rounds are failing at
    /// S3 is in the outage and answers at once (a probe would only delay
    /// an epoch's formation, eating into the carried lease's window).
    async fn probe_s3(&self) -> bool {
        !self.epochs.s3_failing() && {
            let leases = constellation_store_s3::LeaseStore::new(
                self.store.clone(),
                constellation_store_s3::log::PARTITION,
                constellation_store_s3::LeaseMode::Cas,
            );
            matches!(
                tokio::time::timeout(EPOCH_MEMBER_S3_PROBE, leases.get()).await,
                Ok(Ok(_))
            )
        }
    }
}

impl constellation_net::PeerService for P2pBridge {
    /// EC2 follow-up 3c, as fixed for `epoch-member-lost`: a would-be
    /// proposer asks (`PingS3`), and this member probes S3 now — the same
    /// bounded lease GET a proposal's member rule uses — rather than
    /// trusting a last-answer time: a follower on the holder's log stream
    /// may not have made an S3 request for seconds, and in a real bucket
    /// outage its stale "yes" delayed the epoch past the holder's usable
    /// lease (it formed carrying nothing, and every write failed).
    fn s3_probe(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(self.probe_s3())
    }

    fn segment_published(&self, part: &str, seq: u64, epoch: u64) {
        tracing::debug!(part, seq, epoch, "peer published a segment");
        // The core decides: a node following the holder's log stream has
        // it (or will) and ignores the hint; any other node tails now.
        let _ = self
            .nudge
            .send(fusefs::SyncRequest::SegmentHint { seq, epoch });
    }

    fn log_subscribe(
        &self,
        requester: u64,
        req_id: u64,
        from: u64,
    ) -> Option<tokio::sync::mpsc::Receiver<constellation_net::LogEvent>> {
        // The driver queues into `sink` (bounded in frames and in bytes)
        // and never waits; this relay hands frames to the stream writer
        // one at a time, so `queued_bytes` counts exactly what is waiting
        // for the subscriber.
        let (sink, mut queue) = tokio::sync::mpsc::channel(authority_driver::log_stream_queue());
        let (writer_tx, writer_rx) = tokio::sync::mpsc::channel(1);
        let queued_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        self.nudge
            .send(fusefs::SyncRequest::LogSubscribe {
                requester,
                req: req_id,
                from,
                sink,
                queued_bytes: queued_bytes.clone(),
            })
            .ok()?;
        tokio::spawn(async move {
            while let Some(event) = queue.recv().await {
                if let constellation_net::LogEvent::Frame {
                    segment: Some((_, bytes)),
                    ..
                } = &event
                {
                    queued_bytes
                        .fetch_sub(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                if writer_tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        Some(writer_rx)
    }

    fn lease_requested(
        &self,
        part: String,
        requester: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let declined = constellation_net::Payload::LeaseHandoff {
                part: part.clone(),
                epoch: 0,
                released: false,
                etag: None,
                head_seq: None,
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::HandOff {
                    requester,
                    reply: tx,
                })
                .is_err()
            {
                return declined;
            }
            match rx.await {
                Ok(Some(handed)) => {
                    tracing::info!(
                        part,
                        requester,
                        epoch = handed.epoch,
                        "handed the lease to a peer"
                    );
                    constellation_net::Payload::LeaseHandoff {
                        part,
                        epoch: handed.epoch,
                        released: true,
                        etag: handed.etag,
                        head_seq: handed.head_seq,
                    }
                }
                // Not ours, flush failed, or the task went away: the
                // requester falls back to the S3 path, which is always
                // correct — it just costs the TTL wait.
                _ => declined,
            }
        })
    }

    fn deleg_backup_append_requested(
        &self,
        from: u64,
        req_id: u64,
        gen: u64,
        txs: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let sealed = constellation_net::Payload::DelegBackupAck {
                req_id,
                gen,
                acked: 0,
                sealed: true,
            };
            let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::DelegateTx>>(&txs) else {
                return sealed;
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::PeerDelegBackupAppend {
                    from,
                    gen,
                    txs,
                    reply,
                })
                .is_err()
            {
                return sealed;
            }
            match receive.await {
                Ok((acked, is_sealed)) => constellation_net::Payload::DelegBackupAck {
                    req_id,
                    gen,
                    acked,
                    sealed: is_sealed,
                },
                Err(_) => sealed,
            }
        })
    }

    fn deleg_seal_requested(
        &self,
        root: u64,
        req_id: u64,
        gen: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let none = constellation_net::Payload::DelegSealed {
                req_id,
                gen,
                sealed: false,
                txs: Vec::new(),
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::PeerDelegSeal { root, gen, reply })
                .is_err()
            {
                return none;
            }
            match receive.await {
                Ok((sealed, txs)) => constellation_net::Payload::DelegSealed {
                    req_id,
                    gen,
                    sealed,
                    txs: postcard::to_allocvec(&txs).unwrap_or_default(),
                },
                Err(_) => none,
            }
        })
    }

    fn epoch_proposed(
        &self,
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        proposer: u64,
        epoch_slack: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            // Plan 30 §M10 (the member rule, see `handle_propose_checked`):
            // probe S3 now — one lease GET, bounded — rather than trust a
            // last-success time that a follower on the holder's log stream
            // may not have refreshed for seconds.
            // A member whose own rounds are failing at S3 is in the
            // outage: it joins at once (a probe would only delay the
            // formation, eating into the carried lease's usable window).
            let reaches_s3 = self.probe_s3().await;
            self.epochs.handle_propose_checked(
                epoch_id,
                members,
                base,
                proposer,
                epoch_slack,
                reaches_s3,
            )
        })
    }

    fn epoch_aborted(&self, epoch_id: String, proposer: u64) {
        if self.epochs.handle_abort(&epoch_id, proposer) {
            let _ = self.nudge.send(fusefs::SyncRequest::EpochChanged);
        }
    }

    fn epoch_activated(&self, activation: constellation_net::EpochActivation) {
        self.epochs.handle_activate(activation);
        let _ = self.nudge.send(fusefs::SyncRequest::EpochChanged);
        let _ = self.nudge.send(fusefs::SyncRequest::Nudge);
    }

    fn promise_requested(
        &self,
        requester: u64,
        req_id: u64,
        expires_unix_ms: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let refuse = constellation_net::Payload::PromiseReply {
                req_id,
                until: None,
                epoch_slack: 0,
            };
            if crate::fault::p2p_denied(requester) {
                // Fault injection: the link is cut; no answer.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                return refuse;
            }
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::PeerPromiseRequest {
                    requester,
                    expires_unix_ms,
                    reply,
                })
                .is_err()
            {
                return refuse;
            }
            match receive.await {
                Ok((until, epoch_slack)) => constellation_net::Payload::PromiseReply {
                    req_id,
                    until,
                    epoch_slack,
                },
                Err(_) => refuse,
            }
        })
    }

    fn cache_digest(&self, digest: constellation_net::DigestSnapshot) {
        self.coop.apply_digest(digest);
    }

    fn cache_digest_delta(&self, delta: constellation_net::DigestDelta) {
        self.coop.apply_delta(delta);
    }

    fn cache_summary(&self, node_id: u64, summary: constellation_net::reconcile::Summary) {
        self.coop.apply_summary(node_id, summary);
    }

    fn cache_set_delta(&self, node_id: u64, delta: constellation_net::reconcile::Delta) {
        self.coop.apply_set_delta(node_id, delta);
    }

    fn reconcile_requested(
        &self,
        queries: Vec<constellation_net::reconcile::Query>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move { self.coop.reconcile_reply(queries).await })
    }

    fn serve_chunk(
        &self,
        hash: [u8; 32],
        from_hex: String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<u8>, constellation_net::ChunkDecline>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move { self.coop.serve_chunk(hash, &from_hex).await })
    }

    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn chunks_durable(
        &self,
        from: u64,
        hashes: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let (reply, done) = tokio::sync::oneshot::channel();
            let hashes = hashes
                .into_iter()
                .map(constellation_fs_core::ChunkHash)
                .collect();
            if self
                .nudge
                .send(fusefs::SyncRequest::ChunksDurable {
                    from,
                    hashes,
                    reply,
                })
                .is_ok()
            {
                let _ = done.await;
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn mutate_requested(
        &self,
        part: String,
        requester: u64,
        req_id: u64,
        _epoch_seen: u64,
        op: Vec<u8>,
        rid: (u64, u32, u64),
        acked_through: u64,
        deps: Vec<u8>,
        pending: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let _ = part;
            let pending = pending
                .into_iter()
                .map(constellation_fs_core::ChunkHash)
                .collect();
            let rid = constellation_meta::Rid {
                node: rid.0,
                incarnation: rid.1,
                seq: rid.2,
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            let started = std::time::Instant::now();
            tracing::trace!(target: "constellation::fwd", rid = rid.seq, rnode = rid.node, "mutate request queued");
            let (outcome, base, position, gen) = if self
                .nudge
                .send(fusefs::SyncRequest::Mutate {
                    requester,
                    op,
                    rid,
                    acked_through,
                    deps,
                    pending,
                    reply,
                })
                .is_ok()
            {
                receive.await.unwrap_or((
                    constellation_meta::MutateOutcome::Busy,
                    None,
                    constellation_meta::Position::ZERO,
                    0,
                ))
            } else {
                (
                    constellation_meta::MutateOutcome::Busy,
                    None,
                    constellation_meta::Position::ZERO,
                    0,
                )
            };
            tracing::trace!(target: "constellation::fwd", rid = rid.seq, rnode = rid.node, "mutate reply taken");
            tracing::trace!(
                requester,
                service_us = started.elapsed().as_micros() as u64,
                "forwarded mutate served"
            );
            constellation_net::Payload::MutateReply {
                req_id,
                outcome: outcome.to_postcard().unwrap_or_default(),
                base,
                position_seq: position.seq,
                position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
                position_streams: position.streams_wire(),
                gen,
            }
        })
    }

    fn delegate_stream_requested(
        &self,
        from: u64,
        req_id: u64,
        gen: u64,
        txs: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let refuse = constellation_net::Payload::DelegateStreamAck {
                req_id,
                gen,
                through: 0,
                refused: true,
            };
            let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::DelegateTx>>(&txs) else {
                return refuse;
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::PeerDelegateStream {
                    from,
                    gen,
                    txs,
                    reply,
                })
                .is_err()
            {
                return refuse;
            }
            match receive.await {
                Ok((through, refused)) => constellation_net::Payload::DelegateStreamAck {
                    req_id,
                    gen,
                    through,
                    refused,
                },
                Err(_) => refuse,
            }
        })
    }

    fn deleg_renew_requested(
        &self,
        from: u64,
        req_id: u64,
        gen: u64,
        backup: u64,
        stream_head: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let (ttl_ms, locks, lock_grace_ms, lock_floor) = if self
                .nudge
                .send(fusefs::SyncRequest::PeerDelegRenew {
                    from,
                    gen,
                    backup,
                    stream_head,
                    reply,
                })
                .is_ok()
            {
                receive.await.unwrap_or_default()
            } else {
                Default::default()
            };
            constellation_net::Payload::DelegRenewed {
                req_id,
                gen,
                ttl_ms,
                locks: crate::locks::grants_wire(&locks),
                lock_grace_ms,
                lock_floor: crate::locks::floor_wire(&lock_floor),
            }
        })
    }

    fn deleg_recall_requested(
        &self,
        root: u64,
        req_id: u64,
        dir: u64,
        gen: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let (through, locks) = if self
                .nudge
                .send(fusefs::SyncRequest::PeerDelegRecall {
                    root,
                    dir,
                    gen,
                    reply,
                })
                .is_ok()
            {
                receive.await.unwrap_or_default()
            } else {
                Default::default()
            };
            constellation_net::Payload::DelegRecalled {
                req_id,
                gen,
                through,
                locks: crate::locks::grants_wire(&locks.grants),
                lock_floor: crate::locks::floor_wire(&locks.floor),
            }
        })
    }

    fn chunk_handoff_requested(
        &self,
        requester: u64,
        req_id: u64,
        hashes: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(fusefs::SyncRequest::AcceptHandoff {
                    requester,
                    hashes: hashes
                        .into_iter()
                        .map(constellation_fs_core::ChunkHash)
                        .collect(),
                    reply,
                })
                .is_ok();
            let uploaded = sent && receive.await.unwrap_or(false);
            constellation_net::Payload::ChunkHandoffReply { req_id, uploaded }
        })
    }

    fn read_index_requested(
        &self,
        requester: u64,
        req_id: u64,
        ino: u64,
        dir: bool,
        name: Option<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(fusefs::SyncRequest::PeerReadIndex {
                    requester,
                    ino,
                    dir,
                    name,
                    reply,
                })
                .is_ok();
            let outcome = if sent {
                receive
                    .await
                    .unwrap_or(constellation_authority::ReadIndexOutcome::Busy)
            } else {
                constellation_authority::ReadIndexOutcome::Busy
            };
            use constellation_authority::ReadIndexOutcome as O;
            let (status, holder, position, grant) = match outcome {
                O::Ok { position, grant } => {
                    (0, 0, position, grant.map(|g| (g.id, g.ttl_ms, g.epoch)))
                }
                O::NotHolder { holder } => (1, holder, constellation_meta::Position::ZERO, None),
                O::Busy => (2, 0, constellation_meta::Position::ZERO, None),
            };
            constellation_net::Payload::ReadIndexReply {
                req_id,
                status,
                holder,
                position_seq: position.seq,
                position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
                grant,
                position_streams: position.streams_wire(),
            }
        })
    }

    fn backup_append_requested(
        &self,
        holder: u64,
        req_id: u64,
        epoch: u64,
        config_version: u64,
        from: u64,
        txs: Vec<u8>,
        through: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            // `sealed` is the safe refusal: the holder never counts this
            // node again until a reconfiguration (a node that cannot
            // persist the append must not be credited).
            let refuse = constellation_net::Payload::BackupAck {
                req_id,
                epoch,
                acked: 0,
                sealed: true,
            };
            if crate::fault::p2p_denied(holder) {
                // Fault injection: the link is cut; no answer.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                return refuse;
            }
            let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::BackupTx>>(&txs) else {
                return refuse;
            };
            tracing::trace!(target: "constellation::fwd", from, "append queued");
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::PeerBackupAppend {
                    holder,
                    epoch,
                    config_version,
                    from,
                    txs,
                    through,
                    reply,
                })
                .is_err()
            {
                return refuse;
            }
            let out = match receive.await {
                Ok((acked, sealed)) => constellation_net::Payload::BackupAck {
                    req_id,
                    epoch,
                    acked,
                    sealed,
                },
                Err(_) => refuse,
            };
            tracing::trace!(target: "constellation::fwd", from, "append reply taken");
            out
        })
    }

    fn stream_ahead(&self, from: u64, epoch: u64, base: u64, txs: Vec<u8>) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::BackupTx>>(&txs) else {
            return;
        };
        let _ = self.nudge.send(fusefs::SyncRequest::PeerStreamAhead {
            from,
            epoch,
            base,
            txs,
        });
    }

    fn lock_requested(
        &self,
        requester: u64,
        req_id: u64,
        ino: u64,
        exclusive: bool,
        blocking: bool,
        sent: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let outcome = if self
                .nudge
                .send(fusefs::SyncRequest::PeerLockRequest {
                    requester,
                    ino,
                    mode: crate::locks::mode_of(exclusive),
                    blocking,
                    sent,
                    reply,
                })
                .is_ok()
            {
                receive
                    .await
                    .unwrap_or(constellation_authority::LockOutcome::Busy)
            } else {
                constellation_authority::LockOutcome::Busy
            };
            constellation_net::Payload::LockReply {
                req_id,
                outcome: crate::locks::outcome_wire(&outcome),
            }
        })
    }

    fn lock_recall_requested(
        &self,
        owner: u64,
        req_id: u64,
        ino: u64,
        grant: (u64, u64),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(fusefs::SyncRequest::PeerLockRecall {
                    owner,
                    ino,
                    grant: crate::locks::grant_of(grant),
                    reply,
                })
                .is_ok();
            // A dropped reply (daemon shutting down) acks nothing: the
            // owner outwaits the grant.
            if sent && receive.await.is_ok() {
                return constellation_net::Payload::LockRecalled { req_id };
            }
            constellation_net::Payload::LockRecalled { req_id: 0 }
        })
    }

    fn lock_renew_requested(
        &self,
        from: u64,
        req_id: u64,
        entries: Vec<constellation_net::LockRenewWire>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(fusefs::SyncRequest::PeerLockRenew {
                    from,
                    entries: crate::locks::renew_entries_of(entries),
                    reply,
                })
                .is_ok();
            let results = if sent { receive.await.ok() } else { None };
            match results {
                Some(results) => constellation_net::Payload::LockRenewed {
                    req_id,
                    results: crate::locks::renew_results_wire(&results),
                },
                // Unanswered: a reply for no request, which the renewer
                // treats as a failed renewal and retries.
                None => constellation_net::Payload::LockRenewed {
                    req_id: 0,
                    results: Vec::new(),
                },
            }
        })
    }

    fn lock_test_requested(
        &self,
        requester: u64,
        req_id: u64,
        ino: u64,
        exclusive: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let outcome = if self
                .nudge
                .send(fusefs::SyncRequest::PeerLockTest {
                    requester,
                    ino,
                    mode: crate::locks::mode_of(exclusive),
                    reply,
                })
                .is_ok()
            {
                receive
                    .await
                    .unwrap_or(constellation_authority::LockTestOutcome::NotOwner { owner: 0 })
            } else {
                constellation_authority::LockTestOutcome::NotOwner { owner: 0 }
            };
            constellation_net::Payload::LockTestReply {
                req_id,
                outcome: crate::locks::test_outcome_wire(outcome),
            }
        })
    }

    fn lock_granted(
        &self,
        from: u64,
        ino: u64,
        sent: i64,
        outcome: constellation_net::LockOutcomeWire,
    ) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let _ = self.nudge.send(fusefs::SyncRequest::PeerLockGranted {
            from,
            ino,
            sent,
            outcome: crate::locks::outcome_of(outcome),
        });
    }

    fn lock_released(&self, from: u64, ino: u64, grant: (u64, u64), position: &[u8]) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let _ = self.nudge.send(fusefs::SyncRequest::PeerLockReleased {
            from,
            ino,
            grant: crate::locks::grant_of(grant),
            position: constellation_meta::Position::from_postcard(position),
        });
    }

    fn lock_mirror(&self, from: u64, ver: u64, grants: Vec<u8>, floor: Vec<u8>) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let _ = self.nudge.send(fusefs::SyncRequest::PeerLockMirror {
            from,
            ver,
            grants: crate::locks::grants_of(&grants),
            floor: crate::locks::floor_of(&floor),
        });
    }

    fn read_recall_requested(
        &self,
        holder: u64,
        req_id: u64,
        ino: u64,
        grant: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::PeerRecall {
                    holder,
                    ino,
                    grant,
                    reply,
                })
                .is_ok()
            {
                // Acked only once the delegation is no longer honoured;
                // a dropped reply (daemon shutting down) acks nothing.
                if receive.await.is_err() {
                    return constellation_net::Payload::ReadRecalled { req_id: 0 };
                }
                return constellation_net::Payload::ReadRecalled { req_id };
            }
            constellation_net::Payload::ReadRecalled { req_id: 0 }
        })
    }

    fn lease_offered(&self, _part: String, epoch: u64) {
        let _ = self.nudge.send(fusefs::SyncRequest::ClaimOffer { epoch });
    }

    fn peer_rtts(&self, node_id: u64, rtts: Vec<(u64, u16)>) {
        self.placement.note_peer_rtts(node_id, rtts);
    }
}

/// Start the P2P fast path, or return a disabled handle.
///
/// Everything here is best-effort by design (plan 02 / DESIGN.md §8): a
/// missing node key, an unbindable endpoint, or an unreachable gossip
/// topic all degrade to the S3 polling path rather than failing the
/// mount. `CONSTELLATION_P2P=off` skips it entirely.
async fn start_p2p(
    fsmeta: &constellation_store_s3::FsMeta,
    e2e_keys: Option<&constellation_store_s3::SharedE2eKeys>,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    node_id: u64,
) -> constellation_net::Peers {
    if !constellation_net::enabled() {
        tracing::info!("P2P disabled by CONSTELLATION_P2P; using the S3 path only");
        return constellation_net::Peers::disabled();
    }
    let key_path = constellation_net::identity::default_key_path();
    let (key, generated) = match constellation_net::load_or_create(&key_path) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, path = %key_path.display(),
                "no usable node key; running without the P2P fast path");
            return constellation_net::Peers::disabled();
        }
    };
    if generated {
        tracing::info!(path = %key_path.display(), "generated a host node key");
    }
    // E2E filesystems seed the topic from the keyring (never on S3 in the
    // clear); non-E2E uses `meta.json`. A pre-secret filesystem with no
    // seed at all falls back to the UUID.
    let e2e_seed = e2e_keys.map(|keys| *keys.gossip_secret());
    let seed = e2e_seed.or_else(|| fsmeta.gossip_seed());
    let topic = constellation_net::topic_for(seed.as_ref(), &fsmeta.uuid.to_string());
    if seed.is_none() {
        tracing::info!(
            "filesystem predates gossip_secret; deriving the topic from its UUID \
             (weaker: the UUID is not a secret)"
        );
    }
    let p2p = match constellation_net::P2p::spawn(key, topic).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "could not bind the P2P endpoint; using the S3 path only");
            return constellation_net::Peers::disabled();
        }
    };
    let relay = p2p.relay_label().to_string();
    let addr = p2p.addr();
    let pubkey = p2p.pubkey_hex();
    let peers = constellation_net::Peers::new(p2p, node_id);
    // Publish how peers reach us, then learn about them.
    match serde_json::to_value(&addr) {
        Ok(addr_json) => {
            if let Err(e) = constellation_store_s3::publish_p2p(
                store.clone(),
                node_id,
                &pubkey,
                addr_json,
                env!("CONSTELLATION_VERSION"),
            )
            .await
            {
                tracing::warn!(error = %e, "could not publish our P2P address; peers cannot dial us");
            }
        }
        Err(e) => tracing::warn!(error = %e, "could not serialize our P2P address"),
    }
    let _ = refresh_peers(&peers, store, None).await;
    tracing::info!(
        node_id,
        peers = peers.snapshot().len(),
        %relay,
        "P2P fast path ready"
    );
    peers
}

/// Re-read the registry into the peer directory and allowlist.
async fn refresh_peers(
    peers: &constellation_net::Peers,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    epochs: Option<&epoch::EpochManager>,
) -> Option<constellation_store_s3::RegistryScan> {
    if !peers.is_enabled() {
        return None;
    }
    // One LIST of the registry serves both reads (EC2 finding R2-2: they
    // used to list and read every record separately, every 5 s). The
    // peer directory is tolerant of unreadable records (a peer we cannot
    // dial only loses its fast path); the epoch roster is not, so it
    // takes the scan's fail-closed view (`crate::epoch::apply_roster`).
    let scan = constellation_store_s3::registry_scan(store.as_ref()).await;
    if let Some(epochs) = epochs {
        crate::epoch::apply_roster(
            epochs,
            match &scan {
                Ok(scan) => scan.roster(),
                Err(e) => Err(constellation_store_s3::StoreError::Registry(format!(
                    "registry unreachable: {e}"
                ))),
            },
            scan.is_err(),
        );
    }
    let scan = scan.ok();
    match scan.as_ref().map(|scan| scan.live()).ok_or(()) {
        Ok(nodes) => {
            let records: Vec<constellation_net::PeerEnrollment> = nodes
                .into_iter()
                .filter_map(|n| {
                    Some(constellation_net::PeerEnrollment {
                        node_id: n.node_id,
                        pubkey_hex: n.pubkey?,
                        addr_json: n.p2p_addr?,
                        hostname: n.hostname,
                        version: n.version.unwrap_or_default(),
                        created_unix: n.created_unix,
                        p2p_updated_unix: n.p2p_updated_unix,
                        ro: n.ro,
                    })
                })
                .collect();
            peers.refresh_registry(records);
        }
        Err(()) => {
            tracing::debug!("registry refresh failed; keeping the cached peer set")
        }
    }
    scan
}

/// Send one control-API request to a running mount and print the answer.
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
            rt.block_on(control_call(&dir, constellation_api::Request::PruneList))
        }
        PruneCommand::Run {
            target,
            state_dir,
            path,
            dry_run,
        } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(control_call(
                &dir,
                constellation_api::Request::PruneRun { path, dry_run },
            ))
        }
        PruneCommand::Status { target, state_dir } => {
            let (_, dir) = resolve_target(&target, state_dir)?;
            rt.block_on(async {
                let sock = dir.join(constellation_api::SOCKET_NAME);
                match constellation_api::call_bounded(
                    &sock,
                    &constellation_api::Request::Status,
                    daemon_lock::control_timeout(),
                )
                .await?
                {
                    constellation_api::Response::Status(report) => {
                        println!("{}", serde_json::to_string_pretty(&report.prune)?);
                        Ok(())
                    }
                    constellation_api::Response::Error { message } => bail!(message),
                    other => bail!("unexpected response: {other:?}"),
                }
            })
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
        return match err.raw_os_error() {
            Some(libc::ENODATA) => Ok(None),
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

async fn control_call(state_dir: &std::path::Path, req: constellation_api::Request) -> Result<()> {
    let sock = state_dir.join(constellation_api::SOCKET_NAME);
    match constellation_api::call(&sock, &req).await? {
        constellation_api::Response::Ok { detail } => {
            println!("{detail}");
            Ok(())
        }
        constellation_api::Response::Pins { pins } => {
            if pins.is_empty() {
                println!("no pinned subtrees");
            } else {
                println!("{}", serde_json::to_string_pretty(&pins)?);
            }
            Ok(())
        }
        constellation_api::Response::Designations { designations } => {
            if designations.is_empty() {
                println!("no active designations");
            } else {
                println!("{}", serde_json::to_string_pretty(&designations)?);
            }
            Ok(())
        }
        constellation_api::Response::Snapshots { snapshots } => {
            println!("{}", serde_json::to_string_pretty(&snapshots)?);
            Ok(())
        }
        constellation_api::Response::Refs { hashes } => {
            for hash in hashes {
                println!("{hash}");
            }
            Ok(())
        }
        constellation_api::Response::Status(status) => {
            println!("{}", serde_json::to_string_pretty(&status)?);
            Ok(())
        }
        constellation_api::Response::Inspection { entry } => {
            println!("{}", serde_json::to_string_pretty(&entry)?);
            Ok(())
        }
        constellation_api::Response::CacheEntries { entries } => {
            println!("{}", serde_json::to_string_pretty(&entries)?);
            Ok(())
        }
        constellation_api::Response::Logs { lines } => {
            for line in lines {
                println!("{line}");
            }
            Ok(())
        }
        constellation_api::Response::Quota {
            max_bytes,
            used_bytes,
        } => {
            match max_bytes {
                Some(cap) => println!("quota: {used_bytes} / {cap} bytes used"),
                None => println!("quota: {used_bytes} bytes used (unlimited)"),
            }
            Ok(())
        }
        constellation_api::Response::PruneRoots { roots } => {
            if roots.is_empty() {
                println!("no prune policies");
            } else {
                println!("{}", serde_json::to_string_pretty(&roots)?);
            }
            Ok(())
        }
        constellation_api::Response::Error { message } => bail!("{message}"),
        other => bail!("unexpected response: {other:?}"),
    }
}

/// Drain `Meta::pending_uploads()` — the durable not-yet-uploaded
/// set (plan 07 step 1) — rather than `DiskCache::dirty_chunks()`,
/// which cannot survive a crash (`DiskCache::rescan` legitimately marks
/// everything `Clean`; only the meta journal's transaction-coupled
/// table knows what still owes S3 a PUT).
///
/// A pending row whose chunk is missing from the local cache is
/// unrecoverable content (a torn-disk case, impossible on a clean crash
/// given the write ordering `flush_inode` uses): log loudly, leave the
/// row, and refuse rather than silently drop it — returning an error
/// here already makes every caller treat the round as failed and skip
/// shipping (see `run_managed_sync_round`'s existing epoch-propose-on-
/// failure path), which is exactly the "journal must wait" behavior.
use constellation_upload_concurrency::{AdaptiveConcurrency, ConcurrencyGate, ConcurrencyPermit};

/// Ceiling for the adaptive search and for a user-pinned override alike.
/// Bounds both real S3 concurrency and (via [`ConcurrencyGate`]) worst-case
/// pending-upload memory: at most this many chunk buffers are ever held
/// at once regardless of how many rows `pending_upload` has queued.
const UPLOAD_CONCURRENCY_HARD_MAX: usize = 128;
/// Plan 30 §M7: upload slots above the adaptive target that small inode
/// drains may use, and what counts as small. Without them an `fsync` of a
/// one-chunk marker file written during a write-back burst waited for
/// most of the burst's upload backlog (2.5 s of a 3.4 s pass on floci):
/// the pass queues up to the hard maximum of uploads on the gate, and a
/// release wakes every waiter at once.
const PRIORITY_UPLOAD_RESERVE: usize = 4;
const PRIORITY_DRAIN_CHUNKS: u64 = 4;

/// See docs/explanation/DESIGN.md §5b step 2 / `docs/plans/v1/done/08-p5b-streaming-writeback.md`.
/// A durable pending-upload queue in SQLite is drained by a bounded pool;
/// the pool costs two things once it exists (dedup-probe RTT and the
/// create-vs-overwrite decision), both handled by `put_mode` below.
///
/// Concurrency itself is adaptive by default
/// (`constellation_upload_concurrency::AdaptiveConcurrency`): a single
/// upload's latency is dominated by RTT to the bucket region, so a
/// client far from the bucket but sitting on a fat pipe (e.g. a home
/// connection in the EU against a `us-west-2` bucket) needs a lot more
/// parallelism than one on a thin or nearby link to fill that
/// bandwidth-delay product, and a fixed pool size tuned for one path is
/// wrong for the other. `CONSTELLATION_UPLOAD_CONCURRENCY` still pins a
/// fixed value for anyone who wants to opt out of the search entirely.
///
/// The policy and gate live in their own crate
/// (`crates/upload-concurrency`) so `bench/uploadbench` can drive the
/// exact production algorithm against a synthetic or live S3 target,
/// rather than a reimplementation that could drift from what ships here.
struct UploadRuntime {
    gate: ConcurrencyGate,
    controller: Option<std::sync::Mutex<AdaptiveConcurrency>>,
    max_concurrency: usize,
    create_if_absent: bool,
    probe: std::sync::Mutex<writeback::ProbePolicy>,
    decisions: std::sync::atomic::AtomicU64,
    coop: Option<std::sync::Arc<crate::coop::Coop>>,
    existence: std::sync::Arc<crate::existence::Existence>,
    /// Chunks a drain is uploading right now. A sync round's drain and a
    /// flush's `drain_inode` overlap freely; without this, both read the
    /// same pending row and both PUT it (plan 30 §M11's
    /// `delegated-subtrees` measured 1.3–1.7 chunk PUTs per file). The
    /// second drain leaves the row to the first and waits for its ack.
    in_flight: std::sync::Mutex<std::collections::HashSet<constellation_fs_core::ChunkHash>>,
    /// Chunks smaller than this skip `Probe` (see [`Self::put_mode`]).
    probe_min_bytes: u64,
    /// Chunks this node named in a forwarded manifest while they were
    /// still pending here (a `back` close), and the nodes it forwarded to:
    /// each is told once the chunk is up (`meta::store::remote`).
    forwarded: std::sync::Mutex<
        std::collections::HashMap<
            constellation_fs_core::ChunkHash,
            std::collections::BTreeSet<u64>,
        >,
    >,
    /// Reports owed, per node: forwarded chunks now durable.
    durable_reports:
        std::sync::Mutex<std::collections::HashMap<u64, Vec<constellation_fs_core::ChunkHash>>>,
    /// Since when reports to a node have not been delivered (see
    /// [`Self::requeue_report`]).
    report_attempts: std::sync::Mutex<std::collections::HashMap<u64, std::time::Instant>>,
    /// When this node next checks S3 itself for a chunk another node
    /// forwarded as pending (the fallback when its report never comes),
    /// and the current backoff.
    remote_polls: std::sync::Mutex<
        std::collections::HashMap<
            constellation_fs_core::ChunkHash,
            (std::time::Instant, std::time::Duration),
        >,
    >,
    /// Chunks another node reported durable recently, kept for a minute:
    /// its report can overtake the forward that names them (the report
    /// and the forward travel on different streams), and a forward whose
    /// chunks were already reported must not await them.
    reported: std::sync::Mutex<
        std::collections::HashMap<constellation_fs_core::ChunkHash, std::time::Instant>,
    >,
    /// Pending chunks this node found at mount: a crash may have lost
    /// whom it forwarded them to (`forwarded` is in memory), so once up
    /// they are reported to every peer — a node that awaits one acks it,
    /// the rest ignore the report.
    inherited: std::sync::Mutex<std::collections::HashSet<constellation_fs_core::ChunkHash>>,
    /// EC2 finding 1: when a chunk PUT (or existence probe) last
    /// completed on this node, unix ms — how a drain that is taking long
    /// tells a slow upload (progress) from an unreachable S3 (none), and
    /// only the latter hands its chunks to a peer.
    last_put_ms: std::sync::atomic::AtomicI64,
    /// EC2 finding 1: chunk handoffs (`authority_driver::ChunkHandoff`).
    handoff: HandoffStats,
}

/// The `durable_reports` key of a report owed to every peer.
const REPORT_TO_ALL: u64 = 0;

/// How long an undelivered durable report is retried.
const REPORT_RETRY: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a durable report is remembered for a forward it overtook.
const REPORTED_KEEP: std::time::Duration = std::time::Duration::from_secs(60);

/// First S3 check of a chunk another node forwarded as pending, if its
/// report has not come by then; the checks back off to [`REMOTE_POLL_MAX`].
const REMOTE_POLL_FIRST: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_POLL_MAX: std::time::Duration = std::time::Duration::from_secs(16);

/// `CONSTELLATION_REMOTE_CHUNK_WAIT_S` (default 60): how long a pass that
/// must leave nothing pending (a barrier, a forced publish, an unmount's
/// final flush) — and a reader that needs the bytes — waits for chunks
/// another node forwarded as pending before giving up on them.
pub(crate) fn remote_chunk_wait() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("CONSTELLATION_REMOTE_CHUNK_WAIT_S")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60),
    )
}

/// The chunks a forwarded `SetManifest` names that are still pending here
/// (`meta::store::remote`): empty for any other op, and for a close that
/// uploaded first (`--write-mode through`). A spilled manifest's chunk
/// list comes from the local cache, where this node's flush put it.
pub(crate) fn forwarded_pending_chunks(
    meta: &Meta,
    cache: &DiskCache,
    op: &constellation_meta::MutateOp,
) -> Vec<constellation_fs_core::ChunkHash> {
    let constellation_meta::MutateOp::SetManifest { ino, manifest, .. } = op else {
        return Vec::new();
    };
    let mut named = std::collections::BTreeSet::new();
    let spilled = match constellation_fs_core::Manifest::decode(manifest).map(|m| m.chunks) {
        Ok(constellation_fs_core::ChunkInfo::Inline(chunks)) => {
            named.extend(chunks.into_values());
            None
        }
        Ok(constellation_fs_core::ChunkInfo::Spilled(blob)) => Some(blob),
        // Undecodable: whatever is pending for the inode (below).
        Err(_) => Some(constellation_fs_core::ChunkHash([0; 32])),
    };
    if let Some(blob) = spilled {
        named.insert(blob);
        if let Ok(Some(bytes)) = cache.get(&blob) {
            if let Ok(list) = constellation_fs_core::manifest::decode_chunk_list(&bytes) {
                named.extend(list.into_values());
            }
        }
        // The list may not be readable here (the blob uploaded and
        // evicted): every chunk still pending for the inode counts too.
        // Rare (files past the inline limit) and a local scan.
        if let Ok(rows) = meta.pending_uploads() {
            named.extend(rows.into_iter().filter(|(_, i)| i == ino).map(|(h, _)| h));
        }
    }
    named
        .into_iter()
        .filter(|hash| meta.upload_pending_for_hash(hash).unwrap_or(true))
        .collect()
}

/// `CONSTELLATION_PROBE_MIN_BYTES` (default 256 KiB; `0` lets every
/// chunk probe): the size below which an upload always uses a
/// conditional create instead of a HEAD-first probe. 256 KiB is about
/// 2 ms at 1 Gbit/s — less than any S3 round trip it saves.
fn probe_min_bytes() -> u64 {
    std::env::var("CONSTELLATION_PROBE_MIN_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(256 * 1024)
}

/// EC2 finding 1: this node's chunk handoffs, for `status`.
#[derive(Default)]
struct HandoffStats {
    sent: std::sync::atomic::AtomicU64,
    ok: std::sync::atomic::AtomicU64,
    chunks: std::sync::atomic::AtomicU64,
    accepted: std::sync::atomic::AtomicU64,
    /// When a drain last needed a handoff (unix ms): while recent, and
    /// uploads still make no progress, the next drain hands off at once.
    last_needed_ms: std::sync::atomic::AtomicI64,
    /// A round's handoff of forwarded chunks is in flight.
    round_busy: std::sync::atomic::AtomicBool,
}

/// Releases a drain's claim on its chunks when it ends, however it ends.
struct InFlightClaim<'a> {
    upload: &'a UploadRuntime,
    hashes: Vec<constellation_fs_core::ChunkHash>,
}

impl Drop for InFlightClaim<'_> {
    fn drop(&mut self) {
        let mut in_flight = self.upload.in_flight.lock().unwrap();
        for hash in &self.hashes {
            in_flight.remove(hash);
        }
    }
}

impl UploadRuntime {
    fn new(
        create_if_absent: bool,
        coop: Option<std::sync::Arc<crate::coop::Coop>>,
        existence: std::sync::Arc<crate::existence::Existence>,
    ) -> Self {
        let max = std::env::var("CONSTELLATION_UPLOAD_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(UPLOAD_CONCURRENCY_HARD_MAX)
            .clamp(1, UPLOAD_CONCURRENCY_HARD_MAX);
        let fixed = std::env::var("CONSTELLATION_UPLOAD_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|n| n.clamp(1, max));
        let (initial, controller) = match fixed {
            Some(n) => (n, None),
            // Start conservatively (like TCP slow start) and let the
            // controller climb; an aggressive initial guess on a
            // constrained link just causes early retries/backoff.
            None => (
                4.min(max),
                Some(std::sync::Mutex::new(AdaptiveConcurrency::new(
                    4.min(max),
                    1,
                    max,
                ))),
            ),
        };
        debug_assert!(
            controller
                .as_ref()
                .is_none_or(|c| c.lock().unwrap().current() == initial),
            "gate and controller must start in agreement"
        );
        Self {
            gate: ConcurrencyGate::new(initial),
            controller,
            max_concurrency: max,
            create_if_absent,
            probe: std::sync::Mutex::new(writeback::ProbePolicy::default()),
            decisions: std::sync::atomic::AtomicU64::new(0),
            coop,
            existence,
            in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            probe_min_bytes: probe_min_bytes(),
            forwarded: Default::default(),
            durable_reports: Default::default(),
            report_attempts: Default::default(),
            remote_polls: Default::default(),
            reported: Default::default(),
            inherited: Default::default(),
            last_put_ms: std::sync::atomic::AtomicI64::new(0),
            handoff: HandoffStats::default(),
        }
    }

    /// EC2 finding 1: whether this node's S3 path has made no progress for
    /// `for_ms`: no chunk PUT and no S3 request of any kind has succeeded
    /// (`backend::last_s3_completion_ms`) in that long.
    pub(crate) fn uploads_stalled(&self, for_ms: i64) -> bool {
        let last = self
            .last_put_ms
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(backend::last_s3_completion_ms());
        constellation_store_s3::lease::now_unix_ms() - last >= for_ms
    }

    /// `hashes` went out in a manifest forwarded to `to` while still
    /// pending here.
    pub(crate) fn note_forwarded(&self, hashes: &[constellation_fs_core::ChunkHash], to: u64) {
        if hashes.is_empty() {
            return;
        }
        let mut forwarded = self.forwarded.lock().unwrap();
        for hash in hashes {
            forwarded.entry(*hash).or_default().insert(to);
        }
    }

    /// Another node reported `hashes` durable.
    pub(crate) fn note_reported(&self, hashes: &[constellation_fs_core::ChunkHash]) {
        let now = std::time::Instant::now();
        let mut reported = self.reported.lock().unwrap();
        reported.retain(|_, at| now.duration_since(*at) < REPORTED_KEEP);
        for hash in hashes {
            reported.insert(*hash, now);
        }
    }

    /// `hashes` without the ones another node reported durable recently.
    pub(crate) fn not_reported(
        &self,
        hashes: Vec<constellation_fs_core::ChunkHash>,
    ) -> Vec<constellation_fs_core::ChunkHash> {
        let reported = self.reported.lock().unwrap();
        if reported.is_empty() {
            return hashes;
        }
        hashes
            .into_iter()
            .filter(|hash| !reported.contains_key(hash))
            .collect()
    }

    /// The pending chunks found at mount (see `inherited`).
    pub(crate) fn inherit(
        &self,
        hashes: impl IntoIterator<Item = constellation_fs_core::ChunkHash>,
    ) {
        self.inherited.lock().unwrap().extend(hashes);
    }

    /// Whether any chunk forwarded as pending is still owed a report.
    pub(crate) fn has_forwarded(&self) -> bool {
        !self.forwarded.lock().unwrap().is_empty()
    }

    /// Chunks forwarded as pending that are still pending here, with the
    /// nodes they were forwarded to (EC2 finding 1: what a round hands to
    /// a peer when this node's S3 path is down).
    pub(crate) fn forwarded_pending(&self) -> Vec<(constellation_fs_core::ChunkHash, Vec<u64>)> {
        self.forwarded
            .lock()
            .unwrap()
            .iter()
            .map(|(h, nodes)| (*h, nodes.iter().copied().collect()))
            .collect()
    }

    /// `hash` is up: owe its report to every node it was forwarded to
    /// (to every peer, for one found pending at mount).
    pub(crate) fn note_up(&self, hash: &constellation_fs_core::ChunkHash) {
        let nodes = self.forwarded.lock().unwrap().remove(hash);
        let inherited = self.inherited.lock().unwrap().remove(hash);
        let mut reports = self.durable_reports.lock().unwrap();
        if inherited {
            reports.entry(REPORT_TO_ALL).or_default().push(*hash);
        }
        for node in nodes.into_iter().flatten() {
            reports.entry(node).or_default().push(*hash);
        }
    }

    /// A report that could not go out (its node unreachable, or no peer
    /// known yet right after a restart): owed again at the next pass, for
    /// up to [`REPORT_RETRY`] of failures in a row; past that the
    /// recipient's own S3 check covers it.
    pub(crate) fn requeue_report(&self, node: u64, hashes: Vec<constellation_fs_core::ChunkHash>) {
        let mut attempts = self.report_attempts.lock().unwrap();
        let since = *attempts.entry(node).or_insert_with(std::time::Instant::now);
        if since.elapsed() > REPORT_RETRY {
            attempts.remove(&node);
            return;
        }
        self.durable_reports
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .extend(hashes);
    }

    pub(crate) fn report_delivered(&self, node: u64) {
        self.report_attempts.lock().unwrap().remove(&node);
    }

    /// The reports owed, per node (taken: see [`Self::requeue_report`]).
    pub(crate) fn take_durable_reports(
        &self,
    ) -> std::collections::HashMap<u64, Vec<constellation_fs_core::ChunkHash>> {
        std::mem::take(&mut *self.durable_reports.lock().unwrap())
    }

    /// Whether this node should check S3 now for `hash`, which another
    /// node forwarded as pending; schedules the next check if so.
    fn remote_poll_due(&self, hash: &constellation_fs_core::ChunkHash) -> bool {
        let now = std::time::Instant::now();
        let mut polls = self.remote_polls.lock().unwrap();
        match polls.get_mut(hash) {
            None => {
                polls.insert(*hash, (now + REMOTE_POLL_FIRST, REMOTE_POLL_FIRST));
                false
            }
            Some((next, delay)) if now >= *next => {
                *delay = (*delay * 2).min(REMOTE_POLL_MAX);
                *next = now + *delay;
                true
            }
            Some(_) => false,
        }
    }

    pub(crate) fn forget_remote_poll(&self, hash: &constellation_fs_core::ChunkHash) {
        self.remote_polls.lock().unwrap().remove(hash);
    }

    /// Hard ceiling on real concurrency and thus on worst-case pending-
    /// upload memory: at most this many chunk buffers are held at once,
    /// however many rows `pending_upload` has queued.
    fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    async fn permit(&self) -> ConcurrencyPermit<'_> {
        self.gate.acquire().await
    }

    /// Plan 30 §M7: a permit for a small inode drain (an `fsync` or a
    /// write-through close of a small file) that must not wait behind the
    /// round's bulk upload pass — see `ConcurrencyGate::acquire_priority`.
    async fn permit_priority(&self) -> ConcurrencyPermit<'_> {
        self.gate.acquire_priority(PRIORITY_UPLOAD_RESERVE).await
    }

    fn record_success(&self, bytes: u64, latency: std::time::Duration, now: std::time::Instant) {
        let Some(controller) = &self.controller else {
            return;
        };
        let new_target = controller.lock().unwrap().on_success(now, bytes, latency);
        if new_target != self.gate.target() {
            tracing::debug!(
                concurrency = new_target,
                previous = self.gate.target(),
                "adaptive upload concurrency adjusted"
            );
            self.gate.set_target(new_target);
        }
    }

    fn record_error(&self, now: std::time::Instant) {
        let Some(controller) = &self.controller else {
            return;
        };
        let new_target = controller.lock().unwrap().on_error(now);
        let previous = self.gate.target();
        if new_target != previous {
            tracing::debug!(
                concurrency = new_target,
                previous,
                "upload failed; backing off adaptive concurrency"
            );
            self.gate.set_target(new_target);
        } else {
            tracing::debug!(
                concurrency = new_target,
                "upload failure coalesced with current congestion episode"
            );
        }
    }

    /// The dedup-ladder rung for a chunk of `len` bytes.
    ///
    /// A small chunk probes only on a positive hint (a peer's digest or
    /// this node's existence cache says the bytes are already there): the
    /// adaptive policy's guess alone does not make it. `Probe` spends a
    /// HEAD to save sending the body on a hit, and a miss then costs a
    /// second serialized round trip (HEAD, then PUT). Below
    /// [`probe_min_bytes`] the body is cheaper than that round trip, so a
    /// conditional create (one round trip, hit or miss) is never slower —
    /// the OVH run's non-owner paid HEAD + PUT on every small file once
    /// its ladder had switched to `Probe`.
    fn put_mode(
        &self,
        hash: &constellation_fs_core::ChunkHash,
        len: usize,
    ) -> constellation_store_s3::ChunkPutMode {
        let small = self.create_if_absent && (len as u64) < self.probe_min_bytes;
        if self.existence.peer_hints_enabled()
            && self
                .coop
                .as_ref()
                .is_some_and(|coop| coop.peer_digest_contains(hash))
        {
            self.existence.note_peer_hint();
            return constellation_store_s3::ChunkPutMode::Probe;
        }
        if self.existence.contains(hash) {
            return constellation_store_s3::ChunkPutMode::Probe;
        }
        if small {
            return constellation_store_s3::ChunkPutMode::Create;
        }
        let n = self
            .decisions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.probe.lock().unwrap().enabled() || n.is_multiple_of(16) {
            constellation_store_s3::ChunkPutMode::Probe
        } else if self.create_if_absent {
            constellation_store_s3::ChunkPutMode::Create
        } else {
            constellation_store_s3::ChunkPutMode::Overwrite
        }
    }

    #[cfg(test)]
    fn for_test(create_if_absent: bool) -> Self {
        Self::new(
            create_if_absent,
            None,
            crate::existence::Existence::new(1024, false, None),
        )
    }

    /// Test helper: pin a fixed concurrency (no adaptive controller),
    /// bypassing environment variables so tests are hermetic.
    #[cfg(test)]
    fn for_test_fixed(create_if_absent: bool, concurrency: usize) -> Self {
        Self {
            gate: ConcurrencyGate::new(concurrency),
            controller: None,
            max_concurrency: concurrency.max(1),
            create_if_absent,
            probe: std::sync::Mutex::new(writeback::ProbePolicy::default()),
            decisions: std::sync::atomic::AtomicU64::new(0),
            coop: None,
            existence: crate::existence::Existence::new(1024, false, None),
            in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            probe_min_bytes: probe_min_bytes(),
            forwarded: Default::default(),
            durable_reports: Default::default(),
            report_attempts: Default::default(),
            remote_polls: Default::default(),
            reported: Default::default(),
            inherited: Default::default(),
            last_put_ms: std::sync::atomic::AtomicI64::new(0),
            handoff: HandoffStats::default(),
        }
    }
}

/// Plan 30 §M4: [`upload_dirty_chunks_report`] for a caller that needs
/// every pending chunk durable — an inode drain before a replay or an
/// `fsync`, a handoff's or an unmount's final flush — so an unrecoverable
/// chunk is still an error here.
async fn upload_dirty_chunks(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
) -> Result<()> {
    let report =
        upload_dirty_chunks_report(cache, meta, store, compression, upload, only_ino, only_part)
            .await?;
    if let Some((hash, _)) = report.missing.first() {
        bail!("pending upload chunk {hash} missing from local cache");
    }
    Ok(())
}

/// What one upload pass found it cannot upload.
#[derive(Debug, Default)]
struct UploadReport {
    /// Pending `(chunk, ino)` rows whose chunk is gone from the local
    /// cache: unrecoverable content (plan 30 §M4).
    missing: Vec<(constellation_fs_core::ChunkHash, constellation_fs_core::Ino)>,
    /// Chunks another node forwarded as pending that are not in S3 yet
    /// (`meta::store::remote`): not lost, only not up yet.
    awaiting: u64,
}

/// Upload every pending chunk (or one inode's). A chunk missing from the
/// local cache is no longer an error (plan 30 §M4 item 2): it is reported,
/// and recorded in `Meta::note_unrecoverable_chunks`, so the ship plan
/// holds back just the records that need it and everything else ships.
/// Any other failure (S3 unreachable, a PUT that keeps failing) is still
/// an error for the round.
pub(crate) async fn upload_dirty_chunks_report(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
) -> Result<UploadReport> {
    upload_dirty_chunks_pass(
        cache,
        meta,
        store,
        compression,
        upload,
        only_ino,
        only_part,
        0,
    )
    .await
}

/// Plan 30 §M9 × §M4: enroll the chunk lists of adopted spilled
/// manifests (`Meta::adopted_spills`) as pending uploads, from the list
/// blob in the local cache or in S3. A blob that is in neither stays a
/// pending row of its own (this pass then records it unrecoverable, which
/// holds the manifest), and its mark keeps the manifest deferred until a
/// later pass can read it.
async fn expand_adopted_spills(cache: &DiskCache, meta: &Meta, store: &ChunkStore) -> Result<()> {
    for (ino, blob) in meta.adopted_spills()? {
        let bytes = match cache.get(&blob)? {
            Some(bytes) => bytes,
            None => match store.get_chunk(&blob).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::debug!(%error, ino, %blob, "adopted chunk list not readable yet");
                    continue;
                }
            },
        };
        let chunks: Vec<constellation_fs_core::ChunkHash> =
            match constellation_fs_core::manifest::decode_chunk_list(&bytes) {
                Ok(list) => list.into_values().collect(),
                Err(error) => {
                    tracing::warn!(%error, ino, %blob, "adopted chunk list undecodable; left held");
                    continue;
                }
            };
        tracing::info!(
            ino,
            chunks = chunks.len(),
            "enrolled an adopted manifest's chunk list for its durability check"
        );
        meta.expand_adopted_spill(ino, &blob, &chunks)?;
    }
    Ok(())
}

/// One pass of [`upload_dirty_chunks_report`]: `depth` counts the passes
/// a row another drain had claimed sent this one back for.
#[allow(clippy::too_many_arguments)]
async fn upload_dirty_chunks_pass(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
    depth: u8,
) -> Result<UploadReport> {
    use futures::StreamExt;
    if only_ino.is_none() {
        expand_adopted_spills(cache, meta, store).await?;
    }
    let mut grouped: std::collections::HashMap<
        constellation_fs_core::ChunkHash,
        Vec<constellation_fs_core::Ino>,
    > = std::collections::HashMap::new();
    for (hash, ino) in meta.pending_uploads()? {
        if only_ino.is_some_and(|wanted| ino != wanted) {
            continue;
        }
        if let Some(wanted) = only_part {
            if wanted != "p0" {
                continue;
            }
        }
        grouped.entry(hash).or_default().push(ino);
    }
    // Rows another drain is uploading right now are its; this one waits
    // for their acks below instead of uploading them again.
    let mut deferred: Vec<constellation_fs_core::ChunkHash> = Vec::new();
    {
        let mut in_flight = upload.in_flight.lock().unwrap();
        grouped.retain(|hash, _| {
            if in_flight.contains(hash) {
                deferred.push(*hash);
                false
            } else {
                in_flight.insert(*hash);
                true
            }
        });
    }
    let _claim = InFlightClaim {
        upload,
        hashes: grouped.keys().copied().collect(),
    };
    let total = grouped.len() as u64;
    let priority = only_ino.is_some() && total <= PRIORITY_DRAIN_CHUNKS;
    if total > 0 {
        tracing::debug!(
            pending_chunks = total,
            concurrency = upload.gate.target(),
            max_concurrency = upload.max_concurrency(),
            "uploading pending chunks"
        );
    }
    // Materialize at most the configured maximum number of upload
    // futures. Each one reads bytes only after winning the adaptive gate,
    // so both future state and chunk buffers stay independent of a backlog
    // that may contain tens of thousands of rows.
    //
    // Missing-cache rows (torn disk, or a poisoned inherited backlog that
    // self-heal missed) must still fail the round so the journal does not
    // ship — but logging ERROR once per hash per round produced multi-GB
    // logs (plan 25). Count them and emit a single summary below.
    let missing_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let missing_sample = std::sync::Arc::new(std::sync::Mutex::new(
        None::<constellation_fs_core::ChunkHash>,
    ));
    let missing_rows = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
        constellation_fs_core::ChunkHash,
        constellation_fs_core::Ino,
    )>::new()));
    let awaiting = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let in_flight = futures::stream::iter(grouped.into_iter().map(|(hash, inos)| {
        let missing_count = missing_count.clone();
        let missing_sample = missing_sample.clone();
        let missing_rows = missing_rows.clone();
        let awaiting = awaiting.clone();
        async move {
            let _permit = if priority {
                upload.permit_priority().await
            } else {
                upload.permit().await
            };
            if fault::lose_chunk(&hash) {
                tracing::warn!(%hash, "fault injection: dropping a pending chunk from the cache");
                let _ = cache.remove(&hash);
            }
            let Some(data) = cache.get(&hash)? else {
                // Another node forwarded a manifest naming this chunk while
                // it was still uploading there (`meta::store::remote`): it
                // reports it once it is up. Until then it is awaited, not
                // lost; S3 is checked here only now and then, in case the
                // report never comes.
                let remote = inos
                    .iter()
                    .any(|ino| meta.remote_chunk(&hash, *ino).ok().flatten().is_some());
                if remote {
                    if upload.remote_poll_due(&hash) && store.chunk_durable(&hash).await? {
                        tracing::debug!(%hash, "a forwarded chunk is in S3; acknowledged");
                        return Ok(Some((
                            hash,
                            inos,
                            constellation_store_s3::ChunkPutMode::Create,
                            true,
                        )));
                    }
                    awaiting.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(None);
                }
                // Not in the cache is not the same as lost: the content
                // may already be durable in the bucket. The common way
                // there: a same-content writer on this node uploaded it
                // and the chunk was demoted to clean (and then evicted)
                // before this row's writer had enrolled it — demotion
                // checks for pending rows, and this one did not exist
                // yet. Content addressing makes that upload this row's
                // too, exactly as a `Probe` HEAD hit would: acknowledge
                // it instead of holding the writer's records back.
                if store.chunk_durable(&hash).await? {
                    tracing::info!(
                        %hash,
                        "pending chunk not in the local cache but durable in S3; acknowledged"
                    );
                    return Ok(Some((
                        hash,
                        inos,
                        constellation_store_s3::ChunkPutMode::Create,
                        true,
                    )));
                }
                missing_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                {
                    let mut sample = missing_sample.lock().unwrap();
                    if sample.is_none() {
                        *sample = Some(hash);
                    }
                }
                missing_rows
                    .lock()
                    .unwrap()
                    .extend(inos.iter().map(|ino| (hash, *ino)));
                return Ok(None);
            };
            let bytes = data.len() as u64;
            let mode = upload.put_mode(&hash, data.len());
            let mut last = None;
            let started = std::time::Instant::now();
            for attempt in 0..3 {
                match store.put_chunk_mode(&hash, &data, compression, mode).await {
                    Ok(result) => {
                        upload.last_put_ms.store(
                            constellation_store_s3::lease::now_unix_ms(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        let now = std::time::Instant::now();
                        // A Probe hit only performed HEAD; counting the
                        // chunk's logical bytes as uploaded would report
                        // impossible goodput and drive concurrency upward
                        // during deduplicated workloads.
                        if !result.existed {
                            upload.record_success(bytes, now.duration_since(started), now);
                        }
                        return Ok(Some((hash, inos, mode, result.existed)));
                    }
                    Err(error) => last = Some(error),
                }
                if attempt < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
            upload.record_error(std::time::Instant::now());
            Err(anyhow::anyhow!("upload {hash} failed: {}", last.unwrap()))
        }
    }))
    .buffer_unordered(upload.max_concurrency());
    futures::pin_mut!(in_flight);
    let mut first_error = None;
    let mut completed = 0u64;
    let mut last_progress = std::time::Instant::now();
    let progress_interval = std::env::var("CONSTELLATION_UPLOAD_PROGRESS_INTERVAL_S")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(10));
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(None) => {}
            Ok(Some((hash, inos, mode, existed))) => {
                upload.existence.insert(&hash);
                if mode == constellation_store_s3::ChunkPutMode::Probe {
                    upload.probe.lock().unwrap().record(existed);
                }
                for ino in inos {
                    meta.ack_upload(&hash, ino)?;
                }
                upload.note_up(&hash);
                upload.forget_remote_poll(&hash);
                if !meta.upload_pending_for_hash(&hash)? {
                    cache.set_state(&hash, constellation_fs_core::cache::ChunkState::Clean);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        completed += 1;
        // Rows only awaited (another node's upload) are not progress:
        // a pass of nothing but those would log this every round.
        let awaited_only =
            completed == total && awaiting.load(std::sync::atomic::Ordering::Relaxed) >= total;
        if total > 0
            && !awaited_only
            && (completed == total || last_progress.elapsed() >= progress_interval)
        {
            tracing::info!(
                completed,
                total,
                concurrency = upload.gate.target(),
                "pending chunk upload progress"
            );
            last_progress = std::time::Instant::now();
        }
    }
    let missing = missing_count.load(std::sync::atomic::Ordering::Relaxed);
    let missing_rows = std::mem::take(&mut *missing_rows.lock().unwrap());
    if missing > 0 {
        let sample = *missing_sample.lock().unwrap();
        tracing::warn!(
            missing_pending_chunks = missing,
            sample_hash = ?sample,
            "pending upload chunks missing from local cache (unrecoverable content); \
             leaving the pending rows and holding back only the records that need them"
        );
    }
    // A full pass sees every pending row, so its list replaces the
    // recorded set (a chunk that turned up again stops poisoning); a
    // limited pass only adds to it.
    meta.note_unrecoverable_chunks(&missing_rows, only_ino.is_none())?;
    if let Some(error) = first_error {
        return Err(error);
    }
    if total > 0 {
        tracing::debug!(uploaded = total, "pending chunk upload complete");
    }
    // The chunks another drain claimed: this pass is complete only once
    // they are acked too (a write-through flush must not name a hash S3
    // lacks). A claim that ends with the row still pending (the other
    // drain failed, or found the chunk missing) leaves the row to this
    // pass: it goes round again and claims it itself, so a missing
    // chunk is reported here (M4's held records) and a transient error
    // there is retried here, never turned into this caller's error.
    let mut unclaimed_pending = false;
    for hash in deferred {
        let started = std::time::Instant::now();
        loop {
            if !meta.upload_pending_for_hash(&hash)? {
                break;
            }
            let claimed = upload.in_flight.lock().unwrap().contains(&hash);
            if !claimed {
                unclaimed_pending = true;
                break;
            }
            if started.elapsed() > std::time::Duration::from_secs(120) {
                anyhow::bail!("upload {hash} still in flight elsewhere after 120 s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
    if unclaimed_pending {
        if depth >= 3 {
            anyhow::bail!("pending uploads left behind by concurrent drains after 4 passes");
        }
        drop(_claim);
        return Box::pin(upload_dirty_chunks_pass(
            cache,
            meta,
            store,
            compression,
            upload,
            only_ino,
            only_part,
            depth + 1,
        ))
        .await;
    }
    Ok(UploadReport {
        missing: missing_rows,
        awaiting: awaiting.load(std::sync::atomic::Ordering::Relaxed),
    })
}

/// Drive either the ordinary S3 authority path or a continuation epoch.
/// An S3 failure may activate an epoch, but the failing round remains an
/// error for spool observability. While active, a successful tail probe
/// means S3 returned: upload dirty chunks first, close the promise, then
/// resume ordinary CAS-serialized shipping.
/// The continuation-epoch machinery (`constellation_net::EpochPromise`)
/// still speaks in per-partition vectors; with plan 29 M0a's single
/// stream that is always a one-entry map keyed by `p0`.
/// Give the root directory to the mounting user on a freshly created
/// filesystem. Skipped entirely unless the root is still 0:0, so this
/// costs nothing (and needs no lease) on every subsequent mount; when it
/// does apply, it takes the lease like any other mutation (through the
/// authority core).
///
/// EC2 finding R2-4: one attempt is not enough. A `Policy::System` op
/// gives up the moment its acquisition does not open the view, and a
/// fresh acquisition routinely waits a moment behind its takeover gate —
/// under `--cto strict`, when two nodes mount together, the kernel-cache
/// drain the first sight of the other node arms (plan 30 §M8) keeps the
/// gate shut for a second. Both nodes then answered "deferred" (the
/// holder's own attempt refused by its gate, the other's forward refused
/// by the holder's), nobody retried, and the root stayed genesis' `root:
/// root 0755` — unwritable by the mounting user and, without
/// `allow_other`, by root too. So: retry until the root has an owner
/// (this node's attempt or anyone else's, seen by tailing), for a short
/// while before the mount appears and then in the background.
async fn adopt_root(
    meta: &std::sync::Arc<Meta>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    forward: &std::sync::Arc<forward::ForwardState>,
    node_id: u64,
) -> Result<()> {
    let euid = unsafe { libc::geteuid() };
    if euid == 0 {
        return Ok(());
    }
    /// How long the mount waits for the root to have an owner before it
    /// appears anyway (and keeps adopting in the background).
    const BEFORE_MOUNT: std::time::Duration = std::time::Duration::from_secs(10);
    const RETRY: std::time::Duration = std::time::Duration::from_millis(250);
    let started = std::time::Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        if adopt_root_once(meta, sync_tx, forward, node_id).await? {
            if attempts > 1 {
                tracing::info!(attempts, "root directory owner adopted");
            }
            return Ok(());
        }
        if started.elapsed() >= BEFORE_MOUNT {
            break;
        }
        tokio::time::sleep(RETRY).await;
    }
    tracing::warn!(
        attempts,
        "root directory still has no owner; mounting anyway and adopting it in the background"
    );
    let (meta, sync_tx, forward) = (meta.clone(), sync_tx.clone(), forward.clone());
    tokio::spawn(async move {
        let mut wait = std::time::Duration::from_secs(1);
        loop {
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(std::time::Duration::from_secs(60));
            match adopt_root_once(&meta, &sync_tx, &forward, node_id).await {
                Ok(true) => {
                    tracing::info!("root directory owner adopted");
                    return;
                }
                Ok(false) => {}
                // The sync task is gone: the mount is shutting down.
                Err(_) => return,
            }
        }
    });
    Ok(())
}

/// One adoption attempt: `Ok(true)` once the root has an owner (it
/// already had one, or this attempt gave it one), `Ok(false)` when the
/// attempt was deferred (the lease is held elsewhere or not open yet).
async fn adopt_root_once(
    meta: &std::sync::Arc<Meta>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    forward: &forward::ForwardState,
    node_id: u64,
) -> Result<bool> {
    let euid = unsafe { libc::geteuid() };
    let egid = unsafe { libc::getegid() };
    // Another node may already have done it; make sure we have its log.
    // A tail that fails (S3 unreachable) is not fatal: the replica we
    // have decides, and a later attempt tails again.
    let (reply, rx) = tokio::sync::oneshot::channel();
    sync_tx
        .send(fusefs::SyncRequest::TailToHead { reply })
        .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
    match rx.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "root adoption: tail failed"),
        Err(_) => anyhow::bail!("sync task stopped"),
    }
    let root =
        constellation_meta::MetaStore::getattr(&**meta, constellation_fs_core::types::ROOT_INO)?;
    tracing::debug!(
        uid = root.as_ref().map(|a| a.uid),
        gid = root.as_ref().map(|a| a.gid),
        applied = meta.applied_seq().unwrap_or(0),
        "root adoption check"
    );
    if !matches!(root, Some(a) if a.uid == 0) {
        return Ok(true);
    }
    let op = constellation_meta::MutateOp::Setattr {
        ino: constellation_fs_core::types::ROOT_INO,
        mode: None,
        uid: Some(euid),
        gid: Some(egid),
        size: None,
        atime_ns: None,
        mtime_ns: None,
    };
    let (reply, rx) = tokio::sync::oneshot::channel();
    sync_tx
        .send(fusefs::SyncRequest::Submit {
            op,
            rid: forward.next_system_rid(node_id),
            policy: constellation_authority::Policy::System,
            in_doubt: false,
            reply,
        })
        .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
    match rx.await {
        Ok(constellation_authority::ClientReply::Outcome(
            constellation_meta::MutateOutcome::Accepted { .. },
        )) => Ok(true),
        Err(_) => anyhow::bail!("sync task stopped"),
        // Another node holds authority (it adopts the root itself, or a
        // later attempt here reaches it), or this node's own fresh
        // acquisition has not opened its view yet (a takeover gate).
        other => {
            tracing::info!(reply = ?other, "root adoption deferred; retrying");
            Ok(false)
        }
    }
}

/// Format dial addresses for status/UI output.
fn peer_addr_strings(addr: &constellation_net::EndpointAddr) -> Vec<String> {
    addr.addrs.iter().map(|a| a.to_string()).collect()
}

/// Live daemon state exposed over the control socket.
struct DaemonStatus {
    meta: std::sync::Arc<Meta>,
    cache: std::sync::Arc<DiskCache>,
    staging_budget: std::sync::Arc<staging::StagingBudget>,
    /// The authority core's observable state (`authority_driver`).
    core: std::sync::Arc<std::sync::Mutex<authority_driver::CoreStatus>>,
    lease: std::sync::Arc<lease::LeaseView>,
    fs_uuid: String,
    backend: String,
    /// Back-reference for the mount-management verbs (`MountAdd`,
    /// `MountRemove`, `MountList`) and multi-view `status`/`leave`
    /// (plan 21, step 1).
    node: std::sync::Arc<node_runtime::NodeRuntime>,
    node_id: u64,
    started: std::time::Instant,
    peers: constellation_net::Peers,
    pins: std::sync::Arc<pin::PinManager>,
    designations: std::sync::Arc<designation::DesignationManager>,
    epochs: std::sync::Arc<epoch::EpochManager>,
    reintegration: std::sync::Arc<reintegrate::ReintegrationState>,
    sync_tx: tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    departed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Handle for the blocking control-API calls that need to await.
    rt: tokio::runtime::Handle,
    coop: std::sync::Arc<crate::coop::Coop>,
    prefetch_stats: std::sync::Arc<crate::prefetch::PrefetchStats>,
    write_mode: std::sync::Arc<writeback::WriteModeState>,
    upload: std::sync::Arc<UploadRuntime>,
    snapshots: std::sync::Arc<snapshot::SnapshotManager>,
    log_buffer: log_buffer::LogBuffer,
    forward: std::sync::Arc<forward::ForwardState>,
    placement: std::sync::Arc<placement::Placement>,
    atime: std::sync::Arc<crate::atime::AtimeAccumulator>,
    prune_stats: std::sync::Arc<crate::prune::PruneStats>,
    lease_mode: constellation_store_s3::LeaseMode,
    read_only_member: bool,
    last_sync_ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Plan 29 M3a: lets `fsck` (and any future offline-tool verb) run
    /// in-process over the control socket instead of racing the mount
    /// for `fjall`'s single-process lock.
    state_dir: PathBuf,
    compression: CompressionSetting,
}

impl DaemonStatus {
    /// `NodeRuntime::mounts()` translated to the control API's wire shape.
    fn api_mounts(&self) -> Vec<constellation_api::MountInfo> {
        self.node
            .mounts()
            .into_iter()
            .map(|m| constellation_api::MountInfo {
                id: m.id.as_u64(),
                subtree: m.subtree,
                mountpoint: m.mountpoint.display().to_string(),
                mounted_ms_ago: m.since.elapsed().as_millis() as u64,
            })
            .collect()
    }

    /// Snapshot/clone control requests are metadata mutations too: acquire
    /// the subtree partition and force its pending data + journal through
    /// before observing or publishing an immutable root.
    fn snapshot_barrier(&self, path: &str) -> std::result::Result<(), String> {
        let ino = self
            .meta
            .resolve_path(path)
            .map_err(|error| error.to_string())?
            .unwrap_or(constellation_fs_core::types::ROOT_INO);
        let part = "p0".to_string();
        let _ = part;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Acquire { reply })
            .map_err(|_| "sync task is not running".to_string())?;
        let progress = tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "lease acquisition stopped".to_string())??;
        if !progress.acquired {
            return Err("subtree write lease is held by another node".into());
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Barrier { ino, reply })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "snapshot barrier stopped".to_string())?
    }

    /// Assemble the pruner's dependency bundle from the daemon's shared
    /// handles (plan 22). `replica_lag` is derived from the sync task's
    /// heartbeat.
    fn prune_deps(&self) -> crate::prune::PruneDeps {
        use std::sync::atomic::Ordering;
        let lag = std::time::Duration::from_millis(
            crate::prune::now_unix_ms().saturating_sub(self.last_sync_ms.load(Ordering::Relaxed)),
        );
        crate::prune::PruneDeps {
            store: self.store.clone(),
            meta: self.meta.clone(),
            sync_tx: self.sync_tx.clone(),
            lease: self.lease.clone(),
            forward: self.forward.clone(),
            node_id: self.node_id,
            lease_mode: self.lease_mode,
            read_only_member: self.read_only_member,
            departed: self.departed.clone(),
            epoch_frozen: Some(self.epochs.writes_refused.clone()),
            stats: self.prune_stats.clone(),
            replica_lag: lag,
        }
    }
}

impl constellation_api::StatusSource for DaemonStatus {
    fn status(&self) -> constellation_api::StatusReport {
        let core = self.core.lock().unwrap().clone();
        let stats = core.stats;
        let speculation = (
            stats.speculation_rolled_back,
            stats.stranded_replayed,
            stats.replay_conflicts,
        );
        let gate_pending = self.lease.gate_pending();
        let usage = self.cache.usage();
        let p0_lease = self.lease.status();
        let designations = self.list_designations();
        let mut epoch = self.epochs.status();
        epoch.promise_puts = stats.promise_puts;
        epoch.promise_requests_answered = stats.promise_requests_answered;
        epoch.promise_requests_refused = stats.promise_requests_refused;
        epoch.promise_checks = stats.promise_checks;
        epoch.takeovers_refused_promises = stats.takeovers_refused_promises;
        epoch.promise_flush_exempt = stats.promise_flush_exempt;
        epoch.stale_claims = stats.epoch_stale_claims;
        let coop = self.coop.report();
        let s3_coop = coop.per_source.iter().find(|s| s.id == "s3").cloned();
        let peer_snap = self.peers.snapshot();
        let mut peers: Vec<constellation_api::PeerStatus> = Vec::with_capacity(1 + peer_snap.len());
        // S3 is always first so operators can compare the durable path
        // against peer lat/BW/hit% in the same table.
        peers.push(constellation_api::PeerStatus {
            node_id: 0,
            connected: true,
            hostname: Some("S3".into()),
            coop: s3_coop,
            s3: true,
            path: String::new(),
            ..Default::default()
        });
        peers.extend(peer_snap.into_iter().map(|p| {
            let addrs = peer_addr_strings(&p.addr);
            let designations: Vec<String> = designations
                .iter()
                .filter(|d| d.designee == p.node_id)
                .map(|d| d.path.clone())
                .collect();
            let coop = coop
                .per_source
                .iter()
                .find(|s| s.id == format!("peer-{}", p.node_id))
                .cloned();
            let path = match p.path {
                constellation_net::PathKind::Unknown => coop
                    .as_ref()
                    .map(|c| c.path.clone())
                    .filter(|s| !s.is_empty() && s != "unknown")
                    .unwrap_or_else(|| "unknown".into()),
                other => other.as_str().into(),
            };
            constellation_api::PeerStatus {
                node_id: p.node_id,
                connected: p.connected,
                rtt_ms: p.rtt_ms,
                last_seen_ms: p.last_seen.map(|t| t.elapsed().as_millis() as u64),
                hostname: (!p.hostname.is_empty()).then_some(p.hostname.clone()),
                version: (!p.version.is_empty()).then_some(p.version.clone()),
                pubkey: Some(p.pubkey_hex.clone()),
                endpoint_id: Some(p.addr.id.to_string()),
                addrs,
                created_unix: (p.created_unix > 0).then_some(p.created_unix),
                p2p_updated_unix: p.p2p_updated_unix,
                ro: p.ro,
                epoch_member: epoch.members.contains(&p.node_id),
                designations,
                coop,
                s3: false,
                path,
                paths: paths::status(self.peers.path_summary(p.node_id)),
            }
        }));
        let p2p = constellation_api::P2pStatus {
            enabled: self.peers.is_enabled(),
            node_addr: self
                .peers
                .node_addr()
                .and_then(|a| serde_json::to_string(&a).ok()),
            relay: self.peers.relay_label(),
            peers,
        };
        let enrolled = !self.departed.load(std::sync::atomic::Ordering::Relaxed)
            && !matches!(
                self.meta.kv_get("left").ok().flatten().as_deref(),
                Some("1")
            );
        constellation_api::StatusReport {
            fs_uuid: self.fs_uuid.clone(),
            backend: self.backend.clone(),
            mounts: self.api_mounts(),
            node_id: self.node_id,
            version: env!("CONSTELLATION_VERSION").to_string(),
            enrolled,
            uptime_s: self.started.elapsed().as_secs(),
            spool: constellation_api::SpoolStatus {
                journal_backlog: constellation_meta::MetaStore::journal_len(&*self.meta)
                    .unwrap_or(0),
                head_seq: core.ship.as_ref().map(|s| s.head_seq).unwrap_or(0),
                conflicts: stats.conflicts,
                last_ship_error: core.last_error.clone(),
                ship_rounds_completed: stats.rounds_completed,
                // Plan 30 M5: a round is never cancelled by a request any
                // more (requests are events the round interleaves with).
                ship_rounds_cancelled: 0,
            },
            cache: constellation_api::CacheStatus {
                used_bytes: usage.used,
                budget_bytes: usage.budget,
                chunks: usage.entries as u64,
                pinned_bytes: usage.pinned,
                staging_bytes: self.staging_budget.used(),
                staging_budget_bytes: self.staging_budget.budget(),
            },
            lease: p0_lease,
            p2p,
            pins: self.list_pins(),
            designations,
            epoch,
            reintegration: self.reintegration.snapshot(
                if matches!(
                    self.meta.kv_get("lease_lost").ok().flatten().as_deref(),
                    Some("1")
                ) {
                    self.meta.unmarked_journal_len().unwrap_or(0)
                } else {
                    0
                },
                speculation.2,
            ),
            speculation: {
                let counts = self.meta.speculation_counts().unwrap_or_default();
                constellation_api::SpeculationStatus {
                    outstanding: counts.outstanding,
                    pending_replay: counts.pending_replay,
                    rolled_back: speculation.0,
                    stranded_replayed: speculation.1,
                    replay_conflicts: speculation.2,
                    local: counts.local,
                    local_rolled_back: stats.local_rolled_back,
                    depositions: stats.depositions,
                    epoch_markers: stats.epoch_markers,
                    gate_pending,
                    copies_pending: core.copies.0,
                    copies_stalled: core.copies.1,
                }
            },
            held: held::status(&self.meta),
            session: {
                let s = self.meta.session().stats();
                constellation_api::SessionStatus {
                    budget_ms: self.meta.session().budget().as_millis() as u64,
                    reads: s.reads,
                    fast: s.fast,
                    covered: s.covered,
                    waited: s.waited,
                    timeouts: s.timeouts,
                    degraded_held: s.degraded_held,
                    replay_blocked: s.replay_blocked,
                    waits_ms: s.waits_ms.to_vec(),
                    wait_ms_total: s.wait_ms_total,
                    raised: s.raised,
                }
            },
            ack: {
                let a = &core.ack;
                let s = self.meta.session().stats();
                constellation_api::AckStatus {
                    ack_s3: core.ack_s3,
                    policy: a.policy.to_string(),
                    backups: a.backups.clone(),
                    candidate: a.candidate,
                    config_version: a.config_version,
                    durable: a.durable,
                    parked_acks: a.parked_acks as u64,
                    gated: self.lease.ack_gated(),
                    backing_holder: a.backing_holder,
                    backing_epoch: a.backing_epoch,
                    backing_acked: a.backing_acked,
                    sealed_epoch: a.sealed_epoch,
                    backups_added: stats.backups_added,
                    backups_removed: stats.backups_removed,
                    reconfig_cas: stats.reconfig_cas,
                    backup_appends: stats.backup_appends,
                    backup_acks: stats.backup_acks,
                    backup_ack_timeouts: stats.backup_ack_timeouts,
                    acks_waited: stats.acks_waited,
                    ack_wait_ms_total: stats.ack_wait_ms_total,
                    acks_aborted: stats.acks_aborted,
                    streamed_ahead: stats.streamed_ahead,
                    streamed_installed: stats.streamed_installed,
                    streamed_dropped: stats.streamed_dropped,
                    awaited_log: stats.awaited_log,
                    awaited_log_streamed: stats.awaited_log_streamed,
                    awaited_log_streamed_deleg: stats.awaited_log_streamed_deleg,
                    backup_persisted: stats.backup_persisted,
                    seals: stats.seals,
                    backup_takeovers: stats.backup_takeovers,
                    backup_tail_applied: stats.backup_tail_applied,
                    s3_fast_takeovers: stats.s3_fast_takeovers,
                    ack_floor_waits: stats.ack_floor_waits,
                    stale_liveness_refusals: stats.stale_liveness_refusals,
                    epoch_carry_refused: stats.epoch_carry_refused,
                    refusals_journaled: stats.refusals_journaled,
                    unacked_replays_refused: stats.unacked_replays_refused,
                    reads_durability_blocked: s.durability_blocked,
                }
            },
            delegation: {
                let v = &core.delegation;
                constellation_api::DelegationReport {
                    enabled: core.delegation_enabled,
                    table: self
                        .meta
                        .delegation_table()
                        .iter()
                        .map(|d| constellation_api::DelegationStatus {
                            dir: d.dir,
                            path: self.meta.path_of(d.dir).unwrap_or_default(),
                            node: d.node,
                            gen: d.gen,
                            designated: d.designated,
                            range: d.range.label(),
                        })
                        .collect(),
                    mine: v.mine.clone(),
                    gens: v.gens.clone(),
                    executed: stats.deleg_executed,
                    fast_path_executed: core.delegation_fast_path_executed,
                    fast_path_routed: core.delegation_fast_path_routed,
                    forwarded_to_delegate: stats.deleg_forwarded,
                    deps_waits: stats.deleg_deps_waits,
                    parked_expired: stats.deleg_parked_expired,
                    not_owner: stats.deleg_not_owner,
                    installed: stats.deleg_installed,
                    streamed_txs: stats.deleg_streamed_txs,
                    stream_refused: stats.deleg_stream_refused,
                    renewals: stats.deleg_renewals,
                    renewals_refused: stats.deleg_renewals_refused,
                    recalls_received: stats.deleg_recalls_received,
                    delegated: stats.deleg_delegated,
                    appended_txs: stats.deleg_appended_txs,
                    stream_refusals: stats.deleg_stream_refusals,
                    deps_unsatisfied_at_append: stats.deleg_deps_unsatisfied_at_append,
                    cross_subtree: stats.deleg_cross_subtree,
                    recalls_sent: stats.deleg_recalls_sent,
                    recalls_drained: stats.deleg_recalls_drained,
                    recalls_expired: stats.deleg_recalls_expired,
                    reclaimed: stats.deleg_reclaimed,
                    ended: stats.deleg_ended,
                    deps_overflow_to_root: stats.deps_overflow_to_root,
                    exec_parked: stats.deleg_exec_parked,
                    stranded: stats.local_rolled_back,
                    kinds: v.kinds.clone(),
                    backups: v.backups.clone(),
                    placement: core.placement_top.clone(),
                    inherited: stats.deleg_inherited,
                    refused_designated: stats.deleg_refused_designated,
                    designated: stats.deleg_designated,
                    redelegated: stats.deleg_redelegated,
                    seals_sent: stats.deleg_seals_sent,
                    sealed_drained: stats.deleg_sealed_drained,
                    restreams: stats.deleg_restreams,
                    backup_appends: stats.deleg_backup_appends,
                    backup_acks: stats.deleg_backup_acks,
                    acks_parked: stats.deleg_acks_parked,
                    backup_persisted: stats.deleg_backup_persisted,
                    backup_seals: stats.deleg_backup_seals,
                    place_evaluations: stats.place_evaluations,
                    place_delegated: stats.place_delegated,
                    place_recalled: stats.place_recalled,
                    place_skipped_cooldown: stats.place_skipped_cooldown,
                    place_skipped_unreachable: stats.place_skipped_unreachable,
                    place_splits: stats.place_splits,
                    place_range_recalls: stats.place_range_recalls,
                    read_index_served: stats.deleg_read_index_served,
                    read_grants: stats.deleg_read_grants,
                }
            },
            cto: {
                let d = self.meta.read_delegations();
                let c = d.stats();
                constellation_api::CtoStatus {
                    strict: self.node.cto_strict(),
                    grants_enabled: core.read_delegations,
                    strict_reads: c.strict_reads,
                    holder_local: c.holder_local,
                    delegation_local: c.delegation_local,
                    read_index: c.read_index,
                    s3_tail: c.s3_tail,
                    degraded: c.degraded,
                    read_index_ms_total: c.read_index_ms_total,
                    read_index_ms: c.read_index_ms.to_vec(),
                    renewals: c.renewals,
                    delegations_installed: c.delegations_installed,
                    delegations_raced: c.delegations_raced,
                    delegations_held: d.held_count() as u64,
                    recalled: c.recalled,
                    read_index_served: stats.read_index_served,
                    read_index_refused: stats.read_index_refused,
                    grants: c.grants,
                    live_grants: d.live_grants(),
                    recalls_sent: stats.recalls_sent,
                    recalls_acked: stats.recalls_acked,
                    recalls_expired: stats.recalls_expired,
                    recall_waits: stats.recall_waits,
                    recall_wait_ms_total: stats.recall_wait_ms_total,
                    held_replies: stats.held_replies,
                    held_retries: stats.held_retries,
                    fuse_writes_recalled: c.fuse_writes_recalled,
                    fuse_recall_wait_ms_total: c.fuse_recall_wait_ms_total,
                    parked_acks: core.read.parked_acks as u64,
                    recalls_in_flight: core.read.recalls_in_flight as u64,
                }
            },
            locks: {
                let t = self.meta.locks();
                let l = t.stats();
                constellation_api::LockStatus {
                    mode: if core.locks_cluster {
                        "cluster"
                    } else {
                        "local"
                    }
                    .to_string(),
                    grants_held: t.held_count() as u64,
                    requests: stats.lock_requests,
                    local_hits: l.local_hits,
                    local_conflicts: l.local_conflicts,
                    granted: l.granted,
                    would_block: stats.lock_would_block,
                    unavailable: stats.lock_unavailable,
                    grant_ms_total: stats.lock_grant_ms_total,
                    grant_ms: stats.lock_grant_ms.to_vec(),
                    renewals: stats.lock_renewals,
                    lost: stats.lock_lost,
                    recalled: l.recalled,
                    recalled_busy: l.recalled_busy,
                    released: stats.lock_released,
                    fenced_io: l.fenced_io,
                    grants_table: t.grants_len() as u64,
                    grants_made: stats.lock_grants,
                    recalls_sent: stats.lock_recalls_sent,
                    recalls_released: stats.lock_recalls_released,
                    recalls_expired: stats.lock_recalls_expired,
                    reclaimed: stats.lock_reclaimed,
                    waiters_parked: stats.lock_waiters_parked,
                    grace_refusals: stats.lock_grace_refusals,
                    requests_in_flight: core.lock_requests_in_flight as u64,
                    waiters: core.lock_waiters as u64,
                    recalls_in_flight: core.lock_recalls_in_flight as u64,
                }
            },
            coop,
            prefetch: self.prefetch_stats.snapshot(),
            writeback: {
                let remote = self.meta.remote_chunks().unwrap_or_default();
                let probe = self.upload.probe.lock().unwrap();
                let existence = self.upload.existence.report();
                constellation_api::WritebackStatus {
                    mode: self.write_mode.get().as_str().into(),
                    dirty_bytes: self
                        .cache
                        .dirty_bytes()
                        .saturating_add(self.staging_budget.used()),
                    pending_uploads: self.meta.pending_upload_count().unwrap_or(0),
                    upload_concurrency: self.upload.gate.target() as u32,
                    remote_probe_enabled: probe.enabled(),
                    remote_probe_hit_rate: probe.hit_rate(),
                    existence_bloom_hits: existence.bloom_hits,
                    existence_chunk_ref_hits: existence.chunk_ref_hits,
                    existence_misses: existence.misses,
                    existence_peer_hints: existence.peer_hints,
                    remote_chunks_awaited: remote.len() as u64,
                    remote_chunks_oldest_s: remote
                        .iter()
                        .map(|r| r.enrolled_ms)
                        .min()
                        .map(|oldest| {
                            (constellation_store_s3::lease::now_unix_ms() - oldest).max(0) as u64
                                / 1000
                        })
                        .unwrap_or(0),
                    handoffs_sent: self
                        .upload
                        .handoff
                        .sent
                        .load(std::sync::atomic::Ordering::Relaxed),
                    handoffs_ok: self
                        .upload
                        .handoff
                        .ok
                        .load(std::sync::atomic::Ordering::Relaxed),
                    handoff_chunks: self
                        .upload
                        .handoff
                        .chunks
                        .load(std::sync::atomic::Ordering::Relaxed),
                    handoff_chunks_accepted: self
                        .upload
                        .handoff
                        .accepted
                        .load(std::sync::atomic::Ordering::Relaxed),
                }
            },
            forwarded_ok: self.forward.ok.load(std::sync::atomic::Ordering::Relaxed),
            forwarded_err: self.forward.err.load(std::sync::atomic::Ordering::Relaxed),
            forward_p50_ms: self.forward.p50_ms(),
            log_stream: {
                let v = core.stream;
                constellation_api::LogStreamStatus {
                    enabled: core.stream_enabled,
                    upstream: v.upstream,
                    live: v.live,
                    buffered: v.buffered,
                    applied: stats.stream_applied,
                    tail_skips: stats.stream_tail_skips,
                    subscribes: stats.stream_subscribes,
                    refused: stats.stream_refused,
                    ended: stats.stream_ended,
                    gaps: stats.stream_gaps,
                    lost: stats.stream_lost,
                    timeouts: stats.stream_timeouts,
                    overflows: stats.stream_overflows,
                    duplicates: stats.stream_duplicates,
                    serving: v.serving,
                    served: stats.stream_served,
                    declined: stats.stream_declined,
                    frames_sent: stats.stream_frames_sent,
                    subscribers_dropped: stats.stream_subscribers_dropped,
                }
            },
            forward_dedup_hits: stats.forward_dedup_hits,
            forward_retries: stats.forward_retries,
            forward_indoubt_resolved: stats.forward_indoubt_resolved,
            inbox: crate::inbox::status(
                crate::inbox::inbox_enabled(),
                &stats,
                &core.inbox,
                self.lease.touches(),
            ),
            placement_reason: self.placement.last_reason.lock().unwrap().clone(),
            atime: {
                use std::sync::atomic::Ordering::Relaxed;
                let s = &self.atime.stats;
                constellation_api::AtimeStatus {
                    mode: self.atime.mode().as_str().to_string(),
                    queued: s.queued.load(Relaxed),
                    coalesced: s.coalesced.load(Relaxed),
                    applied: s.applied.load(Relaxed),
                    dropped_cap: s.dropped_cap.load(Relaxed),
                    forward_ok: s.forward_ok.load(Relaxed),
                    forward_err: s.forward_err.load(Relaxed),
                    local_only: s.local_only.load(Relaxed),
                    skew_clamped: s.skew_clamped.load(Relaxed),
                }
            },
            quota: {
                use constellation_meta::MetaStore;
                constellation_api::QuotaStatus {
                    max_bytes: self.meta.quota().ok().flatten(),
                    used_bytes: self.meta.usage().0,
                }
            },
            prune: {
                use std::sync::atomic::Ordering::Relaxed;
                let s = &self.prune_stats;
                constellation_api::PruneStatus {
                    runs: s.runs.load(Relaxed),
                    roots: s.roots.load(Relaxed),
                    armed_roots: s.armed_roots.load(Relaxed),
                    unparseable_roots: s.unparseable_roots.load(Relaxed),
                    inert_roots: s.inert_roots.load(Relaxed),
                    entries_examined: s.entries_examined.load(Relaxed),
                    selected: s.selected.load(Relaxed),
                    deleted: s.deleted.load(Relaxed),
                    bytes_deleted: s.bytes_deleted.load(Relaxed),
                    bytes_freed: s.bytes_freed.load(Relaxed),
                    skipped_reverify: s.skipped_reverify.load(Relaxed),
                    skipped_forward_err: s.skipped_forward_err.load(Relaxed),
                    skipped_hardlink: s.skipped_hardlink.load(Relaxed),
                    skipped_repartition: s.skipped_repartition.load(Relaxed),
                    leases_acquired: s.leases_acquired.load(Relaxed),
                    refused_lag: s.refused_lag.load(Relaxed),
                    last_run_unix_ms: s.last_run_unix_ms.load(Relaxed),
                    last_parse_error: s.last_parse_error.lock().ok().and_then(|g| g.clone()),
                }
            },
            s3: backend::s3_request_counts(),
        }
    }

    fn pin(&self, path: &str) -> std::result::Result<String, String> {
        let pins = self.pins.clone();
        let path = path.to_string();
        // The API handler runs on the runtime already, so block_in_place
        // keeps the fetch off the async executor without a nested runtime.
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { pins.pin(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn unpin(&self, path: &str) -> std::result::Result<String, String> {
        let pins = self.pins.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { pins.unpin(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn list_pins(&self) -> Vec<constellation_api::PinStatus> {
        let pins = self.pins.clone();
        tokio::task::block_in_place(|| self.rt.block_on(async move { pins.status().await }))
    }

    fn offline(&self, path: &str, read_only: bool) -> std::result::Result<String, String> {
        let designations = self.designations.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { designations.offline(&path, read_only).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn online(&self, path: &str) -> std::result::Result<String, String> {
        let designations = self.designations.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { designations.online(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn list_designations(&self) -> Vec<constellation_api::DesignationStatus> {
        self.designations
            .snapshot()
            .into_iter()
            .map(|d| constellation_api::DesignationStatus {
                path: d.path,
                designee: d.designee,
                read_only: d.read_only,
            })
            .collect()
    }

    fn delegate(
        &self,
        path: &str,
        node: u64,
        range: Option<&str>,
    ) -> std::result::Result<String, String> {
        let dir = self
            .meta
            .resolve_path(path)
            .map_err(|e| format!("{e:#}"))?
            .ok_or_else(|| format!("no such directory: {path}"))?;
        // Plan 30 §M12: `"<idx>/<count>"`, `count` a power of two.
        let range = match range.map(str::trim).filter(|r| !r.is_empty()) {
            None => (0u8, 0u32),
            Some(r) => {
                let (i, k) = r
                    .split_once('/')
                    .ok_or_else(|| format!("range {r}: expected <idx>/<count>"))?;
                let idx: u32 = i.parse().map_err(|_| format!("range {r}: bad index"))?;
                let count: u32 = k.parse().map_err(|_| format!("range {r}: bad count"))?;
                if !count.is_power_of_two() || count > 16 || idx >= count {
                    return Err(format!(
                        "range {r}: count must be 2, 4, 8 or 16 and idx below it"
                    ));
                }
                (count.trailing_zeros() as u8, idx)
            }
        };
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Delegate {
                dir,
                node,
                range,
                reply,
            })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "sync task stopped".to_string())?
        })
    }

    fn undelegate(&self, path: &str) -> std::result::Result<String, String> {
        let dir = self
            .meta
            .resolve_path(path)
            .map_err(|e| format!("{e:#}"))?
            .ok_or_else(|| format!("no such directory: {path}"))?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Undelegate { dir, reply })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "sync task stopped".to_string())?
        })
    }

    fn list_delegations(&self) -> Vec<constellation_api::DelegationStatus> {
        self.meta
            .delegation_table()
            .iter()
            .map(|d| constellation_api::DelegationStatus {
                dir: d.dir,
                path: self.meta.path_of(d.dir).unwrap_or_default(),
                node: d.node,
                gen: d.gen,
                designated: d.designated,
                range: d.range.label(),
            })
            .collect()
    }

    fn reintegrate(&self) -> std::result::Result<String, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Reintegrate(reply))
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "reintegration task stopped".to_string())?
        })
    }

    fn leave(&self, node_id: Option<u64>, force: bool) -> std::result::Result<String, String> {
        match node_id {
            Some(target) => {
                let store = self.store.clone();
                let designations = self.designations.clone();
                let self_id = self.node_id;
                tokio::task::block_in_place(|| {
                    self.rt.block_on(async move {
                        leave::admin_leave(store, &designations, self_id, target, force)
                            .await
                            .map(|_| format!("retired node {target} in the registry"))
                            .map_err(|e| e.to_string())
                    })
                })
            }
            None => {
                leave::refuse_open_epoch(&self.epochs).map_err(|e| e.to_string())?;
                let (reply, receive) = tokio::sync::oneshot::channel();
                self.sync_tx
                    .send(fusefs::SyncRequest::Leave { force, reply })
                    .map_err(|_| "sync task is not running".to_string())?;
                let detail = tokio::task::block_in_place(|| {
                    self.rt
                        .block_on(receive)
                        .map_err(|_| "leave task stopped".to_string())?
                })?;
                self.departed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                // Unmount after the response is on the wire: the control
                // handler writes the Ok then this returns, then we detach
                // every view (plan 21, step 1 — `leave` is node-level, not
                // scoped to whichever view happened to answer the call).
                let node = self.node.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    for id in node.mounts().into_iter().map(|m| m.id) {
                        if let Err(e) = node.remove_mount(id) {
                            tracing::warn!(error = %e, "leave: detaching a view failed");
                        }
                    }
                });
                Ok(detail)
            }
        }
    }

    fn set_write_mode(&self, mode: &str) -> std::result::Result<String, String> {
        let requested: writeback::WriteMode = mode.parse().map_err(str::to_string)?;
        if self.write_mode.get() == requested {
            return Ok(format!("write mode already {}", requested.as_str()));
        }
        if requested == writeback::WriteMode::Through {
            let (reply, receive) = tokio::sync::oneshot::channel();
            self.sync_tx
                .send(fusefs::SyncRequest::DrainInode { ino: 0, reply })
                .map_err(|_| "sync task is not running".to_string())?;
            tokio::task::block_in_place(|| {
                self.rt
                    .block_on(receive)
                    .map_err(|_| "upload drain stopped".to_string())?
            })?;
        }
        self.write_mode.set(requested);
        Ok(format!("write mode set to {}", requested.as_str()))
    }

    fn snapshot_create(&self, selector: &str) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.snapshot_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let result =
            tokio::task::block_in_place(|| self.rt.block_on(snapshots.create(&path, &name)))
                .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(result)
    }

    fn snapshot_list(
        &self,
        path: Option<&str>,
    ) -> std::result::Result<Vec<constellation_api::SnapshotStatus>, String> {
        self.snapshots
            .list(path)
            .map_err(|error| format!("{error:#}"))
            .map(|rows| {
                rows.into_iter()
                    .map(|row| constellation_api::SnapshotStatus {
                        id: row.id,
                        path: row.path,
                        name: row.name,
                        root_hash: row.root_hash,
                        created_unix_ms: row.created_unix_ms,
                    })
                    .collect()
            })
    }

    fn snapshot_delete(&self, selector: &str) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.snapshot_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let result =
            tokio::task::block_in_place(|| self.rt.block_on(snapshots.delete(&path, &name)))
                .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(result)
    }

    fn clone_snapshot(
        &self,
        selector: &str,
        destination: &str,
    ) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.snapshot_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let destination = destination.to_string();
        let result = tokio::task::block_in_place(|| {
            self.rt
                .block_on(snapshots.clone_to(&path, &name, &destination))
        })
        .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(result)
    }

    fn snap_refs(&self, id: &str) -> std::result::Result<Vec<String>, String> {
        let snapshots = self.snapshots.clone();
        let id = id.to_string();
        tokio::task::block_in_place(|| self.rt.block_on(snapshots.refs(&id)))
            .map_err(|error| format!("{error:#}"))
    }

    fn read_dir(
        &self,
        path: &str,
    ) -> std::result::Result<Vec<constellation_api::DirectoryEntry>, String> {
        use constellation_meta::MetaStore;
        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let entries = self.meta.readdir(ino).map_err(|error| error.to_string())?;
        Ok(entries
            .into_iter()
            .map(|entry| constellation_api::DirectoryEntry {
                path: if normalized == "/" {
                    format!("/{}", entry.name)
                } else {
                    format!("{normalized}/{}", entry.name)
                },
                name: entry.name,
                ino: entry.ino,
                kind: format!("{:?}", entry.kind).to_lowercase(),
            })
            .collect())
    }

    fn inspect(&self, path: &str) -> std::result::Result<constellation_api::InspectStatus, String> {
        use constellation_meta::MetaStore;
        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let attr = self
            .meta
            .getattr(ino)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: stale inode"))?;
        let manifest = self
            .meta
            .manifest(ino)
            .map_err(|error| error.to_string())?
            .map(|bytes| {
                constellation_fs_core::manifest::Manifest::decode(&bytes)
                    .map(|manifest| {
                        use constellation_fs_core::manifest::ChunkInfo;
                        let chunks = match &manifest.chunks {
                            ChunkInfo::Inline(map) => map
                                .iter()
                                .take(8)
                                .map(|(index, hash)| format!("{index}:{}", hash.to_hex()))
                                .collect(),
                            ChunkInfo::Spilled(hash) => vec![format!("spilled:{}", hash.to_hex())],
                        };
                        constellation_api::ManifestStatus {
                            chunk_size: manifest.layout.chunk_size,
                            chunk_count: manifest.layout.chunk_count(manifest.file_len),
                            spilled: manifest.is_spilled(),
                            file_len: manifest.file_len,
                            digest: blake3::hash(&bytes).to_hex().to_string(),
                            chunks,
                        }
                    })
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        Ok(constellation_api::InspectStatus {
            path: normalized,
            ino: attr.ino,
            kind: format!("{:?}", attr.kind).to_lowercase(),
            size: attr.size,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            nlink: attr.nlink,
            atime_ns: attr.atime_ns,
            mtime_ns: attr.mtime_ns,
            ctime_ns: attr.ctime_ns,
            rdev: attr.rdev,
            manifest,
        })
    }

    fn open_download(
        &self,
        path: &str,
    ) -> std::result::Result<constellation_api::DownloadSession, String> {
        use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
        use constellation_fs_core::InodeKind;
        use constellation_meta::MetaStore;

        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let attr = self
            .meta
            .getattr(ino)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: stale inode"))?;
        if attr.kind != InodeKind::File {
            return Err(format!("{normalized}: not a regular file"));
        }
        let file_name = normalized
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or("download")
            .to_string();
        let size = attr.size;
        let manifest_bytes = self.meta.manifest(ino).map_err(|error| error.to_string())?;
        let manifest = match manifest_bytes {
            Some(bytes) => Manifest::decode(&bytes).map_err(|error| error.to_string())?,
            None => Manifest::empty(constellation_fs_core::DEFAULT_CHUNK_SIZE),
        };
        if manifest.file_len != size && size > 0 {
            // Prefer the inode size as the wire length; still stream from the
            // manifest's chunk map so a stale size cannot OOM the client.
            tracing::debug!(
                path = %normalized,
                inode_size = size,
                manifest_len = manifest.file_len,
                "download size mismatch; using inode size"
            );
        }

        let coop = self.coop.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, String>>(2);
        self.rt.spawn(async move {
            if size == 0 {
                return;
            }
            let hashes = match &manifest.chunks {
                ChunkInfo::Inline(map) => map.clone(),
                ChunkInfo::Spilled(hash) => match coop.fetch(hash).await {
                    Ok(blob) => match decode_chunk_list(&blob) {
                        Ok(map) => map,
                        Err(error) => {
                            let _ = tx.send(Err(error.to_string())).await;
                            return;
                        }
                    },
                    Err(error) => {
                        let _ = tx
                            .send(Err(format!("fetching spilled chunk list: {error:#}")))
                            .await;
                        return;
                    }
                },
            };
            let layout = manifest.layout;
            let file_len = size;
            let count = layout.chunk_count(file_len);
            for index in 0..count {
                let want = layout.chunk_len(file_len, index) as usize;
                let mut data = match hashes.get(&index) {
                    Some(hash) => match coop.fetch(hash).await {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            let _ = tx
                                .send(Err(format!("fetching chunk {index}: {error:#}")))
                                .await;
                            return;
                        }
                    },
                    None => Vec::new(),
                };
                data.resize(want, 0);
                if tx.send(Ok(data)).await.is_err() {
                    return;
                }
            }
        });
        Ok(constellation_api::DownloadSession {
            file_name,
            size,
            chunks: rx,
        })
    }

    fn force_release(&self, part: &str) -> std::result::Result<String, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        // A cooperative release asked for by the operator: the core's
        // handoff job, addressed as if this node itself asked.
        self.sync_tx
            .send(fusefs::SyncRequest::HandOff {
                requester: self.node_id,
                reply,
            })
            .map_err(|_| "sync task is not running".to_string())?;
        let handed = tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "lease release task stopped".to_string())?;
        match handed {
            Some(handed) => Ok(format!(
                "voluntarily released {part} at epoch {}; this was cooperative, not fencing",
                handed.epoch
            )),
            None => Err(format!(
                "{part} was not held locally or could not be flushed; no fencing was attempted"
            )),
        }
    }

    fn log_tail(&self, lines: usize) -> Vec<String> {
        self.log_buffer.tail(lines)
    }

    fn drop_held(&self, ino: u64) -> std::result::Result<String, String> {
        held::drop_held(&self.meta, ino)
    }

    fn doctor(&self) -> std::result::Result<constellation_api::DoctorStatus, String> {
        let store = ChunkStore::new(self.store.clone());
        tokio::task::block_in_place(|| {
            self.rt.block_on(async {
                let caps = store.probe_conditional_writes().await?;
                let report = constellation_store_s3::probe_cas_semantics(store.inner()).await?;
                Ok::<_, constellation_store_s3::StoreError>((caps, report))
            })
        })
        .map(|(caps, report)| constellation_api::DoctorStatus {
            create_if_absent: caps.create_if_absent,
            etag_cas: caps.etag_cas,
            cas_probes: doctor::api_probes(&report),
            versioning: report.versioning.as_str().to_string(),
        })
        .map_err(|error| error.to_string())
    }

    fn cache_list(&self) -> Vec<constellation_api::CacheEntryStatus> {
        self.cache
            .entries()
            .into_iter()
            .map(|(hash, size, state)| constellation_api::CacheEntryStatus {
                hash: hash.to_hex(),
                size,
                state: format!("{state:?}").to_lowercase(),
            })
            .collect()
    }

    fn cache_prune(&self, target_bytes: u64) -> std::result::Result<String, String> {
        let report = self
            .cache
            .prune_to(target_bytes)
            .map_err(|e| e.to_string())?;
        Ok(format!(
            "pruned {} chunks ({} bytes); {} bytes remain \
             ({} pinned, {} dirty, {} entries)",
            report.freed_chunks,
            report.freed_bytes,
            report.used_bytes,
            report.pinned_bytes,
            report.dirty_bytes,
            report.entries
        ))
    }

    fn set_quota(&self, max_bytes: Option<u64>) -> std::result::Result<String, String> {
        use constellation_meta::MetaStore;
        self.snapshot_barrier("/")?;
        self.meta
            .set_quota(max_bytes)
            .map_err(|e| format!("{e:#}"))?;
        // Node-level cap, but each mounted view caches its own read of it
        // (fusefs::QUOTA_CACHE_TTL) — invalidate every view, not just
        // whichever one happened to build this DaemonStatus.
        self.node.invalidate_quota_caches();
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(match max_bytes {
            Some(cap) => format!("quota set to {cap} bytes"),
            None => "quota cleared (unlimited)".into(),
        })
    }

    fn get_quota(&self) -> std::result::Result<(Option<u64>, u64), String> {
        use constellation_meta::MetaStore;
        let max = self.meta.quota().map_err(|e| format!("{e:#}"))?;
        let (used, _) = self.meta.usage();
        Ok((max, used))
    }

    fn prune_run(&self, path: Option<&str>, dry_run: bool) -> std::result::Result<String, String> {
        // Restrict to the marked root governing `path`, if one was given.
        let only = match path {
            Some(p) => {
                let ino = self
                    .meta
                    .resolve_path(p)
                    .map_err(|e| format!("{e:#}"))?
                    .ok_or_else(|| format!("no such path: {p}"))?;
                match self
                    .meta
                    .effective_prune_policy(ino)
                    .map_err(|e| format!("{e:#}"))?
                {
                    Some((root, _)) => Some(vec![root]),
                    None => return Err(format!("no prune policy governs {p}")),
                }
            }
            None => None,
        };
        let deps = self.prune_deps();
        let report = tokio::task::block_in_place(|| {
            self.rt
                .block_on(async { crate::prune::run(&deps, only, dry_run).await })
        })
        .map_err(|e| format!("{e:#}"))?;
        if let Some(why) = report.refused {
            return Err(format!("prune refused: {why}"));
        }
        let (mut sel, mut del) = (0u64, 0u64);
        for r in &report.roots {
            sel += r.selected;
            del += r.deleted;
        }
        Ok(format!(
            "prune {}: {} roots, {} selected, {} deleted",
            if report.dry_run { "dry-run" } else { "run" },
            report.roots.len(),
            sel,
            del
        ))
    }

    fn prune_ls(&self) -> std::result::Result<Vec<constellation_api::PruneRootStatus>, String> {
        let roots = self.meta.prune_roots().map_err(|e| format!("{e:#}"))?;
        let mut out = Vec::new();
        for (ino, expr) in roots {
            let path = self.meta.path_of(ino).unwrap_or_else(|_| "?".into());
            match constellation_meta::prune::Policy::parse(&expr) {
                Ok(policy) => {
                    let note = if let Some((
                        constellation_meta::prune::Watermark::Percent(_),
                        _,
                        constellation_meta::prune::Of::Fs,
                    )) = policy.lru_watermarks()
                    {
                        use constellation_meta::MetaStore;
                        if self.meta.quota().ok().flatten().unwrap_or(0) == 0 {
                            Some("inert: lru percentage needs a quota".to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    out.push(constellation_api::PruneRootStatus {
                        path,
                        policy: policy.to_string(),
                        armed: policy.armed,
                        valid: !policy.off,
                        note,
                    });
                }
                Err(e) => out.push(constellation_api::PruneRootStatus {
                    path,
                    policy: expr,
                    armed: false,
                    valid: false,
                    note: Some(format!("unparseable: {}", e.msg)),
                }),
            }
        }
        Ok(out)
    }

    fn gc_run(&self, verify_only: bool) -> std::result::Result<serde_json::Value, String> {
        let store = self.store.clone();
        let chunks = self.pins.chunks();
        let meta = self.meta.clone();
        let lease_mode = self.lease_mode;
        let peers = self.peers.clone();
        let tail = gc::GcTail::Daemon(self.sync_tx.clone());
        let report = tokio::task::block_in_place(|| {
            self.rt.block_on(gc::run(
                store,
                chunks,
                meta,
                lease_mode,
                verify_only,
                Some(&peers),
                &tail,
            ))
        })
        .map_err(|e| format!("{e:#}"))?;
        serde_json::to_value(&report).map_err(|e| format!("{e:#}"))
    }

    fn fsck_run(
        &self,
        repair: bool,
        force_release: Option<&str>,
    ) -> std::result::Result<serde_json::Value, String> {
        let store = self.store.clone();
        let chunks = self.pins.chunks();
        let meta = self.meta.clone();
        let lease_mode = self.lease_mode;
        let state_dir = self.state_dir.clone();
        let compression = self.compression;
        let force_release = force_release.map(str::to_string);
        let report = tokio::task::block_in_place(|| {
            self.rt.block_on(async move {
                let logs = match chunks.e2e_keys() {
                    Some(keys) => {
                        constellation_store_s3::LogStore::new_e2e(store.clone(), keys.clone())
                    }
                    None => constellation_store_s3::LogStore::new(store.clone()),
                };
                fsck::run(
                    store,
                    chunks,
                    &logs,
                    meta,
                    Some(&state_dir),
                    compression,
                    lease_mode,
                    repair,
                    force_release.as_deref(),
                )
                .await
            })
        })
        .map_err(|e| format!("{e:#}"))?;
        serde_json::to_value(&report).map_err(|e| format!("{e:#}"))
    }

    fn mount_add(
        &self,
        subtree: &str,
        mountpoint: &std::path::Path,
        opts: &constellation_api::MountViewOpts,
    ) -> std::result::Result<String, String> {
        let fuse_threads = opts
            .fuse_threads
            .unwrap_or_else(|| parallelism::thread_plan().fuse);
        let id = self
            .node
            .add_mount(node_runtime::ViewConfig {
                inner_path: subtree.to_string(),
                mountpoint: mountpoint.to_path_buf(),
                allow_other: opts.allow_other,
                fs_name: opts
                    .fs_name
                    .clone()
                    .unwrap_or_else(|| "constellation".to_string()),
                fuse_threads,
                rw_snapshot: opts.rw,
                clone_name: opts.clone_name.clone(),
                ephemeral: opts.ephemeral,
            })
            .map_err(|e| format!("{e:#}"))?;
        Ok(format!(
            "mounted view {} at {}",
            id.as_u64(),
            mountpoint.display()
        ))
    }

    fn mount_remove(&self, mountpoint: &std::path::Path) -> std::result::Result<String, String> {
        let id = self
            .node
            .mounts()
            .into_iter()
            .find(|m| m.mountpoint == mountpoint)
            .map(|m| m.id)
            .ok_or_else(|| format!("no view mounted at {}", mountpoint.display()))?;
        // `remove_mount` calls `fusermount3 -u` and then joins the FUSE
        // session's OS thread, which can legitimately take a while
        // (draining in-flight requests). `dispatch` runs this on a tokio
        // worker with no `.await` in between, so without `block_in_place`
        // that worker — and every other task scheduled on it — would
        // stall for however long the unmount takes, same class of bug as
        // `reintegrate`/`gc_run`/`fsck_run` below already guard against.
        tokio::task::block_in_place(|| self.node.remove_mount(id)).map_err(|e| format!("{e:#}"))?;
        Ok(format!("unmounted {}", mountpoint.display()))
    }

    fn mount_list(&self) -> Vec<constellation_api::MountInfo> {
        self.api_mounts()
    }
}

fn normalize_control_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        "/".into()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn default_state_dir(meta: &FsMeta) -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_default();
            home.join(".local/share")
        });
    base.join("constellation").join(meta.uuid.to_string())
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

/// Plan 07's `pending_upload`-driven regression tests for
/// `upload_dirty_chunks`: the durable not-yet-uploaded set must survive
/// a crash even though `DiskCache::rescan` legitimately reports every
/// rediscovered chunk `Clean` (prerequisite 1), and a failed drain must
/// leave the pending row rather than silently dropping it (the "journal
/// must wait" state that backs prerequisite 2's unmount gate).
#[cfg(test)]
mod pending_upload_tests {
    use super::*;
    use constellation_fs_core::cache::ChunkState;
    use constellation_fs_core::ChunkHash;
    use constellation_meta::MetaStore;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Wraps an in-memory backend and can be told to fail every `put`,
    /// simulating a cut S3 path without needing toxiproxy for a unit
    /// test.
    #[derive(Debug)]
    struct FailingStore {
        inner: InMemory,
        fail_puts: AtomicBool,
        delay_puts: AtomicBool,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        puts: AtomicUsize,
        heads: AtomicUsize,
    }

    impl FailingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                fail_puts: AtomicBool::new(false),
                delay_puts: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                puts: AtomicUsize::new(0),
                heads: AtomicUsize::new(0),
            })
        }

        fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }

        fn set_delay_puts(&self, delay: bool) {
            self.delay_puts.store(delay, Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for FailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(active, Ordering::SeqCst);
            if self.delay_puts.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            if self.fail_puts.load(Ordering::SeqCst) {
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                return Err(object_store::Error::Generic {
                    store: "FailingStore",
                    source: "S3 path is cut (test injection)".into(),
                });
            }
            let result = self.inner.put_opts(location, payload, opts).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.heads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct Fixture {
        meta: Meta,
        cache: Arc<DiskCache>,
        cache_dir: PathBuf,
        store: Arc<ChunkStore>,
        failing: Arc<FailingStore>,
        _cache_tmp: tempfile::TempDir,
    }

    impl Fixture {
        /// Reopen the cache from the same directory: `DiskCache::open`
        /// rebuilds accounting purely from what is on disk, the same
        /// path a real remount after `kill -9` takes.
        fn reopen_cache_simulating_crash(&mut self) {
            self.cache = Arc::new(DiskCache::open(&self.cache_dir, 64 * 1024 * 1024).unwrap());
        }
    }

    fn fixture() -> Fixture {
        let failing = FailingStore::new();
        let cache_tmp = tempfile::tempdir().unwrap();
        let cache_dir = cache_tmp.path().to_path_buf();
        Fixture {
            meta: Meta::open_in_memory().unwrap(),
            cache: Arc::new(DiskCache::open(&cache_dir, 64 * 1024 * 1024).unwrap()),
            cache_dir,
            store: Arc::new(ChunkStore::new(failing.clone() as Arc<dyn ObjectStore>)),
            failing,
            _cache_tmp: cache_tmp,
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Regression test for prerequisite 1: after a "crash",
    /// `DiskCache::rescan` reports the chunk `Clean` (it cannot tell
    /// uploaded from un-uploaded content from a directory listing
    /// alone), but the durable `pending_upload` row must still drive the
    /// drain to completion.
    #[test]
    fn drain_finds_pending_row_even_though_cache_reports_clean_after_rescan() {
        let mut f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let data = b"post-crash content".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();

        // Simulate the crash: reopening the cache rebuilds accounting
        // purely from disk and legitimately reports Clean (see
        // `cache::tests::rescan_rebuilds_accounting`).
        f.reopen_cache_simulating_crash();
        assert_eq!(f.cache.state_of(&hash), Some(ChunkState::Clean));
        assert_eq!(f.meta.pending_uploads().unwrap(), vec![(hash, file.ino)]);

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();

        assert!(
            f.meta.pending_uploads().unwrap().is_empty(),
            "drain must ack the pending row once the chunk is uploaded"
        );
        let uploaded = rt().block_on(f.store.get_chunk(&hash)).unwrap();
        assert_eq!(uploaded, data);
    }

    /// Regression test for prerequisite 2's failure mode: while S3 is
    /// unreachable, the drain must refuse (not silently drop the
    /// pending row), which is exactly the signal the clean-unmount path
    /// uses to call `set_skip_ship(true)` rather than shipping a
    /// manifest for content that never reached S3.
    #[test]
    fn failed_drain_leaves_the_pending_row_for_the_next_attempt() {
        let f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let data = b"never uploaded".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();

        f.failing.set_fail_puts(true);
        let err = rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ));
        assert!(err.is_err(), "drain must fail while S3 is unreachable");
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(hash, file.ino)],
            "a failed drain must not ack the row it could not upload"
        );

        // Heal, retry: the very next attempt must succeed and ack.
        f.failing.set_fail_puts(false);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    /// Plan 29 M6's characterization test, flipped by plan 30 §M4 item 2
    /// (see `bench/remote/RESULTS.md` anomaly #2): a pending row whose chunk
    /// is gone from the local cache used to fail the *entire* round, and so
    /// block every other inode's manifest from ever shipping, forever. Now
    /// the upload pass reports it instead of failing, records it as
    /// unrecoverable, and the ship plan holds back only that inode's
    /// manifest (and whatever depends on it): **other inodes still
    /// publish**, including ones written after the broken one.
    #[test]
    fn one_missing_chunk_holds_back_only_its_own_records() {
        let f = fixture();
        // Holder capture on (this node holds the lease), so every
        // transaction has a key set and the isolation is exact.
        f.meta.set_holder_epoch(1);
        let healthy_file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "healthy",
                0o644,
                0,
                0,
            )
            .unwrap();
        let healthy_data = b"perfectly fine content".to_vec();
        let healthy_hash = ChunkHash::of(&healthy_data);
        f.cache
            .insert(&healthy_hash, &healthy_data, ChunkState::Dirty)
            .unwrap();
        f.meta
            .set_manifest_dirty(
                healthy_file.ino,
                None,
                b"M",
                healthy_data.len() as u64,
                &[healthy_hash],
            )
            .unwrap();

        // "broken": a pending_upload row with nothing behind it in the
        // cache -- the observed field condition, reproduced directly
        // rather than via whatever race produces it in practice.
        let broken_file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "broken",
                0o644,
                0,
                0,
            )
            .unwrap();
        let broken_hash = ChunkHash::of(b"bytes that are gone");
        f.meta
            .set_manifest_dirty(broken_file.ino, None, b"M2", 4, &[broken_hash])
            .unwrap();
        assert!(
            f.cache.get(&broken_hash).unwrap().is_none(),
            "the broken hash must not be in the cache"
        );
        // Written after the broken one, touching none of its keys.
        let later_file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "later", 0o644, 0, 0)
            .unwrap();

        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .expect("a missing chunk no longer fails the round");
        assert_eq!(report.missing, vec![(broken_hash, broken_file.ino)]);
        assert_eq!(
            f.meta.unrecoverable_chunks().unwrap(),
            vec![(broken_hash, broken_file.ino)]
        );
        // The healthy chunk uploaded and acked; only the broken row stays.
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(broken_hash, broken_file.ino)]
        );
        let uploaded = rt().block_on(f.store.get_chunk(&healthy_hash)).unwrap();
        assert_eq!(uploaded, healthy_data);
        // A caller that needs everything durable (an fsync, an unmount's
        // final flush) still gets the error.
        assert!(rt()
            .block_on(upload_dirty_chunks(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .is_err());

        // The ship plan: everything but the broken manifest ships — the
        // healthy file, the broken file's own create (it names no chunk),
        // and the later file.
        let batch: Vec<(u64, constellation_meta::LogRecord)> = f
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, batch)| batch)
            .collect();
        let manifests: Vec<u64> = batch
            .iter()
            .filter_map(|(_, rec)| match rec {
                constellation_meta::LogRecord::WriteManifest { ino, .. } => Some(*ino),
                _ => None,
            })
            .collect();
        assert_eq!(manifests, vec![healthy_file.ino]);
        let creates: Vec<String> = batch
            .iter()
            .filter_map(|(_, rec)| match rec {
                constellation_meta::LogRecord::Create { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(creates, vec!["healthy", "broken", "later"]);
        let held = f.meta.held_summary();
        assert_eq!(held.transactions, 1, "{held:?}");
        assert_eq!(held.inodes[&broken_file.ino].missing, vec![broken_hash]);
        assert!(later_file.ino != broken_file.ino);

        // Ship what was planned: the held manifest is still journaled,
        // still outstanding speculation, and still held next round.
        let seqs: Vec<u64> = batch.iter().map(|(seq, _)| *seq).collect();
        f.meta.ack_journal_rows_at(&seqs, 1).unwrap();
        let rest: Vec<(u64, constellation_meta::LogRecord)> =
            constellation_meta::MetaStore::take_journal(&f.meta, usize::MAX).unwrap();
        assert!(rest.iter().any(|(_, rec)| matches!(
            rec,
            constellation_meta::LogRecord::WriteManifest { ino, .. } if *ino == broken_file.ino
        )));
        assert!(f.meta.take_journal_grouped(10_000).unwrap().is_empty());
        assert_eq!(f.meta.speculation_counts().unwrap().local, 1);
    }

    /// Plan 30 §M9 × §M4: a successor adopts, from its predecessor's
    /// backup tail, a manifest naming one chunk the predecessor uploaded
    /// and one it never did (a write-back close acknowledged before its
    /// upload). The adopted manifest never ships while a chunk it names is
    /// missing from the bucket: it is deferred until the upload pass has
    /// looked, then held (M4 status) — and it ships once the missing chunk
    /// turns up (the predecessor back, uploading). The same for a spilled
    /// manifest, whose list is read from its blob.
    fn adopted_manifest_ships_only_once_its_chunks_are_durable(spilled: bool) {
        use constellation_fs_core::{manifest::encode_chunk_list, Manifest};
        let f = fixture();
        f.meta.set_holder_epoch(2);
        let file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "adopted",
                0o644,
                0,
                0,
            )
            .unwrap();
        let shipped: Vec<u64> = constellation_meta::MetaStore::take_journal(&f.meta, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        f.meta.ack_journal_rows_at(&shipped, 1).unwrap();

        let uploaded = b"uploaded by the predecessor".to_vec();
        let lost = b"only on the predecessor's disk".to_vec();
        let (up_hash, lost_hash) = (ChunkHash::of(&uploaded), ChunkHash::of(&lost));
        rt().block_on(
            f.store
                .put_chunk(&up_hash, &uploaded, CompressionSetting::RAW),
        )
        .unwrap();
        let chunks: constellation_fs_core::manifest::SparseChunks =
            [(0u64, up_hash), (1u64, lost_hash)].into_iter().collect();
        let (manifest, blob) = if spilled {
            let blob = encode_chunk_list(&chunks);
            let blob_hash = ChunkHash::of(&blob);
            rt().block_on(
                f.store
                    .put_chunk(&blob_hash, &blob, CompressionSetting::RAW),
            )
            .unwrap();
            let (m, spill) = Manifest::from_sparse_chunks(4096, 8192, chunks, 0, ChunkHash::of);
            assert!(spill.is_some());
            (m, Some(blob_hash))
        } else {
            (
                Manifest::from_sparse_chunks(4096, 8192, chunks, 8, ChunkHash::of).0,
                None,
            )
        };
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 7,
        };
        f.meta
            .apply_adopted_records(
                &[
                    constellation_meta::LogRecord::WriteManifest {
                        ino: file.ino,
                        base_manifest: None,
                        manifest: manifest.encode(),
                        size: 8192,
                        time_ns: 1,
                    },
                    constellation_meta::LogRecord::Completed { rid },
                ],
                Some(rid),
            )
            .unwrap();
        let manifests_shipped = |f: &Fixture| -> Vec<u64> {
            f.meta
                .take_journal_grouped(10_000)
                .unwrap()
                .into_iter()
                .flat_map(|(_, batch)| batch)
                .filter_map(|(_, rec)| match rec {
                    constellation_meta::LogRecord::WriteManifest { ino, .. } => Some(ino),
                    _ => None,
                })
                .collect()
        };
        // Enrolled: deferred before any upload pass has looked.
        assert!(!f.meta.pending_uploads().unwrap().is_empty());
        assert!(manifests_shipped(&f).is_empty(), "shipped before any check");
        if spilled {
            assert_eq!(
                f.meta.adopted_spills().unwrap(),
                vec![(file.ino, blob.unwrap())]
            );
        }

        // The upload pass: the uploaded chunk (and the blob) are
        // acknowledged from S3; the other is recorded unrecoverable.
        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert_eq!(report.missing, vec![(lost_hash, file.ino)]);
        assert!(
            f.meta.adopted_spills().unwrap().is_empty(),
            "the list was expanded"
        );
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(lost_hash, file.ino)]
        );
        assert!(
            manifests_shipped(&f).is_empty(),
            "a dangling manifest shipped"
        );
        let held = f.meta.held_summary();
        assert_eq!(held.transactions, 1, "{held:?}");
        assert_eq!(held.inodes[&file.ino].missing, vec![lost_hash]);
        // Held work is `Local` speculation: never published.
        assert_eq!(f.meta.speculation_counts().unwrap().local, 1);

        // The predecessor is back and uploads: the next pass finds the
        // chunk in S3, and the manifest ships.
        rt().block_on(
            f.store
                .put_chunk(&lost_hash, &lost, CompressionSetting::RAW),
        )
        .unwrap();
        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert!(report.missing.is_empty());
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        assert_eq!(manifests_shipped(&f), vec![file.ino]);
    }

    #[test]
    fn an_adopted_manifest_ships_only_once_its_chunks_are_durable() {
        adopted_manifest_ships_only_once_its_chunks_are_durable(false);
    }

    #[test]
    fn an_adopted_spilled_manifest_ships_only_once_its_chunks_are_durable() {
        adopted_manifest_ships_only_once_its_chunks_are_durable(true);
    }

    /// Two failovers inside one upload window: B adopts A's manifest (its
    /// chunk only on A), then B is deposed before shipping it; the
    /// stranded adopted transaction is replayed by rid through the next
    /// holder C as a `Records` op. C must enroll its chunks for the
    /// durability check as B did — before, `Records` executed without
    /// enrolling anything and C shipped a manifest naming a missing chunk.
    #[test]
    fn a_stranded_adopted_manifest_replayed_elsewhere_is_still_held() {
        use constellation_fs_core::Manifest;
        let b = fixture();
        let c = fixture();
        b.meta.set_holder_epoch(2);
        let file = b
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let on_c = c
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        assert_eq!(file.ino, on_c.ino, "the same file on both replicas");
        let lost = ChunkHash::of(b"only on A");
        let chunks: constellation_fs_core::manifest::SparseChunks =
            [(0u64, lost)].into_iter().collect();
        let manifest = Manifest::from_sparse_chunks(4096, 9, chunks, 8, ChunkHash::of).0;
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 3,
        };
        b.meta
            .apply_adopted_records(
                &[
                    constellation_meta::LogRecord::WriteManifest {
                        ino: file.ino,
                        base_manifest: None,
                        manifest: manifest.encode(),
                        size: 9,
                        time_ns: 1,
                    },
                    constellation_meta::LogRecord::Completed { rid },
                ],
                Some(rid),
            )
            .unwrap();
        // B is deposed (C's epoch 3): the adopted transaction strands.
        b.meta.set_holder_epoch(0);
        b.meta.strand_below_epoch(3).unwrap();
        let queue = b.meta.pending_replays().unwrap();
        let queued = queue.iter().find(|q| q.rid == rid).expect("queued by rid");
        assert!(matches!(
            queued.op,
            constellation_meta::MutateOp::Records { .. }
        ));

        // C executes the replay as holder.
        c.meta.set_holder_epoch(3);
        constellation_meta::execute_mutate(&c.meta, &queued.op, Some(rid)).unwrap();
        assert_eq!(c.meta.pending_uploads().unwrap(), vec![(lost, file.ino)]);
        rt().block_on(upload_dirty_chunks_report(
            &c.cache,
            &c.meta,
            &c.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        let shipped: Vec<constellation_meta::LogRecord> = c
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, batch)| batch)
            .map(|(_, rec)| rec)
            .collect();
        assert!(
            !shipped
                .iter()
                .any(|r| matches!(r, constellation_meta::LogRecord::WriteManifest { .. })),
            "C shipped a manifest naming a chunk the bucket lacks: {shipped:?}"
        );
        assert!(c.meta.held_summary().inodes.contains_key(&file.ino));
    }

    /// The other way out: the predecessor never comes back, and the
    /// operator drops the held manifest (`repair drop-held`): it becomes a
    /// refused replay (a conflict copy with the lost chunk as a hole).
    #[test]
    fn an_adopted_manifest_with_a_lost_chunk_can_be_dropped() {
        use constellation_fs_core::Manifest;
        let f = fixture();
        f.meta.set_holder_epoch(2);
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "lost", 0o644, 0, 0)
            .unwrap();
        let lost_hash = ChunkHash::of(b"gone with the predecessor");
        let chunks: constellation_fs_core::manifest::SparseChunks =
            [(0u64, lost_hash)].into_iter().collect();
        let manifest = Manifest::from_sparse_chunks(4096, 25, chunks, 8, ChunkHash::of).0;
        f.meta
            .apply_adopted_records(
                &[constellation_meta::LogRecord::WriteManifest {
                    ino: file.ino,
                    base_manifest: None,
                    manifest: manifest.encode(),
                    size: 25,
                    time_ns: 1,
                }],
                None,
            )
            .unwrap();
        rt().block_on(upload_dirty_chunks_report(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        // (The summary is the last ship plan's; the create ships.)
        assert!(f
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .all(|(_, rec)| !matches!(rec, constellation_meta::LogRecord::WriteManifest { .. })));
        assert!(f.meta.held_summary().inodes.contains_key(&file.ino));
        let dropped = f.meta.drop_held(file.ino, 1).unwrap();
        assert_eq!(dropped.dropped, 1, "{dropped:?}");
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        let queue = f.meta.pending_replays().unwrap();
        assert_eq!(queue.len(), 1);
        assert!(queue[0].refused.is_some(), "a conflict copy is queued");
        assert!(f
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .all(|(_, rec)| !matches!(rec, constellation_meta::LogRecord::WriteManifest { .. })));
    }

    #[test]
    fn upload_pool_honours_its_bound() {
        let f = fixture();
        f.failing.set_delay_puts(true);
        for i in 0..12u8 {
            let file = f
                .meta
                .create(
                    constellation_fs_core::types::ROOT_INO,
                    &format!("f-{i}"),
                    0o644,
                    0,
                    0,
                )
                .unwrap();
            let data = vec![i; 4096];
            let hash = ChunkHash::of(&data);
            f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
            f.meta
                .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
                .unwrap();
        }
        let upload = UploadRuntime::for_test_fixed(true, 3);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert!(f.failing.max_in_flight.load(Ordering::SeqCst) <= 3);
        assert!(
            f.failing.max_in_flight.load(Ordering::SeqCst) >= 2,
            "the test must observe actual parallelism"
        );
    }

    fn queue(f: &Fixture, name: &str, data: &[u8]) -> ChunkHash {
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, name, 0o644, 0, 0)
            .unwrap();
        let hash = ChunkHash::of(data);
        f.cache.insert(&hash, data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        hash
    }

    /// A hinted hash confirms with a HEAD and skips the PUT entirely; an
    /// unhinted one keeps the adaptive fallback, because no hint source can
    /// prove absence any more (plan 26 step 8 deleted the LIST seed that
    /// could). Neither decision costs a LIST.
    #[test]
    fn hinted_hash_probes_and_unhinted_hash_keeps_the_adaptive_fallback() {
        let f = fixture();
        let known_data = b"already in S3";
        let known = ChunkHash::of(known_data);
        rt().block_on(f.store.put_chunk_mode(
            &known,
            known_data,
            CompressionSetting::RAW,
            constellation_store_s3::ChunkPutMode::Create,
        ))
        .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);

        queue(&f, "known", known_data);
        queue(&f, "new", b"not in S3");
        let existence = crate::existence::Existence::new(1024, true, None);
        existence.insert(&known);
        let mut upload = UploadRuntime::new(true, None, existence);
        upload.probe_min_bytes = 0;
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();

        assert_eq!(
            f.failing.heads.load(Ordering::SeqCst),
            2,
            "the hint confirms with a HEAD; the unhinted chunk probes too"
        );
        assert_eq!(
            f.failing.puts.load(Ordering::SeqCst),
            1,
            "only the chunk that is genuinely absent is uploaded"
        );
        assert_eq!(upload.existence.report().bloom_hits, 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn bloom_false_positive_still_calls_store_before_ack() {
        let f = fixture();
        let data = b"forced false positive";
        let hash = queue(&f, "false-positive", data);
        let existence = crate::existence::Existence::new(1024, true, None);
        existence.insert(&hash);
        let mut upload = UploadRuntime::new(true, None, existence);
        upload.probe_min_bytes = 0;

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn peer_hit_selects_probe_but_peer_miss_retains_adaptive_head() {
        let f = fixture();
        let hinted = ChunkHash::of(b"hinted");
        let bloom = constellation_net::Bloom::from_hashes(&[hinted.0]);
        let coop = crate::coop::Coop::new_for_upload_test(f.cache.clone(), f.store.clone());
        coop.apply_digest(constellation_net::DigestSnapshot {
            node_id: 2,
            generation: 1,
            bits: bloom.bits,
            nbits: bloom.nbits,
            k: bloom.k,
            n: bloom.n,
            bucket: 0,
            buckets: 1,
        });
        let existence = crate::existence::Existence::new(1024, true, None);
        let mut upload = UploadRuntime::new(true, Some(coop), existence);
        upload.probe_min_bytes = 0;
        assert_eq!(
            upload.put_mode(&hinted, 1 << 20),
            constellation_store_s3::ChunkPutMode::Probe
        );
        assert_eq!(upload.existence.report().peer_hints, 1);
        assert_eq!(
            upload.put_mode(&ChunkHash::of(b"peer miss"), 1 << 20),
            constellation_store_s3::ChunkPutMode::Probe,
            "an unhinted hash must keep the adaptive probe"
        );
    }

    #[test]
    fn condemned_hash_overwrites_even_when_existence_bloom_claims_present() {
        let f = fixture();
        let data = b"condemned existence hit";
        let hash = queue(&f, "condemned", data);
        rt().block_on(constellation_store_s3::publish_condemned(
            f.store.inner(),
            vec![hash.to_hex()],
            1,
        ))
        .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);
        let existence = crate::existence::Existence::new(1024, true, None);
        existence.insert(&hash);
        let mut upload = UploadRuntime::new(true, None, existence);
        upload.probe_min_bytes = 0;

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        // The hinted HEAD comes first now (`CondemnedView`: the pointer is
        // read only once the object is there); the chunk is not in S3,
        // so the probe's miss uploads it.
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());

        // In S3 and condemned: the HEAD finds it, the pointer (read after
        // it) lists it, and the bytes go up again.
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "again", 0o644, 0, 0)
            .unwrap();
        f.cache.insert(&hash, data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.failing.puts.load(Ordering::SeqCst),
            1,
            "a condemned hit is re-uploaded"
        );
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    /// The sequencer's side of a non-owner's `back` close: a chunk the
    /// forward named as pending is in neither its cache nor S3. The pass
    /// awaits it — not "missing" (no poison, no held records), not an
    /// error even for a strict drain — and checks S3 itself only once the
    /// first backoff is over; found there, the row is acked.
    #[test]
    fn a_chunk_forwarded_as_pending_is_awaited_not_lost() {
        let f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "fwd", 0o644, 0, 0)
            .unwrap();
        let data = b"uploading on the forwarder";
        let hash = ChunkHash::of(data);
        f.meta.enroll_remote_chunks(file.ino, &[hash], 2).unwrap();
        let upload = UploadRuntime::for_test(true);
        let pass = |upload: &UploadRuntime| {
            rt().block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                upload,
                None,
                None,
            ))
            .unwrap()
        };
        let report = pass(&upload);
        assert!(report.missing.is_empty());
        assert_eq!(report.awaiting, 1);
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 0, "no S3 check yet");
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            Some(file.ino),
            None,
        ))
        .expect("an awaited chunk is not a lost one");
        assert!(
            f.meta.unrecoverable_chunks().unwrap().is_empty(),
            "nothing poisoned"
        );
        // The forwarder's upload lands; its report was lost; the backoff
        // runs out and this node finds the chunk itself.
        rt().block_on(f.store.put_chunk(&hash, data, CompressionSetting::RAW))
            .unwrap();
        upload
            .remote_polls
            .lock()
            .unwrap()
            .insert(hash, (std::time::Instant::now(), REMOTE_POLL_FIRST));
        let report = pass(&upload);
        assert_eq!(report.awaiting, 0);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        assert!(!f.meta.awaits_remote_chunk(&hash).unwrap());
    }

    /// The forwarder's side: once its pass puts a chunk it forwarded as
    /// pending up, it owes the node it forwarded to a report — and only
    /// for those chunks.
    #[test]
    fn a_chunk_forwarded_as_pending_is_reported_once_up() {
        let f = fixture();
        let forwarded = queue(&f, "forwarded", b"forwarded while pending");
        let other = queue(&f, "other", b"never forwarded");
        let upload = UploadRuntime::for_test(true);
        upload.note_forwarded(&[forwarded], 9);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        let reports = upload.take_durable_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[&9], vec![forwarded]);
        assert!(!reports[&9].contains(&other));
        assert!(upload.take_durable_reports().is_empty(), "sent once");
    }

    /// What a forward says is still pending: the chunks its manifest names
    /// that have a pending row here, nothing for any other op.
    #[test]
    fn a_forward_names_only_the_chunks_still_pending() {
        let f = fixture();
        let pending = queue(&f, "p", b"pending here");
        let durable = ChunkHash::of(b"already up");
        let manifest = |hashes: &[ChunkHash]| {
            constellation_fs_core::Manifest::from_sparse_chunks(
                4096,
                4096 * hashes.len() as u64,
                hashes
                    .iter()
                    .enumerate()
                    .map(|(i, h)| (i as u64, *h))
                    .collect(),
                8,
                ChunkHash::of,
            )
            .0
            .encode()
        };
        let op = constellation_meta::MutateOp::SetManifest {
            ino: 5,
            base_manifest: None,
            manifest: manifest(&[durable, pending]),
            size: 8192,
        };
        assert_eq!(
            forwarded_pending_chunks(&f.meta, &f.cache, &op),
            vec![pending]
        );
        let other = constellation_meta::MutateOp::Unlink {
            parent: constellation_fs_core::types::ROOT_INO,
            name: "p".into(),
        };
        assert!(forwarded_pending_chunks(&f.meta, &f.cache, &other).is_empty());
    }

    /// The OVH run's finding 3: a small chunk does not probe on the
    /// ladder's guess — a HEAD-first probe of a unique small file is two
    /// serialized round trips where a conditional create is one. A
    /// positive hint (the bytes were seen up) still probes: a HEAD is
    /// cheaper than re-sending them.
    #[test]
    fn a_small_chunk_creates_instead_of_probing() {
        let f = fixture();
        let data = b"a small file's only chunk";
        let hash = queue(&f, "small", data);
        let existence = crate::existence::Existence::new(1024, true, None);
        let hinted = ChunkHash::of(b"seen up before");
        existence.insert(&hinted);
        let upload = UploadRuntime::new(true, None, existence);
        assert_eq!(
            upload.put_mode(&hinted, 16),
            constellation_store_s3::ChunkPutMode::Probe,
            "a hinted small chunk probes"
        );
        assert!(
            upload.probe.lock().unwrap().enabled(),
            "the ladder leans to Probe"
        );
        assert_eq!(
            upload.put_mode(&hash, data.len()),
            constellation_store_s3::ChunkPutMode::Create
        );
        assert_eq!(
            upload.put_mode(&hash, 4 << 20),
            constellation_store_s3::ChunkPutMode::Probe,
            "a large chunk still follows the ladder"
        );
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 0);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    /// The clean-demotion race, as a deterministic interleaving. Writer 1
    /// and writer 2 store the same content on one node. Writer 2's
    /// `cache_for_upload` merges into writer 1's dirty entry, but writer
    /// 2 has not enrolled its pending row yet (that happens at its
    /// manifest commit) when writer 1's upload round finishes: the round
    /// sees no pending row for the hash and demotes the chunk to clean,
    /// and cache pressure evicts it. Writer 2 then enrols. Its chunk is
    /// gone from the cache but durable in S3 (writer 1 uploaded the very
    /// same bytes), so its round must acknowledge it rather than report it
    /// lost and hold writer 2's records back.
    #[test]
    fn a_writer_enrolled_after_demotion_and_eviction_is_acknowledged_not_held() {
        let f = fixture();
        let data = b"same bytes, two writers".to_vec();
        let hash = ChunkHash::of(&data);
        let root = constellation_fs_core::types::ROOT_INO;
        let w1 = f.meta.create(root, "w1", 0o644, 0, 0).unwrap();
        let w2 = f.meta.create(root, "w2", 0o644, 0, 0).unwrap();
        // Writer 1 caches and commits.
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(w1.ino, None, b"M1", data.len() as u64, &[hash])
            .unwrap();
        // Writer 2's cache_for_upload: a merge into the dirty entry.
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        // Writer 1's round uploads and, seeing no pending row, demotes.
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.cache.state_of(&hash), Some(ChunkState::Clean));
        // Cache pressure evicts the clean chunk; then writer 2 enrols.
        f.cache.prune_to(0).unwrap();
        assert!(!f.cache.contains(&hash));
        f.meta
            .set_manifest_dirty(w2.ino, None, b"M2", data.len() as u64, &[hash])
            .unwrap();

        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert!(
            report.missing.is_empty(),
            "held although durable: {:?}",
            report.missing
        );
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        assert!(f.meta.unrecoverable_chunks().unwrap().is_empty());
        assert_eq!(rt().block_on(f.store.get_chunk(&hash)).unwrap(), data);
    }

    /// The acknowledgement above trusts S3 exactly as far as a dedup
    /// does: a chunk bucket GC has condemned may be deleted at any moment,
    /// so a pending row whose chunk is gone from the cache and condemned
    /// in the bucket is still reported lost (and its records held), never
    /// acknowledged.
    #[test]
    fn a_cache_missing_chunk_condemned_in_s3_is_still_reported_lost() {
        let f = fixture();
        let data = b"condemned and evicted".to_vec();
        let hash = queue(&f, "condemned-evicted", &data);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        rt().block_on(constellation_store_s3::publish_condemned(
            f.store.inner(),
            vec![hash.to_hex()],
            1,
        ))
        .unwrap();
        f.cache.prune_to(0).unwrap();
        let late = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "late", 0o644, 0, 0)
            .unwrap();
        f.meta
            .set_manifest_dirty(late.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert_eq!(report.missing, vec![(hash, late.ino)]);
        assert_eq!(f.meta.pending_uploads().unwrap(), vec![(hash, late.ino)]);
    }
}

/// Regression for plan 29 M3b: `constellation umount myfs:/sub` used to
/// hang forever on a shared daemon that still had sibling views mounted.
/// `cmd_umount` unconditionally waited for `control.sock` to disappear,
/// but `NodeRuntime::remove_mount` only ever deletes it when the removed
/// view was the *last* one (`NodeRuntime::shutdown`) — a daemon that keeps
/// serving another view never deletes it, so the wait never ended even
/// though the view being unmounted had cleanly detached
/// (`fusefs::run` logs "FUSE detached" and moves on).
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
                s3: backend.clone(),
                state_dir: Some(state_dir.clone()),
                cache_size: 16 * 1024 * 1024,
                fsync_s3: false,
                cto_strict: false,
                locks: None,
                initial_write_mode: writeback::WriteMode::Through,
                read_only_member: false,
                web_ui: 0,
                log_buffer: log_buffer::LogBuffer::default(),
                atime_mode: atime::AtimeMode::Off,
                passphrase: None,
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

        let sock = state_dir.join(constellation_api::SOCKET_NAME);
        let before = rt
            .block_on(constellation_api::call(
                &sock,
                &constellation_api::Request::MountList,
            ))
            .unwrap();
        assert!(
            matches!(&before, constellation_api::Response::Mounts { mounts } if mounts.len() == 2),
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
        let after = rt
            .block_on(constellation_api::call(
                &sock,
                &constellation_api::Request::MountList,
            ))
            .expect("daemon should still be reachable (root view still mounted)");
        match after {
            constellation_api::Response::Mounts { mounts } => {
                assert_eq!(
                    mounts.len(),
                    1,
                    "expected exactly the root view left: {mounts:?}"
                );
                assert_eq!(mounts[0].subtree, "/");
            }
            other => panic!("unexpected response: {other:?}"),
        }
        assert!(
            std::fs::metadata(root_mnt.join("sub")).is_ok(),
            "root view must still be serving reads after the sub view was unmounted"
        );

        // Clean up: unmount the remaining root view so the daemon exits.
        rt.block_on(cmd_umount("myfs".to_string(), Some(state_dir.clone())))
            .expect("final umount");
    }
}
