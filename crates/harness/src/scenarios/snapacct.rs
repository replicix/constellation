//! Plan 32 §11 `snapacct`: the end-to-end proof that the reclaim estimate
//! is the truth.
//!
//! Two nodes share one filesystem. A seeded workload — creates (small and
//! multi-chunk files), whole and partial overwrites, truncates, reverts to
//! an old content (a chunk's second run, the "afterlife"), hardlinks across
//! the snapshotted subtrees' boundaries, copies (the same chunks in two
//! chains), unlinks, and directories moved into, out of and between the
//! nested subtrees — runs in twelve rounds, the writer alternating between
//! the nodes. After every round the other node snapshots `/proj`
//! (`p01`…`p12`), and every second round `/proj/sub` (`s02`…`s12`): two
//! nested chains. Node `a` maintains its accounting index from the start
//! (`CONSTELLATION_SNAPACCT=on`, applied snapshot by snapshot); node `b`
//! builds its own from scratch on the first size request (`auto`).
//!
//! Then:
//!
//! 1. **Quiesce.** The workload stops, every node's journal and uploads
//!    drain, both mounts hold the same tree, and one GC round runs to
//!    completion. This is a precondition of the equality below, not a
//!    convenience: an overwritten version that no snapshot ever saw is
//!    garbage the index never indexes (the index holds snapshot-referenced
//!    chunks only), so GC would delete it on top of any reclaim set. From
//!    here to the last GC round the live tree is left unchanged; a write in
//!    between would let GC legitimately delete more (or less) than the
//!    estimate covers.
//! 2. `snapshot space --verify` reports 0 mismatches on **both** nodes.
//! 3. `snapshot.reclaim` of the middle range `/proj@p04%p08` with
//!    `list_chunks` (the control method's test aid) on both nodes: the same
//!    chunk set and the same bytes (a chunk counts at the largest size it
//!    occurs at, a function of the snapshots alone), not empty, and the
//!    count `snapshot delete --dry-run` prints.
//! 4. The range is deleted for real (from `b`); both indexes then show
//!    exactly that set as "awaiting GC".
//! 5. A GC round with a zero horizon (`CONSTELLATION_GC_HORIZON_S=0`,
//!    1 s lease TTL so the condemned-list grace wait is short): the chunks
//!    it journals as deleted (`gc/journal/`, rule `orphan-horizon`, this
//!    round's entries only) **equal** the dry-run set — same count, same
//!    hashes — and equal the round's own report.
//! 6. `--verify` is still 0 mismatches on both nodes.

use super::{eventually, journal_drained, raw_objects, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

const ROUNDS: usize = 12;
/// The middle range whose deletion is estimated and then carried out.
const RANGE: &str = "/proj@p04%p08";
const RANGE_LEN: usize = 5;
/// The directories every file and movable directory lives under.
const FIXED: [&str; 3] = ["proj", "proj/sub", "outside"];
const MIB: usize = 1 << 20;

pub(super) fn snapacct(seed: u64) -> Result<()> {
    let (env, root) = setup("snapacct")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("snapacct-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mk = |name: &str, acct: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, &env.endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", "1000")
            .with_env("CONSTELLATION_GC_HORIZON_S", "0")
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")
            .with_env("CONSTELLATION_SNAPACCT", acct)
            .with_env("CONSTELLATION_SNAPACCT_REFRESH_S", "2"))
    };
    let mut a = mk("acct-a", "on")?;
    let mut b = mk("acct-b", "auto")?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;
    let outcome = run(&env.direct_endpoint, &prefix, &a, &b, seed);
    if outcome.is_err() {
        eprintln!(
            "    snapacct: a log tail:\n{}\n    b log tail:\n{}",
            a.tail_log_n(60),
            b.tail_log_n(60)
        );
    }
    let unmounted = b.unmount().and(a.unmount());
    outcome?;
    unmounted
}

fn run(endpoint: &str, prefix: &str, a: &Client, b: &Client, seed: u64) -> Result<()> {
    // ---------------------------------------------------------- workload
    let mut wl = Tree::new(seed);
    for dir in FIXED {
        std::fs::create_dir(a.mnt.join(dir)).with_context(|| format!("mkdir {dir}"))?;
    }
    let mut snapshots = 0usize;
    for round in 1..=ROUNDS {
        let (writer, other) = if round % 2 == 1 { (a, b) } else { (b, a) };
        wait_converged(a, b, false).with_context(|| format!("before round {round}"))?;
        wl.round(&writer.mnt, round)
            .with_context(|| format!("round {round} on {}", writer.name))?;
        wait_converged(a, b, false).with_context(|| format!("after round {round}"))?;
        other.snapshot_create(&format!("/proj@p{round:02}"))?;
        snapshots += 1;
        if round % 2 == 0 {
            other.snapshot_create(&format!("/proj/sub@s{round:02}"))?;
            snapshots += 1;
        }
    }
    eprintln!(
        "    snapacct: {} ops ({}), {snapshots} snapshots, {} files",
        wl.ops.values().sum::<usize>(),
        wl.ops
            .iter()
            .map(|(op, n)| format!("{op} {n}"))
            .collect::<Vec<_>>()
            .join(", "),
        wl.files.len()
    );
    for op in [
        "create",
        "overwrite",
        "partial",
        "truncate",
        "revert",
        "hardlink",
        "copy",
        "unlink",
        "dir-in",
        "dir-out",
        "dir-nested",
    ] {
        anyhow::ensure!(
            wl.ops.get(op).copied().unwrap_or(0) > 0,
            "the workload never ran {op}: {:?}",
            wl.ops
        );
    }

    // ----------------------------------------------------------- quiesce
    let digest = wait_converged(a, b, true).context("final convergence")?;
    let expected: BTreeSet<&str> = wl.files.iter().map(String::as_str).collect();
    let seen: BTreeSet<&str> = digest
        .iter()
        .filter(|(_, (dir, _, _))| !dir)
        .map(|(path, _)| path.as_str())
        .collect();
    anyhow::ensure!(
        seen == expected,
        "the mounts hold other files than the workload made: extra {:?}, missing {:?}",
        seen.difference(&expected).collect::<Vec<_>>(),
        expected.difference(&seen).collect::<Vec<_>>()
    );
    for c in [a, b] {
        eventually(
            &format!("{} drained", c.name),
            Duration::from_secs(60),
            || journal_drained(c),
        )?;
    }
    let quiesce = a.gc_run_control()?;
    eprintln!(
        "    snapacct: quiesce GC deleted {} object(s)",
        quiesce["deleted"].as_array().map_or(0, Vec::len)
    );
    // Nothing below writes to the live tree until the last GC round.

    // ------------------------------------------------- verify, dry run
    for c in [a, b] {
        verify_clean(c, "before the deletion")?;
    }
    // Both indexes are a pure function of the same snapshots: the same
    // chunk set and the same bytes (a hash met at several derived sizes —
    // a tail chunk later extended in place — counts at its largest on
    // every node, whatever order each index applied the snapshots in).
    let listed_a = reclaim_listed(a)?;
    let listed_b = reclaim_listed(b)?;
    eprintln!(
        "    snapacct: estimates: a {} chunks / {} bytes, b {} chunks / {} bytes",
        listed_a.chunks.len(),
        listed_a.bytes,
        listed_b.chunks.len(),
        listed_b.bytes
    );
    anyhow::ensure!(
        listed_a == listed_b,
        "the two nodes' indexes estimate differently: a {} chunks / {} bytes {:?}, \
         b {} chunks / {} bytes {:?}",
        listed_a.chunks.len(),
        listed_a.bytes,
        listed_a.chunks.iter().take(5).collect::<Vec<_>>(),
        listed_b.chunks.len(),
        listed_b.bytes,
        listed_b.chunks.iter().take(5).collect::<Vec<_>>()
    );
    let dry = listed_b;
    // At least round 5's ephemeral files: 3 + 1 + 1 + 1 chunks only
    // `p05`–`p07` hold.
    anyhow::ensure!(
        dry.chunks.len() >= 6,
        "the dry run of {RANGE} covers {} chunks, fewer than the ephemeral files' 6",
        dry.chunks.len()
    );
    let (ok, stdout, stderr) = b.snapshot_cli(&["delete", RANGE, "--dry-run"])?;
    anyhow::ensure!(ok, "snapshot delete --dry-run failed: {stdout}{stderr}");
    let would = stdout
        .lines()
        .filter(|l| l.starts_with("would delete"))
        .count();
    let wanted = format!(" in {} chunks (after GC)", dry.chunks.len());
    anyhow::ensure!(
        would == RANGE_LEN && stdout.contains(&wanted),
        "the dry run is not {RANGE_LEN} snapshots and {} chunks:\n{stdout}",
        dry.chunks.len()
    );
    eprintln!(
        "    snapacct: dry run {RANGE}: {} chunks, {} bytes; first 5: {:?}",
        dry.chunks.len(),
        dry.bytes,
        dry.chunks.iter().take(5).collect::<Vec<_>>()
    );

    // ----------------------------------------------------------- delete
    let (ok, stdout, stderr) = b.snapshot_cli(&["delete", RANGE, "--yes"])?;
    anyhow::ensure!(ok, "snapshot delete failed: {stdout}{stderr}");
    for c in [a, b] {
        eventually(
            &format!("{} lists the range deleted", c.name),
            Duration::from_secs(30),
            || {
                let n = c.snapshot_count()?;
                anyhow::ensure!(n == snapshots - RANGE_LEN, "{n} snapshots");
                Ok(())
            },
        )?;
        // Every chunk of the estimate lost its last snapshot and is not
        // live: exactly the index's "awaiting GC" line (nothing was freed
        // before).
        eventually(
            &format!("{} shows the set awaiting GC", c.name),
            Duration::from_secs(60),
            || {
                let space = c.control("snapshot.space", serde_json::json!({}))?;
                anyhow::ensure!(space["building"] == false, "building: {space}");
                let awaiting = &space["awaiting_gc"];
                anyhow::ensure!(
                    awaiting["chunks"].as_u64() == Some(dry.chunks.len() as u64)
                        && awaiting["bytes"].as_u64() == Some(dry.bytes),
                    "awaiting GC {awaiting}, the dry run {} chunks / {} bytes",
                    dry.chunks.len(),
                    dry.bytes
                );
                Ok(())
            },
        )?;
    }

    // --------------------------------------------------------------- GC
    let journal = format!("{prefix}/gc/journal/");
    let before: BTreeSet<String> = raw_objects(endpoint, &journal)?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    let report = a.gc_run_control()?;
    let mut journaled = BTreeSet::new();
    let mut other_rules = Vec::new();
    for (key, _) in raw_objects(endpoint, &journal)? {
        if before.contains(&key) {
            continue;
        }
        let entry: serde_json::Value = crate::s3auth::get(&format!("{endpoint}/{BUCKET}/{key}"))
            .call()
            .with_context(|| format!("reading {key}"))?
            .into_json()?;
        let deleted = entry["key"].as_str().unwrap_or_default();
        // Entries under other rules (or for objects that are not chunks)
        // are only printed: `journaled == reported` below catches any
        // chunk deletion they would hide, since the round's report lists
        // every chunk it deleted, whatever the rule.
        match chunk_hash(deleted) {
            Some(hash) if entry["rule"] == "orphan-horizon" => {
                journaled.insert(hash.to_string());
            }
            _ => other_rules.push(format!("{} {deleted}", entry["rule"])),
        }
    }
    let reported: BTreeSet<String> = report["deleted"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|key| chunk_hash(key.as_str()?).map(str::to_string))
        .collect();
    eprintln!(
        "    snapacct: GC journaled {} chunk deletions; first 5: {:?}{}",
        journaled.len(),
        journaled.iter().take(5).collect::<Vec<_>>(),
        if other_rules.is_empty() {
            String::new()
        } else {
            format!("; other entries: {other_rules:?}")
        }
    );
    anyhow::ensure!(
        journaled == reported,
        "the GC journal ({} chunks) and the round's report ({} chunks) differ",
        journaled.len(),
        reported.len()
    );
    anyhow::ensure!(
        journaled == dry.chunks,
        "GC deleted other chunks than the dry run estimated: {} journaled, {} estimated; \
         deleted but not estimated {:?}; estimated but kept {:?}",
        journaled.len(),
        dry.chunks.len(),
        journaled
            .difference(&dry.chunks)
            .take(10)
            .collect::<Vec<_>>(),
        dry.chunks
            .difference(&journaled)
            .take(10)
            .collect::<Vec<_>>()
    );

    for c in [a, b] {
        verify_clean(c, "after GC")?;
    }
    Ok(())
}

/// The chunk hash a `chunks/aa/bb/<hash>` key names (with or without the
/// filesystem's prefix in front).
fn chunk_hash(key: &str) -> Option<&str> {
    let (dir, hash) = key.rsplit_once('/')?;
    (dir.starts_with("chunks/") || dir.contains("/chunks/"))
        .then_some(hash)
        .filter(|h| h.len() == 64)
}

#[derive(Debug, PartialEq, Eq)]
struct Listed {
    bytes: u64,
    chunks: BTreeSet<String>,
}

/// `snapshot.reclaim` of [`RANGE`] with the chunk list, once the index is
/// built.
fn reclaim_listed(c: &Client) -> Result<Listed> {
    let mut out = None;
    eventually(
        &format!("{}'s reclaim estimate", c.name),
        Duration::from_secs(120),
        || {
            let est = c.control(
                "snapshot.reclaim",
                serde_json::json!({"selectors": [RANGE], "list_chunks": true}),
            )?;
            anyhow::ensure!(est["building"] == false, "building: {est}");
            let chunks: BTreeSet<String> = est["chunk_hashes"]
                .as_array()
                .context("no chunk_hashes")?
                .iter()
                .filter_map(|h| h.as_str().map(str::to_string))
                .collect();
            anyhow::ensure!(
                est["chunks"].as_u64() == Some(chunks.len() as u64),
                "count and list disagree: {est}"
            );
            out = Some(Listed {
                bytes: est["bytes"].as_u64().unwrap_or(0),
                chunks,
            });
            Ok(())
        },
    )?;
    Ok(out.expect("set on success"))
}

/// `snapshot space --verify` on `c` until it reports 0 mismatches. A
/// verify right after the live tree changed may count live-flag
/// differences until the next refresh the replica covers (documented on
/// `SnapAcctService::verify`), so it is retried for a bounded time; the
/// last report is the error.
pub(super) fn verify_clean(c: &Client, when: &str) -> Result<()> {
    let mut last = String::new();
    eventually(
        &format!("{} verifies clean {when}", c.name),
        Duration::from_secs(120),
        || {
            let (ok, stdout, stderr) = c.snapshot_cli(&["space", "/", "--verify", "--json"])?;
            let value: serde_json::Value = serde_json::from_str(&stdout)
                .with_context(|| format!("snapshot space --verify: {stdout}{stderr}"))?;
            last = serde_json::to_string(&value["verify"])?;
            anyhow::ensure!(
                ok && value["verify"]["mismatches"] == 0 && value["space"]["building"] == false,
                "{last}"
            );
            Ok(())
        },
    )?;
    eprintln!("    snapacct: {} verify {when}: {last}", c.name);
    Ok(())
}

type Digest = BTreeMap<String, (bool, u64, Option<String>)>;

/// Every entry under `mnt`: (is a directory, size, content hash when
/// `content`).
fn digest(mnt: &Path, content: bool) -> Result<Digest> {
    fn walk(base: &Path, dir: &Path, content: bool, out: &mut Digest) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let rel = path.strip_prefix(base)?.to_string_lossy().into_owned();
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.is_dir() {
                out.insert(rel, (true, 0, None));
                walk(base, &path, content, out)?;
            } else {
                let hash = if content {
                    Some(blake3::hash(&std::fs::read(&path)?).to_hex().to_string())
                } else {
                    None
                };
                out.insert(rel, (false, meta.len(), hash));
            }
        }
        Ok(())
    }
    let mut out = Digest::new();
    walk(mnt, mnt, content, &mut out)?;
    Ok(out)
}

/// Wait until both mounts show the same tree (names, kinds and sizes; with
/// `content`, file contents too).
fn wait_converged(a: &Client, b: &Client, content: bool) -> Result<Digest> {
    let mut same = Digest::new();
    eventually("both mounts agree", Duration::from_secs(60), || {
        let left = digest(&a.mnt, content)?;
        let right = digest(&b.mnt, content)?;
        if left != right {
            let differ: Vec<_> = left
                .iter()
                .filter(|(k, v)| right.get(*k) != Some(v))
                .map(|(k, _)| k)
                .chain(right.keys().filter(|k| !left.contains_key(*k)))
                .take(5)
                .collect();
            anyhow::bail!("the mounts differ at {differ:?}");
        }
        same = left;
        Ok(())
    })?;
    Ok(same)
}

/// The workload's view of the namespace: file paths (a hardlink is a path
/// of its own), the movable directories (each directly under one of
/// [`FIXED`]), and earlier contents to revert to.
struct Tree {
    rng: StdRng,
    files: Vec<String>,
    movable: Vec<String>,
    history: Vec<(String, Vec<u8>)>,
    /// Created in round 5 and unlinked in round 8, untouched in between:
    /// only `p05`–`p07` hold them.
    ephemeral: Vec<String>,
    big_files: usize,
    next: usize,
    ops: BTreeMap<&'static str, usize>,
}

impl Tree {
    fn new(seed: u64) -> Tree {
        Tree {
            rng: StdRng::seed_from_u64(seed),
            files: Vec::new(),
            movable: Vec::new(),
            history: Vec::new(),
            ephemeral: Vec::new(),
            big_files: 0,
            next: 0,
            ops: BTreeMap::new(),
        }
    }

    fn fresh(&mut self, dir: &str, kind: &str) -> String {
        self.next += 1;
        format!("{dir}/{kind}{}", self.next)
    }

    fn count(&mut self, op: &'static str) {
        *self.ops.entry(op).or_default() += 1;
    }

    /// A content: mostly small (one chunk), sometimes 1–3 chunks of 4 MiB
    /// with a partial tail.
    fn content(&mut self) -> Vec<u8> {
        let len = if self.big_files < 8 && self.rng.random_range(0..6) == 0 {
            self.big_files += 1;
            self.rng.random_range(4 * MIB + 1..=10 * MIB)
        } else {
            self.rng.random_range(1..=256 * 1024)
        };
        let mut v = vec![0u8; len];
        self.rng.fill(&mut v[..]);
        v
    }

    fn dirs(&self) -> Vec<String> {
        FIXED
            .iter()
            .map(|d| d.to_string())
            .chain(self.movable.iter().cloned())
            .collect()
    }

    fn pick_dir(&mut self) -> String {
        let dirs = self.dirs();
        dirs[self.rng.random_range(0..dirs.len())].clone()
    }

    /// A file for a random operation: never one of the ephemeral files,
    /// whose only purpose is to be kept by the middle range alone.
    fn pick_file(&mut self) -> Option<String> {
        let open: Vec<String> = self
            .files
            .iter()
            .filter(|f| !self.ephemeral.contains(f))
            .cloned()
            .collect();
        (!open.is_empty()).then(|| open[self.rng.random_range(0..open.len())].clone())
    }

    fn pick_file_under(&mut self, dir: &str) -> Option<String> {
        let under: Vec<String> = self
            .files
            .iter()
            .filter(|f| f.rsplit_once('/').is_some_and(|(d, _)| d == dir))
            .cloned()
            .collect();
        (!under.is_empty()).then(|| under[self.rng.random_range(0..under.len())].clone())
    }

    /// Keep `path`'s current content for a later revert (bounded memory).
    fn remember(&mut self, mnt: &Path, path: &str) -> Result<()> {
        let bytes = std::fs::read(mnt.join(path))?;
        if self.history.len() >= 24 {
            self.history.remove(0);
        }
        self.history.push((path.to_string(), bytes));
        Ok(())
    }

    fn create(&mut self, mnt: &Path, dir: &str) -> Result<String> {
        let content = self.content();
        self.create_with(mnt, dir, content)
    }

    fn create_with(&mut self, mnt: &Path, dir: &str, content: Vec<u8>) -> Result<String> {
        let path = self.fresh(dir, "f");
        std::fs::write(mnt.join(&path), content)?;
        self.files.push(path.clone());
        self.count("create");
        Ok(path)
    }

    fn overwrite(&mut self, mnt: &Path, path: &str) -> Result<()> {
        self.remember(mnt, path)?;
        let content = self.content();
        std::fs::write(mnt.join(path), content)?;
        self.count("overwrite");
        Ok(())
    }

    /// Rewrite 4 KiB in place: one chunk of the file changes.
    fn partial(&mut self, mnt: &Path, path: &str) -> Result<()> {
        let len = std::fs::metadata(mnt.join(path))?.len();
        if len == 0 {
            return Ok(());
        }
        self.remember(mnt, path)?;
        let at = self.rng.random_range(0..len);
        let mut patch = vec![0u8; 4096];
        self.rng.fill(&mut patch[..]);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(mnt.join(path))?;
        f.seek(SeekFrom::Start(at))?;
        f.write_all(&patch)?;
        self.count("partial");
        Ok(())
    }

    /// Shrink, or now and then extend past the end (a sparse tail).
    fn truncate(&mut self, mnt: &Path, path: &str) -> Result<()> {
        let len = std::fs::metadata(mnt.join(path))?.len();
        self.remember(mnt, path)?;
        let to = if len > 1 && self.rng.random_range(0..4) != 0 {
            self.rng.random_range(0..len)
        } else {
            len + self.rng.random_range(1..=300_000)
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(mnt.join(path))?
            .set_len(to)?;
        self.count("truncate");
        Ok(())
    }

    /// Write an earlier content back to a path that still exists.
    fn revert(&mut self, mnt: &Path) -> Result<()> {
        let live: Vec<usize> = (0..self.history.len())
            .filter(|&i| self.files.contains(&self.history[i].0))
            .collect();
        if live.is_empty() {
            return Ok(());
        }
        let i = live[self.rng.random_range(0..live.len())];
        let (path, bytes) = self.history[i].clone();
        std::fs::write(mnt.join(&path), bytes)?;
        self.count("revert");
        Ok(())
    }

    fn hardlink(&mut self, mnt: &Path, path: &str, dir: &str) -> Result<()> {
        let link = self.fresh(dir, "l");
        std::fs::hard_link(mnt.join(path), mnt.join(&link))?;
        self.files.push(link);
        self.count("hardlink");
        Ok(())
    }

    /// The same bytes under another name: the same chunks, possibly in
    /// another chain.
    fn copy(&mut self, mnt: &Path, path: &str, dir: &str) -> Result<()> {
        let to = self.fresh(dir, "c");
        std::fs::copy(mnt.join(path), mnt.join(&to))?;
        self.files.push(to);
        self.count("copy");
        Ok(())
    }

    fn unlink(&mut self, mnt: &Path, path: &str) -> Result<()> {
        std::fs::remove_file(mnt.join(path))?;
        self.files.retain(|f| f != path);
        self.count("unlink");
        Ok(())
    }

    fn rename(&mut self, mnt: &Path, path: &str, dir: &str) -> Result<()> {
        let to = self.fresh(dir, "r");
        std::fs::rename(mnt.join(path), mnt.join(&to))?;
        for f in &mut self.files {
            if f == path {
                *f = to.clone();
            }
        }
        self.count("rename");
        Ok(())
    }

    fn mkdir_movable(&mut self, mnt: &Path, parent: &str, files: usize) -> Result<String> {
        let dir = self.fresh(parent, "m");
        std::fs::create_dir(mnt.join(&dir))?;
        self.movable.push(dir.clone());
        for _ in 0..files {
            self.create(mnt, &dir)?;
        }
        Ok(dir)
    }

    /// Move a movable directory (and everything in it) under `parent`.
    fn move_dir(&mut self, mnt: &Path, dir: &str, parent: &str) -> Result<()> {
        let name = dir.rsplit_once('/').map_or(dir, |(_, n)| n);
        let to = format!("{parent}/{name}");
        std::fs::rename(mnt.join(dir), mnt.join(&to))?;
        let moved = |p: &mut String| {
            if let Some(rest) = p.strip_prefix(&format!("{dir}/")) {
                *p = format!("{to}/{rest}");
            }
        };
        self.files.iter_mut().for_each(moved);
        self.history.iter_mut().for_each(|(p, _)| moved(p));
        for m in &mut self.movable {
            if m == dir {
                *m = to.clone();
            }
        }
        let crossing = match (parent_of(dir), parent) {
            ("outside", _) => "dir-in",
            (_, "outside") => "dir-out",
            _ => "dir-nested",
        };
        self.count(crossing);
        Ok(())
    }

    fn movable_under(&self, parent: &str) -> Option<String> {
        self.movable
            .iter()
            .find(|m| parent_of(m) == parent)
            .cloned()
    }

    /// One round: the forced operations that guarantee every kind occurs
    /// (and that the middle range `p04`–`p08` alone keeps some chunks),
    /// then a seeded random mix.
    fn round(&mut self, mnt: &Path, round: usize) -> Result<()> {
        match round {
            1 => {
                self.mkdir_movable(mnt, "outside", 3)?;
                self.mkdir_movable(mnt, "proj/sub", 3)?;
                self.mkdir_movable(mnt, "proj", 2)?;
                self.mkdir_movable(mnt, "proj", 1)?;
                for dir in FIXED {
                    for _ in 0..3 {
                        self.create(mnt, dir)?;
                    }
                }
            }
            3 => {
                if let Some(d) = self.movable_under("outside") {
                    self.move_dir(mnt, &d, "proj/sub")?;
                }
            }
            4 => {
                if let Some(f) = self.pick_file_under("outside") {
                    self.hardlink(mnt, &f, "proj/sub")?;
                }
                if let Some(f) = self.pick_file_under("proj/sub") {
                    self.hardlink(mnt, &f, "proj")?;
                }
            }
            5 => {
                // Only p05–p07 will see these: chunks the range alone keeps.
                // One of them three chunks long (two whole, a partial tail).
                let mut doomed = Vec::new();
                for len in [9 * MIB + 12_345, 70_000, 150_000, 3_000] {
                    let mut content = vec![0u8; len];
                    self.rng.fill(&mut content[..]);
                    doomed.push(self.create_with(mnt, "proj", content)?);
                }
                self.ephemeral = doomed;
            }
            6 => {
                if let Some(d) = self.movable_under("proj/sub") {
                    self.move_dir(mnt, &d, "outside")?;
                }
            }
            7 | 10 => {
                if let Some(f) = self.pick_file() {
                    self.overwrite(mnt, &f)?;
                }
                self.revert(mnt)?;
            }
            8 => {
                for f in std::mem::take(&mut self.ephemeral) {
                    if self.files.contains(&f) {
                        self.unlink(mnt, &f)?;
                    }
                }
                // Across the nested chain's boundary only, whichever way a
                // directory is available (random moves may have taken the
                // one round 1 made under `proj`).
                if let Some(d) = self.movable_under("proj") {
                    self.move_dir(mnt, &d, "proj/sub")?;
                } else if let Some(d) = self.movable_under("proj/sub") {
                    self.move_dir(mnt, &d, "proj")?;
                } else {
                    let d = self.mkdir_movable(mnt, "outside", 2)?;
                    self.move_dir(mnt, &d, "proj")?;
                }
            }
            _ => {}
        }
        let n = self.rng.random_range(10..=16);
        for _ in 0..n {
            self.random_op(mnt)?;
        }
        Ok(())
    }

    fn random_op(&mut self, mnt: &Path) -> Result<()> {
        let dice = self.rng.random_range(0..100);
        let Some(file) = self.pick_file() else {
            let dir = self.pick_dir();
            return self.create(mnt, &dir).map(drop);
        };
        match dice {
            0..=19 => {
                let dir = self.pick_dir();
                self.create(mnt, &dir).map(drop)
            }
            20..=34 => self.overwrite(mnt, &file),
            35..=46 => self.partial(mnt, &file),
            47..=54 => self.truncate(mnt, &file),
            55..=62 => self.revert(mnt),
            63..=68 => {
                let dir = self.pick_dir();
                self.hardlink(mnt, &file, &dir)
            }
            69..=76 => {
                let dir = self.pick_dir();
                self.copy(mnt, &file, &dir)
            }
            77..=86 => self.unlink(mnt, &file),
            87..=94 => {
                let dir = self.pick_dir();
                self.rename(mnt, &file, &dir)
            }
            _ => {
                if self.movable.is_empty() {
                    return Ok(());
                }
                let d = self.movable[self.rng.random_range(0..self.movable.len())].clone();
                let targets: Vec<&str> = FIXED
                    .iter()
                    .copied()
                    .filter(|p| *p != parent_of(&d))
                    .collect();
                let to = targets[self.rng.random_range(0..targets.len())];
                self.move_dir(mnt, &d, to)
            }
        }
    }
}

fn parent_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(p, _)| p)
}
