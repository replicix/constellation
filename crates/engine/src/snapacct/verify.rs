//! `snapshot space --verify`: the accounting index against a brute force.
//!
//! Shares nothing with the index's maintenance but the row order: every
//! snapshot is walked in full with the tree reader (not
//! [`crate::snapwalk`]), every chunk set is materialized, the live tree's
//! chunks come from the replica's manifests (spilled lists expanded), and
//! every number of plan 32 §6.1 is computed from those sets by its
//! definition. O(Σ snapshot sizes): an oracle for tests and for an
//! operator's "do I trust these numbers", never a query path.
//!
//! **Chunk sizes.** A chunk's logical size is derived per occurrence,
//! `min(chunk_size, file_len − offset)`, and one hash can occur at
//! different sizes: a short tail chunk whose file was later extended past
//! it (or cut inside it by a truncate that kept its hash) occurs again
//! with the same hash at another size. Plan 32 §6.1 counts a chunk at the
//! **largest** size it occurs at in any snapshot — a pure function of the
//! snapshots, whatever order an index met them in. The walk records every
//! size each hash occurs at, counts every number at the largest, and
//! reports any chunk whose indexed size is not that largest one by name
//! (the sums alone could hide two errors that cancel).

use super::service::{desired_chains, VerifyReport};
use super::{Amount, SnapAcct};
use crate::snapshot::{SnapshotRoot, TreeAccess};
use anyhow::Result;
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, InodeKind};
use constellation_meta::Meta;
use constellation_store_s3::ChunkStore;
use std::collections::{BTreeSet, HashMap, HashSet};

/// At most this many mismatch lines are kept.
const MAX_DETAILS: usize = 200;

/// One snapshot, fully walked.
#[derive(Clone, Debug, Default)]
pub(crate) struct Walked {
    pub id: String,
    pub dir: u64,
    /// Distinct chunks and the largest plaintext size each occurs at
    /// here.
    pub chunks: HashMap<ChunkHash, u64>,
    /// Every size each chunk occurs at here.
    pub sizes: HashMap<ChunkHash, BTreeSet<u64>>,
    pub lsize: u64,
}

/// Every snapshot's exact chunk set and the live tree's.
pub(crate) struct BruteForce {
    /// In chain order, chains in directory order.
    pub snapshots: Vec<Walked>,
    pub live: HashSet<ChunkHash>,
    /// Which snapshots (indices into `snapshots`) hold each chunk.
    holders: HashMap<ChunkHash, BTreeSet<usize>>,
    /// Each chunk's size: the largest it occurs at in any snapshot.
    sizes: HashMap<ChunkHash, u64>,
    /// Every size each chunk occurs at, over all snapshots.
    seen_sizes: HashMap<ChunkHash, BTreeSet<u64>>,
}

/// Exact numbers of one snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Exact {
    pub used: u64,
    pub written: u64,
    pub refer: u64,
    pub lsize: u64,
}

impl BruteForce {
    pub async fn compute(
        meta: &Meta,
        tree: &TreeAccess,
        chunks: &ChunkStore,
    ) -> Result<BruteForce> {
        let mut snapshots = Vec::new();
        for (dir, chain) in desired_chains(meta)? {
            for snapshot in chain {
                let (sizes, lsize) = walk(tree, chunks, snapshot.root).await?;
                snapshots.push(Walked {
                    id: snapshot.id,
                    dir,
                    chunks: sizes
                        .iter()
                        .map(|(hash, sizes)| (*hash, *sizes.last().expect("a size per chunk")))
                        .collect(),
                    sizes,
                    lsize,
                });
            }
        }
        let mut live = HashSet::new();
        for bytes in meta.live_manifests()? {
            match Manifest::decode(&bytes)?.chunks {
                ChunkInfo::Inline(list) => live.extend(list.into_values()),
                ChunkInfo::Spilled(spill) => {
                    live.insert(spill);
                    live.extend(decode_chunk_list(&chunks.get_chunk(&spill).await?)?.into_values());
                }
            }
        }
        let mut holders: HashMap<ChunkHash, BTreeSet<usize>> = HashMap::new();
        let mut seen_sizes: HashMap<ChunkHash, BTreeSet<u64>> = HashMap::new();
        for (i, snapshot) in snapshots.iter().enumerate() {
            for (hash, sizes) in &snapshot.sizes {
                holders.entry(*hash).or_default().insert(i);
                seen_sizes.entry(*hash).or_default().extend(sizes);
            }
        }
        let sizes = seen_sizes
            .iter()
            .map(|(hash, sizes)| (*hash, *sizes.last().expect("a size per chunk")))
            .collect();
        Ok(BruteForce {
            snapshots,
            live,
            holders,
            sizes,
            seen_sizes,
        })
    }

    /// The numbers of snapshot `i`, by definition.
    pub fn exact(&self, i: usize) -> Exact {
        let snapshot = &self.snapshots[i];
        let prev = i
            .checked_sub(1)
            .map(|p| &self.snapshots[p])
            .filter(|p| p.dir == snapshot.dir);
        let mut out = Exact {
            lsize: snapshot.lsize,
            ..Exact::default()
        };
        for hash in snapshot.chunks.keys() {
            let size = self.sizes[hash];
            out.refer += size;
            if prev.is_none_or(|p| !p.chunks.contains_key(hash)) {
                out.written += size;
            }
            if !self.live.contains(hash) && self.holders[hash].len() == 1 {
                out.used += size;
            }
        }
        out
    }

    /// `reclaim` of the snapshots at `set`: chunks not live whose every
    /// holder is in the set.
    pub fn reclaim(&self, set: &BTreeSet<usize>) -> Amount {
        let mut seen = HashSet::new();
        let mut out = Amount::default();
        for &i in set {
            for hash in self.snapshots[i].chunks.keys() {
                if !seen.insert(*hash) || self.live.contains(hash) {
                    continue;
                }
                if self.holders[hash].is_subset(set) {
                    out.bytes += self.sizes[hash];
                    out.chunks += 1;
                }
            }
        }
        out
    }

    /// `(unique, shared by ≥2 snapshots only, shared with live)`.
    pub fn buckets(&self) -> (Amount, Amount, Amount) {
        let (mut unique, mut shared, mut with_live) =
            (Amount::default(), Amount::default(), Amount::default());
        for (hash, holders) in &self.holders {
            let bucket = if self.live.contains(hash) {
                &mut with_live
            } else if holders.len() == 1 {
                &mut unique
            } else {
                &mut shared
            };
            bucket.bytes += self.sizes[hash];
            bucket.chunks += 1;
        }
        (unique, shared, with_live)
    }

    /// Every number the index answers, against this.
    pub fn diff(&self, ix: &SnapAcct) -> Result<VerifyReport> {
        let mut report = VerifyReport {
            snapshots: self.snapshots.len() as u64,
            chunks: self.holders.len() as u64,
            ..VerifyReport::default()
        };
        let mut miss = |line: String| {
            report.mismatches += 1;
            if report.details.len() < MAX_DETAILS {
                report.details.push(line);
            }
        };
        let mut located = HashMap::new();
        for (i, snapshot) in self.snapshots.iter().enumerate() {
            let Some((chain, ord)) = ix.locate(&snapshot.id)? else {
                miss(format!("snapshot {} is not indexed", snapshot.id));
                continue;
            };
            located.insert(i, (chain, ord));
            let Some(numbers) = ix.snap_numbers(chain, ord)? else {
                miss(format!("snapshot {} located but not recorded", snapshot.id));
                continue;
            };
            let exact = self.exact(i);
            for (what, index, brute) in [
                ("USED", numbers.used, exact.used),
                ("WRITTEN", numbers.written, exact.written),
                ("REFER", numbers.refer, exact.refer),
                ("LSIZE", numbers.lsize, exact.lsize),
            ] {
                if index != brute {
                    miss(format!(
                        "snapshot {}: {what} index {index} ≠ brute force {brute}",
                        snapshot.id
                    ));
                }
            }
        }
        let wanted: HashSet<&str> = self.snapshots.iter().map(|s| s.id.as_str()).collect();
        for (chain, dir) in ix.chains()? {
            for snap in ix.chain_snapshots(chain)? {
                if !wanted.contains(snap.id.as_str()) {
                    miss(format!(
                        "chain {chain} (dir {dir}) indexes {} which has no row",
                        snap.id
                    ));
                }
            }
        }
        // reclaim: each chain whole, and everything.
        let mut by_dir: HashMap<u64, BTreeSet<usize>> = HashMap::new();
        for (i, snapshot) in self.snapshots.iter().enumerate() {
            by_dir.entry(snapshot.dir).or_default().insert(i);
        }
        let mut sets: Vec<(String, BTreeSet<usize>)> = by_dir
            .into_iter()
            .map(|(dir, set)| (format!("chain of dir {dir}"), set))
            .collect();
        sets.push(("every snapshot".into(), (0..self.snapshots.len()).collect()));
        for (what, set) in sets {
            if set.iter().any(|i| !located.contains_key(i)) || set.is_empty() {
                continue;
            }
            let at: Vec<(u32, u32)> = set.iter().map(|i| located[i]).collect();
            let index = ix.reclaim(&at)?;
            let brute = self.reclaim(&set);
            if index != brute {
                miss(format!(
                    "reclaim({what}): index {index:?} ≠ brute force {brute:?}"
                ));
            }
        }
        // Per chunk, so a mismatch in the sums names its chunks: every
        // chunk a snapshot holds is indexed, with the replica's liveness,
        // at the largest size it occurs at.
        let mut hashes: Vec<&ChunkHash> = self.holders.keys().collect();
        hashes.sort_unstable_by_key(|hash| hash.0);
        for hash in hashes {
            let live = self.live.contains(hash);
            let Some(entry) = ix.chunk_entry(hash)? else {
                miss(format!("chunk {} is not indexed", hash.to_hex()));
                continue;
            };
            if entry.live != live {
                miss(format!(
                    "chunk {}: live index {} ≠ brute force {live}",
                    hash.to_hex(),
                    entry.live
                ));
            }
            if entry.size != self.sizes[hash] {
                miss(format!(
                    "chunk {}: size index {} ≠ brute force {} (the largest of {:?})",
                    hash.to_hex(),
                    entry.size,
                    self.sizes[hash],
                    self.seen_sizes[hash]
                ));
            }
        }
        let fs = ix.fs_breakdown()?;
        let (unique, shared, with_live) = self.buckets();
        for (what, index, brute) in [
            ("unique", fs.unique, unique),
            ("shared (snapshots only)", fs.shared_only, shared),
            ("shared with live", fs.shared_with_live, with_live),
            (
                "snapshots total",
                fs.snapshots_total,
                Amount {
                    bytes: unique.bytes + shared.bytes,
                    chunks: unique.chunks + shared.chunks,
                },
            ),
        ] {
            if index != brute {
                miss(format!("{what}: index {index:?} ≠ brute force {brute:?}"));
            }
        }
        if let Err(problem) = ix.check_structure() {
            miss(format!("index self-check: {problem}"));
        }
        Ok(report)
    }
}

/// One snapshot's distinct chunks with every size each occurs at, and its
/// `LSIZE`, by a full walk with the tree reader.
async fn walk(
    tree: &TreeAccess,
    chunks: &ChunkStore,
    snapshot: SnapshotRoot,
) -> Result<(HashMap<ChunkHash, BTreeSet<u64>>, u64)> {
    let SnapshotRoot { root, ino, .. } = snapshot;
    // `(size, manifest)` once per name.
    let files = tree
        .read(root, move |reader, resolver| {
            let mut files = Vec::new();
            let mut stack = vec![ino];
            while let Some(dir) = stack.pop() {
                for child in reader.readdir(dir, None, usize::MAX)? {
                    let kind = child.attrs.kind.as_u8();
                    if kind == InodeKind::Dir.as_u8() {
                        stack.push(child.ino);
                    } else if kind == InodeKind::File.as_u8() {
                        if let Some(row) = reader.inode_row(child.ino, resolver)? {
                            files.push((row.attr.size, row.manifest));
                        }
                    }
                }
            }
            Ok(files)
        })
        .await?;
    let mut out: HashMap<ChunkHash, BTreeSet<u64>> = HashMap::new();
    let mut lsize = 0;
    for (size, manifest) in files {
        lsize += size;
        let Some(bytes) = manifest else { continue };
        let manifest = Manifest::decode(&bytes)?;
        let cs = manifest.layout.chunk_size as u64;
        let at = |index: u64| {
            manifest
                .file_len
                .saturating_sub(index.saturating_mul(cs))
                .min(cs)
        };
        let list = match &manifest.chunks {
            ChunkInfo::Inline(list) => list.clone(),
            ChunkInfo::Spilled(spill) => {
                let blob = chunks.get_chunk(spill).await?;
                out.entry(*spill).or_default().insert(blob.len() as u64);
                decode_chunk_list(&blob)?
            }
        };
        for (index, hash) in list {
            out.entry(hash).or_default().insert(at(index));
        }
    }
    Ok((out, lsize))
}
