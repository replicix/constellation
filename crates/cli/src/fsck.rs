//! Offline-capable filesystem consistency checking (DESIGN.md §10, §14).
//!
//! Detection is always non-destructive. `--repair` performs only repairs
//! with an unambiguous safe action; missing content without a cache copy is
//! reported as loss and never converted into a shorter manifest.

use crate::gc;
use anyhow::{Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::ChunkHash;
use constellation_meta::SqliteMeta;
use constellation_store_s3::{CompressionSetting, GcJournalEntry, LeaseMode, LeaseStore, LogStore};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::Serialize;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct FsckIssue {
    pub class: String,
    pub key: Option<String>,
    pub detail: String,
    pub repaired: bool,
    pub unrepairable: bool,
}

#[derive(Debug, Serialize)]
pub struct FsckReport {
    pub clean: bool,
    pub repair_requested: bool,
    pub issues: Vec<FsckIssue>,
}

impl FsckReport {
    pub fn exit_code(&self) -> i32 {
        if self.issues.iter().any(|issue| issue.unrepairable) {
            3
        } else if self.repair_requested && self.issues.iter().any(|issue| issue.repaired) {
            2
        } else if self.issues.is_empty() {
            0
        } else {
            1
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    logs: &LogStore,
    meta: Arc<SqliteMeta>,
    state_dir: Option<&std::path::Path>,
    compression: CompressionSetting,
    lease_mode: LeaseMode,
    repair: bool,
    force_release: Option<&str>,
) -> Result<FsckReport> {
    let mut issues = Vec::new();
    check_manifest_refs(
        &store,
        &chunks,
        &meta,
        state_dir,
        compression,
        repair,
        &mut issues,
    )
    .await?;
    check_metadata_objects(&store, logs, repair, &mut issues).await?;
    check_metadata_tree(logs, &meta, state_dir, &mut issues).await?;
    check_leases(&store, lease_mode, repair, force_release, &mut issues).await?;
    check_cache_cruft(state_dir, repair, &mut issues)?;
    check_gc_journal(&store, &meta, &mut issues).await?;

    let orphan_report = gc::run(store, chunks, meta, lease_mode, !repair, None).await?;
    for mark in orphan_report
        .candidates
        .iter()
        .filter(|mark| mark.rule == "orphan-horizon")
    {
        issues.push(FsckIssue {
            class: "orphan_chunk".into(),
            key: Some(mark.key.clone()),
            detail: mark.evidence.to_string(),
            repaired: repair && orphan_report.deleted.contains(&mark.key),
            unrepairable: false,
        });
    }
    Ok(FsckReport {
        clean: issues.is_empty(),
        repair_requested: repair,
        issues,
    })
}

async fn check_manifest_refs(
    store: &Arc<dyn ObjectStore>,
    chunks: &constellation_store_s3::ChunkStore,
    meta: &SqliteMeta,
    state_dir: Option<&std::path::Path>,
    compression: CompressionSetting,
    repair: bool,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    for bytes in meta.live_manifests()? {
        let manifest = Manifest::decode(&bytes)?;
        let hashes = match manifest.chunks {
            ChunkInfo::Inline(hashes) => hashes,
            ChunkInfo::Spilled(spill) => {
                if !chunks.has_chunk(&spill).await? {
                    record_missing(store, chunks, state_dir, spill, compression, repair, issues)
                        .await?;
                    continue;
                }
                decode_chunk_list(&chunks.get_chunk(&spill).await?)?
            }
        };
        for hash in hashes.into_values() {
            if !chunks.has_chunk(&hash).await? {
                record_missing(store, chunks, state_dir, hash, compression, repair, issues).await?;
            }
        }
    }
    Ok(())
}

async fn record_missing(
    _store: &Arc<dyn ObjectStore>,
    chunks: &constellation_store_s3::ChunkStore,
    state_dir: Option<&std::path::Path>,
    hash: ChunkHash,
    compression: CompressionSetting,
    repair: bool,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    let cache = state_dir.map(|dir| {
        let hex = hash.to_hex();
        dir.join("cache").join(&hex[..2]).join(&hex[2..4]).join(hex)
    });
    let local = cache.as_ref().filter(|path| path.is_file());
    let repaired = if repair {
        if let Some(path) = local {
            let data = std::fs::read(path)?;
            if chunks.hash(&data) == hash {
                chunks
                    .put_chunk_mode(
                        &hash,
                        &data,
                        compression,
                        constellation_store_s3::ChunkPutMode::Overwrite,
                    )
                    .await?;
                true
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };
    issues.push(FsckIssue {
        class: "dangling_manifest_ref".into(),
        key: Some(constellation_store_s3::layout::chunk_key(&hash).to_string()),
        detail: if local.is_some() {
            "bucket chunk missing; verified local cache copy is available".into()
        } else {
            "bucket chunk missing and no local cache copy exists; affected file is lost".into()
        },
        repaired,
        unrepairable: repair && !repaired,
    });
    Ok(())
}

async fn check_metadata_objects(
    store: &Arc<dyn ObjectStore>,
    logs: &LogStore,
    repair: bool,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    for object in store
        .list(Some(&Path::from("log")))
        .try_collect::<Vec<_>>()
        .await?
    {
        let parts: Vec<_> = object
            .location
            .parts()
            .map(|part| part.as_ref().to_string())
            .collect();
        let Some(name) = object
            .location
            .filename()
            .and_then(|name| name.strip_suffix(".zst"))
        else {
            continue;
        };
        let Ok(seq) = u64::from_str_radix(name, 16) else {
            continue;
        };
        let part = parts.get(1).cloned().unwrap_or_else(|| "p0".into());
        if logs.with_partition(&part).get_segment(seq).await.is_err() {
            let repaired = if repair {
                quarantine(store, &object.location).await?;
                true
            } else {
                false
            };
            issues.push(FsckIssue {
                class: "invalid_segment".into(),
                key: Some(object.location.to_string()),
                detail: "segment cannot be authenticated/decompressed".into(),
                repaired,
                unrepairable: false,
            });
        }
    }
    Ok(())
}

/// Plan 28 S6: `fsck` as "recompute the root hash and compare".
///
/// Two checks against the commit chain, neither of which repairs
/// anything (a commit is immutable; the next publish supersedes it):
///
/// 1. **Every node of the head is read and verified** — hash, structure,
///    and each interior entry's aggregate against its child's — and the
///    root aggregate against the one the commit states. This needs no
///    replica at all.
/// 2. **The replica rebuilds to the same root**, when there is a commit
///    that claims exactly the replica's state: its applied vector equals
///    the replica's and nothing local is unshipped. The rebuild uses the
///    publisher's own code path into the scratch cache (nothing is
///    uploaded), and a mismatch reports the first differing keys.
///
/// When no commit matches the replica's vector the comparison is simply
/// not meaningful yet, and is skipped rather than reported.
async fn check_metadata_tree(
    logs: &LogStore,
    meta: &Arc<SqliteMeta>,
    state_dir: Option<&std::path::Path>,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    let scratch = state_dir
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(".fsck-nodes-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    let result = check_metadata_tree_in(logs, meta, &scratch, issues).await;
    let _ = std::fs::remove_dir_all(&scratch);
    result
}

async fn check_metadata_tree_in(
    logs: &LogStore,
    meta: &Arc<SqliteMeta>,
    scratch: &std::path::Path,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    use crate::mtree_read::{ChainReader, TreeReader};
    use constellation_meta::MetaStore;
    use constellation_store_s3::SHARD0;

    let reader = ChainReader::for_log(logs, scratch)?;
    let Some(head) = reader.head().await? else {
        return Ok(());
    };
    reader.cache.refresh_catalog().await?;
    let mut candidates = vec![head.clone()];
    if let Some(seq) = crate::mtree_publish::remembered_seq(meta)? {
        if seq != head.seq {
            if let Some(own) = reader.chain.get(seq).await? {
                candidates.push(own);
            }
        }
    }

    let tree = reader.tree()?;
    let head_root = head
        .root(SHARD0)
        .with_context(|| format!("commit {} names no shard 0 root", head.seq))?;
    let meta = Arc::clone(meta);
    let blobs = reader.blobs.clone();
    let found = tokio::task::spawn_blocking(move || -> Result<Vec<FsckIssue>> {
        let mut found = Vec::new();
        let walker = TreeReader::new(tree, head_root);
        match walker.verify() {
            Ok(verified) => {
                for hash in &verified.bad_aggregates {
                    found.push(FsckIssue {
                        class: "mtree_aggregate".into(),
                        key: Some(hash.to_hex()),
                        detail: format!(
                            "commit {}: an interior entry's aggregate disagrees with its child",
                            head.seq
                        ),
                        repaired: false,
                        unrepairable: true,
                    });
                }
                if constellation_store_s3::CommitAgg::from(verified.root) != head.agg {
                    found.push(FsckIssue {
                        class: "mtree_aggregate".into(),
                        key: Some(format!("commits/{:016x}", head.seq)),
                        detail: format!(
                            "commit states {:?}, its root sums to {:?}",
                            head.agg, verified.root
                        ),
                        repaired: false,
                        unrepairable: true,
                    });
                }
                tracing::info!(
                    seq = head.seq,
                    nodes = verified.nodes,
                    leaves = verified.leaves,
                    "verified every metadata tree node"
                );
            }
            Err(e) => found.push(FsckIssue {
                class: "mtree_node".into(),
                key: Some(format!("commits/{:016x}", head.seq)),
                detail: format!("a node under the head cannot be read or verified: {e:#}"),
                repaired: false,
                unrepairable: true,
            }),
        }

        let vector = meta.applied_seq()?;
        let Some(matching) = candidates
            .iter()
            .find(|commit| commit.applied == vector && meta.journal_len().unwrap_or(1) == 0)
        else {
            tracing::info!("no commit claims this replica's exact state; skipping the rebuild comparison");
            return Ok(found);
        };
        // Rebuilt into memory rather than into the scratch cache: the
        // cache skips `put`s of nodes the bucket already holds, which
        // would make the rebuild read the very nodes under test. The
        // diff then reads through a store layered over both.
        let layered = Layered {
            rebuilt: constellation_mtree::MemoryNodeStore::default(),
            bucket: walker.tree().store().clone(),
        };
        let tree = constellation_mtree::Tree::with_config(&layered, *walker.tree().config())?;
        let rebuilt = crate::mtree_publish::rebuild_root(&meta, &tree, &blobs)?;
        let want = matching
            .root(SHARD0)
            .with_context(|| format!("commit {} names no shard 0 root", matching.seq))?;
        if rebuilt != want {
            let changed = tree.diff(&want, &rebuilt).unwrap_or_default();
            let sample: Vec<String> = changed
                .iter()
                .take(8)
                .map(|(key, kind)| format!("{kind:?} {}", hex(key)))
                .collect();
            found.push(FsckIssue {
                class: "mtree_root_mismatch".into(),
                key: Some(format!("commits/{:016x}", matching.seq)),
                detail: format!(
                    "the replica rebuilds to {} but the commit names {} ({} keys differ; first: {})",
                    rebuilt.to_hex(),
                    want.to_hex(),
                    changed.len(),
                    sample.join(", ")
                ),
                repaired: false,
                unrepairable: false,
            });
        }
        Ok(found)
    })
    .await
    .context("metadata tree check task")??;
    issues.extend(found);
    Ok(())
}

/// The rebuilt tree in memory, falling back to the bucket for the
/// committed one, so `diff` can walk both.
struct Layered {
    rebuilt: constellation_mtree::MemoryNodeStore,
    bucket: Arc<constellation_store_s3::NodeCache>,
}

impl constellation_mtree::NodeStore for Layered {
    fn get(
        &self,
        hash: &constellation_mtree::NodeHash,
    ) -> Result<Arc<[u8]>, constellation_mtree::MtreeError> {
        match self.rebuilt.get(hash) {
            Ok(bytes) => Ok(bytes),
            Err(_) => self.bucket.get(hash),
        }
    }

    fn put(
        &self,
        hash: constellation_mtree::NodeHash,
        level: u8,
        bytes: Vec<u8>,
    ) -> Result<(), constellation_mtree::MtreeError> {
        self.rebuilt.put(hash, level, bytes)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn quarantine(store: &Arc<dyn ObjectStore>, key: &Path) -> Result<()> {
    let bytes = store.get(key).await?.bytes().await?;
    let target = Path::from(format!("quarantine/{}", key.as_ref()));
    store.put(&target, PutPayload::from(bytes)).await?;
    store.delete(key).await?;
    Ok(())
}

async fn check_leases(
    store: &Arc<dyn ObjectStore>,
    mode: LeaseMode,
    repair: bool,
    force_release: Option<&str>,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    let now = constellation_store_s3::lease::now_unix_ms();
    for object in store
        .list(Some(&Path::from("leases")))
        .try_collect::<Vec<_>>()
        .await?
    {
        let bytes = store.get(&object.location).await?.bytes().await?;
        let Ok(lease) = serde_json::from_slice::<constellation_store_s3::Lease>(&bytes) else {
            continue;
        };
        if !lease.released && lease.is_expired(now) {
            let requested = force_release == Some(lease.partition.as_str());
            let repaired = if repair && requested {
                let lease_store = LeaseStore::new(store.clone(), &lease.partition, mode);
                if let Some((current, tag)) = lease_store.get().await? {
                    lease_store.try_swap(&current.released(), &tag).await?;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            issues.push(FsckIssue {
                class: "stale_lease".into(),
                key: Some(object.location.to_string()),
                detail: "expired lease left untouched unless --repair --force-release names it"
                    .into(),
                repaired,
                unrepairable: false,
            });
        }
    }
    Ok(())
}

fn check_cache_cruft(
    state_dir: Option<&std::path::Path>,
    repair: bool,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    let Some(root) = state_dir.map(|dir| dir.join("cache")) else {
        return Ok(());
    };
    if !root.exists() {
        return Ok(());
    }
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".tmp"))
            {
                let repaired = repair && std::fs::remove_file(&path).is_ok();
                issues.push(FsckIssue {
                    class: "cache_cruft".into(),
                    key: Some(path.display().to_string()),
                    detail: "temporary cache write survived startup cleanup".into(),
                    repaired,
                    unrepairable: repair && !repaired,
                });
            }
        }
    }
    Ok(())
}

async fn check_gc_journal(
    store: &Arc<dyn ObjectStore>,
    meta: &SqliteMeta,
    issues: &mut Vec<FsckIssue>,
) -> Result<()> {
    let live: HashSet<_> = meta.live_manifest_hashes()?;
    for object in store
        .list(Some(&constellation_store_s3::layout::gc_journal_prefix()))
        .try_collect::<Vec<_>>()
        .await?
    {
        let bytes = store.get(&object.location).await?.bytes().await?;
        let entry: GcJournalEntry = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid GC journal {}", object.location))?;
        let hash = Path::from(entry.key.clone())
            .filename()
            .and_then(ChunkHash::from_hex);
        if hash.is_some_and(|hash| live.contains(&hash)) {
            issues.push(FsckIssue {
                class: "gc_journal_reference_violation".into(),
                key: Some(entry.key),
                detail: json!({"journal":object.location.to_string(),"rule":entry.rule})
                    .to_string(),
                repaired: false,
                unrepairable: true,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::{DEFAULT_CHUNK_SIZE, INLINE_CHUNKS_MAX};
    use constellation_meta::{LogRecord, MetaStore};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn dangling_detector_never_silently_truncates() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = constellation_store_s3::ChunkStore::new(object_store.clone());
        let meta = SqliteMeta::open_in_memory().unwrap();
        let file = meta.create(1, "lost", 0o644, 0, 0).unwrap();
        let hash = chunks.hash(b"missing");
        let manifest =
            Manifest::from_chunks(DEFAULT_CHUNK_SIZE, 7, vec![hash], INLINE_CHUNKS_MAX, |b| {
                ChunkHash::of(b)
            })
            .0
            .encode();
        meta.set_manifest(file.ino, &manifest, 7).unwrap();
        let mut issues = Vec::new();
        check_manifest_refs(
            &object_store,
            &chunks,
            &meta,
            None,
            CompressionSetting::RAW,
            true,
            &mut issues,
        )
        .await
        .unwrap();
        assert!(issues[0].unrepairable);
        assert_eq!(meta.manifest(file.ino).unwrap().unwrap(), manifest);
    }

    /// Plan 28 S6's `fsck`: a replica level with a commit rebuilds to its
    /// root; a replica that drifted from what it published is reported
    /// with the keys that differ; and a corrupt pack under the head is
    /// reported as an unreadable node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_tree_check_recomputes_the_root_and_verifies_every_node() {
        use crate::mtree_publish::TreePublisher;
        use constellation_fs_core::cache::DiskCache;
        use constellation_fs_core::types::ROOT_INO;
        use constellation_mtree::{record, Hasher};
        use constellation_store_s3::{BlobStore, CommitChain, NodeCache, PackStore};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = Arc::new(SqliteMeta::open_in_memory().unwrap());
        let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
        for i in 0..300 {
            meta.create(dir, &format!("f{i}"), 0o644, 0, 0).unwrap();
        }
        let drain = |meta: &SqliteMeta| -> Vec<LogRecord> {
            let batch = meta.take_journal(usize::MAX).unwrap();
            if let Some((seq, _)) = batch.last() {
                meta.ack_journal(*seq).unwrap();
            }
            batch.into_iter().map(|(_, record)| record).collect()
        };
        let nodes = tempfile::TempDir::new().unwrap();
        let mut publisher = TreePublisher::new(
            Arc::clone(&meta),
            Arc::new(NodeCache::new(
                PackStore::new(store.clone()),
                Arc::new(DiskCache::open(nodes.path(), 1 << 30).unwrap()),
                Hasher::Plain,
                tokio::runtime::Handle::current(),
            )),
            BlobStore::new(store.clone(), Hasher::Plain),
            CommitChain::new(store.clone()),
            record::config(),
            1,
            tokio::runtime::Handle::current(),
        );
        publisher.note(&drain(&meta));
        publisher.publish(1).await.unwrap().unwrap();

        let logs = LogStore::new(store.clone());
        let state = tempfile::TempDir::new().unwrap();
        let mut issues = Vec::new();
        check_metadata_tree(&logs, &meta, Some(state.path()), &mut issues)
            .await
            .unwrap();
        assert!(issues.is_empty(), "{issues:#?}");

        // Drift: the replica changes and nothing is published.
        let f7 = meta.child_ino(dir, "f7").unwrap().unwrap();
        meta.setattr(f7, Some(0o600), None, None, None, None, None)
            .unwrap();
        drain(&meta);
        check_metadata_tree(&logs, &meta, Some(state.path()), &mut issues)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1, "{issues:#?}");
        assert_eq!(issues[0].class, "mtree_root_mismatch");
        let inode_key: String = constellation_mtree::keys::inode(f7)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert!(
            issues[0].detail.contains(&inode_key),
            "{}",
            issues[0].detail
        );

        // Corruption: flip bytes in every pack body.
        for object in store
            .list(Some(&constellation_store_s3::layout::packs_prefix()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            if object.location.as_ref().ends_with(".idx") {
                continue;
            }
            let mut body = store
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            for byte in body.iter_mut().skip(64) {
                *byte ^= 0x5a;
            }
            store
                .put(&object.location, PutPayload::from(body))
                .await
                .unwrap();
        }
        issues.clear();
        check_metadata_tree(&logs, &meta, Some(state.path()), &mut issues)
            .await
            .unwrap();
        assert!(
            issues
                .iter()
                .any(|issue| issue.class == "mtree_node" && issue.unrepairable),
            "{issues:#?}"
        );
    }
}
