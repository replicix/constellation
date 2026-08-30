//! Offline-capable filesystem consistency checking (DESIGN.md §10, §14).
//!
//! Detection is always non-destructive. `--repair` performs only repairs
//! with an unambiguous safe action; missing content without a cache copy is
//! reported as loss and never converted into a shorter manifest.

use crate::gc;
use anyhow::{Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::ChunkHash;
use constellation_meta::{LogRecord, SqliteMeta};
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
    check_xparts(&meta, repair, &mut issues)?;
    check_metadata_objects(&store, logs, &meta, repair, &mut issues).await?;
    check_leases(&store, lease_mode, repair, force_release, &mut issues).await?;
    check_cache_cruft(state_dir, repair, &mut issues)?;
    check_gc_journal(&store, &meta, &mut issues).await?;

    let orphan_report = gc::run(store, chunks, meta, lease_mode, true, !repair, None).await?;
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

fn check_xparts(meta: &SqliteMeta, repair: bool, issues: &mut Vec<FsckIssue>) -> Result<()> {
    for (txid, half, record) in meta.pending_xparts()? {
        let repaired = if repair && half == "src" {
            let part = match record {
                LogRecord::RenameXpartSrc { part, .. } => part,
                _ => "p0".into(),
            };
            meta.journal_on(&part, &LogRecord::RenameXpartAbort { txid })?;
            meta.clear_pending_xpart(txid)?;
            true
        } else {
            false
        };
        issues.push(FsckIssue {
            class: "half_committed_xpart".into(),
            key: None,
            detail: format!("transaction {txid} has only {half} half"),
            repaired,
            unrepairable: repair && !repaired,
        });
    }
    Ok(())
}

async fn check_metadata_objects(
    store: &Arc<dyn ObjectStore>,
    logs: &LogStore,
    meta: &SqliteMeta,
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
    if logs.get_latest_checkpoint().await.is_err() {
        let key = Path::from("checkpoints/p0/LATEST");
        issues.push(FsckIssue {
            class: "invalid_checkpoint".into(),
            key: Some(key.to_string()),
            detail: "latest checkpoint pointer or payload is invalid".into(),
            repaired: if repair {
                let vector = constellation_store_s3::CheckpointVector {
                    applied: meta.applied_vector()?,
                };
                logs.put_checkpoint_with_vector(
                    meta.applied_seq_of("p0")?,
                    &meta.snapshot()?,
                    &vector,
                )
                .await?;
                true
            } else {
                false
            },
            unrepairable: false,
        });
    }
    Ok(())
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
    use constellation_meta::MetaStore;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn dangling_detector_never_silently_truncates() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = constellation_store_s3::ChunkStore::new(object_store.clone());
        let meta = SqliteMeta::open_in_memory().unwrap();
        let file = meta.create(1, "lost", 0o644, 0, 0).unwrap();
        let hash = chunks.hash(b"missing");
        let manifest = Manifest::from_chunks(DEFAULT_CHUNK_SIZE, 7, vec![hash], INLINE_CHUNKS_MAX)
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
}
