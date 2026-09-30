//! Directory scan-ahead for tar/rsync-style lexicographic tree walks.
//!
//! Detection is metadata-only. After three forward file reads in one
//! directory, upcoming files' first two chunks enter the ordinary prefetch
//! scheduler, sharing its deduplication, adaptive S3 gate, and cache policy.

use constellation_fs_core::manifest::{ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, Ino, InodeKind};
use constellation_meta::{Meta, MetaStore};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TRIGGER_HITS: u32 = 3;
const MAX_FORWARD_SKIP: usize = 8;
const MAX_SCANS: usize = 512;
const MAX_ORDER_ENTRIES: usize = 65_536;
const MAX_DFS_DEPTH: usize = 16;
const SCAN_IDLE: Duration = Duration::from_secs(60);
const MIN_WINDOW: u64 = 16 << 20;
const MAX_WINDOW: u64 = 256 << 20;

pub struct ScanFile {
    pub ino: Ino,
    pub hashes: Vec<ChunkHash>,
    pub bytes: u64,
    pub chunk_list: Option<ChunkHash>,
}

struct Scan {
    order: Vec<Ino>,
    last_ino: Ino,
    pos: Option<usize>,
    hits: u32,
    window: u64,
    resume: usize,
    last_hit: Instant,
}

pub struct ScanAhead {
    meta: Arc<Meta>,
    enabled: bool,
    max_window: u64,
    scans: Mutex<HashMap<Ino, Scan>>,
    file_to_dir: Mutex<HashMap<Ino, Ino>>,
}

impl ScanAhead {
    pub fn new(meta: Arc<Meta>, cache_budget: u64) -> Self {
        let enabled = !std::env::var("CONSTELLATION_SCAN_AHEAD")
            .ok()
            .is_some_and(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "off" | "0" | "false"
                )
            });
        Self::with_enabled(meta, cache_budget, enabled)
    }

    fn with_enabled(meta: Arc<Meta>, cache_budget: u64, enabled: bool) -> Self {
        Self {
            meta,
            enabled,
            max_window: MAX_WINDOW.min(cache_budget / 4).max(1),
            scans: Mutex::new(HashMap::new()),
            file_to_dir: Mutex::new(HashMap::new()),
        }
    }

    fn build_order(&self, root: Ino) -> Vec<Ino> {
        fn visit(meta: &Meta, dir: Ino, depth: usize, out: &mut Vec<Ino>) {
            if depth > MAX_DFS_DEPTH || out.len() >= MAX_ORDER_ENTRIES {
                return;
            }
            let Ok(entries) = meta.readdir(dir) else {
                return;
            };
            for entry in entries {
                if out.len() >= MAX_ORDER_ENTRIES {
                    return;
                }
                match entry.kind {
                    InodeKind::File => out.push(entry.ino),
                    InodeKind::Dir => visit(meta, entry.ino, depth + 1, out),
                    _ => {}
                }
            }
        }

        let mut order = Vec::new();
        visit(&self.meta, root, 0, &mut order);
        order
    }

    pub fn note_read(&self, ino: Ino) -> Vec<ScanFile> {
        if !self.enabled {
            return Vec::new();
        }
        let Ok(Some(parent)) = self.meta.parent_of(ino) else {
            return Vec::new();
        };
        let now = Instant::now();

        let scan_dir = {
            let mut scans = self.scans.lock().unwrap();
            let stale: Vec<Ino> = scans
                .iter()
                .filter(|(_, scan)| now.duration_since(scan.last_hit) > SCAN_IDLE)
                .map(|(dir, _)| *dir)
                .collect();
            for dir in stale {
                scans.remove(&dir);
            }
            let existing = scans
                .iter()
                .find(|(_, scan)| !scan.order.is_empty() && scan.order.contains(&ino))
                .map(|(dir, _)| *dir);
            let dir = existing.unwrap_or(parent);
            if !scans.contains_key(&dir) {
                if scans.len() >= MAX_SCANS {
                    if let Some(oldest) = scans
                        .iter()
                        .min_by_key(|(_, scan)| scan.last_hit)
                        .map(|(dir, _)| *dir)
                    {
                        scans.remove(&oldest);
                    }
                }
                scans.insert(
                    dir,
                    Scan {
                        order: Vec::new(),
                        last_ino: ino,
                        pos: None,
                        hits: 1,
                        window: MIN_WINDOW.min(self.max_window),
                        resume: 0,
                        last_hit: now,
                    },
                );
                drop(scans);
                self.file_to_dir.lock().unwrap().insert(ino, dir);
                return Vec::new();
            }
            let scan = scans.get(&dir).unwrap();
            if scan.last_ino == ino {
                return Vec::new();
            }
            dir
        };

        let needs_order = {
            let scans = self.scans.lock().unwrap();
            scans.get(&scan_dir).unwrap().order.is_empty()
        };
        let order = needs_order.then(|| self.build_order(scan_dir));

        let (candidates, window) = {
            let mut scans = self.scans.lock().unwrap();
            let Some(scan) = scans.get_mut(&scan_dir) else {
                return Vec::new();
            };
            if scan.order.is_empty() {
                scan.order = order.unwrap_or_default();
                scan.pos = scan
                    .order
                    .iter()
                    .position(|candidate| *candidate == scan.last_ino);
            }
            let Some(current) = scan.order.iter().position(|candidate| *candidate == ino) else {
                return Vec::new();
            };
            let forward = scan.pos.is_some_and(|previous| {
                current > previous && current - previous <= MAX_FORWARD_SKIP
            });
            scan.hits = if forward {
                scan.hits.saturating_add(1)
            } else {
                1
            };
            scan.pos = Some(current);
            scan.last_ino = ino;
            scan.last_hit = now;
            scan.resume = scan.resume.max(current + 1);
            if scan.hits < TRIGGER_HITS {
                return Vec::new();
            }
            (scan.order[scan.resume..].to_vec(), scan.window)
        };
        self.file_to_dir.lock().unwrap().insert(ino, scan_dir);

        let mut files = Vec::new();
        let mut bytes = 0u64;
        let mut consumed = 0usize;
        for candidate in candidates {
            if bytes >= window {
                break;
            }
            consumed += 1;
            let Ok(Some(encoded)) = self.meta.manifest(candidate) else {
                continue;
            };
            let Ok(manifest) = Manifest::decode(&encoded) else {
                continue;
            };
            let (hashes, logical, chunk_list) = match manifest.chunks {
                ChunkInfo::Inline(chunks) => (
                    chunks.values().take(2).copied().collect::<Vec<_>>(),
                    manifest
                        .file_len
                        .min(u64::from(manifest.layout.chunk_size) * 2),
                    None,
                ),
                ChunkInfo::Spilled(hash) => (
                    vec![hash],
                    manifest
                        .file_len
                        .min(u64::from(manifest.layout.chunk_size) * 2),
                    Some(hash),
                ),
            };
            if hashes.is_empty() {
                continue;
            }
            bytes = bytes.saturating_add(logical);
            files.push(ScanFile {
                ino: candidate,
                hashes,
                bytes: logical,
                chunk_list,
            });
            self.file_to_dir.lock().unwrap().insert(candidate, scan_dir);
        }
        if consumed > 0 {
            if let Some(scan) = self.scans.lock().unwrap().get_mut(&scan_dir) {
                scan.resume = scan.resume.saturating_add(consumed);
            }
        }
        files
    }

    pub fn note_stall(&self, ino: Ino) {
        let Some(dir) = self.file_to_dir.lock().unwrap().get(&ino).copied() else {
            return;
        };
        if let Some(scan) = self.scans.lock().unwrap().get_mut(&dir) {
            scan.window = scan.window.saturating_mul(2).min(self.max_window);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_fs_core::{ChunkHash, DEFAULT_CHUNK_SIZE, INLINE_CHUNKS_MAX};

    fn fixture(files: usize, chunks: usize) -> (Arc<Meta>, Vec<Ino>) {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let mut inos = Vec::new();
        for i in 0..files {
            let attr = meta
                .create(ROOT_INO, &format!("{i:04}"), 0o644, 1, 1)
                .unwrap();
            let hashes = (0..chunks)
                .map(|chunk| ChunkHash::of(&[i as u8, chunk as u8]))
                .collect();
            let (manifest, spill) = Manifest::from_chunks(
                DEFAULT_CHUNK_SIZE,
                chunks as u64 * u64::from(DEFAULT_CHUNK_SIZE),
                hashes,
                INLINE_CHUNKS_MAX,
                ChunkHash::of,
            );
            assert!(spill.is_none());
            meta.set_manifest(attr.ino, &manifest.encode(), manifest.file_len)
                .unwrap();
            inos.push(attr.ino);
        }
        (meta, inos)
    }

    #[test]
    fn three_ordered_reads_trigger_and_big_files_are_capped_at_two_chunks() {
        let (meta, inos) = fixture(6, 4);
        let scan = ScanAhead::with_enabled(meta, 1 << 30, true);
        assert!(scan.note_read(inos[0]).is_empty());
        assert!(scan.note_read(inos[1]).is_empty());
        let files = scan.note_read(inos[2]);
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|file| file.hashes.len() == 2));
    }

    #[test]
    fn skips_are_tolerated_and_off_disables_detection() {
        let (meta, inos) = fixture(8, 1);
        let scan = ScanAhead::with_enabled(meta.clone(), 1 << 30, true);
        assert!(scan.note_read(inos[0]).is_empty());
        assert!(scan.note_read(inos[2]).is_empty());
        assert!(!scan.note_read(inos[4]).is_empty());

        let off = ScanAhead::with_enabled(meta, 1 << 30, false);
        for ino in inos {
            assert!(off.note_read(ino).is_empty());
        }
    }

    #[test]
    fn ordered_scan_descends_into_subdirectories() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let first = meta.create(ROOT_INO, "0000", 0o644, 1, 1).unwrap();
        let second = meta.create(ROOT_INO, "0001", 0o644, 1, 1).unwrap();
        let dir = meta.mkdir(ROOT_INO, "0002", 0o755, 1, 1).unwrap();
        let child0 = meta.create(dir.ino, "0000", 0o644, 1, 1).unwrap();
        let child1 = meta.create(dir.ino, "0001", 0o644, 1, 1).unwrap();
        for ino in [first.ino, second.ino, child0.ino, child1.ino] {
            let (manifest, _) = Manifest::from_chunks(
                DEFAULT_CHUNK_SIZE,
                u64::from(DEFAULT_CHUNK_SIZE),
                vec![ChunkHash::of(&ino.to_le_bytes())],
                INLINE_CHUNKS_MAX,
                ChunkHash::of,
            );
            meta.set_manifest(ino, &manifest.encode(), manifest.file_len)
                .unwrap();
        }

        let scan = ScanAhead::with_enabled(meta, 1 << 30, true);
        assert!(scan.note_read(first.ino).is_empty());
        assert!(scan.note_read(second.ino).is_empty());
        let files = scan.note_read(child0.ino);
        assert!(files.iter().any(|file| file.ino == child1.ino));
    }
}
