//! Anonymized corpus snapshot and replay.
//!
//! Walk a local tree, hash path components, and record kind + size. Original
//! bytes are hashed only while snapshotting, to detect duplicates. The manifest
//! never stores hashes: files that shared content get the same small integer
//! `i`; unique files omit `i`. Replay fills from that id (or from the path, if
//! unique) so duplicates stay byte-identical at the original size.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const TOKEN_LEN: usize = 12;
const SKIP_DIR_NAMES: &[&str] = &[".git", ".hg", ".svn"];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestMeta {
    pub v: u32,
    pub seed: u64,
    pub files: u64,
    pub dirs: u64,
    pub bytes: u64,
    pub unique_hashes: u64,
    pub duplicate_groups: u64,
    pub skipped_symlinks: u64,
    pub skipped_other: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum ManifestEntry {
    #[serde(rename = "d")]
    Dir { p: String },
    #[serde(rename = "f")]
    File {
        p: String,
        s: u64,
        /// Present only when at least two files shared the same original bytes.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        i: Option<u32>,
    },
}

pub struct Corpus {
    pub meta: ManifestMeta,
    pub entries: Vec<ManifestEntry>,
}

fn keyed_hasher(seed: u64) -> blake3::Hasher {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&seed.to_le_bytes());
    key[8..16].copy_from_slice(b"corpus01");
    blake3::Hasher::new_keyed(&key)
}

fn token(seed: u64, name: &str, is_file: bool, used: &mut BTreeSet<String>) -> String {
    let mut h = keyed_hasher(seed);
    h.update(name.as_bytes());
    h.update(&[if is_file { 1 } else { 0 }]);
    let hex = h.finalize().to_hex();
    let mut n = TOKEN_LEN;
    loop {
        let mut t = hex[..n].to_string();
        if is_file {
            t.push_str(".bin");
        }
        if used.insert(t.clone()) {
            return t;
        }
        n = (n + 2).min(hex.len());
        if n == hex.len() {
            t.push_str("-x");
            used.insert(t.clone());
            return t;
        }
    }
}

fn skip_dir(name: &str) -> bool {
    SKIP_DIR_NAMES.contains(&name)
}

fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut f = File::open(path).with_context(|| format!("hash {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Snapshot `src` into an anonymized corpus. Names are keyed-BLAKE3 tokens;
/// sizes are exact. Duplicate original content is collapsed to a shared `i`.
pub fn snapshot(src: &Path, seed: u64) -> Result<Corpus> {
    anyhow::ensure!(src.is_dir(), "{} is not a directory", src.display());
    let mut dirs: BTreeSet<String> = BTreeSet::new();
    let mut files: Vec<(String, u64, [u8; 32])> = Vec::new();
    let mut skipped_symlinks = 0u64;
    let mut skipped_other = 0u64;
    let mut bytes = 0u64;
    let mut used_by_parent: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut dir_map: BTreeMap<PathBuf, String> = BTreeMap::new();
    dir_map.insert(PathBuf::new(), String::new());

    let mut stack = vec![src.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rel = dir.strip_prefix(src).unwrap_or(Path::new(""));
        let anon_parent = dir_map.get(rel).cloned().unwrap_or_default();
        let mut entries: Vec<_> = fs::read_dir(&dir)
            .with_context(|| format!("read_dir {}", dir.display()))?
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "." || name == ".." {
                continue;
            }
            let ft = entry.file_type()?;
            if ft.is_symlink() {
                skipped_symlinks += 1;
                continue;
            }
            if ft.is_dir() {
                if skip_dir(&name) {
                    continue;
                }
                let used = used_by_parent.entry(anon_parent.clone()).or_default();
                let tok = token(seed, &name, false, used);
                let anon = if anon_parent.is_empty() {
                    tok
                } else {
                    format!("{anon_parent}/{tok}")
                };
                dirs.insert(anon.clone());
                let child_rel = rel.join(name.as_ref());
                dir_map.insert(child_rel, anon);
                stack.push(entry.path());
                continue;
            }
            if !ft.is_file() {
                skipped_other += 1;
                continue;
            }
            let used = used_by_parent.entry(anon_parent.clone()).or_default();
            let tok = token(seed, &name, true, used);
            let anon = if anon_parent.is_empty() {
                tok
            } else {
                format!("{anon_parent}/{tok}")
            };
            let size = entry.metadata()?.len();
            let digest = hash_file(&entry.path())?;
            bytes = bytes.saturating_add(size);
            files.push((anon, size, digest));
        }
    }

    let mut counts: HashMap<[u8; 32], u32> = HashMap::new();
    for (_, _, digest) in &files {
        *counts.entry(*digest).or_insert(0) += 1;
    }
    let mut group_ids: HashMap<[u8; 32], u32> = HashMap::new();
    let mut next_id = 0u32;
    for (digest, n) in &counts {
        if *n >= 2 {
            group_ids.insert(*digest, next_id);
            next_id += 1;
        }
    }

    let mut entries = Vec::with_capacity(dirs.len() + files.len());
    for p in &dirs {
        entries.push(ManifestEntry::Dir { p: p.clone() });
    }
    for (p, s, digest) in &files {
        entries.push(ManifestEntry::File {
            p: p.clone(),
            s: *s,
            i: group_ids.get(digest).copied(),
        });
    }

    Ok(Corpus {
        meta: ManifestMeta {
            v: 2,
            seed,
            files: files.len() as u64,
            dirs: dirs.len() as u64,
            bytes,
            unique_hashes: counts.len() as u64,
            duplicate_groups: group_ids.len() as u64,
            skipped_symlinks,
            skipped_other,
        },
        entries,
    })
}

pub fn save(corpus: &Corpus, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let enc = zstd::Encoder::new(file, 10)?;
    let mut w = BufWriter::new(enc);
    serde_json::to_writer(&mut w, &corpus.meta)?;
    w.write_all(b"\n")?;
    for e in &corpus.entries {
        serde_json::to_writer(&mut w, e)?;
        w.write_all(b"\n")?;
    }
    let enc = w
        .into_inner()
        .map_err(|e| anyhow::anyhow!("flush corpus: {e}"))?;
    enc.finish()?;
    Ok(())
}

pub fn load(path: &Path) -> Result<Corpus> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let dec = zstd::Decoder::new(file)?;
    let mut lines = BufReader::new(dec).lines();
    let meta_line = lines.next().context("empty corpus manifest")??;
    let meta: ManifestMeta = serde_json::from_str(&meta_line)?;
    anyhow::ensure!(
        meta.v == 2,
        "unsupported corpus manifest version {} (want 2)",
        meta.v
    );
    let mut entries = Vec::new();
    for line in lines {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        entries.push(serde_json::from_str(&line)?);
    }
    Ok(Corpus { meta, entries })
}

pub struct StageLimits {
    /// Keep at most this many files (manifest order: dirs first, then files).
    pub max_files: Option<u64>,
    /// Cap each file's staged payload (directory shape is unchanged).
    pub max_file_bytes: Option<u64>,
}

pub struct StageStats {
    pub files: u64,
    pub dirs: u64,
    pub bytes: u64,
}

pub fn stage(root: &Path, corpus: &Corpus, seed: u64, limits: StageLimits) -> Result<StageStats> {
    let mut stats = StageStats {
        files: 0,
        dirs: 0,
        bytes: 0,
    };

    for e in &corpus.entries {
        if let ManifestEntry::Dir { p } = e {
            fs::create_dir_all(root.join(p))?;
        }
    }

    for e in &corpus.entries {
        let ManifestEntry::File { p, s, i } = e else {
            continue;
        };
        if let Some(max) = limits.max_files {
            if stats.files >= max {
                break;
            }
        }
        let size = match limits.max_file_bytes {
            Some(cap) => (*s).min(cap),
            None => *s,
        };
        let path = root.join(p);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        write_payload(&path, seed, p, *i, size)?;
        stats.files += 1;
        stats.bytes += size;
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        stats.dirs += 1;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(stats)
}

fn write_payload(path: &Path, seed: u64, rel: &str, group: Option<u32>, size: u64) -> Result<()> {
    let mut f = File::create(path)?;
    if size == 0 {
        return Ok(());
    }
    let mut hasher = keyed_hasher(seed);
    match group {
        Some(id) => {
            hasher.update(b"dup");
            hasher.update(&id.to_le_bytes());
        }
        None => {
            hasher.update(b"uniq");
            hasher.update(rel.as_bytes());
        }
    }
    let mut reader = hasher.finalize_xof();
    let mut buf = [0u8; 64 * 1024];
    let mut remain = size;
    while remain > 0 {
        let n = remain.min(buf.len() as u64) as usize;
        reader.fill(&mut buf[..n]);
        f.write_all(&buf[..n])?;
        remain -= n as u64;
    }
    Ok(())
}

pub fn snapshot_cmd(src: PathBuf, out: PathBuf, seed: u64) -> Result<()> {
    let corpus = snapshot(&src, seed)?;
    save(&corpus, &out)?;
    eprintln!(
        "wrote {} (files={} dirs={} bytes={} unique_contents={} duplicate_groups={} skipped_symlinks={} skipped_other={})",
        out.display(),
        corpus.meta.files,
        corpus.meta.dirs,
        corpus.meta.bytes,
        corpus.meta.unique_hashes,
        corpus.meta.duplicate_groups,
        corpus.meta.skipped_symlinks,
        corpus.meta.skipped_other
    );
    if corpus.meta.files == 0 {
        bail!("snapshot produced no files");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn snapshot_hides_names_and_preserves_shape() {
        let src = tempdir().unwrap();
        fs::create_dir_all(src.path().join("alpha/beta")).unwrap();
        fs::create_dir_all(src.path().join("alpha/empty")).unwrap();
        fs::write(src.path().join("alpha/beta/readme.md"), vec![1; 1453]).unwrap();
        fs::write(src.path().join("root.txt"), vec![2; 10]).unwrap();
        fs::write(src.path().join("alpha/zero"), b"").unwrap();

        let corpus = snapshot(src.path(), 42).unwrap();
        assert_eq!(corpus.meta.files, 3);
        assert_eq!(corpus.meta.dirs, 3);
        assert_eq!(corpus.meta.bytes, 1463);
        let dumped = serde_json::to_string(&corpus.entries).unwrap();
        assert!(!dumped.contains("alpha"));
        assert!(!dumped.contains("readme"));
        assert!(!dumped.contains("\"h\""));

        let dst = tempdir().unwrap();
        let stats = stage(
            dst.path(),
            &corpus,
            42,
            StageLimits {
                max_files: None,
                max_file_bytes: None,
            },
        )
        .unwrap();
        assert_eq!(stats.files, 3);
        assert_eq!(stats.bytes, 1463);
        assert_eq!(stats.dirs, 4);
        for e in &corpus.entries {
            if let ManifestEntry::File { p, s, .. } = e {
                assert_eq!(fs::metadata(dst.path().join(p)).unwrap().len(), *s);
            }
        }
    }

    #[test]
    fn identical_originals_share_an_integer_id() {
        let src = tempdir().unwrap();
        let blob = vec![7u8; 4096];
        fs::write(src.path().join("a"), &blob).unwrap();
        fs::write(src.path().join("b"), &blob).unwrap();
        fs::write(src.path().join("c"), vec![8u8; 4096]).unwrap();

        let corpus = snapshot(src.path(), 42).unwrap();
        assert_eq!(corpus.meta.files, 3);
        assert_eq!(corpus.meta.unique_hashes, 2);
        assert_eq!(corpus.meta.duplicate_groups, 1);

        let mut groups = Vec::new();
        let mut unique = 0;
        for e in &corpus.entries {
            if let ManifestEntry::File { i, .. } = e {
                match i {
                    Some(id) => groups.push(*id),
                    None => unique += 1,
                }
            }
        }
        assert_eq!(unique, 1);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], groups[1]);
        let dumped = serde_json::to_string(&corpus.entries).unwrap();
        assert!(!dumped.contains("\"h\""));

        let dst = tempdir().unwrap();
        stage(
            dst.path(),
            &corpus,
            99,
            StageLimits {
                max_files: None,
                max_file_bytes: None,
            },
        )
        .unwrap();

        let mut dup_bytes = Vec::new();
        let mut other = None;
        for e in &corpus.entries {
            if let ManifestEntry::File { p, s, i } = e {
                let bytes = fs::read(dst.path().join(p)).unwrap();
                assert_eq!(bytes.len() as u64, *s);
                if i.is_some() {
                    dup_bytes.push(bytes);
                } else {
                    other = Some(bytes);
                }
            }
        }
        assert_eq!(dup_bytes[0], dup_bytes[1]);
        assert_ne!(dup_bytes[0], other.unwrap());
    }

    #[test]
    fn roundtrip_zstd_manifest() {
        let src = tempdir().unwrap();
        fs::write(src.path().join("a"), vec![0; 300]).unwrap();
        let corpus = snapshot(src.path(), 7).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("c.jsonl.zst");
        save(&corpus, &path).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.meta.files, 1);
        assert_eq!(loaded.meta.v, 2);
        assert_eq!(loaded.entries.len(), corpus.entries.len());
        match &loaded.entries[0] {
            ManifestEntry::File { i, .. } => assert!(i.is_none()),
            ManifestEntry::Dir { .. } => panic!("expected file"),
        }
    }
}
