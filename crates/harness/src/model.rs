//! The oracle: an in-memory filesystem model. Every workload op is
//! applied to the model and to the real mount; `verify` then walks the
//! real tree and fails on ANY divergence (missing/extra entries, size,
//! content hash, symlink target). If verification fails, constellation
//! has failed.

use anyhow::{bail, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    File { data: Vec<u8> },
    Dir,
    Symlink { target: String },
}

/// Paths are relative to the mount/model root, in sorted order.
#[derive(Default, Clone)]
pub struct Model {
    pub nodes: BTreeMap<PathBuf, Node>,
}

impl Model {
    pub fn verify(&self, root: &Path) -> Result<()> {
        // 1) Everything in the model exists and matches reality.
        for (rel, node) in &self.nodes {
            let real = root.join(rel);
            let meta = std::fs::symlink_metadata(&real);
            match node {
                Node::Dir => {
                    let m = meta.map_err(|e| divergence(rel, "missing dir", &e.to_string()))?;
                    if !m.is_dir() {
                        bail!(divergence(
                            rel,
                            "expected dir",
                            &format!("{:?}", m.file_type())
                        ));
                    }
                }
                Node::Symlink { target } => {
                    let m = meta.map_err(|e| divergence(rel, "missing symlink", &e.to_string()))?;
                    if !m.is_symlink() {
                        bail!(divergence(
                            rel,
                            "expected symlink",
                            &format!("{:?}", m.file_type())
                        ));
                    }
                    let t = std::fs::read_link(&real)?;
                    if t.to_string_lossy() != target.as_str() {
                        bail!(divergence(rel, target, &t.to_string_lossy()));
                    }
                }
                Node::File { data } => {
                    let m = meta.map_err(|e| divergence(rel, "missing file", &e.to_string()))?;
                    if !m.is_file() {
                        bail!(divergence(
                            rel,
                            "expected file",
                            &format!("{:?}", m.file_type())
                        ));
                    }
                    if m.len() != data.len() as u64 {
                        bail!(divergence(
                            rel,
                            &format!("size {}", data.len()),
                            &format!("size {}", m.len())
                        ));
                    }
                    let real_data = std::fs::read(&real)?;
                    if blake3::hash(&real_data) != blake3::hash(data) {
                        bail!(divergence(rel, "content hash", "differs"));
                    }
                }
            }
        }
        // 2) Reality has nothing the model doesn't know about.
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let rel = entry.path().strip_prefix(root).unwrap().to_path_buf();
                match self.nodes.get(&rel) {
                    None => bail!(divergence(&rel, "absent from model", "exists on disk")),
                    Some(Node::Dir) => stack.push(entry.path()),
                    Some(_) => {}
                }
            }
        }
        Ok(())
    }

    /// The same check as [`Model::verify`], against a tree observed
    /// somewhere the harness cannot `stat` itself (inside a Kubernetes
    /// pod, `crate::k8s`): every entry with its kind, and a file's size and
    /// SHA-256 rather than its bytes. The checks are the same: missing or
    /// extra entries, kind, size, content, symlink target.
    pub fn verify_observed(&self, observed: &BTreeMap<PathBuf, Observed>) -> Result<()> {
        for (rel, node) in &self.nodes {
            let Some(seen) = observed.get(rel) else {
                bail!(divergence(rel, "present", "missing"));
            };
            match (node, seen) {
                (Node::Dir, Observed::Dir) => {}
                (Node::Symlink { target }, Observed::Symlink { target: t }) => {
                    if t != target {
                        bail!(divergence(rel, target, t));
                    }
                }
                (Node::File { data }, Observed::File { size, sha256 }) => {
                    if *size != data.len() as u64 {
                        bail!(divergence(
                            rel,
                            &format!("size {}", data.len()),
                            &format!("size {size}")
                        ));
                    }
                    if *sha256 != sha256_of(data) {
                        bail!(divergence(rel, "content hash", "differs"));
                    }
                }
                (node, seen) => bail!(divergence(
                    rel,
                    &format!("{node:?}").chars().take(40).collect::<String>(),
                    &format!("{seen:?}")
                )),
            }
        }
        if let Some(rel) = observed.keys().find(|k| !self.nodes.contains_key(*k)) {
            bail!(divergence(rel, "absent from model", "exists"));
        }
        Ok(())
    }

    /// This model with every path moved under `dir` (which must be one of
    /// `into`'s directories): two subtrees written by different clients,
    /// each against its own model, checked as one tree.
    pub fn graft(&self, into: &mut Model, dir: &Path) {
        for (rel, node) in &self.nodes {
            into.nodes.insert(dir.join(rel), node.clone());
        }
    }

    // --- model mutations mirroring the workload ops ---

    pub fn mkdir(&mut self, p: &Path) {
        self.nodes.insert(p.into(), Node::Dir);
    }

    pub fn write_file(&mut self, p: &Path, data: Vec<u8>) {
        self.nodes.insert(p.into(), Node::File { data });
    }

    pub fn append(&mut self, p: &Path, extra: &[u8]) {
        if let Some(Node::File { data }) = self.nodes.get_mut(p) {
            data.extend_from_slice(extra);
        }
    }

    pub fn overwrite(&mut self, p: &Path, offset: usize, patch: &[u8]) {
        if let Some(Node::File { data }) = self.nodes.get_mut(p) {
            if data.len() < offset + patch.len() {
                data.resize(offset + patch.len(), 0);
            }
            data[offset..offset + patch.len()].copy_from_slice(patch);
        }
    }

    pub fn truncate(&mut self, p: &Path, size: usize) {
        if let Some(Node::File { data }) = self.nodes.get_mut(p) {
            data.resize(size, 0);
        }
    }

    pub fn symlink(&mut self, p: &Path, target: &str) {
        self.nodes.insert(
            p.into(),
            Node::Symlink {
                target: target.into(),
            },
        );
    }

    pub fn remove(&mut self, p: &Path) {
        self.nodes.remove(p);
    }

    /// Rename `from` -> `to`, moving any subtree under it.
    pub fn rename(&mut self, from: &Path, to: &Path) {
        let moved: Vec<(PathBuf, Node)> = self
            .nodes
            .iter()
            .filter(|(k, _)| k.as_path() == from || k.starts_with(from))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, _) in &moved {
            self.nodes.remove(k);
        }
        // An existing target (file) is replaced.
        self.nodes.remove(to);
        for (k, v) in moved {
            let suffix = k.strip_prefix(from).unwrap();
            let dst = if suffix.as_os_str().is_empty() {
                to.to_path_buf()
            } else {
                to.join(suffix)
            };
            self.nodes.insert(dst, v);
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.nodes
            .iter()
            .filter(|(_, n)| matches!(n, Node::File { .. }))
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn dirs(&self) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = self
            .nodes
            .iter()
            .filter(|(_, n)| matches!(n, Node::Dir))
            .map(|(k, _)| k.clone())
            .collect();
        v.push(PathBuf::new()); // the root
        v
    }

    /// Entries directly inside `dir` (for rmdir emptiness checks).
    pub fn children(&self, dir: &Path) -> usize {
        self.nodes
            .keys()
            .filter(|k| k.parent() == Some(dir))
            .count()
    }
}

/// One entry of a tree observed remotely ([`Model::verify_observed`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    File { size: u64, sha256: [u8; 32] },
    Dir,
    Symlink { target: String },
}

pub fn sha256_of(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

fn divergence(path: &Path, expected: &str, got: &str) -> anyhow::Error {
    anyhow::anyhow!("MODEL DIVERGENCE at {path:?}: expected {expected}, got {got}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed_of(m: &Model) -> BTreeMap<PathBuf, Observed> {
        m.nodes
            .iter()
            .map(|(p, n)| {
                let o = match n {
                    Node::Dir => Observed::Dir,
                    Node::Symlink { target } => Observed::Symlink {
                        target: target.clone(),
                    },
                    Node::File { data } => Observed::File {
                        size: data.len() as u64,
                        sha256: sha256_of(data),
                    },
                };
                (p.clone(), o)
            })
            .collect()
    }

    #[test]
    fn an_observed_tree_must_match_the_model_exactly() {
        let mut m = Model::default();
        m.mkdir(Path::new("d"));
        m.write_file(Path::new("d/f"), b"hello".to_vec());
        m.symlink(Path::new("l"), "d/f");
        let seen = observed_of(&m);
        m.verify_observed(&seen).unwrap();

        let mut changed = m.clone();
        changed.overwrite(Path::new("d/f"), 0, b"J");
        assert!(changed.verify_observed(&seen).is_err(), "content");
        let mut longer = m.clone();
        longer.append(Path::new("d/f"), b"!");
        assert!(longer.verify_observed(&seen).is_err(), "size");
        let mut more = m.clone();
        more.mkdir(Path::new("e"));
        assert!(more.verify_observed(&seen).is_err(), "missing entry");
        let mut fewer = m.clone();
        fewer.remove(Path::new("l"));
        assert!(fewer.verify_observed(&seen).is_err(), "extra entry");
        let mut relinked = m.clone();
        relinked.symlink(Path::new("l"), "elsewhere");
        assert!(relinked.verify_observed(&seen).is_err(), "link target");
    }

    #[test]
    fn a_grafted_model_lives_under_its_directory() {
        let mut sub = Model::default();
        sub.write_file(Path::new("x"), vec![1]);
        let mut root = Model::default();
        root.mkdir(Path::new("a"));
        sub.graft(&mut root, Path::new("a"));
        assert_eq!(root.files(), vec![PathBuf::from("a/x")]);
    }
}
