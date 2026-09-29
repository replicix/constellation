//! The kit's oracle: a small path-based model of a POSIX namespace with
//! file content, and a [`Driver`] that replays a seeded workload against a
//! target and the model side by side, comparing every op's outcome and,
//! at the end (and periodically), the whole tree.
//!
//! The model is written independently of the reference filesystem
//! (`mock::reffs`): it works on names and node ids, not inodes and
//! handles, and knows only the POSIX rules a workload exercises. The
//! workload is restricted to combinations whose error is unambiguous
//! across POSIX systems (it never links or writes a directory, for
//! instance) so a mismatch is a bug in the target, not a precedence
//! judgement in the kit.

use super::client::Client;
use super::Rng;
use crate::types::{FileKind, Ino, SetAttr};
use constellation_types::Code;
use std::collections::{BTreeMap, HashMap};

type NodeId = u32;

#[derive(Debug, Clone)]
enum Kind {
    File(Vec<u8>),
    Dir(BTreeMap<String, NodeId>),
    Link(String),
}

#[derive(Debug, Clone)]
struct MNode {
    kind: Kind,
    nlink: u32,
}

/// The modelled namespace: node 0 is the root directory.
#[derive(Debug, Clone)]
pub(crate) struct Model {
    nodes: HashMap<NodeId, MNode>,
    next: NodeId,
}

type MResult<T> = Result<T, Code>;

impl Model {
    pub(crate) fn new() -> Model {
        let mut nodes = HashMap::new();
        nodes.insert(
            0,
            MNode {
                kind: Kind::Dir(BTreeMap::new()),
                nlink: 1,
            },
        );
        Model { nodes, next: 1 }
    }

    /// The node at directory path `dir` (`""` is the root; otherwise one
    /// top-level name).
    fn dir_node(&self, dir: &str) -> MResult<NodeId> {
        if dir.is_empty() {
            return Ok(0);
        }
        self.children(0)?.get(dir).copied().ok_or(Code::NotFound)
    }

    fn children(&self, id: NodeId) -> MResult<&BTreeMap<String, NodeId>> {
        match &self.nodes[&id].kind {
            Kind::Dir(c) => Ok(c),
            _ => Err(Code::NotDir),
        }
    }

    fn children_mut(&mut self, id: NodeId) -> &mut BTreeMap<String, NodeId> {
        match &mut self.nodes.get_mut(&id).expect("node").kind {
            Kind::Dir(c) => c,
            _ => unreachable!("checked to be a directory"),
        }
    }

    fn add(&mut self, dir: &str, name: &str, kind: Kind) -> MResult<NodeId> {
        let pid = self.dir_node(dir)?;
        if self.children(pid)?.contains_key(name) {
            return Err(Code::Exists);
        }
        let id = self.next;
        self.next += 1;
        self.nodes.insert(id, MNode { kind, nlink: 1 });
        self.children_mut(pid).insert(name.to_string(), id);
        Ok(id)
    }

    pub(crate) fn create(&mut self, dir: &str, name: &str) -> MResult<()> {
        self.add(dir, name, Kind::File(Vec::new())).map(|_| ())
    }

    pub(crate) fn mkdir(&mut self, dir: &str, name: &str) -> MResult<()> {
        self.add(dir, name, Kind::Dir(BTreeMap::new())).map(|_| ())
    }

    pub(crate) fn symlink(&mut self, dir: &str, name: &str, target: &str) -> MResult<()> {
        self.add(dir, name, Kind::Link(target.to_string()))
            .map(|_| ())
    }

    fn drop_link(&mut self, id: NodeId) {
        let n = self.nodes.get_mut(&id).expect("node");
        n.nlink -= 1;
        if n.nlink == 0 {
            self.nodes.remove(&id);
        }
    }

    pub(crate) fn unlink(&mut self, dir: &str, name: &str) -> MResult<()> {
        let pid = self.dir_node(dir)?;
        let id = *self.children(pid)?.get(name).ok_or(Code::NotFound)?;
        if matches!(self.nodes[&id].kind, Kind::Dir(_)) {
            return Err(Code::IsDir);
        }
        self.children_mut(pid).remove(name);
        self.drop_link(id);
        Ok(())
    }

    pub(crate) fn rmdir(&mut self, dir: &str, name: &str) -> MResult<()> {
        let pid = self.dir_node(dir)?;
        let id = *self.children(pid)?.get(name).ok_or(Code::NotFound)?;
        match &self.nodes[&id].kind {
            Kind::Dir(c) if c.is_empty() => {}
            Kind::Dir(_) => return Err(Code::NotEmpty),
            _ => return Err(Code::NotDir),
        }
        self.children_mut(pid).remove(name);
        self.nodes.remove(&id);
        Ok(())
    }

    pub(crate) fn link(&mut self, dir: &str, name: &str, ndir: &str, nname: &str) -> MResult<()> {
        let pid = self.dir_node(dir)?;
        let id = *self.children(pid)?.get(name).ok_or(Code::NotFound)?;
        let dpid = self.dir_node(ndir)?;
        if self.children(dpid)?.contains_key(nname) {
            return Err(Code::Exists);
        }
        self.children_mut(dpid).insert(nname.to_string(), id);
        self.nodes.get_mut(&id).expect("node").nlink += 1;
        Ok(())
    }

    pub(crate) fn rename(&mut self, dir: &str, name: &str, ndir: &str, nname: &str) -> MResult<()> {
        let sp = self.dir_node(dir)?;
        let dp = self.dir_node(ndir)?;
        // Both parents must be directories before the source is looked for
        // (a path is resolved before the name in it is).
        self.children(sp)?;
        self.children(dp)?;
        let src = *self.children(sp)?.get(name).ok_or(Code::NotFound)?;
        let dst = self.children(dp)?.get(nname).copied();
        if sp == dp && name == nname {
            return Ok(());
        }
        let src_dir = matches!(self.nodes[&src].kind, Kind::Dir(_));
        // A directory cannot move beneath itself (the workload's trees are
        // two levels deep, so "beneath" is "into").
        if src_dir && dp == src {
            return Err(Code::Invalid);
        }
        if dst == Some(src) {
            return Ok(());
        }
        if let Some(dst) = dst {
            let dst_dir = matches!(self.nodes[&dst].kind, Kind::Dir(_));
            match (src_dir, dst_dir) {
                (true, false) => return Err(Code::NotDir),
                (false, true) => return Err(Code::IsDir),
                _ => {}
            }
            if dst_dir && !self.children(dst)?.is_empty() {
                return Err(Code::NotEmpty);
            }
            self.children_mut(dp).remove(nname);
            if dst_dir {
                self.nodes.remove(&dst);
            } else {
                self.drop_link(dst);
            }
        }
        self.children_mut(sp).remove(name);
        self.children_mut(dp).insert(nname.to_string(), src);
        Ok(())
    }

    fn file(&mut self, dir: &str, name: &str) -> MResult<&mut Vec<u8>> {
        let pid = self.dir_node(dir)?;
        let id = *self.children(pid)?.get(name).ok_or(Code::NotFound)?;
        match &mut self.nodes.get_mut(&id).expect("node").kind {
            Kind::File(data) => Ok(data),
            Kind::Dir(_) => Err(Code::IsDir),
            Kind::Link(_) => Err(Code::Invalid),
        }
    }

    pub(crate) fn write(&mut self, dir: &str, name: &str, off: usize, bytes: &[u8]) -> MResult<()> {
        let data = self.file(dir, name)?;
        if data.len() < off + bytes.len() {
            data.resize(off + bytes.len(), 0);
        }
        data[off..off + bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn truncate(&mut self, dir: &str, name: &str, size: usize) -> MResult<()> {
        self.file(dir, name)?.resize(size, 0);
        Ok(())
    }

    /// The kind and (for files) content of a path, if it exists.
    fn peek(&self, dir: &str, name: &str) -> Option<&Kind> {
        let pid = self.dir_node(dir).ok()?;
        let id = self.children(pid).ok()?.get(name)?;
        Some(&self.nodes[id].kind)
    }

    /// Whether `dir` names something that exists and is not a directory.
    fn is_non_dir(&self, dir: &str) -> bool {
        self.dir_node(dir)
            .is_ok_and(|id| !matches!(self.nodes[&id].kind, Kind::Dir(_)))
    }

    fn is_file(&self, dir: &str, name: &str) -> bool {
        matches!(self.peek(dir, name), Some(Kind::File(_)))
    }
}

/// The names the workload draws from.
const NAMES: &[&str] = &["a", "b", "c", "d"];
const DIRS: &[&str] = &["d0", "d1"];

/// What the workload did, for a failure message.
type Trail = Vec<String>;

/// Replays a seeded workload on a target (rooted at directory `base` of
/// `client`'s view) and on a [`Model`], comparing as it goes.
pub(crate) struct Driver<'a> {
    c: &'a Client,
    base: Ino,
    model: Model,
    hard_links: bool,
    trail: Trail,
}

fn code<T>(r: &Result<T, crate::error::VfsError>) -> Result<(), Code> {
    match r {
        Ok(_) => Ok(()),
        Err(e) => Err(e.code()),
    }
}

impl<'a> Driver<'a> {
    pub(crate) fn new(c: &'a Client, base: Ino, hard_links: bool) -> Self {
        Driver {
            c,
            base,
            model: Model::new(),
            hard_links,
            trail: Vec::new(),
        }
    }

    /// The target's inode of directory `dir` under the base, or the
    /// lookup's failure.
    fn dir_ino(&self, dir: &str) -> Result<Ino, Code> {
        if dir.is_empty() {
            return Ok(self.base);
        }
        self.c
            .lookup(self.base, dir)
            .map(|e| e.attr.ino)
            .map_err(|e| e.code())
    }

    fn path(dir: &str, name: &str) -> String {
        if dir.is_empty() {
            name.to_string()
        } else {
            format!("{dir}/{name}")
        }
    }

    fn pick_dir(rng: &mut Rng) -> &'static str {
        // Mostly the base directory itself, sometimes one of two
        // subdirectories.
        if rng.chance(50) {
            ""
        } else {
            rng.pick(DIRS)
        }
    }

    fn pick_path(rng: &mut Rng) -> (&'static str, &'static str) {
        // A directory-named entry only ever lives at the top: `d0`/`d1`
        // are among the names of the base directory.
        match rng.below(10) {
            0 | 1 => ("", rng.pick(DIRS)),
            _ => (Self::pick_dir(rng), rng.pick(NAMES)),
        }
    }

    /// Compare a target result with the model's; a mismatch is the error.
    fn agree(
        &mut self,
        what: &str,
        target: Result<(), Code>,
        model: Result<(), Code>,
    ) -> Result<(), String> {
        self.trail
            .push(format!("{what} -> target {target:?}, model {model:?}"));
        if target != model {
            return Err(format!(
                "{what}: the target answered {target:?}, the model {model:?}\n  trail: {}",
                self.trail.join("\n         ")
            ));
        }
        Ok(())
    }

    /// One seeded step; `Err` describes a divergence.
    pub(crate) fn step(&mut self, rng: &mut Rng) -> Result<(), String> {
        let (dir, name) = Self::pick_path(rng);
        let path = Self::path(dir, name);
        match rng.below(100) {
            0..=19 => {
                let t = self.dir_ino(dir).and_then(|p| {
                    let r = self.c.create_excl(p, name);
                    if let Ok((e, o)) = &r {
                        self.c.close(e.attr.ino, o.fh).map_err(|e| e.code())?;
                    }
                    code(&r)
                });
                let m = self.model.create(dir, name);
                self.agree(&format!("create {path}"), t, m)
            }
            20..=27 => {
                let t = self.dir_ino(dir).and_then(|p| code(&self.c.mkdir(p, name)));
                let m = self.model.mkdir(dir, name);
                self.agree(&format!("mkdir {path}"), t, m)
            }
            28..=39 => {
                let t = self
                    .dir_ino(dir)
                    .and_then(|p| code(&self.c.unlink(p, name)));
                let m = self.model.unlink(dir, name);
                self.agree(&format!("unlink {path}"), t, m)
            }
            40..=45 => {
                let t = self.dir_ino(dir).and_then(|p| code(&self.c.rmdir(p, name)));
                let m = self.model.rmdir(dir, name);
                self.agree(&format!("rmdir {path}"), t, m)
            }
            46..=60 => {
                let (ndir, nname) = Self::pick_path(rng);
                let npath = Self::path(ndir, nname);
                // Whether a missing source or a non-directory destination
                // parent is reported first is a matter of the system's
                // path resolution: not exercised.
                if self.model.is_non_dir(ndir) {
                    return Ok(());
                }
                let t = self.dir_ino(dir).and_then(|p| {
                    let np = self.dir_ino(ndir)?;
                    code(&self.c.rename(p, name, np, nname))
                });
                let m = self.model.rename(dir, name, ndir, nname);
                self.agree(&format!("rename {path} {npath}"), t, m)
            }
            61..=68 if self.hard_links => {
                let (ndir, nname) = Self::pick_path(rng);
                let npath = Self::path(ndir, nname);
                // Only files (or names that do not exist) are linked: the
                // error of linking a directory to an existing name differs
                // between systems.
                if self.model.peek(dir, name).is_some() && !self.model.is_file(dir, name) {
                    return Ok(());
                }
                let t = self.dir_ino(dir).and_then(|p| {
                    let e = self.c.lookup(p, name).map_err(|e| e.code())?;
                    let np = self.dir_ino(ndir)?;
                    code(&self.c.link(e.attr.ino, np, nname))
                });
                let m = self.model.link(dir, name, ndir, nname);
                self.agree(&format!("link {path} {npath}"), t, m)
            }
            69..=73 => {
                let target = format!("to-{}", rng.below(100));
                let t = self
                    .dir_ino(dir)
                    .and_then(|p| code(&self.c.symlink(p, name, &target)));
                let m = self.model.symlink(dir, name, &target);
                self.agree(&format!("symlink {path} -> {target}"), t, m)
            }
            74..=89 => {
                if !self.model.is_file(dir, name) {
                    return Ok(());
                }
                let off = rng.below(8192) as usize;
                let len = rng.range(1, 4096) as usize;
                let bytes = rng.bytes(len);
                let t = self.dir_ino(dir).and_then(|p| {
                    let e = self.c.lookup(p, name).map_err(|e| e.code())?;
                    let ino = e.attr.ino;
                    let o = self.c.open_rw(ino).map_err(|e| e.code())?;
                    let w = self.c.write(ino, o.fh, off as u64, &bytes);
                    let closed = self.c.close(ino, o.fh);
                    let w = w.map(|_| ());
                    closed.map_err(|e| e.code())?;
                    code(&w)
                });
                let m = self.model.write(dir, name, off, &bytes);
                self.agree(&format!("write {path} @{off}+{len}"), t, m)
            }
            90..=93 => {
                if !self.model.is_file(dir, name) {
                    return Ok(());
                }
                let size = rng.below(10_000) as usize;
                let t = self.dir_ino(dir).and_then(|p| {
                    let e = self.c.lookup(p, name).map_err(|e| e.code())?;
                    code(&self.c.setattr(
                        e.attr.ino,
                        None,
                        &SetAttr {
                            size: Some(size as u64),
                            ..SetAttr::default()
                        },
                    ))
                });
                let m = self.model.truncate(dir, name, size);
                self.agree(&format!("truncate {path} {size}"), t, m)
            }
            _ => {
                // A read: the whole file must match the model.
                let Some(Kind::File(want)) = self.model.peek(dir, name).cloned() else {
                    return Ok(());
                };
                let got = self
                    .dir_ino(dir)
                    .map_err(|c| format!("lookup of the directory of {path}: {c:?}"))
                    .and_then(|p| {
                        let e = self
                            .c
                            .lookup(p, name)
                            .map_err(|e| format!("lookup {path}: {:?}", e.code()))?;
                        self.c
                            .slurp(e.attr.ino)
                            .map_err(|e| format!("read {path}: {:?}", e.code()))
                    })?;
                self.trail
                    .push(format!("read {path} ({} bytes)", want.len()));
                if got != want {
                    return Err(format!(
                        "read {path}: {} bytes differ from the model's {} (first difference at {:?})\n  trail: {}",
                        got.len(),
                        want.len(),
                        got.iter().zip(&want).position(|(a, b)| a != b),
                        self.trail.join("\n         ")
                    ));
                }
                Ok(())
            }
        }
    }

    /// Compare the whole tree with the model.
    pub(crate) fn verify_tree(&self) -> Result<(), String> {
        let mut inos: HashMap<NodeId, Ino> = HashMap::new();
        self.verify_dir(0, self.base, "", &mut inos)
    }

    fn verify_dir(
        &self,
        id: NodeId,
        ino: Ino,
        at: &str,
        inos: &mut HashMap<NodeId, Ino>,
    ) -> Result<(), String> {
        let want = self.model.children(id).expect("a directory");
        let got = self
            .c
            .names(ino)
            .map_err(|e| format!("readdir {at:?}: {:?}", e.code()))?;
        let names: Vec<String> = want.keys().cloned().collect();
        if got != names {
            return Err(format!(
                "directory {at:?} lists {got:?}, the model has {names:?}\n  trail: {}",
                self.trail.join("\n         ")
            ));
        }
        for (name, &child) in want {
            let here = format!("{at}/{name}");
            let e = self
                .c
                .lookup(ino, name)
                .map_err(|e| format!("lookup {here}: {:?}", e.code()))?;
            let node = &self.model.nodes[&child];
            if let Some(&seen) = inos.get(&child) {
                if seen != e.attr.ino {
                    return Err(format!(
                        "{here} should be a name of inode {seen}, is {}",
                        e.attr.ino
                    ));
                }
            }
            inos.insert(child, e.attr.ino);
            match &node.kind {
                Kind::Dir(_) => {
                    if e.attr.kind != FileKind::Dir {
                        return Err(format!(
                            "{here} is {:?}, the model says a directory",
                            e.attr.kind
                        ));
                    }
                    self.verify_dir(child, e.attr.ino, &here, inos)?;
                }
                Kind::File(data) => {
                    if e.attr.kind != FileKind::File {
                        return Err(format!(
                            "{here} is {:?}, the model says a file",
                            e.attr.kind
                        ));
                    }
                    let a = self
                        .c
                        .getattr(e.attr.ino)
                        .map_err(|e| format!("getattr {here}: {:?}", e.code()))?;
                    if a.size != data.len() as u64 || (self.hard_links && a.nlink != node.nlink) {
                        return Err(format!(
                            "{here}: size {} nlink {}, the model has size {} nlink {}\n  trail: {}",
                            a.size,
                            a.nlink,
                            data.len(),
                            node.nlink,
                            self.trail.join("\n         ")
                        ));
                    }
                    let got = self
                        .c
                        .slurp(e.attr.ino)
                        .map_err(|e| format!("read {here}: {:?}", e.code()))?;
                    if &got != data {
                        return Err(format!(
                            "{here}: content differs from the model's ({} vs {} bytes)\n  trail: {}",
                            got.len(),
                            data.len(),
                            self.trail.join("\n         ")
                        ));
                    }
                }
                Kind::Link(target) => {
                    if e.attr.kind != FileKind::Symlink {
                        return Err(format!(
                            "{here} is {:?}, the model says a symlink",
                            e.attr.kind
                        ));
                    }
                    let got = self
                        .c
                        .readlink(e.attr.ino)
                        .map_err(|e| format!("readlink {here}: {:?}", e.code()))?;
                    if got != target.as_bytes() {
                        return Err(format!("{here} -> {got:?}, the model has {target:?}"));
                    }
                }
            }
        }
        Ok(())
    }
}
