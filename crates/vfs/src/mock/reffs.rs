//! The reference filesystem behind [`super::MockVfs`]'s reference mode: a
//! small, correct, in-memory POSIX-ish namespace with content, xattrs,
//! byte-range locks, snapshots and subtree views, written to the *contract*
//! (plan 31 §6) rather than to any engine's habits.
//!
//! It is a stand-in engine for frontend tests and the conformance kit's
//! reference target, so it is deliberately simple and deliberately strict:
//! one mutex around all state (every op is atomic, which the kit's
//! not-torn-writes test relies on), an unknown or foreign handle is
//! `BadFd`, and every result is computed under the lock and returned so
//! the caller completes the op's responder *after* releasing it (a
//! responder that panics or re-enters the filesystem cannot poison or
//! deadlock it).
//!
//! What it models, and how it is meant to be read:
//!
//! - **Namespace.** Inodes with kinds, modes, owners (from the
//!   [`PolicyStack`]'s identity map), link counts, timestamps from a
//!   logical clock. Directory entries carry a creation sequence number
//!   that is also the `readdir` cookie, so a listing resumes correctly
//!   across creates and unlinks (an entry present throughout is returned
//!   exactly once). `.`/`..` are cookies 1 and 2.
//! - **Content.** Sparse files: bytes plus the list of ranges that hold
//!   data, so `SEEK_DATA`/`SEEK_HOLE`, `fallocate` (plain, `KEEP_SIZE`,
//!   `PUNCH_HOLE`, `ZERO_RANGE`) and truncate-then-grow-reads-zeros mean
//!   what POSIX says. An unlinked file lives until its last handle closes.
//! - **Capabilities.** Every op the [`FrontendCaps`] withhold (hard links,
//!   xattrs, special files, `fallocate`, `SEEK_HOLE`, cluster locks) is
//!   refused the way the contract says: `NotSupported`, or `NotImplemented`
//!   for the lock ops.
//! - **Views** ([`RefView`]). A view is rooted at a directory: its root is
//!   `ROOT_INO` whatever inode it is underneath, `..` at the root is the
//!   root, an inode the root does not dominate is `Stale`, and with
//!   `confine_links` a `link()` (or a rename of a multiply-linked file)
//!   across a *link domain* is `CrossDevice`; a domain is the view's root
//!   or a directory carrying the root-only marker
//!   [`LINK_DOMAIN_XATTR`]. `.constellation/snapshot/<name>/...` is the
//!   synthetic, read-only, per-directory mirror of the snapshots that cover
//!   that directory's own path (§6.12).
//! - **Events.** A view with a [`FrontendEvents`] sink is told, from a
//!   dedicated notifier thread and coalesced, about mutations *other*
//!   views made (a name changed, an inode's attributes or pages).
//! - **Locks.** POSIX-style byte-range locks with per-owner split/merge; a
//!   blocking acquire waits on its own thread and completes the responder
//!   from there (the deferral the kit exercises), noticing a cancelled
//!   [`CancelToken`] and a passed deadline.
//!
//! What it does not model: permission bits (the kernel's
//! `DefaultPermissions` does that above the trait), timestamps beyond a
//! monotonic logical clock, durability levels (every `fsync` succeeds),
//! and the engine's write-back or leases.

use crate::caps::{FrontendCaps, XattrSupport};
use crate::ctx::{Caller, CancelToken, OpKind};
use crate::error::{VfsError, VfsResult};
use crate::events::{FrontendEvents, Invalidation};
use crate::name::{Name, NameBuf, XattrName, XattrNameBuf};
use crate::policy::{PolicyStack, RSIZE_XATTR};
use crate::responder::{DirSink, Responder};
use crate::types::{
    mode as modebits, Attr, Entry, FallocateMode, Fh, FileKind, Ino, LockKind, LockOwner,
    LockRange, LockSpec, LockStatus, OpenFlags, Opened, ReadData, RenameFlags, SeekWhence, SetAttr,
    SetXattrFlags, SetXattrMode, StatFs, TimeSet, ROOT_INO,
};
use constellation_types::{Code, Rdev};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

/// The root-only directory marker that starts a *link domain* inside a
/// view (`ViewSpec::confine_links`, plan 31 §6.12). Mirrors the engine's
/// `constellation_engine::LINK_DOMAIN_XATTR`; a `trusted.*` name, so only
/// uid 0 can set or clear it.
pub const LINK_DOMAIN_XATTR: &str = "trusted.constellation.link_domain";

/// Inode numbers from here up name synthetic (`.constellation`) nodes.
const SYN_BASE: Ino = 1 << 62;

/// `setxattr`'s value limit (Linux `XATTR_SIZE_MAX`).
const XATTR_SIZE_MAX: usize = 64 * 1024;

/// How many ancestors a walk follows before giving up (a cyclic parent
/// chain would be a bug in this file; the bound makes it a refusal).
const WALK_LIMIT: usize = 4096;

/// How long attributes may be cached, as `Attr::ttl` says.
const TTL: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct Node {
    kind: FileKind,
    /// Permission bits (`0o7777`).
    mode: u32,
    uid: u32,
    gid: u32,
    nlink: u32,
    rdev: Rdev,
    atime: i64,
    mtime: i64,
    ctime: i64,
    size: u64,
    /// File content; bytes outside `extents` are zero.
    data: Vec<u8>,
    /// Sorted, disjoint, non-adjacent `(start, end)` ranges holding data.
    extents: Vec<(u64, u64)>,
    target: Vec<u8>,
    dir: Option<Dir>,
    xattrs: BTreeMap<String, Vec<u8>>,
    /// Directories that hold a name for this inode (one for a directory).
    parents: Vec<Ino>,
    /// A directory's own name in its parent (for snapshot paths).
    name: Vec<u8>,
}

#[derive(Clone, Default)]
struct Dir {
    by_name: BTreeMap<Vec<u8>, (Ino, u64)>,
    by_seq: BTreeMap<u64, Vec<u8>>,
}

impl Node {
    fn new(kind: FileKind, mode: u32, uid: u32, gid: u32, now: i64) -> Node {
        Node {
            kind,
            mode: mode & 0o7777,
            uid,
            gid,
            nlink: 1,
            rdev: Rdev::default(),
            atime: now,
            mtime: now,
            ctime: now,
            size: 0,
            data: Vec::new(),
            extents: Vec::new(),
            target: Vec::new(),
            dir: (kind == FileKind::Dir).then(Dir::default),
            xattrs: BTreeMap::new(),
            parents: Vec::new(),
            name: Vec::new(),
        }
    }

    fn is_dir(&self) -> bool {
        self.kind == FileKind::Dir
    }

    fn dir(&self) -> &Dir {
        self.dir.as_ref().expect("a directory")
    }

    fn dir_mut(&mut self) -> &mut Dir {
        self.dir.as_mut().expect("a directory")
    }

    /// Write `bytes` at `off`, extending the file.
    fn write_at(&mut self, off: u64, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let end = off + bytes.len() as u64;
        if self.data.len() < end as usize {
            self.data.resize(end as usize, 0);
        }
        self.data[off as usize..end as usize].copy_from_slice(bytes);
        add_extent(&mut self.extents, off, end);
        self.size = self.size.max(end);
    }

    /// Cut or grow to `size`; growth is a hole.
    fn set_size(&mut self, size: u64) {
        if size < self.size {
            self.data.truncate(size as usize);
            cut_extents(&mut self.extents, size, u64::MAX);
        }
        self.size = size;
    }

    /// Deallocate `[off, end)`: reads there give zeros, `SEEK_DATA` skips
    /// it.
    fn punch(&mut self, off: u64, end: u64) {
        let end = end.min(self.data.len() as u64);
        if off < end {
            self.data[off as usize..end as usize].fill(0);
        }
        cut_extents(&mut self.extents, off, end);
    }

    fn read_at(&self, off: u64, len: u32) -> Vec<u8> {
        if off >= self.size {
            return Vec::new();
        }
        let end = (off + len as u64).min(self.size);
        let mut out = vec![0u8; (end - off) as usize];
        let have = (self.data.len() as u64).min(end);
        if off < have {
            out[..(have - off) as usize].copy_from_slice(&self.data[off as usize..have as usize]);
        }
        out
    }
}

fn add_extent(extents: &mut Vec<(u64, u64)>, start: u64, end: u64) {
    let mut merged = (start, end);
    extents.retain(|&(s, e)| {
        if e < merged.0 || s > merged.1 {
            true
        } else {
            merged = (merged.0.min(s), merged.1.max(e));
            false
        }
    });
    let at = extents.partition_point(|&(s, _)| s < merged.0);
    extents.insert(at, merged);
}

/// Remove `[start, end)` from the extents.
fn cut_extents(extents: &mut Vec<(u64, u64)>, start: u64, end: u64) {
    let mut out = Vec::with_capacity(extents.len() + 1);
    for &(s, e) in extents.iter() {
        if e <= start || s >= end {
            out.push((s, e));
            continue;
        }
        if s < start {
            out.push((s, start));
        }
        if e > end {
            out.push((end, e));
        }
    }
    *extents = out;
}

struct Handle {
    ino: Ino,
    /// The view that opened it (a confined view keeps addressing an
    /// inode it holds open even after its last name is gone).
    view: u64,
}

#[derive(Clone, Copy)]
struct LockEntry {
    owner: LockOwner,
    range: LockRange,
    kind: LockKind,
    pid: u32,
}

/// A frozen copy of the tree, taken at `path`.
struct Snapshot {
    name: Vec<u8>,
    /// The snapshotted directory's path from the filesystem root.
    path: Vec<Vec<u8>>,
    frozen: Arc<HashMap<Ino, Node>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum Syn {
    /// `<dir>/.constellation`
    Meta { dir: Ino },
    /// `<dir>/.constellation/snapshot`
    Snaps { dir: Ino },
    /// Snapshot `snap`'s mirror of `frozen` (a node of the frozen tree),
    /// seen under `<dir>/.constellation/snapshot/<name>`.
    Mirror { snap: usize, frozen: Ino, dir: Ino },
}

struct State {
    nodes: HashMap<Ino, Node>,
    next_ino: Ino,
    next_fh: u64,
    handles: HashMap<u64, Handle>,
    opens: HashMap<Ino, u32>,
    locks: HashMap<Ino, Vec<LockEntry>>,
    snapshots: Vec<Snapshot>,
    clock: i64,
    next_seq: u64,
}

/// The synthetic nodes handed out so far, numbered from [`SYN_BASE`].
#[derive(Default)]
struct SynReg {
    list: Vec<Syn>,
    ids: HashMap<Syn, Ino>,
}

impl State {
    fn tick(&mut self) -> i64 {
        self.clock += 1_000;
        self.clock
    }

    fn node(&self, ino: Ino) -> Result<&Node, Code> {
        self.nodes.get(&ino).ok_or(Code::NotFound)
    }

    fn node_mut(&mut self, ino: Ino) -> Result<&mut Node, Code> {
        self.nodes.get_mut(&ino).ok_or(Code::NotFound)
    }

    /// The path of directory `dir` from the filesystem root.
    fn path_of(&self, dir: Ino) -> Vec<Vec<u8>> {
        let mut path = Vec::new();
        let mut cur = dir;
        for _ in 0..WALK_LIMIT {
            if cur == ROOT_INO {
                break;
            }
            let Some(node) = self.nodes.get(&cur) else {
                break;
            };
            path.push(node.name.clone());
            cur = node.parents.first().copied().unwrap_or(ROOT_INO);
        }
        path.reverse();
        path
    }

    fn resolve_path(nodes: &HashMap<Ino, Node>, path: &[Vec<u8>]) -> Option<Ino> {
        let mut cur = ROOT_INO;
        for part in path {
            let node = nodes.get(&cur)?;
            if !node.is_dir() {
                return None;
            }
            cur = node.dir().by_name.get(part)?.0;
        }
        Some(cur)
    }

    /// Whether `root` has one of `ino`'s names beneath it (or is it).
    fn dominated(&self, ino: Ino, root: Ino) -> bool {
        if ino == root || root == ROOT_INO {
            return true;
        }
        let mut frontier = vec![ino];
        let mut seen = HashSet::new();
        while let Some(cur) = frontier.pop() {
            if seen.len() > WALK_LIMIT {
                return false;
            }
            let Some(node) = self.nodes.get(&cur) else {
                continue;
            };
            for &parent in &node.parents {
                if parent == root {
                    return true;
                }
                if parent != ROOT_INO && seen.insert(parent) {
                    frontier.push(parent);
                }
            }
        }
        false
    }

    /// The link domain of directory `dir` in a view rooted at `root`.
    fn link_domain(&self, dir: Ino, root: Ino) -> Option<Ino> {
        let mut cur = dir;
        for _ in 0..WALK_LIMIT {
            if cur == root {
                return Some(cur);
            }
            let node = self.nodes.get(&cur)?;
            if node
                .xattrs
                .get(LINK_DOMAIN_XATTR)
                .is_some_and(|v| v.as_slice() != b"0")
            {
                return Some(cur);
            }
            if cur == ROOT_INO {
                return None;
            }
            cur = *node.parents.first()?;
        }
        None
    }

    fn rsize_rcount(&self, ino: Ino) -> (u64, u64) {
        let Some(node) = self.nodes.get(&ino) else {
            return (0, 0);
        };
        if !node.is_dir() {
            return (node.size, 1);
        }
        let (mut size, mut count) = (0, 0);
        for &(child, _) in node.dir().by_name.values() {
            let (s, c) = self.rsize_rcount(child);
            size += s;
            count += c;
        }
        (size, count)
    }
}

/// What a node address resolved to, in a view.
#[derive(Clone)]
enum Target {
    Real(Ino),
    Syn(Syn),
}

/// One view's event sink and the thread that delivers to it.
struct Sink {
    view: u64,
    root: Ino,
    tx: Mutex<Sender<Msg>>,
}

enum Msg {
    Batch(Vec<Invalidation>),
    Barrier(Sender<()>),
}

/// The filesystem every [`RefView`] of one tree shares.
pub struct RefFs {
    state: Mutex<State>,
    /// Lock order: `state`, then `syn`.
    syn: Mutex<SynReg>,
    /// Wakes blocked lock waiters; paired with `state`.
    lock_cv: Condvar,
    sinks: Mutex<Vec<Weak<Sink>>>,
    next_view: Mutex<u64>,
    caps: FrontendCaps,
    policies: PolicyStack,
    /// Inodes whose next read is *cold* (the conformance kit's `evict`
    /// hook): it waits for a stand-in backing store (see
    /// [`RefView::cold_read`]).
    cold: Mutex<HashSet<Ino>>,
}

impl RefFs {
    /// An empty filesystem behaving as a frontend with `caps` sees it.
    pub fn new(caps: FrontendCaps) -> Arc<RefFs> {
        let policies = PolicyStack::for_caps(&caps);
        let mut root = Node::new(FileKind::Dir, 0o755, 0, 0, 1_700_000_000_000_000_000);
        root.nlink = 2;
        root.parents.push(ROOT_INO);
        let mut nodes = HashMap::new();
        nodes.insert(ROOT_INO, root);
        Arc::new(RefFs {
            state: Mutex::new(State {
                nodes,
                next_ino: 2,
                next_fh: 1,
                handles: HashMap::new(),
                opens: HashMap::new(),
                locks: HashMap::new(),
                snapshots: Vec::new(),
                clock: 1_700_000_000_000_000_000,
                next_seq: 3,
            }),
            syn: Mutex::new(SynReg::default()),
            lock_cv: Condvar::new(),
            sinks: Mutex::new(Vec::new()),
            next_view: Mutex::new(1),
            caps,
            policies,
            cold: Mutex::new(HashSet::new()),
        })
    }

    pub fn caps(&self) -> &FrontendCaps {
        &self.caps
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A poisoned lock means an op panicked in this file: the state may
        // be half-written, and tests should see that, not paper over it.
        self.state.lock().expect("reference filesystem poisoned")
    }

    /// A view rooted at the directory at `path` (`/`, `/a/b`).
    pub fn view(self: &Arc<Self>, path: &str, confine_links: bool) -> Result<RefView, Code> {
        let parts: Vec<Vec<u8>> = path
            .split('/')
            .filter(|p| !p.is_empty())
            .map(|p| p.as_bytes().to_vec())
            .collect();
        let root = {
            let st = self.lock();
            let ino = State::resolve_path(&st.nodes, &parts).ok_or(Code::NotFound)?;
            if !st.node(ino)?.is_dir() {
                return Err(Code::NotDir);
            }
            ino
        };
        let mut next = self.next_view.lock().unwrap();
        let id = *next;
        *next += 1;
        Ok(RefView {
            fs: self.clone(),
            root,
            confine_links,
            id,
            sink: Mutex::new(None),
        })
    }

    /// Freeze the tree as of now under `name`, as a snapshot of the
    /// directory at `path`.
    pub fn snapshot(&self, path: &str, name: &str) -> Result<(), Code> {
        let parts: Vec<Vec<u8>> = path
            .split('/')
            .filter(|p| !p.is_empty())
            .map(|p| p.as_bytes().to_vec())
            .collect();
        let mut st = self.lock();
        State::resolve_path(&st.nodes, &parts).ok_or(Code::NotFound)?;
        if st.snapshots.iter().any(|s| s.name == name.as_bytes()) {
            return Err(Code::Exists);
        }
        let frozen = Arc::new(st.nodes.clone());
        st.snapshots.push(Snapshot {
            name: name.as_bytes().to_vec(),
            path: parts,
            frozen,
        });
        Ok(())
    }

    /// Deliver `changes` (in real inode numbers) to every view's sink but
    /// `origin`'s, translated to that view's numbering.
    fn publish(&self, origin: u64, changes: Vec<Invalidation>) {
        if changes.is_empty() {
            return;
        }
        let sinks: Vec<Arc<Sink>> = {
            let mut sinks = self.sinks.lock().unwrap();
            sinks.retain(|s| s.strong_count() > 0);
            sinks.iter().filter_map(Weak::upgrade).collect()
        };
        if sinks.iter().all(|s| s.view == origin) {
            return;
        }
        let st = self.lock();
        let mut out: Vec<(Arc<Sink>, Vec<Invalidation>)> = Vec::new();
        for sink in sinks.into_iter().filter(|s| s.view != origin) {
            let batch: Vec<Invalidation> = changes
                .iter()
                .filter_map(|c| translate(&st, sink.root, c))
                .collect();
            if !batch.is_empty() {
                out.push((sink, batch));
            }
        }
        drop(st);
        for (sink, batch) in out {
            let _ = sink.tx.lock().unwrap().send(Msg::Batch(batch));
        }
    }
}

/// `change` (real inode numbers) as a view rooted at `root` numbers it, or
/// `None` when the view cannot see it.
fn translate(st: &State, root: Ino, change: &Invalidation) -> Option<Invalidation> {
    let virt = |ino: Ino| if ino == root { ROOT_INO } else { ino };
    let visible = |ino: Ino| st.dominated(ino, root) || !st.nodes.contains_key(&ino);
    Some(match change {
        Invalidation::Entry { parent, name } => {
            if !visible(*parent) {
                return None;
            }
            Invalidation::Entry {
                parent: virt(*parent),
                name: name.clone(),
            }
        }
        Invalidation::Attr { ino } => {
            if !visible(*ino) {
                return None;
            }
            Invalidation::Attr { ino: virt(*ino) }
        }
        Invalidation::Data { ino, range } => {
            if !visible(*ino) {
                return None;
            }
            Invalidation::Data {
                ino: virt(*ino),
                range: *range,
            }
        }
        Invalidation::Deleted { parent, name, ino } => {
            if !visible(*parent) {
                return None;
            }
            Invalidation::Deleted {
                parent: virt(*parent),
                name: name.clone(),
                ino: virt(*ino),
            }
        }
        other => other.clone(),
    })
}

/// One view of a [`RefFs`]: rooted at a directory, numbering it
/// [`ROOT_INO`], confined to its subtree.
pub struct RefView {
    fs: Arc<RefFs>,
    root: Ino,
    confine_links: bool,
    id: u64,
    sink: Mutex<Option<Arc<Sink>>>,
}

/// The outcome of a lock attempt.
pub enum LockTry {
    Granted,
    Conflict,
}

type Changes = Vec<Invalidation>;

impl RefView {
    pub fn fs(&self) -> &Arc<RefFs> {
        &self.fs
    }

    /// Route this view's incoming invalidations (other views' mutations) to
    /// `events`, from a notifier thread of its own.
    pub fn set_events(&self, events: Arc<dyn FrontendEvents>) {
        let (tx, rx) = mpsc::channel::<Msg>();
        std::thread::Builder::new()
            .name(format!("ref-notifier-{}", self.id))
            .spawn(move || notifier(rx, events))
            .expect("spawn the notifier thread");
        let sink = Arc::new(Sink {
            view: self.id,
            root: self.root,
            tx: Mutex::new(tx),
        });
        self.fs.sinks.lock().unwrap().push(Arc::downgrade(&sink));
        *self.sink.lock().unwrap() = Some(sink);
    }

    /// Wait until everything published to this view's sink so far has been
    /// delivered.
    pub fn settle(&self) {
        let Some(sink) = self.sink.lock().unwrap().clone() else {
            return;
        };
        let (tx, rx) = mpsc::channel();
        if sink.tx.lock().unwrap().send(Msg::Barrier(tx)).is_ok() {
            let _ = rx.recv_timeout(Duration::from_secs(10));
        }
    }

    fn virt(&self, real: Ino) -> Ino {
        if real == self.root {
            ROOT_INO
        } else {
            real
        }
    }

    /// Resolve a frontend inode number in this view.
    fn enter(&self, st: &State, ino: Ino) -> Result<Target, Code> {
        if ino >= SYN_BASE {
            let syn = self.syn_get(ino).ok_or(Code::Stale)?;
            return Ok(Target::Syn(syn));
        }
        let real = if ino == ROOT_INO { self.root } else { ino };
        if self.root != ROOT_INO {
            // A confined view answers `Stale` for anything its root does
            // not dominate, existing or not; an inode it holds open stays
            // addressable (an unlinked-open file has no name to walk).
            let held = st
                .handles
                .values()
                .any(|h| h.view == self.id && h.ino == real);
            if !held && !st.dominated(real, self.root) {
                return Err(Code::Stale);
            }
        }
        if !st.nodes.contains_key(&real) {
            return Err(Code::NotFound);
        }
        Ok(Target::Real(real))
    }

    fn enter_real(&self, st: &State, ino: Ino) -> Result<Ino, Code> {
        match self.enter(st, ino)? {
            Target::Real(real) => Ok(real),
            Target::Syn(_) => Err(Code::ReadOnly),
        }
    }

    fn attr_of(&self, st: &State, real: Ino) -> Attr {
        let n = &st.nodes[&real];
        let type_bits = match n.kind {
            FileKind::File => modebits::S_IFREG,
            FileKind::Dir => modebits::S_IFDIR,
            FileKind::Symlink => modebits::S_IFLNK,
            FileKind::Fifo => modebits::S_IFIFO,
            FileKind::Socket => modebits::S_IFSOCK,
            FileKind::BlockDev => modebits::S_IFBLK,
            FileKind::CharDev => modebits::S_IFCHR,
        };
        let nlink = if n.is_dir() {
            2 + n
                .dir()
                .by_name
                .values()
                .filter(|(c, _)| st.nodes.get(c).is_some_and(Node::is_dir))
                .count() as u32
        } else {
            n.nlink
        };
        Attr {
            ino: self.virt(real),
            kind: n.kind,
            size: if n.kind == FileKind::Symlink {
                n.target.len() as u64
            } else {
                n.size
            },
            blocks: n.size.div_ceil(512),
            mode: type_bits | n.mode,
            nlink,
            uid: n.uid,
            gid: n.gid,
            rdev: n.rdev,
            atime_ns: n.atime,
            mtime_ns: n.mtime,
            ctime_ns: n.ctime,
            blksize: 4096,
            ttl: TTL,
        }
    }

    fn syn_attr(&self, st: &State, syn: &Syn, ino: Ino) -> Attr {
        let (kind, size, mode, nlink, times) = match syn {
            Syn::Mirror { snap, frozen, .. } => {
                let n = &st.snapshots[*snap].frozen[frozen];
                (
                    n.kind,
                    if n.kind == FileKind::Symlink {
                        n.target.len() as u64
                    } else {
                        n.size
                    },
                    n.mode & 0o555,
                    n.nlink.max(1),
                    (n.atime, n.mtime, n.ctime),
                )
            }
            _ => (FileKind::Dir, 0, 0o555, 2, (0, 0, 0)),
        };
        let (atime_ns, mtime_ns, ctime_ns) = times;
        Attr {
            ino,
            kind,
            size,
            blocks: size.div_ceil(512),
            mode: match kind {
                FileKind::Dir => modebits::S_IFDIR,
                FileKind::Symlink => modebits::S_IFLNK,
                _ => modebits::S_IFREG,
            } | mode,
            nlink,
            uid: 0,
            gid: 0,
            rdev: Rdev::default(),
            atime_ns,
            mtime_ns,
            ctime_ns,
            blksize: 4096,
            ttl: TTL,
        }
    }

    fn entry_of(&self, st: &State, real: Ino) -> Entry {
        Entry {
            attr: self.attr_of(st, real),
            generation: 0,
        }
    }

    fn check_name(&self, name: &Name) -> Result<Vec<u8>, Code> {
        let stored = self.fs.policies.names.check(name)?;
        if stored.is_empty() || stored.contains('/') || stored.contains('\0') {
            return Err(Code::Invalid);
        }
        Ok(stored.into_owned().into_bytes())
    }

    /// Run `f` under the state lock, then publish what it changed.
    fn with<T>(&self, f: impl FnOnce(&mut State, &mut Changes) -> VfsResult<T>) -> VfsResult<T> {
        let mut changes = Vec::new();
        let result = {
            let mut st = self.fs.lock();
            f(&mut st, &mut changes)
        };
        self.fs.publish(self.id, changes);
        result
    }

    /// Make `ino`'s next read cold ([`RefFs::cold`]).
    pub fn evict(&self, ino: Ino) -> VfsResult<()> {
        let real = self.read_only(|st| Ok(self.enter_real(st, ino)?))?;
        self.fs.cold.lock().unwrap().insert(real);
        Ok(())
    }

    /// Whether this read of `ino` is cold; asking warms it (a cold read
    /// fills the cache it missed).
    pub fn take_cold(&self, ino: Ino) -> bool {
        let Ok(real) = self.read_only(|st| Ok(self.enter_real(st, ino)?)) else {
            return false;
        };
        self.fs.cold.lock().unwrap().remove(&real)
    }

    /// A cold read: it waits for the backing store (a short sleep here),
    /// on a thread of its own when the frontend allows `read` to defer
    /// ([`FrontendCaps::deferrable`]), else on the calling thread — what the
    /// engine's `View` does with a chunk in no local cache.
    pub fn cold_read<R: Responder<ReadData>>(
        self: &Arc<Self>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u32,
        r: R,
    ) {
        const STORE: Duration = Duration::from_millis(20);
        if !self.fs.caps.deferrable.contains(OpKind::Read) {
            std::thread::sleep(STORE);
            r.done(self.read(ino, fh, off, len));
            return;
        }
        let view = self.clone();
        let spawn = std::thread::Builder::new()
            .name("ref-cold-read".into())
            .spawn(move || {
                std::thread::sleep(STORE);
                r.done(view.read(ino, fh, off, len));
            });
        // A thread that could not start drops `r` with the closure: the
        // responder's own drop fail-safe answers.
        let _ = spawn;
    }

    fn read_only<T>(&self, f: impl FnOnce(&State) -> VfsResult<T>) -> VfsResult<T> {
        let st = self.fs.lock();
        f(&st)
    }

    // ----------------------------------------------------------- namespace

    pub fn lookup(&self, parent: Ino, name: &Name) -> VfsResult<Entry> {
        self.read_only(|st| {
            let target = self.enter(st, parent)?;
            let stored = self
                .fs
                .policies
                .names
                .check(name)?
                .into_owned()
                .into_bytes();
            match target {
                Target::Syn(syn) => self.syn_lookup(st, &syn, &stored),
                Target::Real(dir) => {
                    if stored == b".constellation" {
                        let ino = self.syn_ino(Syn::Meta { dir });
                        return Ok(self.syn_entry(st, ino));
                    }
                    let node = st.node(dir)?;
                    if !node.is_dir() {
                        return Err(VfsError::new(Code::NotDir));
                    }
                    if stored == b"." {
                        return Ok(self.entry_of(st, dir));
                    }
                    if stored == b".." {
                        let up = if dir == self.root {
                            dir
                        } else {
                            node.parents.first().copied().unwrap_or(dir)
                        };
                        return Ok(self.entry_of(st, up));
                    }
                    let (child, _) = *node.dir().by_name.get(&stored).ok_or(Code::NotFound)?;
                    Ok(self.entry_of(st, child))
                }
            }
        })
    }

    pub fn getattr(&self, ino: Ino, _fh: Option<Fh>) -> VfsResult<Attr> {
        self.read_only(|st| match self.enter(st, ino)? {
            Target::Real(real) => Ok(self.attr_of(st, real)),
            Target::Syn(syn) => Ok(self.syn_attr(st, &syn, ino)),
        })
    }

    pub fn setattr(&self, ino: Ino, _fh: Option<Fh>, set: &SetAttr) -> VfsResult<Attr> {
        self.with(|st, changes| {
            let real = self.enter_real(st, ino)?;
            let now = st.tick();
            let size_change = set.size.is_some();
            {
                let node = st.node_mut(real)?;
                if let Some(size) = set.size {
                    match node.kind {
                        FileKind::Dir => return Err(Code::IsDir.into()),
                        FileKind::File => {}
                        _ => return Err(Code::Invalid.into()),
                    }
                    node.set_size(size);
                    node.mtime = now;
                }
                if let Some(mode) = set.mode {
                    node.mode = mode & 0o7777;
                }
                if let Some(uid) = set.uid {
                    node.uid = uid;
                }
                if let Some(gid) = set.gid {
                    node.gid = gid;
                }
                let time = |t: TimeSet| match t {
                    TimeSet::Now => now,
                    TimeSet::At(ns) => ns,
                };
                if let Some(t) = set.atime {
                    node.atime = time(t);
                }
                if let Some(t) = set.mtime {
                    node.mtime = time(t);
                }
                node.ctime = now;
            }
            changes.push(if size_change {
                Invalidation::Data {
                    ino: real,
                    range: None,
                }
            } else {
                Invalidation::Attr { ino: real }
            });
            Ok(self.attr_of(st, real))
        })
    }

    pub fn readlink(&self, ino: Ino) -> VfsResult<Vec<u8>> {
        self.read_only(|st| match self.enter(st, ino)? {
            Target::Real(real) => {
                let n = st.node(real)?;
                if n.kind == FileKind::Symlink {
                    Ok(n.target.clone())
                } else {
                    Err(Code::Invalid.into())
                }
            }
            Target::Syn(Syn::Mirror { snap, frozen, .. }) => {
                let n = &st.snapshots[snap].frozen[&frozen];
                if n.kind == FileKind::Symlink {
                    Ok(n.target.clone())
                } else {
                    Err(Code::Invalid.into())
                }
            }
            Target::Syn(_) => Err(Code::Invalid.into()),
        })
    }

    /// Add a new node `name` in `parent`, owned by `caller`.
    fn add_node(
        &self,
        st: &mut State,
        changes: &mut Changes,
        parent: Ino,
        name: &Name,
        mut node: Node,
        caller: &Caller,
    ) -> VfsResult<Ino> {
        let stored = self.check_name(name)?;
        let dir = self.enter_real(st, parent)?;
        if !st.node(dir)?.is_dir() {
            return Err(Code::NotDir.into());
        }
        if st.node(dir)?.dir().by_name.contains_key(&stored) {
            return Err(Code::Exists.into());
        }
        let (uid, gid) = self.fs.policies.identity.owner(caller);
        node.uid = uid;
        node.gid = gid;
        node.parents = vec![dir];
        node.name = stored.clone();
        let ino = st.next_ino;
        st.next_ino += 1;
        let now = st.tick();
        st.nodes.insert(ino, node);
        Self::attach(st, dir, &stored, ino, now);
        changes.push(Invalidation::Entry {
            parent: dir,
            name: NameBuf::new(stored),
        });
        Ok(ino)
    }

    fn attach(st: &mut State, dir: Ino, name: &[u8], child: Ino, now: i64) {
        let seq = st.next_seq;
        st.next_seq += 1;
        let d = st.nodes.get_mut(&dir).expect("parent exists");
        d.dir_mut().by_name.insert(name.to_vec(), (child, seq));
        d.dir_mut().by_seq.insert(seq, name.to_vec());
        d.mtime = now;
        d.ctime = now;
    }

    fn detach(st: &mut State, dir: Ino, name: &[u8], now: i64) -> Option<Ino> {
        let d = st.nodes.get_mut(&dir)?;
        let (child, seq) = d.dir_mut().by_name.remove(name)?;
        d.dir_mut().by_seq.remove(&seq);
        d.mtime = now;
        d.ctime = now;
        Some(child)
    }

    pub fn mknod(
        &self,
        caller: &Caller,
        parent: Ino,
        name: &Name,
        mode: u32,
        rdev: Rdev,
    ) -> VfsResult<Entry> {
        self.with(|st, changes| {
            let kind = match mode & modebits::S_IFMT {
                0 | modebits::S_IFREG => FileKind::File,
                modebits::S_IFIFO => FileKind::Fifo,
                modebits::S_IFSOCK => FileKind::Socket,
                modebits::S_IFCHR => FileKind::CharDev,
                modebits::S_IFBLK => FileKind::BlockDev,
                _ => return Err(Code::Invalid.into()),
            };
            if kind != FileKind::File && !self.fs.caps.special_files {
                return Err(Code::NotSupported.into());
            }
            let now = st.tick();
            let mut node = Node::new(kind, mode, 0, 0, now);
            node.rdev = rdev;
            let ino = self.add_node(st, changes, parent, name, node, caller)?;
            Ok(self.entry_of(st, ino))
        })
    }

    pub fn mkdir(&self, caller: &Caller, parent: Ino, name: &Name, mode: u32) -> VfsResult<Entry> {
        self.with(|st, changes| {
            let now = st.tick();
            let node = Node::new(FileKind::Dir, mode, 0, 0, now);
            let ino = self.add_node(st, changes, parent, name, node, caller)?;
            Ok(self.entry_of(st, ino))
        })
    }

    pub fn symlink(
        &self,
        caller: &Caller,
        parent: Ino,
        name: &Name,
        target: &[u8],
    ) -> VfsResult<Entry> {
        self.with(|st, changes| {
            if target.is_empty() {
                return Err(Code::NotFound.into());
            }
            let now = st.tick();
            let mut node = Node::new(FileKind::Symlink, 0o777, 0, 0, now);
            node.target = target.to_vec();
            let ino = self.add_node(st, changes, parent, name, node, caller)?;
            Ok(self.entry_of(st, ino))
        })
    }

    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &Name) -> VfsResult<Entry> {
        self.with(|st, changes| {
            if !self.fs.caps.hard_links {
                return Err(Code::NotSupported.into());
            }
            let stored = self.check_name(new_name)?;
            let src = self.enter_real(st, ino)?;
            let dir = self.enter_real(st, new_parent)?;
            if !st.node(dir)?.is_dir() {
                return Err(Code::NotDir.into());
            }
            if st.node(src)?.is_dir() {
                return Err(Code::Perm.into());
            }
            if self.confine_links {
                self.link_within_domain(st, src, dir)?;
            }
            if st.node(dir)?.dir().by_name.contains_key(&stored) {
                return Err(Code::Exists.into());
            }
            let now = st.tick();
            Self::attach(st, dir, &stored, src, now);
            let n = st.node_mut(src)?;
            n.nlink += 1;
            n.parents.push(dir);
            n.ctime = now;
            changes.push(Invalidation::Entry {
                parent: dir,
                name: NameBuf::new(stored),
            });
            changes.push(Invalidation::Attr { ino: src });
            Ok(self.entry_of(st, src))
        })
    }

    /// `confine_links`: may `src` get a name in `dir`? Only when one of its
    /// names is in the same link domain.
    fn link_within_domain(&self, st: &State, src: Ino, dir: Ino) -> Result<(), Code> {
        let domain = st.link_domain(dir, self.root).ok_or(Code::CrossDevice)?;
        for &parent in &st.node(src)?.parents {
            if st.link_domain(parent, self.root) == Some(domain) {
                return Ok(());
            }
        }
        Err(Code::CrossDevice)
    }

    /// Drop one name of `ino`; reap it when nothing names or holds it.
    fn drop_name(st: &mut State, ino: Ino, parent: Ino, now: i64) {
        let open = st.opens.get(&ino).copied().unwrap_or(0) > 0;
        let node = st.nodes.get_mut(&ino).expect("linked node exists");
        node.nlink = node.nlink.saturating_sub(1);
        node.ctime = now;
        if let Some(at) = node.parents.iter().position(|p| *p == parent) {
            node.parents.remove(at);
        }
        if node.nlink == 0 && !open {
            st.nodes.remove(&ino);
            st.locks.remove(&ino);
        }
    }

    pub fn unlink(&self, parent: Ino, name: &Name) -> VfsResult<()> {
        self.with(|st, changes| {
            let stored = self.check_name(name)?;
            let dir = self.enter_real(st, parent)?;
            let d = st.node(dir)?;
            if !d.is_dir() {
                return Err(Code::NotDir.into());
            }
            let (child, _) = *d.dir().by_name.get(&stored).ok_or(Code::NotFound)?;
            if st.node(child)?.is_dir() {
                return Err(Code::IsDir.into());
            }
            let now = st.tick();
            Self::detach(st, dir, &stored, now);
            Self::drop_name(st, child, dir, now);
            changes.push(Invalidation::Entry {
                parent: dir,
                name: NameBuf::new(stored),
            });
            changes.push(Invalidation::Attr { ino: child });
            Ok(())
        })
    }

    pub fn rmdir(&self, parent: Ino, name: &Name) -> VfsResult<()> {
        self.with(|st, changes| {
            let stored = self.check_name(name)?;
            let dir = self.enter_real(st, parent)?;
            let d = st.node(dir)?;
            if !d.is_dir() {
                return Err(Code::NotDir.into());
            }
            if stored == b"." || stored == b".." {
                return Err(Code::Invalid.into());
            }
            let (child, _) = *d.dir().by_name.get(&stored).ok_or(Code::NotFound)?;
            let c = st.node(child)?;
            if !c.is_dir() {
                return Err(Code::NotDir.into());
            }
            if !c.dir().by_name.is_empty() {
                return Err(Code::NotEmpty.into());
            }
            let now = st.tick();
            Self::detach(st, dir, &stored, now);
            st.nodes.remove(&child);
            changes.push(Invalidation::Entry {
                parent: dir,
                name: NameBuf::new(stored),
            });
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rename(
        &self,
        parent: Ino,
        name: &Name,
        new_parent: Ino,
        new_name: &Name,
        flags: RenameFlags,
    ) -> VfsResult<()> {
        self.with(|st, changes| {
            let old = self.check_name(name)?;
            let new = self.check_name(new_name)?;
            let sdir = self.enter_real(st, parent)?;
            let ddir = self.enter_real(st, new_parent)?;
            // renameat2(2): `EINVAL` for a flag the filesystem does not
            // support (never `ENOSYS`/`EOPNOTSUPP`).
            if flags.contains(RenameFlags::UNSUPPORTED) || flags.contains(RenameFlags::WHITEOUT) {
                return Err(Code::Invalid.into());
            }
            let noreplace = flags.contains(RenameFlags::NOREPLACE);
            let exchange = flags.contains(RenameFlags::EXCHANGE);
            if noreplace && exchange {
                return Err(Code::Invalid.into());
            }
            if !st.node(sdir)?.is_dir() || !st.node(ddir)?.is_dir() {
                return Err(Code::NotDir.into());
            }
            let (src, _) = *st
                .node(sdir)?
                .dir()
                .by_name
                .get(&old)
                .ok_or(Code::NotFound)?;
            let dst = st.node(ddir)?.dir().by_name.get(&new).map(|(c, _)| *c);
            if sdir == ddir && old == new {
                return Ok(());
            }
            if exchange && dst.is_none() {
                return Err(Code::NotFound.into());
            }
            if noreplace && dst.is_some() {
                return Err(Code::Exists.into());
            }
            let src_dir = st.node(src)?.is_dir();
            let dst_dir = match dst {
                Some(d) => st.node(d)?.is_dir(),
                None => false,
            };
            // A directory cannot move beneath itself.
            let beneath = |st: &State, dir: Ino, of: Ino| {
                let mut cur = dir;
                for _ in 0..WALK_LIMIT {
                    if cur == of {
                        return true;
                    }
                    if cur == ROOT_INO {
                        return false;
                    }
                    match st.nodes.get(&cur).and_then(|n| n.parents.first()) {
                        Some(p) => cur = *p,
                        None => return false,
                    }
                }
                true
            };
            if src_dir && beneath(st, ddir, src) {
                return Err(Code::Invalid.into());
            }
            if exchange && dst_dir && beneath(st, sdir, dst.expect("exchange has a target")) {
                return Err(Code::Invalid.into());
            }
            if self.confine_links {
                let crossing = st.link_domain(sdir, self.root) != st.link_domain(ddir, self.root);
                let multi = |ino: Ino| {
                    st.nodes
                        .get(&ino)
                        .is_some_and(|n| !n.is_dir() && n.nlink > 1)
                };
                if crossing && (multi(src) || (exchange && dst.is_some_and(multi))) {
                    return Err(Code::CrossDevice.into());
                }
            }
            if Some(src) == dst {
                // Two names of one inode: POSIX makes this a no-op.
                return Ok(());
            }
            let now = st.tick();
            if exchange {
                let dst = dst.expect("checked above");
                Self::detach(st, sdir, &old, now);
                Self::detach(st, ddir, &new, now);
                Self::attach(st, sdir, &old, dst, now);
                Self::attach(st, ddir, &new, src, now);
                Self::reparent(st, src, sdir, ddir, &new);
                Self::reparent(st, dst, ddir, sdir, &old);
            } else {
                if let Some(dst) = dst {
                    if src_dir && !dst_dir {
                        return Err(Code::NotDir.into());
                    }
                    if !src_dir && dst_dir {
                        return Err(Code::IsDir.into());
                    }
                    if dst_dir && !st.node(dst)?.dir().by_name.is_empty() {
                        return Err(Code::NotEmpty.into());
                    }
                    Self::detach(st, ddir, &new, now);
                    if dst_dir {
                        st.nodes.remove(&dst);
                    } else {
                        Self::drop_name(st, dst, ddir, now);
                    }
                }
                Self::detach(st, sdir, &old, now);
                Self::attach(st, ddir, &new, src, now);
                Self::reparent(st, src, sdir, ddir, &new);
            }
            changes.push(Invalidation::Entry {
                parent: sdir,
                name: NameBuf::new(old),
            });
            changes.push(Invalidation::Entry {
                parent: ddir,
                name: NameBuf::new(new),
            });
            Ok(())
        })
    }

    /// `ino` moved from `from` to `to` under `name`: fix its parent list
    /// (and a directory's own name).
    fn reparent(st: &mut State, ino: Ino, from: Ino, to: Ino, name: &[u8]) {
        if let Some(n) = st.nodes.get_mut(&ino) {
            if let Some(at) = n.parents.iter().position(|p| *p == from) {
                n.parents[at] = to;
            }
            if n.is_dir() {
                n.name = name.to_vec();
            }
        }
    }

    // ------------------------------------------------------------- file io

    fn alloc_handle(&self, st: &mut State, ino: Ino) -> Fh {
        let fh = st.next_fh;
        st.next_fh += 1;
        st.handles.insert(fh, Handle { ino, view: self.id });
        if ino < SYN_BASE {
            *st.opens.entry(ino).or_insert(0) += 1;
        }
        Fh(fh)
    }

    /// `fh` must be a live handle on `ino`.
    fn handle_ino(st: &State, fh: Fh, ino: Ino) -> Result<(), Code> {
        match st.handles.get(&fh.0) {
            Some(h) if h.ino == ino => Ok(()),
            _ => Err(Code::BadFd),
        }
    }

    pub fn open(&self, ino: Ino, flags: OpenFlags) -> VfsResult<Opened> {
        self.with(|st, changes| match self.enter(st, ino)? {
            Target::Syn(syn) => {
                let is_dir = !matches!(&syn, Syn::Mirror { snap, frozen, .. }
                    if !st.snapshots[*snap].frozen[frozen].is_dir());
                if is_dir {
                    return Err(Code::IsDir.into());
                }
                if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC) {
                    return Err(Code::ReadOnly.into());
                }
                // A frozen file is read through a handle of its own: the
                // synthetic inode has no live node to count opens on.
                Ok(Opened::new(self.alloc_handle(st, ino)))
            }
            Target::Real(real) => {
                if st.node(real)?.is_dir() {
                    return Err(Code::IsDir.into());
                }
                if flags.contains(OpenFlags::TRUNC) && flags.contains(OpenFlags::WRITE) {
                    self.truncate_open(st, changes, real);
                }
                Ok(Opened::new(self.alloc_handle(st, real)))
            }
        })
    }

    fn truncate_open(&self, st: &mut State, changes: &mut Changes, real: Ino) {
        let now = st.tick();
        if let Some(n) = st.nodes.get_mut(&real) {
            if n.kind == FileKind::File {
                n.set_size(0);
                n.mtime = now;
                n.ctime = now;
                changes.push(Invalidation::Data {
                    ino: real,
                    range: None,
                });
            }
        }
    }

    pub fn create(
        &self,
        caller: &Caller,
        parent: Ino,
        name: &Name,
        mode: u32,
        flags: OpenFlags,
    ) -> VfsResult<(Entry, Opened)> {
        self.with(|st, changes| {
            let stored = self.check_name(name)?;
            let dir = self.enter_real(st, parent)?;
            let d = st.node(dir)?;
            if !d.is_dir() {
                return Err(Code::NotDir.into());
            }
            let existing = d.dir().by_name.get(&stored).map(|(c, _)| *c);
            let ino = match existing {
                Some(child) => {
                    if flags.contains(OpenFlags::EXCL) {
                        return Err(Code::Exists.into());
                    }
                    if st.node(child)?.is_dir() {
                        return Err(Code::IsDir.into());
                    }
                    if flags.contains(OpenFlags::TRUNC) && flags.contains(OpenFlags::WRITE) {
                        self.truncate_open(st, changes, child);
                    }
                    child
                }
                None => {
                    let now = st.tick();
                    let node = Node::new(FileKind::File, mode, 0, 0, now);
                    self.add_node(st, changes, parent, name, node, caller)?
                }
            };
            let fh = self.alloc_handle(st, ino);
            Ok((self.entry_of(st, ino), Opened::new(fh)))
        })
    }

    pub fn read(&self, ino: Ino, fh: Fh, off: u64, len: u32) -> VfsResult<ReadData> {
        self.read_only(|st| match self.enter(st, ino)? {
            Target::Syn(Syn::Mirror { snap, frozen, .. }) => {
                st.handles.get(&fh.0).ok_or(Code::BadFd)?;
                Ok(ReadData::from_vec(
                    st.snapshots[snap].frozen[&frozen].read_at(off, len),
                ))
            }
            Target::Syn(_) => Err(Code::IsDir.into()),
            Target::Real(real) => {
                Self::handle_ino(st, fh, real)?;
                let n = st.node(real)?;
                match n.kind {
                    FileKind::Dir => Err(Code::IsDir.into()),
                    _ => Ok(ReadData::from_vec(n.read_at(off, len))),
                }
            }
        })
    }

    pub fn write(&self, ino: Ino, fh: Fh, off: u64, data: &[u8]) -> VfsResult<u32> {
        self.with(|st, changes| {
            let real = self.enter_real(st, ino)?;
            Self::handle_ino(st, fh, real)?;
            if off
                .checked_add(data.len() as u64)
                .is_none_or(|e| e > i64::MAX as u64)
            {
                return Err(Code::FileTooBig.into());
            }
            let now = st.tick();
            let n = st.node_mut(real)?;
            if n.kind == FileKind::Dir {
                return Err(Code::IsDir.into());
            }
            n.write_at(off, data);
            n.mtime = now;
            n.ctime = now;
            changes.push(Invalidation::Data {
                ino: real,
                range: Some((off, data.len() as u64)),
            });
            Ok(data.len() as u32)
        })
    }

    pub fn flush(&self, ino: Ino, fh: Fh, owner: LockOwner) -> VfsResult<()> {
        let result = self.with(|st, _| {
            let real = self.enter_real(st, ino)?;
            Self::handle_ino(st, fh, real)?;
            Self::drop_owner_locks(st, real, owner);
            Ok(())
        });
        self.fs.lock_cv.notify_all();
        result
    }

    pub fn release(&self, ino: Ino, fh: Fh, owner: Option<LockOwner>) -> VfsResult<()> {
        let result = self.with(|st, _| {
            let h = st.handles.remove(&fh.0).ok_or(Code::BadFd)?;
            if h.ino >= SYN_BASE {
                return Ok(());
            }
            if let Some(owner) = owner {
                Self::drop_owner_locks(st, h.ino, owner);
            }
            let _ = ino;
            if let Some(open) = st.opens.get_mut(&h.ino) {
                *open -= 1;
                if *open == 0 {
                    st.opens.remove(&h.ino);
                    if st.nodes.get(&h.ino).is_some_and(|n| n.nlink == 0) {
                        st.nodes.remove(&h.ino);
                        st.locks.remove(&h.ino);
                    }
                }
            }
            Ok(())
        });
        self.fs.lock_cv.notify_all();
        result
    }

    pub fn fsync(&self, ino: Ino, fh: Fh) -> VfsResult<()> {
        self.read_only(|st| {
            let real = self.enter_real(st, ino)?;
            Self::handle_ino(st, fh, real)?;
            Ok(())
        })
    }

    pub fn readdir(&self, ino: Ino, cookie: u64, sink: &mut dyn DirSink) -> VfsResult<()> {
        let st = self.fs.lock();
        let target = self.enter(&st, ino)?;
        // Collect (ino, next, kind, name) first: the sink is the
        // frontend's, and nothing of it runs under our lock's borrows.
        let mut entries: Vec<(Ino, u64, FileKind, Vec<u8>)> = Vec::new();
        match target {
            Target::Real(dir) => {
                let node = st.node(dir)?;
                if !node.is_dir() {
                    return Err(Code::NotDir.into());
                }
                let up = if dir == self.root {
                    dir
                } else {
                    node.parents.first().copied().unwrap_or(dir)
                };
                entries.push((self.virt(dir), 1, FileKind::Dir, b".".to_vec()));
                entries.push((self.virt(up), 2, FileKind::Dir, b"..".to_vec()));
                for (&seq, name) in node.dir().by_seq.range(cookie.saturating_add(1)..) {
                    let (child, _) = node.dir().by_name[name];
                    entries.push((self.virt(child), seq, st.nodes[&child].kind, name.clone()));
                }
            }
            Target::Syn(syn) => {
                let mut names: Vec<(Vec<u8>, Ino, FileKind)> = Vec::new();
                match &syn {
                    Syn::Meta { dir } => {
                        let ino = self.syn_ino(Syn::Snaps { dir: *dir });
                        names.push((b"snapshot".to_vec(), ino, FileKind::Dir));
                    }
                    Syn::Snaps { dir } => {
                        for (snap, name, frozen) in self.covering(&st, *dir) {
                            let ino = self.syn_ino(Syn::Mirror {
                                snap,
                                frozen,
                                dir: *dir,
                            });
                            names.push((name, ino, FileKind::Dir));
                        }
                    }
                    Syn::Mirror { snap, frozen, dir } => {
                        let f = &st.snapshots[*snap].frozen;
                        let node = &f[frozen];
                        if !node.is_dir() {
                            return Err(Code::NotDir.into());
                        }
                        for (name, (child, _)) in &node.dir().by_name {
                            let ino = self.syn_ino(Syn::Mirror {
                                snap: *snap,
                                frozen: *child,
                                dir: *dir,
                            });
                            names.push((name.clone(), ino, f[child].kind));
                        }
                    }
                }
                entries.push((ino, 1, FileKind::Dir, b".".to_vec()));
                entries.push((ino, 2, FileKind::Dir, b"..".to_vec()));
                for (i, (name, child, kind)) in names.into_iter().enumerate() {
                    entries.push((child, 3 + i as u64, kind, name));
                }
                entries.retain(|e| e.1 > cookie);
                drop(st);
                return Self::feed(entries, cookie, sink);
            }
        }
        entries.retain(|e| e.1 > cookie);
        drop(st);
        Self::feed(entries, cookie, sink)
    }

    fn feed(
        entries: Vec<(Ino, u64, FileKind, Vec<u8>)>,
        _cookie: u64,
        sink: &mut dyn DirSink,
    ) -> VfsResult<()> {
        for (ino, next, kind, name) in entries {
            if sink.add(ino, next, kind, &name) {
                break;
            }
        }
        Ok(())
    }

    pub fn statfs(&self, ino: Ino) -> VfsResult<StatFs> {
        self.read_only(|st| {
            self.enter(st, ino)?;
            let used: u64 = st.nodes.values().map(|n| n.size.div_ceil(4096)).sum();
            let blocks = 1 << 20;
            Ok(StatFs {
                blocks,
                bfree: blocks.saturating_sub(used),
                bavail: blocks.saturating_sub(used),
                files: st.nodes.len() as u64,
                ffree: 1 << 20,
                bsize: 4096,
                namelen: crate::policy::NAME_MAX as u32,
                frsize: 4096,
            })
        })
    }

    pub fn fallocate(
        &self,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
    ) -> VfsResult<()> {
        self.with(|st, changes| {
            if !self.fs.caps.fallocate {
                return Err(Code::NotSupported.into());
            }
            let real = self.enter_real(st, ino)?;
            Self::handle_ino(st, fh, real)?;
            if mode.contains(FallocateMode::UNSUPPORTED) {
                return Err(Code::NotSupported.into());
            }
            if len == 0 {
                return Err(Code::Invalid.into());
            }
            let end = off.checked_add(len).ok_or(Code::FileTooBig)?;
            let punch = mode.contains(FallocateMode::PUNCH_HOLE);
            let keep = mode.contains(FallocateMode::KEEP_SIZE);
            if punch && !keep {
                return Err(Code::NotSupported.into());
            }
            let now = st.tick();
            let n = st.node_mut(real)?;
            match n.kind {
                FileKind::File => {}
                FileKind::Dir => return Err(Code::IsDir.into()),
                _ => return Err(Code::NoDevice.into()),
            }
            if punch {
                n.punch(off, end);
            } else if mode.contains(FallocateMode::ZERO_RANGE) {
                n.punch(off, end);
                if !keep && end > n.size {
                    n.size = end;
                }
            } else if !keep && end > n.size {
                n.size = end;
            }
            n.mtime = now;
            n.ctime = now;
            changes.push(Invalidation::Data {
                ino: real,
                range: Some((off, len)),
            });
            Ok(())
        })
    }

    pub fn seek(&self, ino: Ino, fh: Fh, off: u64, whence: SeekWhence) -> VfsResult<u64> {
        self.read_only(|st| {
            if !self.fs.caps.seek_hole {
                return Err(Code::NotSupported.into());
            }
            let real = self.enter_real(st, ino)?;
            Self::handle_ino(st, fh, real)?;
            let n = st.node(real)?;
            match whence {
                SeekWhence::Set | SeekWhence::Cur | SeekWhence::End => {
                    return Err(Code::Invalid.into())
                }
                SeekWhence::Data | SeekWhence::Hole => {}
            }
            if off >= n.size {
                return Err(Code::NoDeviceOrAddress.into());
            }
            if whence == SeekWhence::Data {
                n.extents
                    .iter()
                    .find(|&&(_, e)| e > off)
                    .map(|&(s, _)| s.max(off))
                    .filter(|p| *p < n.size)
                    .ok_or_else(|| Code::NoDeviceOrAddress.into())
            } else {
                Ok(
                    match n.extents.iter().find(|&&(s, e)| s <= off && off < e) {
                        Some(&(_, e)) => e.min(n.size),
                        None => off,
                    },
                )
            }
        })
    }

    // -------------------------------------------------------------- xattrs

    fn xattr_name(&self, name: &XattrName, caller: &Caller) -> Result<String, Code> {
        if self.fs.caps.xattrs == XattrSupport::None {
            return Err(Code::NotSupported);
        }
        self.fs.policies.xattrs.check_name(name, caller)
    }

    pub fn getxattr(&self, caller: &Caller, ino: Ino, name: &XattrName) -> VfsResult<Vec<u8>> {
        self.read_only(|st| {
            let name = self.xattr_name(name, caller)?;
            let target = self.enter(st, ino)?;
            let Target::Real(real) = target else {
                return Err(Code::NoData.into());
            };
            if self.fs.policies.xattrs.is_virtual(&name) {
                let (size, count) = st.rsize_rcount(real);
                let v = if name == RSIZE_XATTR { size } else { count };
                return Ok(v.to_string().into_bytes());
            }
            st.node(real)?
                .xattrs
                .get(&name)
                .cloned()
                .ok_or_else(|| Code::NoData.into())
        })
    }

    pub fn setxattr(
        &self,
        caller: &Caller,
        ino: Ino,
        name: &XattrName,
        value: &[u8],
        flags: SetXattrFlags,
    ) -> VfsResult<()> {
        self.with(|st, changes| {
            let name = self.xattr_name(name, caller)?;
            let real = self.enter_real(st, ino)?;
            if self.fs.policies.xattrs.is_virtual(&name) {
                return Err(Code::Perm.into());
            }
            let mode = flags.mode()?;
            if value.len() > XATTR_SIZE_MAX {
                return Err(Code::TooBig.into());
            }
            let now = st.tick();
            let n = st.node_mut(real)?;
            let exists = n.xattrs.contains_key(&name);
            match mode {
                SetXattrMode::Create if exists => return Err(Code::Exists.into()),
                SetXattrMode::Replace if !exists => return Err(Code::NoData.into()),
                _ => {}
            }
            n.xattrs.insert(name, value.to_vec());
            n.ctime = now;
            changes.push(Invalidation::Attr { ino: real });
            Ok(())
        })
    }

    pub fn listxattr(&self, caller: &Caller, ino: Ino) -> VfsResult<Vec<XattrNameBuf>> {
        self.read_only(|st| {
            if self.fs.caps.xattrs == XattrSupport::None {
                return Err(Code::NotSupported.into());
            }
            let stored = match self.enter(st, ino)? {
                Target::Real(real) => st
                    .node(real)?
                    .xattrs
                    .keys()
                    // `trusted.*` is for uid 0's eyes only.
                    .filter(|k| caller.uid == 0 || !k.starts_with("trusted."))
                    .cloned()
                    .collect(),
                Target::Syn(_) => Vec::new(),
            };
            Ok(self.fs.policies.xattrs.listing(stored))
        })
    }

    pub fn removexattr(&self, caller: &Caller, ino: Ino, name: &XattrName) -> VfsResult<()> {
        self.with(|st, changes| {
            let name = self.xattr_name(name, caller)?;
            let real = self.enter_real(st, ino)?;
            if self.fs.policies.xattrs.is_virtual(&name) {
                return Err(Code::Perm.into());
            }
            let now = st.tick();
            let n = st.node_mut(real)?;
            n.xattrs.remove(&name).ok_or(Code::NoData)?;
            n.ctime = now;
            changes.push(Invalidation::Attr { ino: real });
            Ok(())
        })
    }

    // --------------------------------------------------------------- locks

    fn lock_target(&self, ino: Ino) -> VfsResult<Ino> {
        if !self.fs.caps.cluster_locks {
            return Err(Code::NotImplemented.into());
        }
        let st = self.fs.lock();
        Ok(self.enter_real(&st, ino)?)
    }

    pub fn lock_test(&self, ino: Ino, lock: LockSpec) -> VfsResult<LockStatus> {
        let real = self.lock_target(ino)?;
        let st = self.fs.lock();
        Ok(
            conflict(&st, real, &lock).map_or(LockStatus::Unlocked, |c| LockStatus::Locked {
                range: c.range,
                kind: c.kind,
                pid: c.pid,
            }),
        )
    }

    /// Try to take `lock` now.
    pub fn lock_try(&self, ino: Ino, lock: LockSpec) -> VfsResult<LockTry> {
        let real = self.lock_target(ino)?;
        let mut st = self.fs.lock();
        if conflict(&st, real, &lock).is_some() {
            return Ok(LockTry::Conflict);
        }
        apply_lock(st.locks.entry(real).or_default(), &lock);
        Ok(LockTry::Granted)
    }

    /// Wait for `lock` and complete `r`: granted, `Intr` when `cancel` is
    /// set, `TimedOut` at `deadline`. On a thread of its own when the
    /// frontend allows `lock_acquire` to defer
    /// ([`FrontendCaps::deferrable`]); otherwise the calling thread is
    /// parked here until the answer, as the contract says of every op a
    /// frontend cannot defer.
    pub fn lock_wait<R: Responder<()>>(
        self: &Arc<Self>,
        ino: Ino,
        lock: LockSpec,
        cancel: Option<CancelToken>,
        deadline: Option<Instant>,
        r: R,
    ) {
        if !self.fs.caps.deferrable.contains(OpKind::LockAcquire) {
            self.wait_for_lock(ino, lock, cancel, deadline, r);
            return;
        }
        let view = self.clone();
        let spawn = std::thread::Builder::new()
            .name("ref-lock-wait".into())
            .spawn(move || view.wait_for_lock(ino, lock, cancel, deadline, r));
        // A thread that could not start drops `r` with the closure: the
        // responder's own drop fail-safe answers.
        let _ = spawn;
    }

    fn wait_for_lock<R: Responder<()>>(
        &self,
        ino: Ino,
        lock: LockSpec,
        cancel: Option<CancelToken>,
        deadline: Option<Instant>,
        r: R,
    ) {
        let real = match self.lock_target(ino) {
            Ok(real) => real,
            Err(e) => return r.done(Err(e)),
        };
        let mut st = self.fs.lock();
        let result = loop {
            if conflict(&st, real, &lock).is_none() {
                apply_lock(st.locks.entry(real).or_default(), &lock);
                break Ok(());
            }
            if cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
                break Err(VfsError::new(Code::Intr));
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                break Err(VfsError::new(Code::TimedOut));
            }
            // Bounded, so a cancellation is noticed without a wake-up (a
            // token is a flag, not a condvar).
            st = self
                .fs
                .lock_cv
                .wait_timeout(st, Duration::from_millis(5))
                .expect("reference filesystem poisoned")
                .0;
        };
        drop(st);
        r.done(result);
    }

    pub fn lock_release(&self, ino: Ino, owner: LockOwner, range: LockRange) -> VfsResult<()> {
        let real = self.lock_target(ino)?;
        {
            let mut st = self.fs.lock();
            if let Some(list) = st.locks.get_mut(&real) {
                remove_range(list, owner, range);
            }
        }
        self.fs.lock_cv.notify_all();
        Ok(())
    }

    fn drop_owner_locks(st: &mut State, ino: Ino, owner: LockOwner) {
        if let Some(list) = st.locks.get_mut(&ino) {
            list.retain(|l| l.owner != owner);
        }
    }

    pub fn sync_view(&self) -> VfsResult<()> {
        Ok(())
    }

    // ------------------------------------------------------------ synthetic

    fn syn_ino(&self, syn: Syn) -> Ino {
        let mut reg = self.fs.syn.lock().unwrap();
        if let Some(&ino) = reg.ids.get(&syn) {
            return ino;
        }
        let ino = SYN_BASE + reg.list.len() as Ino;
        reg.list.push(syn.clone());
        reg.ids.insert(syn, ino);
        ino
    }

    fn syn_get(&self, ino: Ino) -> Option<Syn> {
        self.fs
            .syn
            .lock()
            .unwrap()
            .list
            .get((ino - SYN_BASE) as usize)
            .cloned()
    }

    fn syn_entry(&self, st: &State, ino: Ino) -> Entry {
        let syn = self.syn_get(ino).expect("interned");
        Entry {
            attr: self.syn_attr(st, &syn, ino),
            generation: 0,
        }
    }

    /// The snapshots covering directory `dir`'s own path, with the frozen
    /// inode mirrored there: `(snapshot index, name, frozen inode)`.
    fn covering(&self, st: &State, dir: Ino) -> Vec<(usize, Vec<u8>, Ino)> {
        let path = st.path_of(dir);
        let mut out = Vec::new();
        for (i, snap) in st.snapshots.iter().enumerate() {
            if !path.starts_with(&snap.path) {
                continue;
            }
            if let Some(frozen) = State::resolve_path(&snap.frozen, &path) {
                if snap.frozen[&frozen].is_dir() {
                    out.push((i, snap.name.clone(), frozen));
                }
            }
        }
        out.sort_by(|a, b| a.1.cmp(&b.1));
        out
    }

    fn syn_lookup(&self, st: &State, syn: &Syn, name: &[u8]) -> VfsResult<Entry> {
        let child = match syn {
            Syn::Meta { dir } => match name {
                b"snapshot" => Syn::Snaps { dir: *dir },
                b"." => syn.clone(),
                _ => return Err(Code::NotFound.into()),
            },
            Syn::Snaps { dir } => {
                if name == b"." {
                    syn.clone()
                } else if name == b".." {
                    Syn::Meta { dir: *dir }
                } else {
                    let (snap, _, frozen) = self
                        .covering(st, *dir)
                        .into_iter()
                        .find(|(_, n, _)| n == name)
                        .ok_or(Code::NotFound)?;
                    Syn::Mirror {
                        snap,
                        frozen,
                        dir: *dir,
                    }
                }
            }
            Syn::Mirror { snap, frozen, dir } => {
                let f = &st.snapshots[*snap].frozen;
                let node = &f[frozen];
                if !node.is_dir() {
                    return Err(Code::NotDir.into());
                }
                if name == b"." {
                    syn.clone()
                } else if name == b".." {
                    // A mirror never reaches above its own root: `..` of
                    // the root is the snapshot list it hangs from.
                    let top = self
                        .covering(st, *dir)
                        .into_iter()
                        .find(|(i, _, _)| i == snap)
                        .map(|(_, _, top)| top);
                    match node.parents.first() {
                        Some(&parent) if top != Some(*frozen) && f.contains_key(&parent) => {
                            Syn::Mirror {
                                snap: *snap,
                                frozen: parent,
                                dir: *dir,
                            }
                        }
                        _ => Syn::Snaps { dir: *dir },
                    }
                } else {
                    let (child, _) = *node.dir().by_name.get(name).ok_or(Code::NotFound)?;
                    Syn::Mirror {
                        snap: *snap,
                        frozen: child,
                        dir: *dir,
                    }
                }
            }
        };
        let ino = self.syn_ino(child);
        Ok(self.syn_entry(st, ino))
    }
}

fn overlaps(a: LockRange, b: LockRange) -> bool {
    a.start <= b.end && b.start <= a.end
}

fn conflict(st: &State, ino: Ino, lock: &LockSpec) -> Option<LockEntry> {
    st.locks.get(&ino)?.iter().copied().find(|l| {
        l.owner != lock.owner
            && overlaps(l.range, lock.range)
            && (l.kind == LockKind::Write || lock.kind == LockKind::Write)
    })
}

/// Remove `range` from `owner`'s locks, splitting the ones it cuts.
fn remove_range(list: &mut Vec<LockEntry>, owner: LockOwner, range: LockRange) {
    let mut out = Vec::with_capacity(list.len() + 1);
    for l in list.drain(..) {
        if l.owner != owner || !overlaps(l.range, range) {
            out.push(l);
            continue;
        }
        if l.range.start < range.start {
            out.push(LockEntry {
                range: LockRange {
                    start: l.range.start,
                    end: range.start - 1,
                },
                ..l
            });
        }
        if l.range.end > range.end {
            out.push(LockEntry {
                range: LockRange {
                    start: range.end + 1,
                    end: l.range.end,
                },
                ..l
            });
        }
    }
    *list = out;
}

/// `lock` replaces whatever its owner held over its range.
fn apply_lock(list: &mut Vec<LockEntry>, lock: &LockSpec) {
    remove_range(list, lock.owner, lock.range);
    list.push(LockEntry {
        owner: lock.owner,
        range: lock.range,
        kind: lock.kind,
        pid: lock.pid,
    });
}

/// The notifier thread of one view: batches coalesced (whatever queued
/// while the previous delivery ran goes out as one), delivered on this
/// thread and no other, never under a filesystem lock.
fn notifier(rx: Receiver<Msg>, events: Arc<dyn FrontendEvents>) {
    let mut pending: VecDeque<Msg> = VecDeque::new();
    loop {
        let first = match pending.pop_front() {
            Some(m) => m,
            None => match rx.recv() {
                Ok(m) => m,
                Err(_) => return,
            },
        };
        match first {
            Msg::Barrier(done) => {
                let _ = done.send(());
            }
            Msg::Batch(mut batch) => {
                while let Ok(next) = rx.try_recv() {
                    match next {
                        Msg::Batch(more) => batch.extend(more),
                        barrier @ Msg::Barrier(_) => {
                            pending.push_back(barrier);
                            break;
                        }
                    }
                }
                batch.dedup();
                events.invalidate(&batch);
            }
        }
    }
}
