//! Subtree confinement (plan 31 §6.12): the checks behind the rules in
//! [`crate::view`]'s module doc ("Confinement").

use super::*;

/// The directory marker that starts a link domain inside a view (see the
/// module doc of [`crate::view`], "Link domains"). A `trusted.*` name, so
/// only root (the provisioner) can set or clear it.
pub const LINK_DOMAIN_XATTR: &str = "trusted.constellation.link_domain";

/// How many ancestors a domination or domain walk follows before it
/// gives up (refusing): far deeper than any real tree, and a bound on a
/// corrupt, cyclic parent chain.
const WALK_LIMIT: usize = 4096;

const REACH_SHARDS: usize = 16;

/// The inodes a confined view has resolved from inside its subtree (every
/// entry it handed out, every inode an ancestor walk proved inside): the
/// fast path of [`View::enter_ino`]. A cache, never the authority — a
/// shard that fills is simply cleared, and a miss walks the replica.
pub(crate) struct Reach {
    shards: [Mutex<HashSet<Ino>>; REACH_SHARDS],
    per_shard: usize,
}

impl Reach {
    pub(crate) fn new() -> Self {
        // `CONSTELLATION_VIEW_REACH_CACHE`: entries per confined view
        // (default 262,144, ~4-8 MiB at most).
        let total: usize = std::env::var("CONSTELLATION_VIEW_REACH_CACHE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1 << 18);
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashSet::new())),
            per_shard: (total / REACH_SHARDS).max(16),
        }
    }

    pub(crate) fn shard(&self, ino: Ino) -> &Mutex<HashSet<Ino>> {
        // Inode numbers carry the node id in their high bits and count up
        // in the low ones: the low bits spread them.
        &self.shards[(ino as usize) % REACH_SHARDS]
    }

    pub(crate) fn contains(&self, ino: Ino) -> bool {
        self.shard(ino).lock().unwrap().contains(&ino)
    }

    /// Every inode cached (a session handover carries them, `handoff`).
    pub(crate) fn all(&self) -> Vec<Ino> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.lock().unwrap().iter().copied());
        }
        out
    }

    pub(crate) fn mark(&self, ino: Ino) {
        let mut shard = self.shard(ino).lock().unwrap();
        if shard.len() >= self.per_shard && !shard.contains(&ino) {
            shard.clear();
        }
        shard.insert(ino);
    }
}

impl View {
    /// Whether this view is confined to less than the whole live tree (a
    /// subtree or a snapshot): only then are its inodes checked.
    pub(crate) fn is_confined(&self) -> bool {
        self.view_root != constellation_fs_core::types::ROOT_INO
    }

    /// An inode a frontend addressed, in the replica's numbering: the
    /// view's root renumbered, and — in a confined view — refused
    /// (`Stale`) unless the view's root dominates it.
    ///
    /// Cost on the common path: a whole-filesystem view pays one
    /// comparison. A confined view pays one lock of one of 16 shards and
    /// a hash probe for an inode it handed out or already proved (every
    /// inode the kernel knows came from a lookup or a create through this
    /// view, so that is every op in steady state); only a miss walks the
    /// replica's parent chain, stopping at the first ancestor already
    /// known to be inside.
    pub(crate) fn enter_ino(&self, ino: Ino) -> Result<Ino, Code> {
        let ino = self.real_ino(ino);
        if !self.is_confined() || ino == self.view_root {
            return Ok(ino);
        }
        if Self::is_synthetic(ino) {
            // The synthetic tree is numbered per view (its registry is
            // this view's own, filled only by this view's lookups): a
            // number it never handed out is unknown, and answered as
            // before (`Stale`/`NotFound`).
            return Ok(ino);
        }
        if Self::is_synthetic(self.view_root) {
            // A snapshot view hands out synthetic inodes only: a live
            // inode number can only be forged or replayed from elsewhere.
            return Err(Code::Stale);
        }
        if self.reach.contains(ino) {
            return Ok(ino);
        }
        // Open through this view (an unlinked-open orphan has no name to
        // walk from, but was reachable when it was opened here).
        if self.opens.lock().unwrap().contains_key(&ino) {
            return Ok(ino);
        }
        match self.dominated(ino, true) {
            Ok(true) => {
                self.reach.mark(ino);
                Ok(ino)
            }
            Ok(false) => Err(Code::Stale),
            Err(code) => Err(code),
        }
    }

    /// Note an inode this view resolved (an entry it hands out).
    pub(crate) fn note_reached(&self, ino: Ino) {
        if self.is_confined() && !Self::is_synthetic(ino) && ino != self.view_root {
            self.reach.mark(ino);
        }
    }

    /// Whether the view's root dominates `ino`: one of its names lies
    /// under the root. `use_cache`: stop at an ancestor the view already
    /// resolved (the op path); without it, the replica alone decides
    /// (`confine_links`).
    pub(crate) fn dominated(&self, ino: Ino, use_cache: bool) -> Result<bool, Code> {
        let root = self.view_root;
        if ino == root {
            return Ok(true);
        }
        let mut frontier = vec![ino];
        let mut seen = HashSet::new();
        while let Some(cur) = frontier.pop() {
            if seen.len() > WALK_LIMIT {
                return Ok(false);
            }
            for parent in self.meta.parents_of(cur).map_err(|e| e.code())? {
                if parent == root || (use_cache && self.reach.contains(parent)) {
                    return Ok(true);
                }
                if parent != constellation_fs_core::types::ROOT_INO && seen.insert(parent) {
                    frontier.push(parent);
                }
            }
        }
        Ok(false)
    }

    /// The link domain `dir` belongs to: its nearest ancestor (itself
    /// included) that is the view's root or carries
    /// [`LINK_DOMAIN_XATTR`]; `None` outside the view.
    pub(crate) fn link_domain(&self, dir: Ino) -> Result<Option<Ino>, Code> {
        let mut cur = dir;
        for _ in 0..WALK_LIMIT {
            if cur == self.view_root {
                return Ok(Some(cur));
            }
            let marked = self
                .meta
                .get_xattr(cur, LINK_DOMAIN_XATTR)
                .map_err(|e| e.code())?
                .is_some_and(|v| v.as_slice() != b"0");
            if marked {
                return Ok(Some(cur));
            }
            if cur == constellation_fs_core::types::ROOT_INO {
                return Ok(None);
            }
            match self.meta.parents_of(cur).map_err(|e| e.code())?.first() {
                Some(parent) => cur = *parent,
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    /// `confine_links`: may `ino` get a name in `new_parent`? Only when
    /// one of its existing names is in the same link domain.
    pub(crate) fn link_within_domain(&self, ino: Ino, new_parent: Ino) -> Result<(), Code> {
        let Some(domain) = self.link_domain(new_parent)? else {
            return Err(Code::CrossDevice);
        };
        for parent in self.meta.parents_of(ino).map_err(|e| e.code())? {
            if self.link_domain(parent)? == Some(domain) {
                return Ok(());
            }
        }
        Err(Code::CrossDevice)
    }
}

impl View {
    /// `confine_links`: may `name` in `parent` move to `new_parent`? A
    /// directory, or a file with no other name, always may; a file with
    /// other names only within its link domain.
    pub(crate) fn rename_within_domains(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
    ) -> Result<(), Code> {
        let Some(attr) = self.meta.lookup(parent, name).map_err(|e| e.code())? else {
            return Ok(()); // the rename itself answers `NotFound`
        };
        if attr.kind == InodeKind::Dir || attr.nlink <= 1 {
            return Ok(());
        }
        if self.link_domain(parent)? == self.link_domain(new_parent)? {
            Ok(())
        } else {
            Err(Code::CrossDevice)
        }
    }
}
