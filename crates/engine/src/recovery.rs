//! Conflict copies for refused replays (plan 30 §M3a/§M3b/§M4).
//!
//! Replaying stranded ops by rid, the takeover gate, the deposition
//! recovery and the drain's backoff and lease fallback all moved into the
//! authority core (`constellation_authority::core::replay`) in plan 30
//! M5. What the core cannot do — execute the several system ops that
//! materialize a refused op as a
//! `<parent>/.constellation-conflict/<name>@<node>-<ts>-<seq>` copy (an empty
//! file or directory, carrying the op's manifest when it had one) — it
//! asks the driver for (`Action::ConflictCopy`), and the driver runs it
//! here through the ordinary submit path (holder-local when this node
//! holds, forwarded otherwise), reporting `Event::ConflictCopyDone`.
//! Each step is a system-generated op with its own rid; an entry that
//! already exists (an earlier interrupted attempt, or another node's
//! conflict directory) is as good as a fresh one.
//! A copy is its source's owner's with the owner's permission bits only,
//! in a `0700` conflict directory owned by the directory it sits in
//! (`copy_op`, `conflict_dir_op`).

use crate::forward::ForwardState;
use crate::sync::SyncRequest;
use constellation_authority::{ClientReply, Policy};
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::reintegrate::{conflict_dentry_name, CONFLICT_DIR};
use constellation_meta::{Meta, MetaStore, MutateOp, MutateOutcome, Refusal, Rid};
use constellation_types::Code;

/// Send `op` down the ordinary submit path (holder-local execution, or a
/// forward to the holder) and wait for the outcome. `None` if the sync
/// task is gone or the op ended in doubt.
async fn submit(
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    op: MutateOp,
    rid: Rid,
) -> Option<MutateOutcome> {
    let (reply, rx) = tokio::sync::oneshot::channel();
    sync_tx
        .send(SyncRequest::Submit {
            op,
            rid,
            policy: Policy::System,
            in_doubt: false,
            tag: constellation_meta::locks::LockTag::NONE,
            reply,
        })
        .ok()?;
    match rx.await.ok()? {
        ClientReply::Outcome(outcome) => Some(outcome),
        ClientReply::InDoubt => None,
    }
}

// --------------------------------------------------------- conflict copies

/// What a refused op's conflict copy looks like: under which directory,
/// named what, a directory or a file, with which manifest, and whose:
/// `owner` is the source's `(uid, gid, mode)` (the op's, or the inode's),
/// `None` when it is unknown.
struct ConflictCopy {
    parent: Ino,
    name: String,
    dir: bool,
    manifest: Option<(Vec<u8>, u64)>,
    owner: Option<(u32, u32, u32)>,
}

/// The conflict copy for `op`, mirroring `reintegrate::materialize`'s
/// choices. `None` for ops that leave nothing worth copying (a rename, an
/// xattr edit, an atime batch): those are counted and logged only.
fn conflict_copy(meta: &Meta, op: &MutateOp) -> Option<ConflictCopy> {
    let at_ino = |ino: Ino, manifest: Option<(Vec<u8>, u64)>| {
        let parent = meta.parent_of(ino).ok().flatten().unwrap_or(ROOT_INO);
        let path = meta.path_of(ino).unwrap_or_else(|_| format!("ino-{ino}"));
        let name = path.rsplit('/').next().unwrap_or("file").to_string();
        let owner = meta
            .getattr(ino)
            .ok()
            .flatten()
            .map(|a| (a.uid, a.gid, a.mode));
        ConflictCopy {
            parent,
            name,
            dir: false,
            manifest,
            owner,
        }
    };
    let copy = match op {
        MutateOp::Mkdir {
            parent,
            name,
            mode,
            uid,
            gid,
            ..
        } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: true,
            manifest: None,
            owner: Some((*uid, *gid, *mode)),
        },
        MutateOp::Create {
            parent,
            name,
            mode,
            uid,
            gid,
            ..
        }
        | MutateOp::Mknod {
            parent,
            name,
            mode,
            uid,
            gid,
            ..
        } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: None,
            owner: Some((*uid, *gid, *mode)),
        },
        MutateOp::Symlink {
            parent,
            name,
            uid,
            gid,
            ..
        } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: None,
            owner: Some((*uid, *gid, 0o600)),
        },
        MutateOp::Link { parent, name, ino } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: None,
            owner: meta
                .getattr(*ino)
                .ok()
                .flatten()
                .map(|a| (a.uid, a.gid, a.mode)),
        },
        MutateOp::Publish {
            parent,
            name,
            manifest,
            size,
            mode,
            uid,
            gid,
            ..
        } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: Some((manifest.clone(), *size)),
            owner: Some((*uid, *gid, *mode)),
        },
        MutateOp::SetManifest {
            ino,
            manifest,
            size,
            ..
        } => at_ino(*ino, Some((manifest.clone(), *size))),
        MutateOp::Setattr { ino, .. } => {
            let manifest = meta.manifest(*ino).ok().flatten().map(|m| {
                let size = meta
                    .getattr(*ino)
                    .ok()
                    .flatten()
                    .map(|a| a.size)
                    .unwrap_or(0);
                (m, size)
            });
            at_ino(*ino, manifest)
        }
        MutateOp::Unlink { .. }
        | MutateOp::Rmdir { .. }
        | MutateOp::Rename { .. }
        | MutateOp::Exchange { .. }
        | MutateOp::SetXattr { .. }
        | MutateOp::RemoveXattr { .. }
        | MutateOp::AtimeBatch { .. }
        | MutateOp::Records { .. } => return None,
    };
    let parent = if meta.getattr(copy.parent).ok().flatten().is_some() {
        copy.parent
    } else {
        ROOT_INO
    };
    Some(ConflictCopy { parent, ..copy })
}

/// A file's whole path as one name (`/` escaped as `%2F`, `%` as `%25`),
/// for its conflict copy under a view's root; past 200 bytes only the
/// tail is kept (the copy adds `@<node>-<time>-<seq>`).
pub(crate) fn conflict_name_for_path(path: &str) -> String {
    let flat = path
        .trim_start_matches('/')
        .replace('%', "%25")
        .replace('/', "%2F");
    if flat.len() <= 200 {
        return flat;
    }
    let mut start = flat.len() - 200;
    while !flat.is_char_boundary(start) {
        start += 1;
    }
    flat[start..].to_string()
}

/// Plan 30 §M14 phase 2: a refused op issued under a cluster lock keeps
/// its copy under the deepest of `roots` (the mounted views' roots) above
/// its file, named after the file's path below that root
/// ([`conflict_name_for_path`]), as a refused commit's copy is
/// (`View::keep_refused_commit`): not beside the file, whose directory
/// means something to the application the lock serves (beside
/// `.git/refs/heads/master.lock` a copy is a ref with a bad name, and
/// `git fsck` fails). No mounted view above it (or no `roots`): beside
/// the file, as for any refused replay.
fn under_view_root(meta: &Meta, copy: ConflictCopy, roots: &[Ino]) -> ConflictCopy {
    let path = |ino: Ino| -> Option<String> {
        if ino == ROOT_INO {
            return Some(String::new());
        }
        let p = meta.path_of(ino).ok()?;
        Some(p.trim_end_matches('/').to_string())
    };
    let Some(dir) = path(copy.parent) else {
        return copy;
    };
    let root = roots
        .iter()
        .filter_map(|&r| Some((r, path(r)?)))
        .filter(|(_, rp)| dir == *rp || dir.starts_with(&format!("{rp}/")))
        .max_by_key(|(_, rp)| rp.len());
    let Some((root, rp)) = root else {
        return copy;
    };
    let below = format!("{}/{}", &dir[rp.len()..], copy.name);
    ConflictCopy {
        parent: root,
        name: conflict_name_for_path(&below),
        ..copy
    }
}

/// The op that creates `copy` under `dir` as `dest` (its manifest, if
/// any, is a second step). The copy belongs to the source's owner and
/// keeps only its owner's permission bits (`mode & 0o700`): it holds the
/// source's content, but no longer sits under the source's ancestors,
/// which may have been all that kept other users out. An unknown owner
/// gives `root`, `0600` (`0700` for a directory).
fn copy_op(meta: &Meta, copy: &ConflictCopy, dir: Ino, dest: &str) -> anyhow::Result<MutateOp> {
    let ino = meta.allocate_ino(dir)?;
    let (uid, gid, mode) = copy.owner.unwrap_or((0, 0, 0o700));
    let perm = mode & 0o700;
    Ok(if copy.dir {
        MutateOp::Mkdir {
            parent: dir,
            name: dest.to_string(),
            ino,
            mode: perm,
            uid,
            gid,
        }
    } else {
        MutateOp::Create {
            parent: dir,
            name: dest.to_string(),
            ino,
            mode: perm,
            uid,
            gid,
        }
    })
}

/// The conflict directory under `parent`: `0700`, owned by `parent`'s
/// owner (the volume's owner for a subtree view's root; `root` for the
/// filesystem root), so a copy is reachable by whoever owns the place
/// it was put in and by nobody else.
fn conflict_dir_op(meta: &Meta, parent: Ino) -> anyhow::Result<MutateOp> {
    let (uid, gid) = meta
        .getattr(parent)?
        .map(|a| (a.uid, a.gid))
        .unwrap_or((0, 0));
    Ok(MutateOp::Mkdir {
        parent,
        name: CONFLICT_DIR.to_string(),
        ino: meta.allocate_ino(parent)?,
        mode: 0o700,
        uid,
        gid,
    })
}

fn lookup_ino(meta: &Meta, parent: Ino, name: &str) -> Option<Ino> {
    meta.lookup(parent, name).ok().flatten().map(|a| a.ino)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn materialize_remote(
    meta: &Meta,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    rid: Rid,
    op: &MutateOp,
    refusal: &Refusal,
    roots: &[Ino],
) -> anyhow::Result<bool> {
    let Some(copy) = conflict_copy(meta, op) else {
        return Ok(true);
    };
    let copy = under_view_root(meta, copy, roots);
    // Each step is a system-generated op with its own rid: an entry that
    // already exists (an earlier interrupted attempt, or another node's
    // conflict directory) is as good as a fresh one.
    let step = |op: MutateOp| async move {
        match submit(sync_tx, op, forward.next_system_rid(node_id)).await {
            Some(MutateOutcome::Accepted { .. })
            | Some(MutateOutcome::Exists { .. })
            | Some(MutateOutcome::Errno(Code::Exists)) => true,
            Some(other) => {
                // The caller logs (with backoff) when a copy keeps failing.
                tracing::debug!(?other, "conflict copy step not accepted yet");
                false
            }
            None => false,
        }
    };
    if lookup_ino(meta, copy.parent, CONFLICT_DIR).is_none()
        && !step(conflict_dir_op(meta, copy.parent)?).await
    {
        return Ok(false);
    }
    let Some(dir) = lookup_ino(meta, copy.parent, CONFLICT_DIR) else {
        return Ok(false);
    };
    let dest = conflict_dentry_name(&copy.name, node_id, refusal.ts_unix, rid.seq);
    if lookup_ino(meta, dir, &dest).is_none() && !step(copy_op(meta, &copy, dir, &dest)?).await {
        return Ok(false);
    }
    if let Some((manifest, size)) = &copy.manifest {
        let Some(ino) = lookup_ino(meta, dir, &dest) else {
            return Ok(false);
        };
        let set = MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest: manifest.clone(),
            size: *size,
            mtime_ns: None,
        };
        if !step(set).await {
            return Ok(false);
        }
    }
    tracing::error!(
        path = %format!("{CONFLICT_DIR}/{dest}"),
        reason = %refusal.reason,
        "stranded op materialized as a conflict copy"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_meta::execute_mutate;
    use std::sync::Arc;

    fn create_op(name: &str, ino: Ino) -> MutateOp {
        MutateOp::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    }

    /// A refused replay's conflict copy is made through the submit path
    /// (here: a stand-in sync task executing every step locally, as the
    /// core does for a holder), lands under `.constellation-conflict/`
    /// with the deterministic name, and is idempotent: a second attempt
    /// finds every step done.
    #[tokio::test]
    async fn a_refused_op_is_materialized_as_a_conflict_copy() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(2).unwrap();
        let forward = ForwardState::new(1);
        let (sync_tx, mut sync_rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let meta = meta.clone();
            tokio::spawn(async move {
                while let Some(req) = sync_rx.recv().await {
                    if let SyncRequest::Submit { op, rid, reply, .. } = req {
                        let outcome = match execute_mutate(&meta, &op, Some(rid)) {
                            Ok(records) => MutateOutcome::Accepted { epoch: 1, records },
                            Err(e) => MutateOutcome::Errno(e.code()),
                        };
                        let _ = reply.send(ClientReply::Outcome(outcome));
                    }
                }
            });
        }
        // The name the stranded create wanted went to someone else.
        meta.create(ROOT_INO, "taken", 0o644, 0, 0).unwrap();
        let refusal = Refusal {
            reason: "the name now exists".into(),
            ts_unix: 7,
        };
        let op = create_op("taken", (5 << 40) | 1);
        assert!(materialize_remote(
            &meta,
            &sync_tx,
            &forward,
            2,
            forward_rid(),
            &op,
            &refusal,
            &[]
        )
        .await
        .unwrap());
        let dir = meta
            .lookup(ROOT_INO, CONFLICT_DIR)
            .unwrap()
            .expect("conflict directory");
        let copies = meta.readdir(dir.ino).unwrap();
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert_eq!(
            copies[0].name,
            conflict_dentry_name("taken", 2, 7, forward_rid().seq)
        );
        assert!(
            materialize_remote(
                &meta,
                &sync_tx,
                &forward,
                2,
                forward_rid(),
                &op,
                &refusal,
                &[]
            )
            .await
            .unwrap(),
            "idempotent"
        );
        assert_eq!(meta.readdir(dir.ino).unwrap().len(), 1);
        // An op with nothing to copy (a rename) is done at once.
        let rename = MutateOp::Rename {
            parent: ROOT_INO,
            name: "a".into(),
            new_parent: ROOT_INO,
            new_name: "b".into(),
            noreplace: false,
        };
        assert!(materialize_remote(
            &meta,
            &sync_tx,
            &forward,
            2,
            forward_rid(),
            &rename,
            &refusal,
            &[]
        )
        .await
        .unwrap());
    }

    fn forward_rid() -> Rid {
        Rid {
            node: 2,
            incarnation: 1,
            seq: 11,
        }
    }

    /// A stand-in sync task executing every submitted step locally.
    fn local_core(meta: &Arc<Meta>) -> tokio::sync::mpsc::UnboundedSender<SyncRequest> {
        let (sync_tx, mut sync_rx) = tokio::sync::mpsc::unbounded_channel();
        let meta = meta.clone();
        tokio::spawn(async move {
            while let Some(req) = sync_rx.recv().await {
                if let SyncRequest::Submit { op, rid, reply, .. } = req {
                    let outcome = match execute_mutate(&meta, &op, Some(rid)) {
                        Ok(records) => MutateOutcome::Accepted { epoch: 1, records },
                        Err(e) => MutateOutcome::Errno(e.code()),
                    };
                    let _ = reply.send(ClientReply::Outcome(outcome));
                }
            }
        });
        sync_tx
    }

    /// A refused commit's copy (`View::keep_refused_commit` queues it as
    /// a `Publish` under the view's root) is its source's owner's with
    /// the owner's bits only, in a `0700` conflict directory owned by the
    /// directory it sits in; two copies of one file within one second
    /// (distinct rids) do not land on each other.
    #[tokio::test]
    async fn a_conflict_copy_keeps_its_owner_and_its_own_name() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(2).unwrap();
        let forward = ForwardState::new(1);
        let sync_tx = local_core(&meta);
        let vol = meta.mkdir(ROOT_INO, "vol", 0o755, 1000, 1000).unwrap();
        let publish = |ino: Ino, manifest: &[u8]| MutateOp::Publish {
            ino,
            parent: vol.ino,
            name: "db%2Fdata.sqlite".into(),
            mode: 0o640,
            uid: 1000,
            gid: 1001,
            mtime_ns: 0,
            manifest: manifest.to_vec(),
            size: 0,
            xattrs: Vec::new(),
            noreplace: false,
        };
        let refusal = Refusal {
            reason: "the lock grant ended".into(),
            ts_unix: 7,
        };
        let rid = |seq| Rid {
            node: 2,
            incarnation: 1,
            seq,
        };
        for seq in [1, 2] {
            assert!(materialize_remote(
                &meta,
                &sync_tx,
                &forward,
                2,
                rid(seq),
                &publish(77, &[]),
                &refusal,
                &[]
            )
            .await
            .unwrap());
        }
        let dir = meta.lookup(vol.ino, CONFLICT_DIR).unwrap().expect("dir");
        assert!(meta.lookup(ROOT_INO, CONFLICT_DIR).unwrap().is_none());
        assert_eq!((dir.uid, dir.gid, dir.mode & 0o7777), (1000, 1000, 0o700));
        let mut copies: Vec<String> = meta
            .readdir(dir.ino)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        copies.sort();
        assert_eq!(
            copies,
            vec![
                conflict_dentry_name("db%2Fdata.sqlite", 2, 7, 1),
                conflict_dentry_name("db%2Fdata.sqlite", 2, 7, 2),
            ]
        );
        for name in &copies {
            let a = meta.lookup(dir.ino, name).unwrap().unwrap();
            assert_eq!((a.uid, a.gid, a.mode & 0o7777), (1000, 1001, 0o600));
        }
    }

    /// A refused replay of an op issued under a lock (`roots` given: the
    /// mounted views' roots) keeps its copy under the deepest view root
    /// above its file, named after its path below that root, never beside
    /// the file (faults seed 33: copies in `.git/refs/heads/` and
    /// `.git/objects/6a/` failed `git fsck`); a link to an inode that is
    /// gone (its create refused too) is `root`'s there. Without `roots`
    /// (no lock) it stays beside the file.
    #[tokio::test]
    async fn a_locked_replays_copy_goes_under_the_view_root() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(2).unwrap();
        let forward = ForwardState::new(1);
        let sync_tx = local_core(&meta);
        let vol = meta.mkdir(ROOT_INO, "vol", 0o755, 1000, 1000).unwrap();
        let mut dir = vol.ino;
        for name in ["repo", ".git", "refs", "heads"] {
            dir = meta.mkdir(dir, name, 0o755, 1000, 1000).unwrap().ino;
        }
        let heads = dir;
        let refusal = Refusal {
            reason: "the lock grant it was issued under has ended".into(),
            ts_unix: 7,
        };
        let create = MutateOp::Create {
            parent: heads,
            name: "master.lock".into(),
            ino: (5 << 40) | 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        };
        let link = MutateOp::Link {
            ino: (5 << 40) | 2,
            parent: heads,
            name: "master".into(),
        };
        let rid = |seq| Rid {
            node: 2,
            incarnation: 1,
            seq,
        };
        let roots = [ROOT_INO, vol.ino];
        for (seq, op) in [(1, &create), (2, &link)] {
            assert!(materialize_remote(
                &meta,
                &sync_tx,
                &forward,
                2,
                rid(seq),
                op,
                &refusal,
                &roots
            )
            .await
            .unwrap());
        }
        assert!(meta.lookup(heads, CONFLICT_DIR).unwrap().is_none());
        assert!(meta.lookup(ROOT_INO, CONFLICT_DIR).unwrap().is_none());
        let cdir = meta.lookup(vol.ino, CONFLICT_DIR).unwrap().expect("dir");
        let copy = |name: &str, seq| {
            meta.lookup(cdir.ino, &conflict_dentry_name(name, 2, 7, seq))
                .unwrap()
                .expect(name)
        };
        let a = copy("repo%2F.git%2Frefs%2Fheads%2Fmaster.lock", 1);
        assert_eq!((a.uid, a.gid, a.mode & 0o7777), (1000, 1000, 0o600));
        let a = copy("repo%2F.git%2Frefs%2Fheads%2Fmaster", 2);
        assert_eq!((a.uid, a.mode & 0o7777), (0, 0o700));
        // No lock: beside the file, as before.
        assert!(
            materialize_remote(&meta, &sync_tx, &forward, 2, rid(3), &create, &refusal, &[])
                .await
                .unwrap()
        );
        let beside = meta.lookup(heads, CONFLICT_DIR).unwrap().expect("beside");
        assert!(meta
            .lookup(beside.ino, &conflict_dentry_name("master.lock", 2, 7, 3))
            .unwrap()
            .is_some());
    }
}
