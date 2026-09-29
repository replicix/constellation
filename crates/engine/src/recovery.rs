//! Conflict copies for refused replays (plan 30 §M3a/§M3b/§M4).
//!
//! Replaying stranded ops by rid, the takeover gate, the deposition
//! recovery and the drain's backoff and lease fallback all moved into the
//! authority core (`constellation_authority::core::replay`) in plan 30
//! M5. What the core cannot do — execute the several system ops that
//! materialize a refused op as a
//! `<parent>/.constellation-conflict/<name>@<node>-<ts>` copy (an empty
//! file or directory, carrying the op's manifest when it had one) — it
//! asks the driver for (`Action::ConflictCopy`), and the driver runs it
//! here through the ordinary submit path (holder-local when this node
//! holds, forwarded otherwise), reporting `Event::ConflictCopyDone`.
//! Each step is a system-generated op with its own rid; an entry that
//! already exists (an earlier interrupted attempt, or another node's
//! conflict directory) is as good as a fresh one.

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
/// named what, a directory or a file, and with which manifest.
struct ConflictCopy {
    parent: Ino,
    name: String,
    dir: bool,
    manifest: Option<(Vec<u8>, u64)>,
}

/// The conflict copy for `op`, mirroring `reintegrate::materialize`'s
/// choices. `None` for ops that leave nothing worth copying (a rename, an
/// xattr edit, an atime batch): those are counted and logged only.
fn conflict_copy(meta: &Meta, op: &MutateOp) -> Option<ConflictCopy> {
    let at_ino = |ino: Ino, manifest: Option<(Vec<u8>, u64)>| {
        let parent = meta.parent_of(ino).ok().flatten().unwrap_or(ROOT_INO);
        let path = meta.path_of(ino).unwrap_or_else(|_| format!("ino-{ino}"));
        let name = path.rsplit('/').next().unwrap_or("file").to_string();
        ConflictCopy {
            parent,
            name,
            dir: false,
            manifest,
        }
    };
    let copy = match op {
        MutateOp::Mkdir { parent, name, .. } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: true,
            manifest: None,
        },
        MutateOp::Create { parent, name, .. }
        | MutateOp::Symlink { parent, name, .. }
        | MutateOp::Mknod { parent, name, .. }
        | MutateOp::Link { parent, name, .. } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: None,
        },
        MutateOp::Publish {
            parent,
            name,
            manifest,
            size,
            ..
        } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: Some((manifest.clone(), *size)),
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

/// The op that creates `copy` under `dir` as `dest` (its manifest, if
/// any, is a second step).
fn copy_op(meta: &Meta, copy: &ConflictCopy, dir: Ino, dest: &str) -> anyhow::Result<MutateOp> {
    let ino = meta.allocate_ino(dir)?;
    Ok(if copy.dir {
        MutateOp::Mkdir {
            parent: dir,
            name: dest.to_string(),
            ino,
            mode: 0o755,
            uid: 0,
            gid: 0,
        }
    } else {
        MutateOp::Create {
            parent: dir,
            name: dest.to_string(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    })
}

fn conflict_dir_op(meta: &Meta, parent: Ino) -> anyhow::Result<MutateOp> {
    Ok(MutateOp::Mkdir {
        parent,
        name: CONFLICT_DIR.to_string(),
        ino: meta.allocate_ino(parent)?,
        mode: 0o755,
        uid: 0,
        gid: 0,
    })
}

fn lookup_ino(meta: &Meta, parent: Ino, name: &str) -> Option<Ino> {
    meta.lookup(parent, name).ok().flatten().map(|a| a.ino)
}

pub(crate) async fn materialize_remote(
    meta: &Meta,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    op: &MutateOp,
    refusal: &Refusal,
) -> anyhow::Result<bool> {
    let Some(copy) = conflict_copy(meta, op) else {
        return Ok(true);
    };
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
    let dest = conflict_dentry_name(&copy.name, node_id, refusal.ts_unix);
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
        assert!(
            materialize_remote(&meta, &sync_tx, &forward, 2, &op, &refusal)
                .await
                .unwrap()
        );
        let dir = meta
            .lookup(ROOT_INO, CONFLICT_DIR)
            .unwrap()
            .expect("conflict directory");
        let copies = meta.readdir(dir.ino).unwrap();
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert_eq!(copies[0].name, conflict_dentry_name("taken", 2, 7));
        assert!(
            materialize_remote(&meta, &sync_tx, &forward, 2, &op, &refusal)
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
        assert!(
            materialize_remote(&meta, &sync_tx, &forward, 2, &rename, &refusal)
                .await
                .unwrap()
        );
    }
}
