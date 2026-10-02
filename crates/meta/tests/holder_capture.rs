//! Plan 30 §M3b: holder-side capture (`store::local`, `SpecKind::Local`).
//!
//! The first test is the capture twin of `dirty.rs`: every journaled write
//! API, run as a holder, must be undone byte for byte by a stranding
//! rollback, with its op queued for replay — the invariant that every
//! `ns` write a holder makes is captured, checked generically rather than
//! per key. The rest pin down one rule each: retirement on ship,
//! deposition, a tailed segment inserted before local work, publish
//! substitution, a forward reply older than the held epoch, transaction
//! boundaries, and the capture-off fallback.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::InodeKind;
use constellation_meta::{
    execute_mutate, CloneSpec, LogRecord, Meta, MetaStore, MutateOp, PublishBasis, Rid,
    SetXattrMode, SnapshotRow, TouchSet,
};
use std::collections::BTreeMap;

fn rid(seq: u64) -> Rid {
    Rid {
        node: 3,
        incarnation: 1,
        seq,
    }
}

/// Ship everything journaled so far: the state it leaves is log prefix.
fn ship_all(meta: &Meta, segment: u64) {
    let rows = meta.take_journal(usize::MAX).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    meta.ack_journal_rows_at(&seqs, segment).unwrap();
}

fn raw_ns(meta: &Meta) -> Vec<(Vec<u8>, Vec<u8>)> {
    meta.ns_dump().unwrap()
}

/// `ns` as the publisher would see it through `basis`.
fn published_view(meta: &Meta, basis: &PublishBasis) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = raw_ns(meta).into_iter().collect();
    if let PublishBasis::Substituted(view) = basis {
        for (k, v) in view.iter() {
            match v {
                Some(v) => {
                    map.insert(k.to_vec(), v.to_vec());
                }
                None => {
                    map.remove(k);
                }
            }
        }
    }
    map
}

fn basis(meta: &Meta) -> PublishBasis {
    meta.read_consistent(|snap| meta.publish_basis_at(snap))
        .unwrap()
}

/// Run `body` as the holder at epoch 1, then strand it as a deposition
/// would, and check the replica is back to exactly where it started with
/// one op queued for replay.
fn assert_captured<T>(meta: &Meta, what: &str, body: impl FnOnce(&Meta) -> T) -> T {
    ship_all(meta, meta.applied_seq().unwrap() + 1);
    let before = raw_ns(meta);
    let usage = meta.usage_bytes_files();
    meta.set_holder_epoch(1);
    let out = body(meta);
    meta.set_holder_epoch(0);
    let counts = meta.speculation_counts().unwrap();
    assert!(counts.local >= 1, "{what}: nothing was captured");
    let stranded = meta.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.locals as u64, counts.local, "{what}");
    assert_eq!(raw_ns(meta), before, "{what}: rollback left ns changed");
    assert_eq!(
        meta.usage_bytes_files(),
        usage,
        "{what}: usage not restored"
    );
    assert_eq!(
        meta.journal_len().unwrap(),
        0,
        "{what}: journal rows survived"
    );
    let queued = meta.pending_replays().unwrap();
    assert_eq!(
        queued.len() as u64,
        counts.local,
        "{what}: every stranded transaction is queued: {queued:?}"
    );
    for op in queued {
        meta.forget_replay(op.queue_seq).unwrap();
    }
    out
}

#[test]
fn every_journaled_api_is_captured_and_rolls_back_byte_for_byte() {
    let meta = Meta::open_in_memory().unwrap();
    let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let f1 = meta.create(dir, "f1", 0o644, 1, 1).unwrap().ino;
    meta.set_manifest(f1, &[1, 2, 3], 3).unwrap();
    let other = meta.mkdir(ROOT_INO, "other", 0o755, 0, 0).unwrap().ino;
    meta.create(other, "victim", 0o644, 0, 0).unwrap();
    meta.mkdir(ROOT_INO, "empty", 0o755, 0, 0).unwrap();
    meta.link(f1, other, "hardlink").unwrap();
    for i in 0..40 {
        meta.set_xattr(f1, &format!("user.k{i}"), b"vvvvvvvv", SetXattrMode::Set)
            .unwrap();
    }
    let snap0 = constellation_meta::snapshot_id("/", "s0");
    meta.record_snapshot(&SnapshotRow::new(&snap0, "/", "s0", "abc", 0))
        .unwrap();

    assert_captured(&meta, "mkdir", |m| {
        m.mkdir(dir, "sub", 0o755, 0, 0).unwrap()
    });
    assert_captured(&meta, "create", |m| {
        m.create(dir, "f2", 0o644, 0, 0).unwrap()
    });
    assert_captured(&meta, "symlink", |m| {
        m.symlink(dir, "s", "t", 0, 0).unwrap()
    });
    assert_captured(&meta, "mknod", |m| {
        m.mknod(dir, "n", InodeKind::Fifo, 0o600, 0, 0, Default::default())
            .unwrap()
    });
    assert_captured(&meta, "link", |m| m.link(f1, dir, "l2").unwrap());
    assert_captured(&meta, "unlink (last link)", |m| {
        m.unlink(other, "victim").unwrap()
    });
    assert_captured(&meta, "unlink (one of two links)", |m| {
        m.unlink(other, "hardlink").unwrap()
    });
    assert_captured(&meta, "rmdir", |m| m.rmdir(ROOT_INO, "empty").unwrap());
    assert_captured(&meta, "rename", |m| {
        m.rename(dir, "f1", other, "moved").unwrap()
    });
    assert_captured(&meta, "rename over a file", |m| {
        m.rename(dir, "f1", other, "victim").unwrap()
    });
    assert_captured(&meta, "setattr size", |m| {
        m.setattr(f1, Some(0o600), None, None, Some(10), None, None)
            .unwrap()
    });
    assert_captured(&meta, "set_manifest", |m| {
        m.set_manifest(f1, &[4, 5], 2).unwrap()
    });
    assert_captured(&meta, "set_manifest_dirty", |m| {
        m.set_manifest_dirty(f1, None, &[6, 7], 2, &[]).unwrap()
    });
    assert_captured(&meta, "set_manifest_with_base", |m| {
        let base = m.manifest(f1).unwrap().unwrap();
        m.set_manifest_with_base(f1, Some(&base), &[8], 1).unwrap()
    });
    assert_captured(&meta, "set_xattr (spilled set)", |m| {
        m.set_xattr(f1, "user.new", b"x", SetXattrMode::Set)
            .unwrap()
    });
    assert_captured(&meta, "remove_xattr (spilled set)", |m| {
        m.remove_xattr(f1, "user.k3").unwrap()
    });
    assert_captured(&meta, "record_snapshot", |m| {
        m.record_snapshot(&SnapshotRow::new("snap1", "/", "s1", "def", 1))
            .unwrap()
    });
    // Plan 32 §0.4: a hold is a journaled `ns` write like any other, so a
    // stranded one must roll back to exactly the unheld row.
    assert_captured(&meta, "set_snapshot_hold", |m| {
        m.set_snapshot_hold(&snap0, true, Some("csi:content-uid"), false)
            .unwrap()
            .expect("the row seeded above")
    });
    assert_captured(&meta, "delete_snapshot", |m| {
        assert!(m.delete_snapshot("/", "s0").unwrap())
    });
    assert_captured(&meta, "write_quota", |m| {
        m.write_quota(Some(1 << 30)).unwrap()
    });
    assert_captured(&meta, "eager_clone", |m| {
        m.eager_clone(
            "/d",
            "snap0",
            "abc",
            "cloned",
            &[CloneSpec {
                parent_index: None,
                name: "cloned".into(),
                kind: InodeKind::Dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                size: 0,
                mtime_ns: 1,
                target: None,
                manifest: None,
                xattrs: vec![],
            }],
        )
        .unwrap()
    });
    assert_captured(&meta, "publish_file", |m| {
        let ino = m.allocate_ino(ROOT_INO).unwrap();
        m.publish_file(ROOT_INO, "pub", ino, 0o644, 0, 0, 1, &[9], 1, &[], false)
            .unwrap()
    });
    assert_captured(&meta, "execute_mutate with a rid", |m| {
        execute_mutate(
            m,
            &MutateOp::Create {
                parent: dir,
                name: "by-rid".into(),
                ino: (3 << 40) | 77,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(rid(1)),
        )
        .unwrap()
    });
    assert_eq!(
        meta.completed_position(rid(1)).unwrap(),
        None,
        "a stranded rid must not look completed"
    );
}

#[test]
fn a_shipped_transaction_retires_its_local_speculation() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    for name in ["a", "b", "c"] {
        meta.create(ROOT_INO, name, 0o644, 0, 0).unwrap();
    }
    assert_eq!(meta.speculation_counts().unwrap().local, 3);
    assert!(
        !meta.has_outstanding_speculation(),
        "a holder's own work is not requester speculation"
    );
    let rows = meta.take_journal(1).unwrap();
    meta.ack_journal_rows_at(&[rows[0].0], 1).unwrap();
    assert_eq!(meta.speculation_counts().unwrap().local, 2);
    ship_all(&meta, 2);
    let counts = meta.speculation_counts().unwrap();
    assert_eq!((counts.local, counts.outstanding), (0, 0));
    assert_eq!(meta.applied_seq().unwrap(), 2);
    assert_eq!(basis(&meta), PublishBasis::AsIs);
}

/// A holder deposed with an unshipped create: the next epoch's segment
/// strands it, the create is rolled back, its rid no longer looks
/// completed, and its op is queued for replay by that rid.
#[test]
fn a_segment_from_a_later_epoch_strands_a_deposed_holders_journal() {
    let meta = Meta::open_in_memory().unwrap();
    let reference = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: "mine".into(),
        ino: (3 << 40) | 1,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    execute_mutate(&meta, &op, Some(rid(1))).unwrap();
    assert!(meta.completed_position(rid(1)).unwrap().is_some());
    meta.set_holder_epoch(0);

    let winner = vec![LogRecord::Create {
        parent: ROOT_INO,
        name: "theirs".into(),
        ino: (4 << 40) | 1,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: 5,
    }];
    let applied = meta
        .apply_segment(1, 2, &winner, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.stranded.locals, 1);
    assert!(meta.lookup(ROOT_INO, "mine").unwrap().is_none());
    assert!(meta.lookup(ROOT_INO, "theirs").unwrap().is_some());
    assert_eq!(meta.completed_position(rid(1)).unwrap(), None);
    assert_eq!(meta.journal_len().unwrap(), 0);
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!((queued[0].rid, &queued[0].op), (rid(1), &op));

    reference.apply_records(&winner).unwrap();
    assert_eq!(
        meta.dump_replicated().unwrap(),
        reference.dump_replicated().unwrap()
    );
    assert_eq!(meta.usage_bytes_files(), reference.usage_bytes_files());
}

/// A late, unfenced segment tailed while this node holds unshipped work
/// belongs *before* that work in the log: the replica must equal the
/// segment followed by the local records, and the publish view must equal
/// the segment alone.
#[test]
fn a_segment_tailed_under_local_speculation_is_inserted_before_it() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(2);
    meta.create(ROOT_INO, "local", 0o644, 0, 0).unwrap();
    meta.setattr(ROOT_INO, Some(0o700), None, None, None, None, None)
        .unwrap();
    let local_records: Vec<LogRecord> = meta
        .take_journal(usize::MAX)
        .unwrap()
        .into_iter()
        .map(|(_, r)| r)
        .collect();
    let late = vec![
        LogRecord::Create {
            parent: ROOT_INO,
            name: "late".into(),
            ino: (4 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 5,
        },
        LogRecord::Setattr {
            ino: ROOT_INO,
            mode: Some(0o711),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
            time_ns: 6,
        },
    ];
    // Epoch 1 < 2: a late write of the previous holder, not a deposition.
    let applied = meta
        .apply_segment(1, 1, &late, &TouchSet::default())
        .unwrap();
    assert!(applied.inserted_before_local);
    assert!(!applied.stranded.any());
    assert_eq!(meta.speculation_counts().unwrap().local, 2);

    let log_order = Meta::open_in_memory().unwrap();
    log_order.apply_records(&late).unwrap();
    let prefix = log_order.ns_dump().unwrap();
    log_order.apply_records(&local_records).unwrap();
    assert_eq!(
        meta.dump_replicated().unwrap(),
        log_order.dump_replicated().unwrap(),
        "the local transactions are redone on top of the segment"
    );
    let published = published_view(&meta, &basis(&meta));
    let prefix: BTreeMap<Vec<u8>, Vec<u8>> = prefix.into_iter().collect();
    assert_eq!(
        published.keys().collect::<Vec<_>>(),
        prefix.keys().collect::<Vec<_>>()
    );
    // The genesis root differs in timestamps between the two replicas;
    // the root's mode is what the segment set and the local op changed.
    let root_key = constellation_mtree::keys::inode(ROOT_INO);
    let mode_of = |bytes: &[u8]| {
        constellation_mtree::record::InodeRecord::decode(bytes)
            .unwrap()
            .attrs
            .mode
    };
    assert_eq!(mode_of(&published[&root_key]), 0o711);

    // Deposed now: rolling back the local work leaves the segment.
    let stranded = meta.strand_below_epoch(3).unwrap();
    assert_eq!(stranded.locals, 2);
    assert!(meta.lookup(ROOT_INO, "late").unwrap().is_some());
    assert!(meta.lookup(ROOT_INO, "local").unwrap().is_none());
    let root_now = meta
        .ns_dump()
        .unwrap()
        .into_iter()
        .find(|(k, _)| *k == root_key)
        .unwrap()
        .1;
    assert_eq!(mode_of(&root_now), 0o711);
}

/// The publish rule: a holder with unshipped work publishes the log
/// prefix at `applied_seq` — exactly the state it last shipped.
#[test]
fn a_holder_publishes_the_log_prefix_by_before_image_substitution() {
    let meta = Meta::open_in_memory().unwrap();
    let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let f = meta.create(dir, "f", 0o644, 0, 0).unwrap().ino;
    for i in 0..40 {
        meta.set_xattr(f, &format!("user.k{i}"), b"vvvvvvvv", SetXattrMode::Set)
            .unwrap();
    }
    ship_all(&meta, 1);
    let shipped: BTreeMap<Vec<u8>, Vec<u8>> = raw_ns(&meta).into_iter().collect();
    assert_eq!(basis(&meta), PublishBasis::AsIs);

    meta.set_holder_epoch(1);
    meta.create(dir, "g", 0o644, 0, 0).unwrap();
    meta.set_xattr(f, "user.k1", b"changed", SetXattrMode::Set)
        .unwrap();
    meta.remove_xattr(f, "user.k2").unwrap();
    meta.rename(dir, "f", ROOT_INO, "f-moved").unwrap();
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    let b = basis(&meta);
    assert!(matches!(b, PublishBasis::Substituted(_)), "{b:?}");
    assert_eq!(published_view(&meta, &b), shipped);

    // The inode's published form reads its spilled xattrs through the
    // overlay too.
    let PublishBasis::Substituted(view) = &b else {
        unreachable!()
    };
    let inode = meta
        .read_consistent(|snap| meta.tree_inode_via_at(snap, view, f))
        .unwrap()
        .unwrap();
    assert_eq!(inode.manifest, None);
    let k1 = inode
        .xattrs
        .iter()
        .find(|(n, _)| n == "user.k1")
        .map(|(_, v)| v.clone());
    assert_eq!(k1.as_deref(), Some(&b"vvvvvvvv"[..]));
    assert!(inode.xattrs.iter().any(|(n, _)| n == "user.k2"));

    // Requester speculation still defers the publish.
    meta.install_hint(
        &[LogRecord::Create {
            parent: ROOT_INO,
            name: "hinted".into(),
            ino: (5 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }],
        10,
        1,
    )
    .unwrap();
    assert_eq!(basis(&meta), PublishBasis::Defer);
}

/// A forward reply accepted at a lower epoch than the one this node now
/// holds is not installed: its op is queued for replay instead.
#[test]
fn a_forward_reply_older_than_the_held_epoch_is_queued_not_installed() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(3);
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: "late-reply".into(),
        ino: (5 << 40) | 1,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    let recs = vec![
        LogRecord::Create {
            parent: ROOT_INO,
            name: "late-reply".into(),
            ino: (5 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        },
        LogRecord::Completed { rid: rid(9) },
    ];
    assert!(!meta.install_shadow(rid(9), 2, &op, &recs).unwrap());
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.lookup(ROOT_INO, "late-reply").unwrap().is_none());
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].rid, rid(9));
    // A reply from the held epoch itself installs normally.
    meta.set_holder_epoch(2);
    assert!(meta.install_shadow(rid(10), 2, &op, &recs).unwrap());
}

/// An op's records and its `Completed { rid }` never land in different
/// segments: batches and byte cuts stop on transaction boundaries.
#[test]
fn a_cut_never_splits_a_transaction() {
    let meta = Meta::open_in_memory().unwrap();
    execute_mutate(
        &meta,
        &MutateOp::Create {
            parent: ROOT_INO,
            name: "one".into(),
            ino: (3 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        Some(rid(1)),
    )
    .unwrap();
    execute_mutate(
        &meta,
        &MutateOp::Publish {
            ino: (3 << 40) | 2,
            parent: ROOT_INO,
            name: "two".into(),
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 1,
            manifest: b"M".to_vec(),
            size: 1,
            xattrs: vec![("user.x".into(), b"y".to_vec())],
            noreplace: false,
        },
        Some(rid(2)),
    )
    .unwrap();
    let all = meta.take_journal(usize::MAX).unwrap();
    assert_eq!(all.len(), 2 + 4);
    // Cutting inside the second transaction backs off to the first.
    assert_eq!(meta.whole_tx_prefix(&all, 3).unwrap(), 2);
    assert_eq!(meta.whole_tx_prefix(&all, 5).unwrap(), 2);
    // Cutting inside the first (and only candidate) takes it whole.
    assert_eq!(meta.whole_tx_prefix(&all[..], 1).unwrap(), 2);
    assert_eq!(meta.whole_tx_prefix(&all, 6).unwrap(), 6);
    // A count-limited read runs on to the end of the transaction.
    let grouped = meta.take_journal_grouped(3).unwrap();
    assert_eq!(grouped[0].1.len(), 6);
    let grouped = meta.take_journal_grouped(1).unwrap();
    assert_eq!(grouped[0].1.len(), 2);
}

/// A stranded transaction without an op of its own is replayed from its
/// records: a snapshot row comes back as `MutateOp::Records`, which
/// re-applies on the new holder.
#[test]
fn a_stranded_snapshot_row_replays_as_records() {
    let deposed = Meta::open_in_memory().unwrap();
    let holder = Meta::open_in_memory().unwrap();
    deposed.set_node_prefix(3).unwrap();
    deposed.set_holder_epoch(1);
    deposed
        .record_snapshot(&SnapshotRow::new("s", "/", "snap", "h", 7))
        .unwrap();
    let stranded = deposed.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.locals, 1);
    let queued = deposed.pending_replays().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].rid.node, 3);
    assert_eq!(
        queued[0].rid.incarnation,
        constellation_meta::LOCAL_REPLAY_INCARNATION
    );
    assert!(
        matches!(&queued[0].op, MutateOp::Records { records }
            if matches!(records.as_slice(), [LogRecord::SnapCreate2 { id, .. }] if id == "s")),
        "{:?}",
        queued[0].op
    );
    execute_mutate(&holder, &queued[0].op, Some(queued[0].rid)).unwrap();
    assert!(holder.completed_position(queued[0].rid).unwrap().is_some());
    assert_eq!(holder.snapshots(None).unwrap().len(), 1);
}

/// A local manifest commit (no op of its own) is replayed as an
/// optimistic `SetManifest`, so it is refused if the log moved the
/// manifest on meanwhile.
#[test]
fn a_stranded_manifest_commit_replays_as_an_optimistic_set_manifest() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest(f, b"base", 4).unwrap();
    ship_all(&meta, 1);
    meta.set_holder_epoch(1);
    meta.set_manifest_dirty(f, Some(b"base"), b"mine", 4, &[])
        .unwrap();
    meta.strand_below_epoch(2).unwrap();
    let queued = meta.pending_replays().unwrap();
    assert_eq!(
        queued[0].op,
        MutateOp::SetManifest {
            ino: f,
            base_manifest: Some(b"base".to_vec()),
            manifest: b"mine".to_vec(),
            size: 4,
        }
    );
    assert_eq!(meta.manifest(f).unwrap().as_deref(), Some(&b"base"[..]));
}

/// With capture off (the performance gate's fallback), a holder's writes
/// still record their transaction (boundary and op) but carry no
/// speculation; its publish defers while the journal is non-empty, and a
/// wholesale rebuild queues the journal for replay.
#[test]
fn with_capture_off_a_holder_defers_publishing_and_rebuilds_on_deposition() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_capture(false);
    meta.set_holder_epoch(1);
    execute_mutate(
        &meta,
        &MutateOp::Mkdir {
            parent: ROOT_INO,
            name: "uncaptured".into(),
            ino: (3 << 40) | 1,
            mode: 0o755,
            uid: 0,
            gid: 0,
        },
        Some(rid(1)),
    )
    .unwrap();
    assert_eq!(meta.speculation_counts().unwrap().local, 0);
    assert_eq!(basis(&meta), PublishBasis::Defer);

    let side = Meta::open_in_memory().unwrap();
    side.mkdir(ROOT_INO, "from-the-log", 0o755, 0, 0).unwrap();
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "uncaptured").unwrap().is_none());
    assert!(meta.lookup(ROOT_INO, "from-the-log").unwrap().is_some());
    assert_eq!(meta.journal_len().unwrap(), 0);
    assert_eq!(meta.completed_position(rid(1)).unwrap(), None);
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].rid, rid(1));
}

/// A one-chunk manifest of `len` bytes whose chunk hashes `tag`.
fn one_chunk_manifest(tag: &[u8], len: u64) -> Vec<u8> {
    use constellation_fs_core::{ChunkHash, Manifest};
    let chunks = [(0u64, ChunkHash::of(tag))].into_iter().collect();
    Manifest::from_sparse_chunks(1 << 20, len, chunks, 8, ChunkHash::of)
        .0
        .encode()
}

/// Harness `deposed-reintegration`'s `a-only`, in the store: a deposed
/// holder's `O_TRUNC` rewrite of a file — a size-only `setattr` to 0,
/// then the write session's manifest commit — strands as those two ops,
/// and the replay drain folds the truncate into the commit. A truncate
/// clips the stored manifest (`replay::clip_manifest`), so the commit
/// composed on the *clipped* manifest; queued, it must carry the
/// manifest the truncate cut as its base instead, or on the successor
/// (which never ran the truncate) it is refused as a stale base even
/// when nobody else touched the file — a conflict copy for a clean edit.
/// Rebased, it is accepted there, and still refused where another node
/// wrote the file since (the genuine edit-vs-edit overlap).
#[test]
fn a_stranded_truncate_then_write_replays_on_the_uncut_base() {
    let holder = Meta::open_in_memory().unwrap();
    let successor = Meta::open_in_memory().unwrap();
    let overwritten = Meta::open_in_memory().unwrap();
    let f = holder.create(ROOT_INO, "a-only", 0o644, 0, 0).unwrap().ino;
    let baseline = one_chunk_manifest(b"baseline-a", 10);
    holder.set_manifest(f, &baseline, 10).unwrap();
    // The baseline ships: every replica has it.
    let rows = holder.take_journal(usize::MAX).unwrap();
    let records: Vec<LogRecord> = rows.iter().map(|(_, r)| r.clone()).collect();
    successor.apply_records(&records).unwrap();
    overwritten.apply_records(&records).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    holder.ack_journal_rows_at(&seqs, 1).unwrap();
    // Another node's write, on one replica only.
    let theirs = one_chunk_manifest(b"winner-from-b", 13);
    execute_mutate(
        &overwritten,
        &MutateOp::SetManifest {
            ino: f,
            base_manifest: Some(baseline.clone()),
            manifest: theirs.clone(),
            size: 13,
        },
        Some(rid(9)),
    )
    .unwrap();

    // The holder's O_TRUNC rewrite, unshipped when it is deposed.
    holder.set_holder_epoch(1);
    let truncate = MutateOp::Setattr {
        ino: f,
        mode: None,
        uid: None,
        gid: None,
        size: Some(0),
        atime_ns: None,
        mtime_ns: None,
    };
    execute_mutate(&holder, &truncate, Some(rid(1))).unwrap();
    let clipped = holder.manifest(f).unwrap().expect("a clipped manifest");
    assert_ne!(clipped, baseline, "the truncate clips the manifest");
    let mine = one_chunk_manifest(b"stranded-from-a", 15);
    holder
        .set_manifest_dirty(f, Some(&clipped), &mine, 15, &[])
        .unwrap();
    holder.set_holder_epoch(0);
    holder.strand_below_epoch(2).unwrap();
    assert_eq!(holder.manifest(f).unwrap(), Some(baseline.clone()));

    let queued = holder.pending_replays().unwrap();
    assert_eq!(queued.len(), 2, "{queued:?}");
    assert_eq!(queued[0].op, truncate);
    let MutateOp::SetManifest {
        base_manifest,
        manifest,
        ..
    } = &queued[1].op
    else {
        panic!("the write replays as a manifest commit: {:?}", queued[1].op)
    };
    assert_eq!(
        base_manifest.as_deref(),
        Some(baseline.as_slice()),
        "the commit is rebased onto the manifest the folded truncate cut"
    );
    assert_eq!(manifest, &mine);

    // Folded into the commit (the drain forgets the truncate), it lands
    // where nobody else wrote the file ...
    execute_mutate(&successor, &queued[1].op, Some(queued[1].rid)).unwrap();
    assert_eq!(successor.manifest(f).unwrap(), Some(mine.clone()));
    assert_eq!(successor.getattr(f).unwrap().unwrap().size, 15);
    // ... and is refused where somebody did.
    assert!(
        execute_mutate(&overwritten, &queued[1].op, Some(queued[1].rid)).is_err(),
        "an edit-vs-edit overlap must still fail the base check"
    );
    assert_eq!(overwritten.manifest(f).unwrap(), Some(theirs));
}

/// The rebase applies only to a commit composed on exactly the cut: one
/// whose base is something else keeps it (and fails its check as
/// before), and a truncate that cut nothing leaves the commit alone.
#[test]
fn a_stranded_commit_not_composed_on_the_cut_keeps_its_base() {
    let holder = Meta::open_in_memory().unwrap();
    let f = holder.create(ROOT_INO, "f", 0o644, 0, 0).unwrap().ino;
    let baseline = one_chunk_manifest(b"baseline", 8);
    holder.set_manifest(f, &baseline, 8).unwrap();
    ship_all(&holder, 1);
    holder.set_holder_epoch(1);
    let truncate = |size| MutateOp::Setattr {
        ino: f,
        mode: None,
        uid: None,
        gid: None,
        size: Some(size),
        atime_ns: None,
        mtime_ns: None,
    };
    // A truncate-up cuts nothing: the commit's base is the uncut manifest.
    execute_mutate(&holder, &truncate(20), Some(rid(1))).unwrap();
    let grown = one_chunk_manifest(b"grown", 20);
    holder
        .set_manifest_dirty(f, Some(&baseline), &grown, 20, &[])
        .unwrap();
    // A truncate whose following commit composed on some other base.
    execute_mutate(&holder, &truncate(0), Some(rid(2))).unwrap();
    let elsewhere = one_chunk_manifest(b"elsewhere", 9);
    let last = one_chunk_manifest(b"last", 4);
    // The holder itself would refuse a commit on that base, so it is
    // queued behind the stranded truncate directly.
    holder.set_holder_epoch(0);
    holder.strand_below_epoch(2).unwrap();
    holder
        .queue_replay(
            rid(3),
            &MutateOp::SetManifest {
                ino: f,
                base_manifest: Some(elsewhere.clone()),
                manifest: last,
                size: 4,
            },
        )
        .unwrap();
    let queued = holder.pending_replays().unwrap();
    let bases: Vec<Option<Vec<u8>>> = queued
        .iter()
        .filter_map(|q| match &q.op {
            MutateOp::SetManifest { base_manifest, .. } => Some(base_manifest.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(bases, vec![Some(baseline), Some(elsewhere)], "{queued:?}");
}
