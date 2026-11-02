//! L1: a former delegate's replica after the root's pre-S3 stream and the
//! segment that carry its own row, the generation's `RecallCut` and a later
//! root row on the same name must equal the log prefix.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MetaStore, MutateOp, Rid, TouchSet};
use constellation_types::clock::Now;

const GEN: u64 = 1;
const HOLDER: u64 = 1;

fn rid(node: u64, seq: u64) -> Rid {
    Rid {
        node,
        incarnation: 1,
        seq,
    }
}

fn mkdir(name: &str, ino: u64) -> LogRecord {
    LogRecord::Mkdir {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o755,
        uid: 0,
        gid: 0,
        time_ns: 1,
    }
}

/// The root's later row on `x` (after the recall): an unlink, or a rename
/// over to `y`.
#[derive(Clone, Copy, Debug)]
enum Later {
    Unlink,
    Rename,
}

/// This node as the delegate of `d` (generation 1), with its own row
/// `create d/x` (idx 1) and an outstanding shadow in `e` older than it.
struct Fx {
    dir: tempfile::TempDir,
    reopen: bool,
    meta: Meta,
    d: u64,
    e: u64,
    x_ino: u64,
    x_recs: Vec<LogRecord>,
}

impl Fx {
    fn open(dir: &tempfile::TempDir, reopen: bool) -> Meta {
        if reopen {
            Meta::open(dir.path()).unwrap()
        } else {
            Meta::open_in_memory().unwrap()
        }
    }

    fn new(reopen: bool) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let meta = Fx::open(&dir, reopen);
        let (d, e) = ((2 << 40) | 1, (2 << 40) | 2);
        meta.apply_segment(1, 1, &[mkdir("d", d), mkdir("e", e)], &TouchSet::default())
            .unwrap();
        // An outstanding shadow older than the delegate's own row: this
        // node's op in `e`, answered and not in the log yet.
        let s_rid = rid(2, 1);
        let s_ino = (2 << 40) | 10;
        let s_op = MutateOp::Create {
            parent: e,
            name: "s".into(),
            ino: s_ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let s_recs = vec![
            LogRecord::Create {
                parent: e,
                name: "s".into(),
                ino: s_ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 2,
            },
            LogRecord::Completed { rid: s_rid },
        ];
        assert!(meta.install_shadow(s_rid, 1, &s_op, &s_recs).unwrap());
        // This node, the delegate of `d` (generation 1), executes
        // `create d/x`.
        let x_ino = (2 << 40) | 20;
        let x_op = MutateOp::Create {
            parent: d,
            name: "x".into(),
            ino: x_ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let (x_recs, idx) = meta
            .delegate_execute(
                &x_op,
                Some(rid(1, 5)),
                GEN,
                Default::default(),
                &Default::default(),
                Now::same(0),
            )
            .unwrap();
        assert_eq!(idx, 1);
        Fx {
            dir,
            reopen,
            meta,
            d,
            e,
            x_ino,
            x_recs,
        }
    }

    fn restart(&mut self) {
        let placeholder = Meta::open_in_memory().unwrap();
        drop(std::mem::replace(&mut self.meta, placeholder));
        self.meta = Fx::open(&self.dir, self.reopen);
    }

    fn recall(&self) -> LogRecord {
        LogRecord::RecallCut {
            dir: self.d,
            gen: GEN,
            through: 1,
        }
    }

    /// The root's own later row on `x`, and its `Completed`.
    fn root_tx(&self, later: Later) -> Vec<LogRecord> {
        let d = self.d;
        let later_rec = match later {
            Later::Unlink => LogRecord::Unlink {
                parent: d,
                name: "x".into(),
                time_ns: 9,
            },
            Later::Rename => LogRecord::Rename {
                parent: d,
                name: "x".into(),
                new_parent: d,
                new_name: "y".into(),
                time_ns: 9,
            },
        };
        vec![later_rec, LogRecord::Completed { rid: rid(1, 8) }]
    }

    /// The segment that carries all five rows (own row 1..=2, the recall
    /// 3, the root's rows 4..=5).
    fn apply_segment(&self, later: Later) {
        let mut records = self.x_recs.clone();
        records.push(self.recall());
        records.extend(self.root_tx(later));
        assert_eq!(records.len(), 5);
        self.meta
            .apply_segment_rows(
                2,
                1,
                5,
                &[1, 2, 3, 4, 5],
                &[(GEN, 1), (GEN, 1), (0, 0), (0, 0), (0, 0)],
                &records,
                &TouchSet::default(),
            )
            .unwrap();
    }
}

fn run(later: Later, via_stream: bool, reopen: bool) {
    let mut fx = Fx::new(reopen);
    let d = fx.d;
    // The root appends the own row (rows 1..=2), recalls the generation
    // (row 3) and removes the name itself (rows 4..=5).
    if via_stream {
        // The root's pre-S3 stream delivers the three transactions first.
        fx.meta
            .install_streamed_committed(1, HOLDER, 1, 2, &fx.x_recs, true, (GEN, 1))
            .unwrap();
        if reopen {
            // The mark is the store's, not the session's.
            fx.restart();
        }
        fx.meta
            .install_streamed_committed(
                1,
                HOLDER,
                3,
                3,
                std::slice::from_ref(&fx.recall()),
                true,
                (0, 0),
            )
            .unwrap();
        fx.meta
            .install_streamed_committed(1, HOLDER, 4, 5, &fx.root_tx(later), true, (0, 0))
            .unwrap();
        assert!(
            fx.meta.lookup(d, "x").unwrap().is_none(),
            "{later:?}: the stream's later root row is under the delegate's own row"
        );
    }
    fx.apply_segment(later);
    let meta = &fx.meta;
    assert!(
        meta.lookup(d, "x").unwrap().is_none(),
        "{later:?} (stream {via_stream}): the former delegate keeps the removed name"
    );
    if let Later::Rename = later {
        let y = meta.lookup(d, "y").unwrap().expect("renamed");
        assert_eq!(y.ino, fx.x_ino);
    }
    assert_eq!(meta.speculation_counts().unwrap().local, 0);
    assert!(
        meta.lookup(fx.e, "s").unwrap().is_some(),
        "the shadow stays"
    );
    // The log's `RecallCut` cleared the generation's mark.
    assert_eq!(meta.deleg_streamed(GEN).unwrap(), None);
}

#[test]
fn a_former_delegates_own_row_then_the_roots_unlink_in_one_segment() {
    run(Later::Unlink, false, false);
}

#[test]
fn a_former_delegates_own_row_then_the_roots_rename_in_one_segment() {
    run(Later::Rename, false, false);
}

#[test]
fn a_streamed_root_unlink_after_a_former_delegates_own_row_keeps_log_order() {
    run(Later::Unlink, true, false);
}

#[test]
fn a_streamed_root_rename_after_a_former_delegates_own_row_keeps_log_order() {
    run(Later::Rename, true, false);
}

#[test]
fn the_streamed_order_of_a_former_delegates_own_row_survives_a_restart() {
    run(Later::Unlink, true, true);
}

#[test]
fn a_streamed_recall_cut_keeps_the_mark_and_the_durable_one_clears_it() {
    let fx = Fx::new(false);
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), None);
    // The root's append of the own row marks it, in the stream's epoch.
    fx.meta
        .install_streamed_committed(1, HOLDER, 1, 2, &fx.x_recs, true, (GEN, 1))
        .unwrap();
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), Some((1, 1)));
    // A later stream of the same epoch does not lower it (a re-delivery).
    fx.meta
        .install_streamed_committed(1, HOLDER, 1, 2, &fx.x_recs, true, (GEN, 1))
        .unwrap();
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), Some((1, 1)));
    // The streamed `RecallCut` arrives before the root's later rows: the
    // mark must still order the own row ahead of them.
    fx.meta
        .install_streamed_committed(
            1,
            HOLDER,
            3,
            3,
            std::slice::from_ref(&fx.recall()),
            true,
            (0, 0),
        )
        .unwrap();
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), Some((1, 1)));
    // The log's `RecallCut` ends the generation: the mark goes.
    fx.apply_segment(Later::Unlink);
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), None);
}

/// Pins the mechanism, not a wanted state: `x` is present at the end, which
/// is the inverted order. It holds only for this synthetic log: an epoch 2
/// root cannot unlink a live generation's name before its re-ship has
/// delivered the own row, so the epoch 1 mark must not order the own row
/// ahead of an epoch 2 row.
#[test]
fn a_mark_of_an_older_epoch_orders_nothing() {
    let fx = Fx::new(false);
    fx.meta
        .install_streamed_committed(1, HOLDER, 1, 2, &fx.x_recs, true, (GEN, 1))
        .unwrap();
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), Some((1, 1)));
    // A successor root's stream (epoch 2) delivers a row on `x` without
    // having delivered the own row: the own row is still later than it.
    fx.meta
        .install_streamed_committed(2, HOLDER, 1, 2, &fx.root_tx(Later::Unlink), true, (0, 0))
        .unwrap();
    assert!(
        fx.meta.lookup(fx.d, "x").unwrap().is_some(),
        "the own row is redone after the epoch 2 row, as without a mark"
    );
    // Its own append, delivered in epoch 2, replaces the mark.
    fx.meta
        .install_streamed_committed(2, HOLDER, 3, 4, &fx.x_recs, true, (GEN, 1))
        .unwrap();
    assert_eq!(fx.meta.deleg_streamed(GEN).unwrap(), Some((2, 1)));
}
