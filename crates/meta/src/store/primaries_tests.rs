//! The `primaries` index against the from-start scan it replaces
//! (`store::primaries`, invariant I-P): a property test over a real
//! `Meta` through every write path that reaches the `ns` funnel, and the
//! targeted cases that pin the documented corners.

use crate::delegation::Namespace;
use crate::error::MetaError;
use crate::locks::LockTag;
use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::replay::TouchSet;
use crate::rid::Rid;
use crate::store::primaries::{self, PrimaryRow};
use crate::store::spec::ShadowInstall;
use crate::store::{BootstrapIndexBuilder, Meta, KV_PRIMARIES_INDEX};
use constellation_fs_core::types::{now_ns, ROOT_INO};
use constellation_mtree::keys;
use proptest::prelude::*;
use std::cell::Cell;
use std::collections::HashSet;

/// Directories above 2^32, and the root, which sorts first.
const PARENTS: [u64; 5] = [
    ROOT_INO,
    (1 << 33) | 2,
    (1 << 40) | 3,
    (1 << 48) | 4,
    (7 << 40) | 5,
];
const FILES: [u64; 3] = [(1 << 40) | 9, (1 << 40) | 10, (1 << 40) | 11];
const GEN: u64 = 5;

/// The from-start scan: the `(parent, name)` of every `0x04` key of `ino`,
/// in key order.
fn scan(meta: &Meta, ino: u64) -> Vec<(u64, String)> {
    let r = meta.db.read_tx();
    let range = keys::names_of(ino);
    use fjall::Readable;
    r.range(&meta.ns, crate::store::ns::key_range_bounds(&range))
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            match keys::Key::parse(&k).unwrap() {
                keys::Key::RDentry {
                    parent_ino, name, ..
                } => (parent_ino, String::from_utf8_lossy(name).into_owned()),
                other => panic!("{other:?} in the 0x04 range"),
            }
        })
        .collect()
}

fn dump(ks: &fjall::SingleWriterTxKeyspace, meta: &Meta) -> Vec<(Vec<u8>, Vec<u8>)> {
    use fjall::Readable;
    let r = meta.db.read_tx();
    r.iter(ks)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// A store with the delegation that makes the names readable at all.
fn delegated_store(dirs: &[u64]) -> Meta {
    let meta = Meta::open_in_memory().unwrap();
    let mkdirs: Vec<LogRecord> = dirs
        .iter()
        .filter(|p| **p != ROOT_INO)
        .map(|p| LogRecord::Mkdir {
            parent: ROOT_INO,
            name: format!("d{p}"),
            ino: *p,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        })
        .collect();
    meta.apply_records(&mkdirs).unwrap();
    meta.apply_segment(
        1,
        1,
        &[LogRecord::delegate_one(
            4,
            (1 << 32) | 1,
            PARENTS[1],
            crate::SubtreeKind::Placed,
        )],
        &TouchSet::default(),
    )
    .unwrap();
    meta
}

/// Everything the index promises, for `files`, on one snapshot of `meta`.
fn verify(meta: &Meta, files: &[u64]) -> Result<(), TestCaseError> {
    if let Err(e) = meta.check_primary_index() {
        return Err(TestCaseError::fail(format!("check_primary_index: {e}")));
    }
    for &f in files {
        let want = scan(meta, f);
        for k in [1usize, 2, 3, usize::MAX] {
            let got = meta.names_of_ino(f, k);
            prop_assert_eq!(
                &got.names[..],
                &want[..k.min(want.len())],
                "inode {} limit {}",
                f,
                k
            );
            prop_assert!(!got.complete || got.names.len() == want.len());
            prop_assert!(got.complete || got.names.len() == k);
            prop_assert!(
                want.len() >= k || got.complete,
                "short of the limit must be complete"
            );
        }
        prop_assert_eq!(
            meta.primary_parent(f),
            want.first().map(|n| n.0),
            "primary_parent of {}",
            f
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum Op1 {
    Create {
        f: usize,
        p: usize,
        n: String,
    },
    Link {
        f: usize,
        p: usize,
        n: String,
    },
    /// `pick` chooses the source among the names the store has now, so the
    /// record applies whenever there is one; `(p, n)` stands in when the
    /// files have none.
    Unlink {
        p: usize,
        n: String,
        pick: usize,
    },
    Rename {
        p: usize,
        n: String,
        np: usize,
        nn: String,
        pick: usize,
    },
    Exchange {
        p: usize,
        n: String,
        np: usize,
        nn: String,
        pick: usize,
    },
}

#[derive(Debug, Clone)]
enum Op {
    One(Op1),
    Batch(Vec<Op1>),
    /// A shadow of the batch, rolled back by stranding its generation.
    Rolled(Vec<Op1>),
    /// A shadow of the batch, then a segment that rewinds and redoes it.
    Redone(Vec<Op1>, Vec<Op1>),
    Flush,
    Bootstrap,
}

fn name() -> impl Strategy<Value = String> {
    "[ab~\u{e9}\u{65e5}]{1,6}"
}

fn op1() -> impl Strategy<Value = Op1> {
    let f = 0..FILES.len();
    let p = 0..PARENTS.len();
    prop_oneof![
        1 => (f.clone(), p.clone(), name()).prop_map(|(f, p, n)| Op1::Create { f, p, n }),
        5 => (f, p.clone(), name()).prop_map(|(f, p, n)| Op1::Link { f, p, n }),
        5 => (p.clone(), name(), any::<usize>()).prop_map(|(p, n, pick)| Op1::Unlink { p, n, pick }),
        3 => (p.clone(), name(), p.clone(), name(), any::<usize>())
            .prop_map(|(p, n, np, nn, pick)| Op1::Rename { p, n, np, nn, pick }),
        2 => (p.clone(), name(), p, name(), any::<usize>())
            .prop_map(|(p, n, np, nn, pick)| Op1::Exchange { p, n, np, nn, pick }),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        16 => op1().prop_map(Op::One),
        3 => prop::collection::vec(op1(), 1..=6).prop_map(Op::Batch),
        1 => prop::collection::vec(op1(), 1..=4).prop_map(Op::Rolled),
        1 => (prop::collection::vec(op1(), 1..=4), prop::collection::vec(op1(), 1..=3))
            .prop_map(|(a, b)| Op::Redone(a, b)),
        1 => Just(Op::Flush),
        1 => Just(Op::Bootstrap),
    ]
}

/// A deterministic-per-case clock, above the root's creation stamp.
struct Clock(i64);

impl Clock {
    fn tick(&mut self) -> i64 {
        self.0 += 1;
        self.0
    }
}

/// Every `(parent, name)` the files hold now, in a fixed order.
fn live_names(meta: &Meta) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for f in FILES {
        for (parent, name) in meta.names_of_ino(f, usize::MAX).names {
            if let Some(p) = PARENTS.iter().position(|x| *x == parent) {
                out.push((p, name));
            }
        }
    }
    out
}

/// The source of an unlink, rename or exchange: the `pick`th live name
/// when there is one, so most of these records apply.
fn source(meta: &Meta, pick: usize, p: usize, n: &str) -> (usize, String) {
    let live = live_names(meta);
    if live.is_empty() {
        (p, n.to_owned())
    } else {
        live[pick % live.len()].clone()
    }
}

fn record(meta: &Meta, clock: &mut Clock, op: &Op1) -> Option<LogRecord> {
    let time_ns = clock.tick();
    Some(match op {
        Op1::Create { f, p, n } => LogRecord::Create {
            parent: PARENTS[*p],
            name: n.clone(),
            ino: FILES[*f],
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns,
        },
        Op1::Link { f, p, n } => LogRecord::Link {
            ino: FILES[*f],
            parent: PARENTS[*p],
            name: n.clone(),
            time_ns,
        },
        Op1::Unlink { p, n, pick } => {
            let (p, n) = source(meta, *pick, *p, n);
            LogRecord::Unlink {
                parent: PARENTS[p],
                name: n,
                time_ns,
            }
        }
        Op1::Rename { p, n, np, nn, pick } => {
            let (p, n) = source(meta, *pick, *p, n);
            LogRecord::Rename {
                parent: PARENTS[p],
                name: n,
                new_parent: PARENTS[*np],
                new_name: nn.clone(),
                time_ns,
            }
        }
        Op1::Exchange { p, n, np, nn, pick } => {
            let (p, n) = source(meta, *pick, *p, n);
            let (np, nn) = source(meta, *pick / 7 + 1, *np, nn);
            LogRecord::Exchange {
                parent: PARENTS[p],
                new_parent: PARENTS[np],
                time_ns,
                ino: meta.child_ino(PARENTS[p], &n).ok().flatten()?,
                new_ino: meta.child_ino(PARENTS[np], &nn).ok().flatten()?,
                name: n,
                new_name: nn,
            }
        }
    })
}

fn records(meta: &Meta, clock: &mut Clock, ops: &[Op1]) -> Vec<LogRecord> {
    ops.iter().filter_map(|o| record(meta, clock, o)).collect()
}

/// The wholesale copy and the ingestion of `meta`'s namespace, checked
/// against `meta`'s rows.
fn bootstrap_copies(meta: &Meta) -> Result<(), TestCaseError> {
    // The wholesale copy (`replace_ns_from_rebuilt`).
    let fresh = Meta::open_in_memory().unwrap();
    fresh
        .replace_ns_from_rebuilt(meta, &HashSet::new())
        .unwrap();
    if let Err(e) = fresh.check_primary_index() {
        return Err(TestCaseError::fail(format!("wholesale copy: {e}")));
    }
    // Ingestion and the builder, fed in key order.
    let ingested = ingest_copy(meta);
    if let Err(e) = ingested.check_primary_index() {
        return Err(TestCaseError::fail(format!("ingestion: {e}")));
    }
    // Both copies see the keys at once: their rows are tight (`ONE` exactly
    // when one name). The store they copy may hold a conservative row (a
    // name that came and went clears `ONE` for good): same primary, and
    // its `ONE` only where the copy's is.
    let tight = dump(&ingested.primaries, &ingested);
    prop_assert_eq!(dump(&fresh.primaries, &fresh), tight.clone());
    // Streamed, a run after every settled row.
    let streamed = stream_copy(meta, 1);
    if let Err(e) = streamed.check_primary_index() {
        return Err(TestCaseError::fail(format!("streamed ingestion: {e}")));
    }
    prop_assert_eq!(dump(&streamed.primaries, &streamed), tight.clone());
    let own = dump(&meta.primaries, meta);
    prop_assert_eq!(own.len(), tight.len());
    for ((ka, va), (kb, vb)) in own.iter().zip(&tight) {
        prop_assert_eq!(ka, kb);
        let (a, b) = (
            PrimaryRow::decode(va).unwrap(),
            PrimaryRow::decode(vb).unwrap(),
        );
        prop_assert_eq!(a.parent(), b.parent());
        prop_assert_eq!(a.name(), b.name());
        prop_assert!(!a.is_one() || b.is_one());
    }
    Ok(())
}

/// A new store holding `meta`'s `ns` by ingestion, its `primaries` from the
/// builder.
fn ingest_copy(meta: &Meta) -> Meta {
    let fresh = Meta::open_in_memory().unwrap();
    fresh.begin_bootstrap().unwrap();
    let mut builder = BootstrapIndexBuilder::new();
    let rows = meta.ns_dump().unwrap();
    for page in rows.chunks(64) {
        for (k, _) in page {
            if k.first() == Some(&keys::RANGE_RDENTRY) {
                builder.observe_name(k);
            }
        }
        fresh.ns_ingest_page(page).unwrap();
    }
    fresh.apply_bootstrap_indexes(builder).unwrap();
    fresh
}

/// As [`ingest_copy`], with the primary rows streamed into their own
/// ingestion on a second thread, a run whenever more than `min` settle
/// (`BootstrapIndexBuilder::take_settled_primaries`, as `load_tree` does).
fn stream_copy(meta: &Meta, min: usize) -> Meta {
    let fresh = Meta::open_in_memory().unwrap();
    assert!(fresh.primaries_ingestible().unwrap());
    fresh.begin_bootstrap().unwrap();
    let mut builder = BootstrapIndexBuilder::new();
    let rows = meta.ns_dump().unwrap();
    std::thread::scope(|s| {
        let (runs, rx) = std::sync::mpsc::sync_channel(1);
        let target = &fresh;
        let worker = s.spawn(move || target.ingest_primary_runs(rx));
        for page in rows.chunks(64) {
            for (k, _) in page {
                builder.observe_name(k);
                if let Some(run) = builder.take_settled_primaries(min) {
                    runs.send(Some(run)).unwrap();
                }
            }
            fresh.ns_ingest_page(page).unwrap();
        }
        runs.send(Some(builder.take_all_primaries())).unwrap();
        runs.send(None).unwrap();
        worker.join().unwrap().unwrap();
    });
    fresh.apply_bootstrap_indexes(builder).unwrap();
    fresh
}

/// What one run's shadow installs did (the run's own, not shared with a
/// parallel test).
#[derive(Default)]
struct Tally {
    installed: Cell<usize>,
    unapplied: Cell<usize>,
    /// Installed shadows whose records unlink or rename a name: a batch with
    /// a skipped record installs nothing, so each of these applied.
    moved: Cell<usize>,
    /// Of those, the ones a rollback then undid.
    moved_rolled_back: Cell<usize>,
}

/// Count what a shadow install did. A batch that skips a record installs
/// nothing (`Unapplied`): fine, but counted, so the rollback and redo cases
/// cannot silently stop installing. Any other outcome is a test bug.
fn count_install(
    tally: &Tally,
    got: ShadowInstall,
    recs: &[LogRecord],
    rolled_back: bool,
) -> Result<(), TestCaseError> {
    match got {
        ShadowInstall::Installed => {
            tally.installed.set(tally.installed.get() + 1);
            if recs
                .iter()
                .any(|r| matches!(r, LogRecord::Unlink { .. } | LogRecord::Rename { .. }))
            {
                tally.moved.set(tally.moved.get() + 1);
                if rolled_back {
                    tally
                        .moved_rolled_back
                        .set(tally.moved_rolled_back.get() + 1);
                }
            }
        }
        ShadowInstall::Unapplied => tally.unapplied.set(tally.unapplied.get() + 1),
        other => return Err(TestCaseError::fail(format!("shadow install: {other:?}"))),
    };
    Ok(())
}

fn rid(n: u64) -> Rid {
    Rid {
        node: 2,
        incarnation: 1,
        seq: n,
    }
}

/// One case of the property: `ops` through every write path, `verify`
/// after each.
fn run_case(ops: &[Op], final_copy: bool, tally: &Tally) -> Result<(), TestCaseError> {
    let meta = delegated_store(&PARENTS);
    let mut clock = Clock(now_ns() + 3_600_000_000_000);
    let first: Vec<LogRecord> = FILES
        .iter()
        .enumerate()
        .map(|(i, f)| LogRecord::Create {
            parent: PARENTS[i],
            name: format!("f{i}"),
            ino: *f,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: clock.tick(),
        })
        .collect();
    meta.apply_records(&first).unwrap();
    verify(&meta, &FILES)?;
    let (mut seq, mut next_rid) = (2u64, 1u64);
    for op in ops {
        match op {
            Op::One(o) => {
                let recs = records(&meta, &mut clock, std::slice::from_ref(o));
                meta.apply_records(&recs).unwrap();
            }
            Op::Batch(b) => {
                let recs = records(&meta, &mut clock, b);
                meta.apply_records(&recs).unwrap();
            }
            Op::Rolled(b) => {
                let mut recs = records(&meta, &mut clock, b);
                recs.push(LogRecord::Completed { rid: rid(next_rid) });
                let op = MutateOp::Records {
                    records: recs.clone(),
                };
                let got = meta
                    .install_shadow_from(rid(next_rid), 1, GEN, &op, &LockTag::NONE, &recs)
                    .unwrap();
                next_rid += 1;
                count_install(tally, got, &recs, true)?;
                verify(&meta, &FILES)?;
                meta.strand_recalled_speculation(GEN).unwrap();
            }
            Op::Redone(shadow, segment) => {
                let mut recs = records(&meta, &mut clock, shadow);
                recs.push(LogRecord::Completed { rid: rid(next_rid) });
                let op = MutateOp::Records {
                    records: recs.clone(),
                };
                let got = meta.install_shadow(rid(next_rid), 1, &op, &recs).unwrap();
                next_rid += 1;
                count_install(tally, got, &recs, false)?;
                verify(&meta, &FILES)?;
                let seg = records(&meta, &mut clock, segment);
                meta.apply_segment(seq, 1, &seg, &TouchSet::default())
                    .unwrap();
                seq += 1;
            }
            Op::Flush => {
                meta.ns.inner().rotate_memtable_and_wait().unwrap();
                meta.primaries.inner().rotate_memtable_and_wait().unwrap();
            }
            Op::Bootstrap => bootstrap_copies(&meta)?,
        }
        verify(&meta, &FILES)?;
    }
    if final_copy {
        bootstrap_copies(&meta)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// After every op, whichever write path it took (replayed records,
    /// batches, a rolled-back shadow, a rewind that redoes one, a memtable
    /// flush, the bootstrap copies), the index holds and every bounded name
    /// read is the scan's prefix.
    #[test]
    fn the_primary_rows_equal_the_scan_after_every_op(
        ops in prop::collection::vec(op(), 1..=40),
        final_copy in prop::bool::weighted(0.25),
    ) {
        run_case(&ops, final_copy, &Tally::default())?;
    }
}

/// The rollback and redo cases must keep installing shadows: main's
/// `install_shadow` installs nothing when a record skips, and a property
/// that quietly stopped installing would test nothing.
#[test]
fn the_shadow_cases_install_shadows() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let tally = Tally::default();
    runner
        .run(
            &(
                prop::collection::vec(op(), 1..=40),
                prop::bool::weighted(0.25),
            ),
            |(ops, final_copy)| run_case(&ops, final_copy, &tally),
        )
        .unwrap();
    let (got, moved, rolled) = (
        tally.installed.get(),
        tally.moved.get(),
        tally.moved_rolled_back.get(),
    );
    eprintln!(
        "shadow installs: {got} installed, {} unapplied, {moved} with an unlink or rename, \
         {rolled} of those rolled back",
        tally.unapplied.get()
    );
    assert!(
        got >= 100,
        "only {got} shadows installed ({} unapplied)",
        tally.unapplied.get()
    );
    assert!(
        moved >= 100 && rolled >= 40,
        "only {moved} installed shadows moved or removed a name ({rolled} rolled back)"
    );
}

/// A store with `n` names for one file under one directory, the base the
/// stress tool's order leaves the primary at, delegated.
fn stress_ng_store(n: usize) -> (Meta, u64, u64) {
    let dir = (1u64 << 33) | 2;
    let meta = delegated_store(&[dir]);
    let f = (1u64 << 40) | 9;
    let t = now_ns() + 3_600_000_000_000;
    let mut recs = vec![LogRecord::Create {
        parent: dir,
        name: "sng-1-0-18446744073709551615".into(),
        ino: f,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: t,
    }];
    recs.extend((0..n).map(|i| LogRecord::Link {
        ino: f,
        parent: dir,
        name: format!("sng-1-0-{i}"),
        time_ns: t + 1 + i as i64,
    }));
    meta.apply_records(&recs).unwrap();
    (meta, f, dir)
}

/// The documented conservative case: the row follows the scan's first key
/// through every unlink of the stress tool's order, `ONE` is never set
/// again, so the last name left reads incomplete at limit 1.
#[test]
fn a_primary_row_follows_the_stress_ng_unlink_order() {
    let n = 2048;
    let (meta, f, dir) = stress_ng_store(n);
    let t = now_ns() + 3_700_000_000_000;
    let row_of = |meta: &Meta| {
        let r = meta.db.read_tx();
        primaries::primary_of(&r, &meta.primaries, f)
            .unwrap()
            .unwrap()
    };
    for i in 0..n {
        meta.apply_records(&[LogRecord::Unlink {
            parent: dir,
            name: format!("sng-1-0-{i}"),
            time_ns: t + i as i64,
        }])
        .unwrap();
        let names = scan(&meta, f);
        let row = row_of(&meta);
        assert_eq!(
            (row.parent(), row.name_lossy()),
            names[0].clone(),
            "after unlink {i}"
        );
        assert!(
            !row.is_one(),
            "after unlink {i}: {} names left",
            names.len()
        );
        if i % 128 == 0 {
            meta.check_primary_index().unwrap();
        }
    }
    let names = scan(&meta, f);
    assert_eq!(
        names,
        vec![(dir, "sng-1-0-18446744073709551615".to_string())]
    );
    let row = row_of(&meta);
    assert!(!row.is_one(), "the row never sets ONE again");
    assert_eq!(row.name(), b"sng-1-0-18446744073709551615");
    assert!(!meta.names_of_ino(f, 1).complete);
    assert!(meta.names_of_ino(f, 2).complete);
    meta.check_primary_index().unwrap();
}

/// A speculative unlink of the primary moves the row to the next name; the
/// rollback (through `restore_key_tx`, which writes through the funnel)
/// moves it back.
#[test]
fn a_rolled_back_unlink_of_the_primary_restores_its_row() {
    let dir = (1u64 << 33) | 2;
    let meta = delegated_store(&[dir]);
    let f = (1u64 << 40) | 9;
    let t = now_ns() + 3_600_000_000_000;
    meta.apply_records(&[
        LogRecord::Create {
            parent: dir,
            name: "b".into(),
            ino: f,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: t,
        },
        LogRecord::Link {
            ino: f,
            parent: dir,
            name: "c".into(),
            time_ns: t + 1,
        },
        LogRecord::Link {
            ino: f,
            parent: dir,
            name: "a".into(),
            time_ns: t + 2,
        },
    ])
    .unwrap();
    let before = dump(&meta.primaries, &meta);
    let recs = vec![
        LogRecord::Unlink {
            parent: dir,
            name: "a".into(),
            time_ns: t + 3,
        },
        LogRecord::Completed { rid: rid(1) },
    ];
    let op = MutateOp::Records {
        records: recs.clone(),
    };
    assert_eq!(
        meta.install_shadow_from(rid(1), 1, GEN, &op, &LockTag::NONE, &recs)
            .unwrap(),
        ShadowInstall::Installed
    );
    assert_ne!(
        dump(&meta.primaries, &meta),
        before,
        "the shadow moved the primary"
    );
    meta.check_primary_index().unwrap();
    meta.strand_recalled_speculation(GEN).unwrap();
    assert_eq!(dump(&meta.primaries, &meta), before);
    meta.check_primary_index().unwrap();
    assert_eq!(meta.names_of_ino(f, 1).names, vec![(dir, "a".to_string())]);
}

#[test]
fn a_store_without_the_index_marker_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let meta = Meta::open(dir.path()).unwrap();
        assert_eq!(meta.kv_get(KV_PRIMARIES_INDEX).unwrap(), Some("1".into()));
        meta.local.remove(KV_PRIMARIES_INDEX).unwrap();
        meta.sync().unwrap();
    }
    match Meta::open(dir.path()) {
        Err(MetaError::Invalid(msg)) => {
            assert!(msg.contains("predates the primaries index"), "{msg}")
        }
        Err(other) => panic!("not the marker refusal: {other}"),
        Ok(_) => panic!("a store without the marker opened"),
    }
}

/// A crash between the first ingested page and `apply_bootstrap_indexes`
/// leaves names without rows. The store must not open as a complete
/// replica then (it would answer "no names, complete"); finishing the
/// bootstrap makes it open again.
#[test]
fn a_bootstrap_that_never_finished_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let page = vec![(keys::rdentry(FILES[0], PARENTS[1], b"x"), Vec::new())];
    {
        let meta = Meta::open(dir.path()).unwrap();
        meta.begin_bootstrap().unwrap();
        meta.ns_ingest_page(&page).unwrap();
        // The process dies here: no `apply_bootstrap_indexes`.
    }
    assert_refused(dir.path());
    // Not even an open that gets as far as the marker check opens it, twice.
    assert_refused(dir.path());
    let finished = tempfile::tempdir().unwrap();
    {
        let meta = Meta::open(finished.path()).unwrap();
        let mut builder = BootstrapIndexBuilder::new();
        builder.observe_name(&page[0].0);
        meta.begin_bootstrap().unwrap();
        meta.ns_ingest_page(&page).unwrap();
        meta.apply_bootstrap_indexes(builder).unwrap();
        meta.finish_bootstrap().unwrap();
    }
    Meta::open(finished.path()).unwrap();
}

/// Power loss after the first ingested page: the page's tables were
/// fsynced when they were registered, but a buffered commit reaches only
/// the OS. The marker must be on the disk by then, or the journal replays
/// the genesis without it and `Meta::open` takes the partial store for a
/// complete one. Fail-before: the marker was committed buffered, never
/// synced.
#[test]
fn the_marker_survives_a_power_loss_after_the_first_page() {
    let dir = tempfile::tempdir().unwrap();
    let page = vec![(keys::rdentry(FILES[0], PARENTS[1], b"x"), Vec::new())];
    {
        let meta = Meta::open(dir.path()).unwrap();
        // The genesis reached the disk (the tree fetch outlasted the
        // kernel's writeback delay); the marker's commit is not synced.
        meta.arm_power_loss_for_tests().unwrap();
        meta.begin_bootstrap().unwrap();
        meta.ns_ingest_page(&page).unwrap();
        meta.drop_losing_unsynced_for_tests();
    }
    assert_refused(dir.path());
}

/// The same for the streamed rows' own ingestion (the worker thread's
/// first call marks the store).
#[test]
fn the_marker_survives_a_power_loss_after_the_streamed_rows() {
    let dir = tempfile::tempdir().unwrap();
    {
        let meta = Meta::open(dir.path()).unwrap();
        meta.arm_power_loss_for_tests().unwrap();
        let mut builder = BootstrapIndexBuilder::new();
        builder.observe_name(&keys::rdentry(FILES[0], PARENTS[1], b"x"));
        let (runs, rx) = std::sync::mpsc::sync_channel(2);
        runs.send(Some(builder.take_all_primaries())).unwrap();
        runs.send(None).unwrap();
        meta.begin_bootstrap().unwrap();
        meta.ingest_primary_runs(rx).unwrap();
        meta.drop_losing_unsynced_for_tests();
    }
    assert_refused(dir.path());
}

/// The window after the derived rows are in and before the caller has
/// replayed the log and recorded the commit: a crash there (even one that
/// loses nothing, the journal intact) is refused, so the node bootstraps
/// again rather than tailing the log from segment 1 over a tree already at
/// the commit. Finishing the bootstrap clears the marker durably.
/// Fail-before: `apply_bootstrap_indexes` cleared the marker itself.
#[test]
fn a_crash_between_the_load_and_the_finish_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let page = vec![(keys::rdentry(FILES[0], PARENTS[1], b"x"), Vec::new())];
    {
        let meta = Meta::open(dir.path()).unwrap();
        let mut builder = BootstrapIndexBuilder::new();
        builder.observe_name(&page[0].0);
        meta.begin_bootstrap().unwrap();
        meta.ns_ingest_page(&page).unwrap();
        meta.apply_bootstrap_indexes(builder).unwrap();
        // The replay of the log tail, the recorded commit: not yet.
        meta.drop_without_close_for_tests();
    }
    assert_refused(dir.path());
    let finished = tempfile::tempdir().unwrap();
    {
        let meta = Meta::open(finished.path()).unwrap();
        meta.arm_power_loss_for_tests().unwrap();
        let mut builder = BootstrapIndexBuilder::new();
        builder.observe_name(&page[0].0);
        meta.begin_bootstrap().unwrap();
        meta.ns_ingest_page(&page).unwrap();
        meta.apply_bootstrap_indexes(builder).unwrap();
        meta.finish_bootstrap().unwrap();
        // Power loss right after the finish: the clear was synced.
        meta.drop_losing_unsynced_for_tests();
    }
    Meta::open(finished.path()).unwrap();
}

/// The streamed rows' own ingestion marks the store too: a crash after the
/// rows went in, before the indexes did, is refused.
#[test]
fn a_streamed_bootstrap_that_never_finished_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let meta = Meta::open(dir.path()).unwrap();
        let mut builder = BootstrapIndexBuilder::new();
        builder.observe_name(&keys::rdentry(FILES[0], PARENTS[1], b"x"));
        let (runs, rx) = std::sync::mpsc::sync_channel(2);
        runs.send(Some(builder.take_all_primaries())).unwrap();
        runs.send(None).unwrap();
        meta.begin_bootstrap().unwrap();
        meta.ingest_primary_runs(rx).unwrap();
    }
    assert_refused(dir.path());
}

/// The ingestion path's rows are the funnel's, byte for byte, after a
/// history with renames, exchanges and unlinks.
#[test]
fn bootstrap_rows_equal_the_funnels_rows() {
    let meta = delegated_store(&PARENTS);
    let t = now_ns() + 3_600_000_000_000;
    let mut recs = Vec::new();
    for (i, f) in FILES.iter().enumerate() {
        recs.push(LogRecord::Create {
            parent: PARENTS[i],
            name: format!("f{i}"),
            ino: *f,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: t + i as i64,
        });
        for j in 0..5 {
            recs.push(LogRecord::Link {
                ino: *f,
                parent: PARENTS[(i + j) % PARENTS.len()],
                name: format!("l{}", 9 - j),
                time_ns: t + 10 + (i * 5 + j) as i64,
            });
        }
    }
    meta.apply_records(&recs).unwrap();
    meta.apply_records(&[LogRecord::Unlink {
        parent: PARENTS[0],
        name: "f0".into(),
        time_ns: t + 100,
    }])
    .unwrap();
    meta.check_primary_index().unwrap();
    let want = dump(&meta.primaries, &meta);
    assert!(!want.is_empty());
    let ingested = ingest_copy(&meta);
    assert_eq!(dump(&ingested.primaries, &ingested), want);
    for min in [0, 1, 2, 1000] {
        let streamed = stream_copy(&meta, min);
        streamed.check_primary_index().unwrap();
        assert_eq!(dump(&streamed.primaries, &streamed), want, "min {min}");
    }
    let copied = Meta::open_in_memory().unwrap();
    copied
        .replace_ns_from_rebuilt(&meta, &HashSet::new())
        .unwrap();
    assert_eq!(dump(&copied.primaries, &copied), want);
}

/// A builder fed `0x04` keys out of key order (a later inode first, or a
/// lower name of the same inode after a higher one) is refused, and so is a
/// `0x04` key without a name.
#[test]
fn a_disordered_bootstrap_builder_is_refused() {
    let (a, b) = (FILES[0], FILES[1]);
    let p = PARENTS[1];
    let cases: [Vec<Vec<u8>>; 3] = [
        vec![keys::rdentry(b, p, b"x"), keys::rdentry(a, p, b"x")],
        vec![keys::rdentry(a, p, b"y"), keys::rdentry(a, p, b"x")],
        vec![keys::names_of(a)
            .start()
            .iter()
            .chain(&p.to_be_bytes())
            .copied()
            .collect()],
    ];
    for fed in cases {
        let fresh = Meta::open_in_memory().unwrap();
        let mut builder = BootstrapIndexBuilder::new();
        for k in &fed {
            builder.observe_name(k);
        }
        assert!(
            fresh.apply_bootstrap_indexes(builder).is_err(),
            "{fed:02x?} was taken"
        );
    }
}

/// `PrimaryRow` rows of single-name inodes carry `ONE`.
#[test]
fn a_single_name_inode_has_a_one_row_and_a_complete_read() {
    let dir = (1u64 << 33) | 2;
    let meta = delegated_store(&[dir]);
    let f = (1u64 << 40) | 9;
    meta.apply_records(&[LogRecord::Create {
        parent: dir,
        name: "x".into(),
        ino: f,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: now_ns() + 3_600_000_000_000,
    }])
    .unwrap();
    let r = meta.db.read_tx();
    let row: PrimaryRow = primaries::primary_of(&r, &meta.primaries, f)
        .unwrap()
        .unwrap();
    assert!(row.is_one());
    let got = meta.names_of_ino(f, 1);
    assert_eq!(got.names, vec![(dir, "x".to_string())]);
    assert!(got.complete);
}

/// `Meta::open` refuses the store as an interrupted bootstrap; a failure
/// prints what it did instead (opened, or another error).
#[track_caller]
fn assert_refused(path: &std::path::Path) {
    let opened = Meta::open(path).map(|_| "opened");
    assert!(
        matches!(opened, Err(MetaError::BootstrapInterrupted)),
        "expected BootstrapInterrupted, got {opened:?}"
    );
}
