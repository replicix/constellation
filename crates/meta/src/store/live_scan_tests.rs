//! O1: what one `StreamAhead` install costs in scans of the outstanding
//! entries (`spec::live_scan`), as the speculation outstanding since the
//! last segment grows.

use super::spec::live_scan;
use super::Meta;
use crate::mutate::MutateOp;
use crate::{LogRecord, MetaStore, Rid};
use constellation_fs_core::types::ROOT_INO;

const HOLDER: u64 = 7;
const EPOCH: u64 = 1;

fn rid(seq: u64) -> Rid {
    Rid {
        node: 2,
        incarnation: 1,
        seq,
    }
}

/// A replica with one outstanding shadow (so the install's out-of-place
/// check has a cutoff to look at, as on a backup that also serves a
/// delegate's replies), then `n` streamed creates of one epoch with no
/// segment in between: the entries decoded by each install.
fn decoded_per_install(n: u64) -> Vec<u64> {
    let meta = Meta::open_in_memory().unwrap();
    let ino = |i: u64| (HOLDER << 40) | i;
    let t = constellation_fs_core::types::now_ns();
    let shadow = vec![
        LogRecord::Create {
            parent: ROOT_INO,
            name: "shadow".into(),
            ino: ino(1_000_000),
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: t,
        },
        LogRecord::Completed {
            rid: rid(1_000_000),
        },
    ];
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: "shadow".into(),
        ino: ino(1_000_000),
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    assert!(meta
        .install_shadow(rid(1_000_000), EPOCH, &op, &shadow)
        .unwrap());
    let mut out = Vec::new();
    for i in 1..=n {
        let recs = vec![
            LogRecord::Create {
                parent: ROOT_INO,
                name: format!("f{i}"),
                ino: ino(i),
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: t + i as i64,
            },
            LogRecord::Completed { rid: rid(i) },
        ];
        let before = live_scan::totals().1;
        meta.install_streamed(EPOCH, HOLDER, i, i, &recs).unwrap();
        out.push(live_scan::totals().1 - before);
    }
    out
}

/// The shared read: an install decodes the outstanding entries once
/// (plus a small constant), not once per helper.
#[test]
fn install_streamed_decodes_the_outstanding_entries_once() {
    let n = 400;
    let d = decoded_per_install(n);
    let last = d[n as usize - 1];
    assert!(
        last <= n + 8,
        "install #{n} decoded {last} live entries (outstanding {n}); install #10 decoded {}",
        d[9]
    );
}

/// The index step (Step 2) would make this hold: an install's scans do
/// not grow with the outstanding speculation.
#[test]
#[ignore = "Step 2: needs the streamed-tip / shadow index, not the shared read"]
fn install_streamed_scans_do_not_grow_with_outstanding() {
    let n = 400;
    let d = decoded_per_install(n);
    let (at10, at400) = (d[9], d[n as usize - 1]);
    assert!(
        at400 <= at10 + 8,
        "install #10 decoded {at10} live entries, install #{n} decoded {at400}"
    );
}

const GEN: u64 = 5;

fn delegate_rid(seq: u64) -> Rid {
    Rid {
        node: 2,
        incarnation: 1,
        seq,
    }
}

fn delegate_create(name: &str, idx: u64) -> LogRecord {
    LogRecord::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino: (2 << 40) | idx,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: 5,
    }
}

/// A deposed root that appended the delegate's row `idx` at epoch 1.
fn root_appended(meta: &Meta, idx: u64, name: &str) {
    assert!(meta
        .apply_delegate_tx(
            &[
                delegate_create(name, idx),
                LogRecord::Completed {
                    rid: delegate_rid(idx),
                },
            ],
            Some(delegate_rid(idx)),
            GEN,
            idx,
            crate::Position::ZERO,
        )
        .unwrap());
    meta.session().note_stream(GEN, idx);
}

/// The seed-1678 branch of `install_streamed_committed`: a `StreamAhead`
/// install whose transaction completes the rid a past tenure appended as
/// the root of a delegate's row supersedes that append. The install
/// strands it (`strand_tx`, which rolls the append back and drops its
/// journal row) and reads the outstanding entries again, because the
/// strand changed them. A stale snapshot there would be the one read
/// before the strand: still listing the stranded `Local` row. The scan
/// count fails on its absence in debug and release alike; debug builds
/// also cross-check every later use of the snapshot.
#[test]
fn install_streamed_reads_again_after_a_superseded_root_append_strands() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    root_appended(&meta, 1, "a");
    // Deposed; a later tenure's holder streams the same rid's outcome.
    meta.set_holder_epoch(0);
    assert!(meta.journal_len().unwrap() > 0);
    assert_eq!(meta.speculation_counts().unwrap().local, 1);
    assert!(meta
        .speculation_live_debug()
        .iter()
        .any(|e| e.contains("Local")));

    let records = [
        delegate_create("a", 1),
        LogRecord::Completed {
            rid: delegate_rid(1),
        },
    ];
    let before = live_scan::totals().0;
    meta.install_streamed_committed(2, 9, 1, 2, &records, true, (0, 0))
        .unwrap();
    let scans = live_scan::totals().0 - before;

    // The strand happened: the superseded append left the journal and the
    // `Local` backlog, and its rid's outcome is the stream's.
    assert_eq!(meta.journal_len().unwrap(), 0, "the append was stranded");
    assert_eq!(meta.speculation_counts().unwrap().local, 0);
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());

    // The re-read ran: the snapshot read before the install's helpers, the
    // one inside `strand_tx`, and the re-read after it.
    assert!(
        scans >= 3,
        "install scanned the outstanding entries {scans} times; the re-read after the strand did not run"
    );

    // The installed result equals what a fresh full read gives: the stream's
    // transaction is the only entry, and the tip is its last row.
    let fresh = meta.speculation_live_debug();
    assert_eq!(fresh.len(), 1, "{fresh:?}");
    assert!(fresh[0].contains("Streamed"), "{fresh:?}");
    assert_eq!(meta.streamed_tip(2).unwrap(), Some(2));
}
