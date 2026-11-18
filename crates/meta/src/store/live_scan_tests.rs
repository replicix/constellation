//! O1: what one `StreamAhead` install costs in scans of the outstanding
//! entries (`spec::live_scan`), as the speculation outstanding since the
//! last segment grows.

use super::spec::live_scan;
use super::Meta;
use crate::mutate::MutateOp;
use crate::{LogRecord, Rid};
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
