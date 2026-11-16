//! The views' coalesced wake-ups of the driver ([`crate::sync::WakeCoalesce`])
//! against a real engine: a flood of them is taken by the driver, which still
//! does what each one was for (ships the journal, answers a drain), and the
//! flags it clears are left clear, so the next wake-up is sent.
use super::fsync_drain_tests::{node, open_fsync_close, view, write_and_close};
use constellation_vfs::Caller;
use std::time::{Duration, Instant};

/// How long a step of the test may take on a host loaded with builds: a
/// bound on liveness, not on speed.
const DEADLINE: Duration = Duration::from_secs(60);

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < DEADLINE, "{what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Writers wake the driver as fast as they can (a `Nudge` per call, as a
/// sealed chunk does) while files are written and an `fsync` drains one:
/// the driver ships the journal, the `fsync` is answered, and once the
/// flood is over the flag is clear again (the driver took what was queued).
/// A flag left set with nothing queued would silence every later wake-up.
#[test]
fn the_driver_ships_and_answers_under_a_flood_of_wakeups() {
    let node = node(false, crate::writeback::WriteMode::Back);
    let view = view(&node);
    let caller = Caller::new(0, 0, Some(1001));
    let wake = node.engine.coalesce().clone();
    let stop = std::sync::atomic::AtomicBool::new(false);
    let meta = node.engine.meta().clone();
    std::thread::scope(|scope| {
        let flooders: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        view.nudge_sync();
                    }
                })
            })
            .collect();
        let mut last = 0;
        for i in 0..20 {
            last = write_and_close(&view, &caller, &format!("f{i}"), b"data");
        }
        // The drain request goes through the same driver, behind the flood.
        open_fsync_close(&view, &caller, last);
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for f in flooders {
            f.join().unwrap();
        }
    });
    // Every row is shipped (the journal empties), and the flag is clear.
    until("the journal never shipped", || {
        constellation_meta::MetaStore::journal_len(&*meta).unwrap() == 0
    });
    until("the nudge flag stayed set with nothing queued", || {
        !wake.nudge_queued()
    });
    // A wake-up after the flood works: a new row ships.
    write_and_close(&view, &caller, "later", b"more");
    view.nudge_sync();
    until("a wake-up after the flood did not ship", || {
        constellation_meta::MetaStore::journal_len(&*meta).unwrap() == 0
    });
}
