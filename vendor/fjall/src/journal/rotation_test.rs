// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): journal rotation
// without fsyncs under the journal lock, and its crash ordering.
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "tests: a failed step is a failed test"
)]

use super::rotation::{test_hooks, RotationPoint};
use crate::{Database, KeyspaceCreateOptions};
use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use test_log::test;

/// Every journal sync of the database is slowed by this much.
const SLOW_SYNC: Duration = Duration::from_millis(300);

/// Commits while the journal rotates under slow fsyncs: none of them may
/// wait for a sync. Upstream's rotation held the journal lock across three
/// syncs (the sealed journal's, the new file's, the folder's), so a commit
/// that came during a rotation waited 600–900 ms here.
#[test]
fn commits_never_wait_for_a_rotation_sync() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().to_path_buf();
    test_hooks::set_threshold(&folder, 1 << 20);
    test_hooks::set_slow_sync(&folder, SLOW_SYNC);
    let rotations = Arc::new(AtomicUsize::new(0));
    {
        let rotations = rotations.clone();
        test_hooks::set_hook(&folder, move |point| {
            if point == RotationPoint::Sealed {
                rotations.fetch_add(1, Ordering::SeqCst);
            }
        });
    }

    let db = Database::builder(&folder).worker_threads(2).open()?;
    let big = db.keyspace("big", || {
        KeyspaceCreateOptions::default().max_memtable_size(512 << 10)
    })?;
    let small = db.keyspace("small", KeyspaceCreateOptions::default)?;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (small, stop) = (small, stop.clone());
        std::thread::spawn(move || -> crate::Result<Vec<Duration>> {
            let mut latencies = Vec::new();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                small.insert(i.to_be_bytes(), b"row")?;
                latencies.push(started.elapsed());
                i += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(latencies)
        })
    };

    let value = vec![7u8; 64 << 10];
    let started = Instant::now();
    let mut i = 0u64;
    while rotations.load(Ordering::SeqCst) < 3 && started.elapsed() < Duration::from_secs(60) {
        big.insert(i.to_be_bytes(), &value)?;
        i += 1;
        if i.is_multiple_of(16) {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    // The last rotation's sync runs on: commits after it must not wait either.
    std::thread::sleep(SLOW_SYNC);
    stop.store(true, Ordering::Relaxed);
    let mut latencies = writer.join().expect("writer panicked")?;
    test_hooks::clear(&folder);

    latencies.sort();
    let max = *latencies.last().expect("commits");
    log::info!(
        "{} rotations, {} commits: p50 {:?}, max {max:?}",
        rotations.load(Ordering::SeqCst),
        latencies.len(),
        latencies.get(latencies.len() / 2),
    );
    assert!(rotations.load(Ordering::SeqCst) >= 3, "the journal rotated");
    assert!(
        max < SLOW_SYNC / 2,
        "a commit waited {max:?} during a journal rotation (syncs take {SLOW_SYNC:?})"
    );
    Ok(())
}

/// Copies a database folder: what a process crash leaves (every write that
/// reached the OS).
fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Loses a journal's bytes from `from` on, as a power loss loses what was
/// not synced (the file keeps its pre-allocated length, read back as
/// zeros).
fn lose_tail(journal: &Path, from: u64) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new().write(true).open(journal)?;
    let len = file.metadata()?.len();
    file.set_len(from)?;
    file.set_len(len)
}

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

/// The keys a database folder recovers: they must be `0..n`, a prefix of
/// the writes, with `n >= durable`.
fn recovered_prefix(folder: &Path, durable: u64) -> crate::Result<u64> {
    let db = Database::builder(folder).worker_threads(1).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    let mut n = 0u64;
    for guard in ks.iter() {
        let k = guard.key()?;
        assert_eq!(
            &*k,
            &key(n),
            "{}: not a prefix of the writes",
            folder.display()
        );
        n += 1;
    }
    assert!(
        n >= durable,
        "{}: recovered {n} writes, {durable} were acknowledged as durable",
        folder.display(),
    );
    Ok(n)
}

/// Crash images taken at one point of a rotation, recovered.
struct Images {
    /// Writes acknowledged as durable (a `SyncAll` persist) before the
    /// rotation.
    durable: u64,
    /// Writes in the sealed journal.
    in_sealed: u64,
    /// Every write made.
    total: u64,
    /// What each image recovered: (name, keys).
    recovered: Vec<(String, u64)>,
}

/// Writes keys into a journal, persists some of them durably, rotates the
/// journal and, at `point`, writes more and takes crash images:
/// - `process`: the folder as is (a process crash);
/// - `power-N`: the sealed journal's unsynced tail lost from its N-th
///   cut on (the first cut is the durable length), the new journal kept
///   whole, as if the kernel had written its pages back first.
fn crash_at(point: RotationPoint) -> crate::Result<Images> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("db");
    let images = dir.path().join("images");
    test_hooks::set_threshold(&folder, 64 << 10);

    let db = Database::builder(&folder).worker_threads(1).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    let value = vec![3u8; 1 << 10];

    let durable = 100;
    for i in 0..durable {
        ks.insert(key(i), &value)?;
    }
    db.persist(crate::PersistMode::SyncAll)?;
    let sealed_path = db.supervisor.journal.path()?;
    let durable_len = db.supervisor.journal.get_writer()?.pos()?;

    // The sealed journal's unsynced tail.
    let in_sealed_before = 140;
    for i in durable..in_sealed_before {
        ks.insert(key(i), &value)?;
    }
    let full_len = db.supervisor.journal.get_writer()?.pos()?;

    // During the rotation: 40 more writes (into the new journal from
    // `Swapped` on), then the images.
    let during = 40;
    let state = Arc::new(std::sync::Mutex::new(None::<(u64, Vec<String>)>));
    {
        let (ks, value, state) = (ks.clone(), value, state.clone());
        let (folder, images, sealed_path) = (folder.clone(), images.clone(), sealed_path.clone());
        test_hooks::set_hook(&folder.clone(), move |at| {
            if at != point {
                return;
            }
            for i in in_sealed_before..in_sealed_before + during {
                ks.insert(key(i), &value).expect("insert");
            }
            let in_sealed = if point == RotationPoint::Prepared {
                in_sealed_before + during
            } else {
                in_sealed_before
            };
            let mut names = vec![String::from("process")];
            copy_dir(&folder, &images.join("process")).expect("copy");
            if point != RotationPoint::Sealed {
                // Cuts from the durable length up to the tail written before
                // the rotation (at `Prepared` the writes made here went to
                // the sealed journal too, and are lost with every cut).
                for n in 0..4 {
                    let cut = durable_len + (full_len - durable_len) * n / 4;
                    let name = format!("power-{n}");
                    let image = images.join(&name);
                    copy_dir(&folder, &image).expect("copy");
                    lose_tail(&image.join(sealed_path.file_name().expect("name")), cut)
                        .expect("cut");
                    names.push(name);
                }
            }
            *state.lock().expect("lock") = Some((in_sealed, names));
        });
    }

    ks.rotate_memtable_and_wait()?;
    test_hooks::clear(&folder);
    let (in_sealed, names) = state
        .lock()
        .expect("lock")
        .take()
        .expect("the rotation reached the point");
    assert_ne!(
        db.supervisor.journal.path()?,
        sealed_path,
        "the journal rotated"
    );
    drop(ks);
    drop(db);

    let mut recovered = Vec::new();
    for name in names {
        let n = recovered_prefix(&images.join(&name), durable)?;
        recovered.push((name, n));
    }
    Ok(Images {
        durable,
        in_sealed,
        total: in_sealed_before + during,
        recovered,
    })
}

#[test]
fn a_crash_before_the_swap_recovers_a_prefix() -> crate::Result<()> {
    let images = crash_at(RotationPoint::Prepared)?;
    log::info!("prepared: {:?}", images.recovered);
    for (name, n) in &images.recovered {
        if name == "process" {
            assert_eq!(*n, images.total, "a process crash loses nothing");
        } else {
            assert!(*n <= images.in_sealed);
        }
    }
    Ok(())
}

/// The case the rotation marker is for: the sealed journal's tail is lost
/// while the new journal's pages, marker included, reached the disk.
#[test]
fn a_crash_before_the_sealed_journal_is_synced_recovers_a_prefix() -> crate::Result<()> {
    let images = crash_at(RotationPoint::Swapped)?;
    log::info!("swapped: {:?}", images.recovered);
    for (name, n) in &images.recovered {
        match name.as_str() {
            "process" => assert_eq!(*n, images.total, "a process crash loses nothing"),
            // The tail lost from the durable length on: exactly the durable
            // writes, the new journal discarded.
            "power-0" => assert_eq!(*n, images.durable),
            _ => assert!(
                *n < images.in_sealed,
                "{name}: the new journal is discarded"
            ),
        }
    }
    Ok(())
}

#[test]
fn a_crash_after_the_sealed_journal_is_synced_loses_nothing() -> crate::Result<()> {
    let images = crash_at(RotationPoint::Sealed)?;
    log::info!("sealed: {:?}", images.recovered);
    assert_eq!(
        images.recovered,
        vec![(String::from("process"), images.total)]
    );
    Ok(())
}

/// A durable persist after the swap returns only once the sealed journal
/// is synced: that is what makes everything before it durable.
#[test]
fn a_durable_persist_after_the_swap_waits_for_the_sealed_journal() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().to_path_buf();
    test_hooks::set_threshold(&folder, 64 << 10);
    test_hooks::set_slow_sync(&folder, Duration::from_millis(200));

    let db = Database::builder(&folder).worker_threads(1).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    for i in 0..100 {
        ks.insert(key(i), vec![1u8; 1 << 10])?;
    }

    let sealed = Arc::new(AtomicBool::new(false));
    let persisted = Arc::new(std::sync::Mutex::new(None));
    {
        let (db, ks, sealed, persisted) = (db, ks.clone(), sealed, persisted.clone());
        test_hooks::set_hook(&folder, move |at| match at {
            RotationPoint::Prepared => {}
            RotationPoint::Swapped => {
                ks.insert(key(100), b"after the swap").expect("insert");
                let (db, sealed, persisted) = (db.clone(), sealed.clone(), persisted.clone());
                *persisted.lock().expect("lock") = Some(std::thread::spawn(move || {
                    db.persist(crate::PersistMode::SyncAll).expect("persist");
                    sealed.load(Ordering::SeqCst)
                }));
            }
            RotationPoint::Sealed => sealed.store(true, Ordering::SeqCst),
        });
    }
    ks.rotate_memtable_and_wait()?;
    test_hooks::clear(&folder);

    let persist = persisted
        .lock()
        .expect("lock")
        .take()
        .expect("the rotation swapped");
    assert!(
        persist.join().expect("persist panicked"),
        "the persist returned before the sealed journal was synced"
    );
    Ok(())
}

/// A rotation does not wait for the previous sealed journal's sync (the
/// active journal would grow past the threshold meanwhile), and a durable
/// persist waits for every pending one.
#[test]
fn a_rotation_does_not_wait_for_the_previous_sealed_sync() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().to_path_buf();
    test_hooks::set_threshold(&folder, 64 << 10);

    let db = Database::builder(&folder).worker_threads(2).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;

    let swapped = Arc::new(AtomicUsize::new(0));
    let sealed = Arc::new(AtomicUsize::new(0));
    let (release, held) = std::sync::mpsc::channel::<()>();
    {
        let (swapped, sealed) = (swapped.clone(), sealed.clone());
        let held = std::sync::Mutex::new(held);
        // The first rotation's worker is held before its sealed sync.
        test_hooks::set_hook(&folder, move |at| match at {
            RotationPoint::Prepared => {}
            RotationPoint::Swapped => {
                if swapped.fetch_add(1, Ordering::SeqCst) == 0 {
                    held.lock().expect("lock").recv().expect("release");
                }
            }
            RotationPoint::Sealed => {
                sealed.fetch_add(1, Ordering::SeqCst);
            }
        });
    }
    let wait_for = |what: &str, done: &dyn Fn() -> bool| {
        let started = Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(10), "{what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    };

    for i in 0..100 {
        ks.insert(key(i), vec![1u8; 1 << 10])?;
    }
    ks.rotate_memtable()?;
    wait_for("the first rotation swapped", &|| {
        swapped.load(Ordering::SeqCst) == 1
    });

    for i in 100..200 {
        ks.insert(key(i), vec![1u8; 1 << 10])?;
    }
    ks.rotate_memtable()?;
    wait_for("the second rotation finished, the first sync held", &|| {
        swapped.load(Ordering::SeqCst) == 2 && sealed.load(Ordering::SeqCst) == 1
    });

    let persist = {
        let db = db;
        std::thread::spawn(move || db.persist(crate::PersistMode::SyncAll))
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !persist.is_finished(),
        "a durable persist returned with the first sealed journal not synced"
    );
    release.send(()).expect("release");
    persist.join().expect("persist panicked")?;
    assert_eq!(sealed.load(Ordering::SeqCst), 2);

    test_hooks::clear(&folder);
    Ok(())
}

/// Each journal's contents at its last sync, by file name: what a power
/// loss keeps of it (at worst). Trailing zeros are kept as a length (the
/// pre-allocation).
type Synced = Arc<std::sync::Mutex<HashMap<OsString, (u64, Vec<u8>)>>>;

/// Records into `synced` what every journal sync under `folder` made
/// durable, and the synced paths into `paths`.
fn record_syncs(folder: &Path, synced: &Synced, paths: &Arc<std::sync::Mutex<Vec<PathBuf>>>) {
    let (synced, paths) = (synced.clone(), paths.clone());
    test_hooks::set_sync_hook(folder, move |path| {
        if path.extension().is_none_or(|ext| ext != "jnl") {
            return;
        }
        let mut bytes = std::fs::read(path).expect("read a synced journal");
        let len = bytes.len() as u64;
        let end = bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
        bytes.truncate(end);
        let name = path.file_name().expect("name").to_owned();
        synced.lock().expect("lock").insert(name, (len, bytes));
        paths.lock().expect("lock").push(path.to_path_buf());
    });
}

/// A power-loss image of `folder` in `image`: every journal as it was at
/// its last sync (every journal file is synced when it is created).
fn power_loss_image(folder: &Path, image: &Path, synced: &Synced) -> std::io::Result<()> {
    copy_dir(folder, image)?;
    let synced = synced.lock().expect("lock");
    for entry in std::fs::read_dir(image)? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "jnl") {
            continue;
        }
        let (len, bytes) = synced
            .get(path.file_name().expect("name"))
            .expect("every journal is synced when it is created");
        std::fs::write(&path, bytes)?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)?
            .set_len(*len)?;
    }
    Ok(())
}

/// The process dies at `point` of a rotation (the journal being sealed has
/// an unsynced tail), restarts, makes a write durable, and then the power
/// is lost: the restart's durable write, and with it everything before it,
/// must survive. Recovery syncs the sealed journals, which nothing in the
/// new process would otherwise wait for.
fn restart_then_power_loss(point: RotationPoint) -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("db");
    let restarted = dir.path().join("restarted");
    let lost = dir.path().join("power-loss");
    test_hooks::set_threshold(&folder, 64 << 10);
    let synced = Synced::default();
    let paths = Arc::default();
    record_syncs(&folder, &synced, &paths);

    let db = Database::builder(&folder).worker_threads(1).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    let value = vec![3u8; 1 << 10];

    let durable = 100;
    for i in 0..durable {
        ks.insert(key(i), &value)?;
    }
    db.persist(crate::PersistMode::SyncAll)?;
    let sealed_path = db.supervisor.journal.path()?;
    let total = 180;
    for i in durable..140 {
        ks.insert(key(i), &value)?;
    }
    let sealed_len = Arc::new(std::sync::Mutex::new(
        db.supervisor.journal.get_writer()?.pos()?,
    ));

    // At `point`: 40 more writes (into the sealed journal at `Prepared`,
    // into the next one at `Swapped`), then the process "dies": its folder
    // as is, and what its syncs made durable so far.
    let durable_then = Arc::new(std::sync::Mutex::new(None));
    {
        let (db, ks, value) = (db.clone(), ks.clone(), value);
        let (folder, restarted) = (folder.clone(), restarted.clone());
        let (synced, sealed_len, durable_then) = (synced, sealed_len.clone(), durable_then.clone());
        test_hooks::set_hook(&folder.clone(), move |at| {
            if at != point {
                return;
            }
            for i in 140..total {
                ks.insert(key(i), &value).expect("insert");
            }
            if point == RotationPoint::Prepared {
                *sealed_len.lock().expect("lock") = db
                    .supervisor
                    .journal
                    .get_writer()
                    .expect("writer")
                    .pos()
                    .expect("pos");
            }
            copy_dir(&folder, &restarted).expect("copy");
            *durable_then.lock().expect("lock") = Some(synced.lock().expect("lock").clone());
        });
    }
    ks.rotate_memtable_and_wait()?;
    test_hooks::clear(&folder);
    drop(ks);
    drop(db);
    let synced: Synced = Arc::new(std::sync::Mutex::new(
        durable_then
            .lock()
            .expect("lock")
            .take()
            .expect("the rotation reached the point"),
    ));

    // The sealed journal as if it had outgrown its pre-allocation: no zeros
    // after its last batch, so recovery does not truncate (and sync) it
    // anyway.
    let sealed_name = sealed_path.file_name().expect("name");
    std::fs::OpenOptions::new()
        .write(true)
        .open(restarted.join(sealed_name))?
        .set_len(*sealed_len.lock().expect("lock"))?;

    // The restart: no workers, so no flush moves the journals' data into
    // tables behind the test's back.
    let paths = Arc::default();
    record_syncs(&restarted, &synced, &paths);
    let db = Database::builder(&restarted)
        .worker_threads_unchecked(0)
        .open()?;
    assert!(
        paths
            .lock()
            .expect("lock")
            .contains(&restarted.join(sealed_name)),
        "the sealed journal was not synced on open"
    );
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    ks.insert(key(total), b"durable after the restart")?;
    db.persist(crate::PersistMode::SyncAll)?;
    power_loss_image(&restarted, &lost, &synced)?;
    test_hooks::clear(&restarted);
    drop(ks);
    drop(db);

    let n = recovered_prefix(&lost, total + 1)?;
    assert_eq!(n, total + 1);
    Ok(())
}

/// Scenario: the sealed journal was never synced, the next one is empty;
/// after the restart, writes go into the next one with no marker.
#[test]
fn a_restart_after_a_crash_before_the_swap_syncs_the_sealed_journal() -> crate::Result<()> {
    restart_then_power_loss(RotationPoint::Prepared)
}

/// Scenario: the sealed journal's sync was pending; after the restart a
/// durable persist would otherwise wait only for the active journal.
#[test]
fn a_restart_after_a_crash_before_the_sealed_sync_syncs_the_sealed_journal() -> crate::Result<()> {
    restart_then_power_loss(RotationPoint::Swapped)
}

/// A sealed journal with a batch that does not read back whole (a page in
/// its middle lost, a later one kept) ends there: recovery replays its
/// whole batches before it and discards the later journals, instead of
/// failing.
#[test]
fn a_damaged_batch_in_a_sealed_journal_ends_the_prefix() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("db");
    let image = dir.path().join("image");
    test_hooks::set_threshold(&folder, 64 << 10);

    let db = Database::builder(&folder).worker_threads(1).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    let value = vec![3u8; 1 << 10];
    let in_sealed = 140;
    for i in 0..in_sealed {
        ks.insert(key(i), &value)?;
    }
    let sealed_path = db.supervisor.journal.path()?;
    let sealed_len = db.supervisor.journal.get_writer()?.pos()?;

    // Once the sealed journal is synced (before the flush): 40 writes into
    // the next journal, a durable persist, the image.
    {
        let (db, ks, value) = (db.clone(), ks.clone(), value);
        let (folder, image) = (folder.clone(), image.clone());
        test_hooks::set_hook(&folder.clone(), move |at| {
            if at != RotationPoint::Sealed {
                return;
            }
            for i in in_sealed..in_sealed + 40 {
                ks.insert(key(i), &value).expect("insert");
            }
            db.persist(crate::PersistMode::SyncAll).expect("persist");
            copy_dir(&folder, &image).expect("copy");
        });
    }
    ks.rotate_memtable_and_wait()?;
    test_hooks::clear(&folder);
    drop(ks);
    drop(db);

    // One value byte in the middle of the sealed journal changes: that
    // batch's checksum no longer matches.
    let damaged = image.join(sealed_path.file_name().expect("name"));
    let mut bytes = std::fs::read(&damaged)?;
    let middle = usize::try_from(sealed_len / 2).expect("fits");
    let at = middle
        + bytes[middle..]
            .windows(64)
            .position(|w| w.iter().all(|b| *b == 3))
            .expect("a value")
        + 32;
    bytes[at] = 4;
    std::fs::write(&damaged, bytes)?;

    let n = recovered_prefix(&image, 0)?;
    log::info!("damaged sealed journal: recovered {n}");
    assert!(
        n > 0 && n < in_sealed,
        "recovered {n}: the sealed journal's batches up to the damaged one"
    );
    Ok(())
}
