# Inserts block for as long as a flush's or a compaction's fsyncs take: versions are persisted (and old tables unlinked) under the version history write lock

## Version

- lsm-tree 3.1.10 from crates.io (upstream commit `9812163ae144213b6bb576a147fda066e7ab4ab1`); `main` at `3a0214afecc1a06ec749196b6155da609c309bbc` has the same code. Seen through fjall 3.1.10/3.1.11.
- Linux 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28), ext4 on NVMe and on a `dm-delay` device (every flush/FUA delayed 50 ms, so an fsync takes ~80 ms, as on a busy disk).

## Problem

Every insert takes the tree's version history lock for reading (`Tree::append_entry`, `src/tree/mod.rs:915`: `self.version_history.read()…active_memtable.insert(value)`). Several sections take the same lock for writing and do disk I/O while holding it:

- **Every flush**: `Tree::register_tables` (`src/tree/mod.rs:451-489`) calls `SuperVersions::upgrade_version`, which calls `persist_version` (`src/version/super_version.rs:140`): it writes the version file, `sync_all`s it, fsyncs the directory, and rewrites `current` atomically (more fsyncs). Then `maintenance` removes old version files.
- **Every compaction commit**: `merge_tables` (`src/compaction/worker.rs:533-603`) takes the write lock, then `CompactionFlavour::finish` (`src/compaction/flavour.rs`) runs `consume_writer`. That finishes the last output table (index, filter, fsync) and calls `Table::recover` on every output table (open, read the trailer, metadata and index). Then come `upgrade_version` (`persist_version` again) and `maintenance`. `move_tables`, `drop_tables` and `clear` persist under the write lock as well.
- **Version GC**: `SuperVersions::maintenance` drops the popped super versions under the caller's write guard. When it drops the last version that references a compaction's input tables, their `Drop` (`src/table/inner.rs:78`) unlinks the files, so after a major compaction every input file is unlinked with the lock held. fjall calls it this way for every keyspace on every memtable rotation (`keyspace.tree.get_version_history_lock().maintenance(…)`).

`std::sync::RwLock` also blocks new readers once a writer is queued, so a long *read* hold has the same effect: `do_compaction` holds the read lock through the strategy's choice and `merge_tables`' preparation, which opens every input table.

So each flush or compaction stops every insert into the tree for one or more fsyncs. On a busy disk that is seconds. In fjall it stops every write to the whole database: `WriteBatch::commit` holds the journal writer's mutex across its memtable inserts. We saw `slow commit: 15433 ms for 16 items (memtables=14707ms)` in a metadata store, right after `Starting major compaction`. 14.7 s of the 15.4 s were the inserts waiting for the version lock.

## Reproducer

`Cargo.toml`:

```toml
[dependencies]
lsm-tree = "=3.1.10"
```

`src/main.rs` (`cargo run --release`, `REPRO_DIR` a directory on the disk to test):

```rust
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() {
    let dir = std::path::Path::new(&std::env::var("REPRO_DIR").unwrap())
        .join(format!("lsm-version-lock-{}", std::process::id()));
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default())
        .open()
        .unwrap();

    // 4M rows of 100 incompressible bytes, flushed every 40k: 100 tables.
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut value = [0u8; 100];
    for i in 0..4_000_000u64 {
        for b in value.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            b.copy_from_slice(&x.to_le_bytes()[..b.len()]);
        }
        tree.insert(i.to_be_bytes(), value, seqno.next());
        if i % 40_000 == 39_999 {
            tree.flush_active_memtable(0).unwrap();
        }
    }

    // Compact everything into the last level while another thread keeps
    // inserting one small row every millisecond, timing each insert.
    let done = Arc::new(AtomicBool::new(false));
    let compaction = {
        let (tree, done) = (tree.clone(), done.clone());
        std::thread::spawn(move || {
            let started = Instant::now();
            tree.major_compact(64_000_000, 0).unwrap();
            done.store(true, Ordering::Release);
            started.elapsed()
        })
    };
    let mut inserts = Vec::new();
    let mut i = 0u64;
    while !done.load(Ordering::Acquire) {
        let started = Instant::now();
        tree.insert(format!("live-{i:08}"), "v", seqno.next());
        inserts.push(started.elapsed());
        i += 1;
        std::thread::sleep(Duration::from_millis(1));
    }
    let took = compaction.join().unwrap();
    inserts.sort();
    let p = |q: f64| inserts[((inserts.len() - 1) as f64 * q) as usize];
    println!(
        "major compaction of {} tables took {took:?}; {} inserts meanwhile: p50 {:?} p99 {:?} max {:?}",
        100,
        inserts.len(),
        p(0.5),
        p(0.99),
        inserts.last().unwrap(),
    );
    drop(tree);
    let _ = std::fs::remove_dir_all(&dir);
}
```

The slow disk is optional. It makes the stall the size a busy disk gives it:

```sh
truncate -s 16G slow.img && LOOP=$(sudo losetup --find --show slow.img)
echo "0 $(sudo blockdev --getsz $LOOP) delay $LOOP 0 0 $LOOP 0 5 $LOOP 0 50" | sudo dmsetup create slow
sudo mkfs.ext4 -q /dev/mapper/slow && sudo mount /dev/mapper/slow /mnt && sudo chown $USER /mnt
REPRO_DIR=/mnt cargo run --release
```

## Expected vs actual

- Expected: an insert into the active memtable takes microseconds whatever a background flush or compaction is doing.
- Actual: one insert per compaction waits for the whole commit section (3 runs each):

```
$ REPRO_DIR=/var/tmp cargo run --release      # idle NVMe
major compaction of 100 tables took 1.331574956s; 1219 inserts meanwhile: p50 390ns p99 1.48µs max 46.042587ms
major compaction of 100 tables took 1.348556463s; 1230 inserts meanwhile: p50 410ns p99 2.55µs max 48.500654ms
major compaction of 100 tables took 1.363904768s; 1248 inserts meanwhile: p50 410ns p99 3.44µs max 46.867552ms
$ REPRO_DIR=/mnt cargo run --release          # fsync ≈ 80 ms
major compaction of 100 tables took 2.080020208s; 1656 inserts meanwhile: p50 390ns p99 2.93µs max 333.178533ms
major compaction of 100 tables took 2.182711062s; 1737 inserts meanwhile: p50 420ns p99 2.96µs max 345.405151ms
major compaction of 100 tables took 2.166001168s; 1727 inserts meanwhile: p50 410ns p99 3.35µs max 340.952841ms
```

With the fix below (via `[patch.crates-io]`):

```
major compaction of 100 tables took 1.350714085s; 1280 inserts meanwhile: p50 390ns p99 1.84µs max 5.44µs
major compaction of 100 tables took 1.328374144s; 1259 inserts meanwhile: p50 390ns p99 2.41µs max 5.5µs
major compaction of 100 tables took 1.327692569s; 1258 inserts meanwhile: p50 380ns p99 1.63µs max 6.26µs
major compaction of 100 tables took 2.099793695s; 1990 inserts meanwhile: p50 390ns p99 2.42µs max 9.88µs     # fsync ≈ 80 ms
major compaction of 100 tables took 2.230980656s; 2109 inserts meanwhile: p50 440ns p99 4.52µs max 455.033µs
major compaction of 100 tables took 2.242455094s; 2125 inserts meanwhile: p50 400ns p99 2.54µs max 73.671µs
```

Through fjall, the same compaction under a writer committing small transactions gave worst commits of 133–138 ms on the NVMe and 668–712 ms on the slow disk, all in the memtable inserts. With the fix the worst were 40–61 µs on the NVMe. On the slow disk they were 337–351 ms, all of it fjall's own journal rotation (see Notes).

## Suggested fix

Persist outside the lock and only swap under it. Every version change already holds the tree's `compaction_state` mutex (`register_tables`, the compaction commits, moves, drops, ingestion), so it can serialize version changes instead of the `RwLock`. `clear` needs to take it too. Between reading the latest version and installing the next one, only the memtables can change: a rotation seals the active memtable, and inserts land in it. So the installed super version is the one that is latest *at install time*, with the new on-disk version:

```rust
impl SuperVersions {
    pub(crate) fn upgrade_version_unlocked(
        history: &RwLock<SuperVersions>,
        _serialized: &MutexGuard<'_, CompactionState>, // every version change holds it
        tree_path: &Path,
        next_version: impl FnOnce(&SuperVersion) -> crate::Result<Version>,
        memtables: impl FnOnce(&mut SuperVersion), // e.g. a flush releasing its sealed memtables
        seqno: &SequenceNumberCounter,
        visible_seqno: &SequenceNumberCounter,
        gc_watermark: SeqNo,
    ) -> crate::Result<StaleVersions> {
        let base = history.read().expect("lock is poisoned").latest_version();
        let version = next_version(&base)?;
        let seqno = seqno.next();

        persist_version(tree_path, &version)?; // no lock held

        let mut history = history.write().expect("lock is poisoned");
        let mut next = history.latest_version();
        assert_eq!(next.version.id(), base.version.id(), "version changes are serialized");
        next.version = version;
        next.seqno = seqno;
        memtables(&mut next);
        history.append_version(next);
        visible_seqno.fetch_max(seqno + 1);

        // Popped here, dropped (unlinking deleted tables) and their files
        // removed by the caller after the lock is released.
        Ok(history.take_garbage(tree_path, gc_watermark))
    }
}
```

and:

- `merge_tables`: call `consume_writer` (finish and recover the output tables) and the filter's blob writer `finish` *before* taking any lock. Then, under `compaction_state`, check that the inputs still exist and `upgrade_version_unlocked` with `|current| current.version.with_merge(…)`. `mark_as_deleted` the inputs afterwards, as now.
- `do_compaction`: choose from a clone of the latest super version (`read().latest_version()`) instead of holding the read guard through the merge's preparation. `compaction_state` keeps the table set stable meanwhile.
- `register_tables`, `move_tables`, `drop_tables`, `clear`: the same function. For `register_tables`, `memtables` removes the flushed sealed memtables.
- `maintenance` split into `take_garbage` (pops) and a `StaleVersions` that drops the versions and removes their files, so a caller (fjall's rotation) can release the guard first.

Crash safety is unchanged. `current` names a version only after its file and the directory are synced. Versions reach disk in install order. Old tables are deleted only after the switch. A version file left behind by a crash between GC and its removal is an orphan that recovery already removes. The on-disk format is untouched. With this change, upstream's test suite at `9812163` passes (441 tests, one new), and so does fjall 3.1.10's. The new test makes version persists slow through a test-only hook and requires inserts during a flush and during a major compaction to stay fast. The unpatched code fails it (an insert waited 410 ms). Our downstream implementation is `vendor/lsm-tree` in the Constellation repository (`CONSTELLATION-PATCH.md` there).

## Notes

- fjall needs a matching change at its GC call site: `inner_rotate_memtable` holds `get_version_history_lock()` while calling `maintenance`, so the dropped versions' unlinks happen under the lock.
- Ingestion (`tree/ingest.rs`, `blob_tree/ingest.rs`) still persists under the write lock in our version: it allocates its seqno there on purpose, and it is rare. It is serialized with the rest by `compaction_state`.
- Remaining in fjall, not lsm-tree: a flush rotates the journal under the journal writer's mutex. That is the old journal's `sync_all`, the new file's creation, `set_len` and `sync_all`, and the directory's fsync, so every commit waits for three fsyncs per flush (337–351 ms with an 80 ms fsync).
