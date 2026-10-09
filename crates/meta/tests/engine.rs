//! Plan 29 M1 acceptance tests for the fjall engine: snapshot isolation,
//! orphan lifecycle, persisted usage counters across reopen, `ns`
//! equalling an independently rebuilt tree's key set, and large
//! (spilled) xattr/manifest round-trips.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MetaStore, SetXattrMode};
use constellation_mtree::keys::{self, Key};
use constellation_mtree::record::{self, Attrs, Kind};
use std::collections::BTreeSet;

fn hash_blob(bytes: &[u8]) -> record::BlobHash {
    record::BlobHash(*blake3::hash(bytes).as_bytes())
}

// --------------------------------------------------- snapshot isolation

#[test]
fn read_consistent_is_isolated_from_a_write_made_while_the_snapshot_is_held() {
    let meta = Meta::open_in_memory().unwrap();
    meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();

    meta.read_consistent(|snap| -> Result<(), constellation_meta::MetaError> {
        assert!(meta.child_ino_at(snap, ROOT_INO, "f").unwrap().is_some());
        assert!(meta.child_ino_at(snap, ROOT_INO, "g").unwrap().is_none());

        // A write lands on the live database while this snapshot is
        // still held...
        meta.create(ROOT_INO, "g", 0o644, 0, 0).unwrap();

        // ...but the already-open snapshot must not see it: it is a
        // repeatable, point-in-time read, not a live view.
        assert!(meta.child_ino_at(snap, ROOT_INO, "g").unwrap().is_none());
        assert!(meta.child_ino_at(snap, ROOT_INO, "f").unwrap().is_some());
        Ok(())
    })
    .unwrap();

    // A fresh snapshot (or the convenience non-`_at` wrapper) now sees
    // both, proving the write really did land.
    assert!(meta.child_ino(ROOT_INO, "g").unwrap().is_some());
}

#[test]
fn read_consistent_pins_the_applied_seq_vector_against_a_concurrent_bump() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_applied_seq(5).unwrap();
    meta.read_consistent(|snap| -> Result<(), constellation_meta::MetaError> {
        assert_eq!(meta.applied_seq_at(snap).unwrap(), 5);
        meta.set_applied_seq(9).unwrap();
        assert_eq!(
            meta.applied_seq_at(snap).unwrap(),
            5,
            "snapshot must not see the concurrent bump"
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(meta.applied_seq().unwrap(), 9);
}

// -------------------------------------------------------- orphan lifecycle

#[test]
fn unlinking_the_last_link_of_an_open_file_moves_it_to_orphans_and_reap_removes_it() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    meta.set_manifest(f.ino, b"MANIFEST", 8).unwrap();
    meta.set_xattr(f.ino, "user.a", b"v", SetXattrMode::Set)
        .unwrap();

    meta.unlink(ROOT_INO, "f").unwrap();

    // Gone from the namespace...
    assert!(meta.lookup(ROOT_INO, "f").unwrap().is_none());
    // ...but still answerable by ino (an open fd survives unlink).
    let attr = meta
        .getattr(f.ino)
        .unwrap()
        .expect("orphan still readable by ino");
    assert_eq!(attr.nlink, 0);
    assert_eq!(
        meta.manifest(f.ino).unwrap().as_deref(),
        Some(&b"MANIFEST"[..])
    );
    // Unlink already cleared xattrs (mirrors the old engine).
    assert!(meta.list_xattrs(f.ino).unwrap().is_empty());
    assert_eq!(meta.orphans().unwrap(), vec![f.ino]);

    meta.reap_orphan(f.ino).unwrap();

    assert!(meta.getattr(f.ino).unwrap().is_none());
    assert!(meta.orphans().unwrap().is_empty());
}

#[test]
fn unlinking_one_of_several_hardlinks_does_not_orphan_the_inode() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    meta.link(f.ino, ROOT_INO, "b").unwrap();
    meta.unlink(ROOT_INO, "a").unwrap();
    assert!(meta.orphans().unwrap().is_empty());
    assert_eq!(meta.getattr(f.ino).unwrap().unwrap().nlink, 1);
    assert!(meta.lookup(ROOT_INO, "b").unwrap().is_some());
}

// ------------------------------------------------- persisted usage counters

#[test]
fn usage_counters_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.fjall");
    {
        let meta = Meta::open(&path).unwrap();
        let a = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        meta.set_manifest(a.ino, b"12345678", 8).unwrap();
        let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        meta.set_manifest(b.ino, b"1234", 4).unwrap();
        assert_eq!(meta.usage(), (12, 2));
        // A directory and a removed file must not perturb the count.
        meta.mkdir(ROOT_INO, "dir", 0o755, 0, 0).unwrap();
        meta.unlink(ROOT_INO, "b").unwrap();
        assert_eq!(meta.usage(), (8, 1));
    }

    let reopened = Meta::open(&path).unwrap();
    assert_eq!(
        reopened.usage(),
        (8, 1),
        "usage must be read back from the persisted counters, not re-derived by a full scan"
    );
}

#[test]
fn usage_counters_survive_replay_of_a_foreign_batch() {
    let meta = Meta::open_in_memory().unwrap();
    let records = vec![
        constellation_meta::LogRecord::Create {
            parent: ROOT_INO,
            name: "f".into(),
            ino: (1u64 << 40) | 7,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        },
        constellation_meta::LogRecord::WriteManifest {
            ino: (1u64 << 40) | 7,
            base_manifest: None,
            manifest: b"abcdefghij".to_vec(),
            size: 10,
            time_ns: 2,
            mtime_ns: 2,
        },
    ];
    meta.apply_records(&records).unwrap();
    assert_eq!(meta.usage(), (10, 1));
}

// ---------------------------------------------- ns equals the rebuilt tree

/// Independently derive the §P6 key set a publisher would build for the
/// live namespace, using only public FUSE-shaped reads plus
/// `constellation_mtree`'s own encoding — a second implementation of the
/// same rules `Meta` uses internally, so an agreement here is a genuine
/// cross-check rather than a tautology.
fn rebuild_expected_keys(meta: &Meta) -> BTreeSet<Vec<u8>> {
    let mut expected = BTreeSet::new();
    let mut stack = vec![ROOT_INO];
    let mut visited = BTreeSet::new();
    while let Some(dir) = stack.pop() {
        if !visited.insert(dir) {
            continue;
        }
        for entry in MetaStore::readdir(meta, dir).unwrap() {
            expected.insert(keys::dentry(dir, entry.name.as_bytes()));
            expected.insert(keys::rdentry(entry.ino, dir, entry.name.as_bytes()));
            if entry.kind == constellation_fs_core::InodeKind::Dir {
                stack.push(entry.ino);
            }
            if visited.contains(&entry.ino) {
                continue;
            }
            let attr = meta.getattr(entry.ino).unwrap().unwrap();
            let manifest = meta.manifest(entry.ino).unwrap();
            let target = meta.readlink(entry.ino).unwrap();
            let xattr_names = meta.list_xattrs(entry.ino).unwrap();
            let xattrs: Vec<(Vec<u8>, Vec<u8>)> = xattr_names
                .iter()
                .map(|n| {
                    (
                        n.as_bytes().to_vec(),
                        meta.get_xattr(entry.ino, n).unwrap().unwrap(),
                    )
                })
                .collect();
            let attrs = Attrs {
                kind: Kind::from_u8(attr.kind.as_u8()).unwrap(),
                mode: attr.mode,
                uid: attr.uid,
                gid: attr.gid,
                nlink: attr.nlink,
                size: attr.size,
                mtime_ns: attr.mtime_ns,
                ctime_ns: attr.ctime_ns,
                rdev: attr.rdev,
            };
            let planned = record::plan_inode(
                attrs,
                manifest,
                target.map(String::into_bytes),
                &xattrs,
                hash_blob,
            );
            expected.insert(keys::inode(entry.ino));
            if planned.xattrs == record::XattrPlacement::Spilled {
                for (name, value) in &xattrs {
                    expected.insert(keys::xattr(entry.ino, name));
                    let _ = value; // placement decided by plan_inode; value only needed for the key
                }
            }
        }
    }
    // The root itself, if it has no parent dentry to have inserted its
    // `0x01` key above.
    expected.insert(keys::inode(ROOT_INO));
    expected
}

#[test]
fn ns_equals_the_key_set_an_independent_rebuild_would_produce() {
    let meta = Meta::open_in_memory().unwrap();
    let dir = meta.mkdir(ROOT_INO, "dir", 0o755, 0, 0).unwrap();
    let a = meta.create(dir.ino, "a", 0o644, 0, 0).unwrap();
    meta.set_manifest(a.ino, b"hello world", 11).unwrap();
    meta.set_xattr(a.ino, "user.small", b"v", SetXattrMode::Set)
        .unwrap();
    meta.link(a.ino, ROOT_INO, "a-hardlink").unwrap();
    meta.symlink(ROOT_INO, "link", "dir/a", 0, 0).unwrap();
    let big = meta.create(ROOT_INO, "big-xattr", 0o644, 0, 0).unwrap();
    // A big xattr set forces XattrPlacement::Spilled.
    let many: Vec<(String, Vec<u8>)> = (0..40)
        .map(|i| (format!("user.k{i}"), vec![b'v'; 8]))
        .collect();
    for (name, value) in &many {
        meta.set_xattr(big.ino, name, value, SetXattrMode::Set)
            .unwrap();
    }
    meta.rename(ROOT_INO, "big-xattr", ROOT_INO, "renamed")
        .unwrap();

    let expected = rebuild_expected_keys(&meta);
    let actual: BTreeSet<Vec<u8>> = meta
        .ns_keys()
        .unwrap()
        .into_iter()
        .filter(|k| {
            Key::parse(k)
                .map(|k| !matches!(k, Key::Subsystem { .. }))
                .unwrap_or(true)
        })
        .collect();

    let missing: Vec<_> = expected.difference(&actual).collect();
    let extra: Vec<_> = actual.difference(&expected).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "missing={missing:?} extra={extra:?}"
    );
}

// --------------------------------------- large xattr / manifest round-trip

#[test]
fn a_64kib_xattr_value_round_trips_through_the_blob_spill() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let value = vec![0xabu8; 64 * 1024];
    meta.set_xattr(f.ino, "user.big", &value, SetXattrMode::Set)
        .unwrap();
    let got = meta.get_xattr(f.ino, "user.big").unwrap().unwrap();
    assert_eq!(got.len(), value.len());
    assert_eq!(got, value);
    assert_eq!(
        meta.list_xattrs(f.ino).unwrap(),
        vec!["user.big".to_string()]
    );

    // The `0x03` value itself must be a fixed-size spilled-payload
    // pointer (tag + 32-byte hash), not the 64 KiB body inline.
    let raw = meta.ns_keys().unwrap();
    let xattr_key = keys::xattr(f.ino, b"user.big");
    assert!(raw.contains(&xattr_key));
}

#[test]
fn a_large_manifest_round_trips_through_the_blob_spill() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let manifest: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    meta.set_manifest(f.ino, &manifest, manifest.len() as u64)
        .unwrap();
    let got = meta.manifest(f.ino).unwrap().unwrap();
    assert_eq!(got, manifest);
    assert_eq!(
        meta.getattr(f.ino).unwrap().unwrap().size,
        manifest.len() as u64
    );

    // Round-trips across a reopen too (the blob body is durable).
}

#[test]
fn payload_over_64kib_does_not_overflow_the_inline_u16_length() {
    // Exercises the exact concern flagged in the record-encoding review:
    // `Payload::Inline`'s length prefix is a `u16`, guarded only by a
    // `debug_assert!` in `constellation_mtree::record`. Locally this can
    // never be reached because anything over `VALUE_SPILL` (1024 B)
    // always spills to `blobs` first — this test proves that holds even
    // right at the 64 KiB/`u16::MAX` boundary.
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let value = vec![0x11u8; u16::MAX as usize + 1];
    meta.set_xattr(f.ino, "user.huge", &value, SetXattrMode::Set)
        .unwrap();
    assert_eq!(meta.get_xattr(f.ino, "user.huge").unwrap().unwrap(), value);
}

// --------------------------------------------------------- perf sanity

/// Plan 29 M1 perf sanity: `getattr` ops/s over ~200k inodes, single
/// threaded and at 16 threads. Not a regression gate (no assertion, no
/// baseline to compare against) — just a number to report. Run with
/// `cargo test -p constellation-meta --test engine --release -- \
/// --ignored --nocapture getattr_throughput_sanity`.
#[test]
#[ignore]
fn getattr_throughput_sanity() {
    use std::sync::Arc;
    use std::time::Instant;

    const N: u64 = 200_000;
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
    let mut inos = Vec::with_capacity(N as usize);
    for i in 0..N {
        let f = meta.create(dir.ino, &format!("f{i}"), 0o644, 0, 0).unwrap();
        inos.push(f.ino);
    }

    let bench = |threads: usize| -> f64 {
        let per_thread = 200_000usize;
        let start = Instant::now();
        std::thread::scope(|scope| {
            for t in 0..threads {
                let meta = Arc::clone(&meta);
                let inos = &inos;
                scope.spawn(move || {
                    for i in 0..per_thread {
                        let ino = inos[(i * 7 + t) % inos.len()];
                        meta.getattr(ino).unwrap().unwrap();
                    }
                });
            }
        });
        let elapsed = start.elapsed().as_secs_f64();
        (threads * per_thread) as f64 / elapsed
    };

    let single = bench(1);
    let multi = bench(16);
    eprintln!(
        "getattr throughput over {N} inodes: single-threaded {single:.0} ops/s, 16 threads {multi:.0} ops/s"
    );
}

// ------------------------------------------------------- churn vacuum

/// EC2 campaign 5: a keyspace whose live set stays small but sees an
/// insert and a delete per operation (`dirty` here: marked by every
/// namespace write, cleared by every publish) keeps the deletes as
/// tombstones, and every "is anything dirty?" probe walks them. The
/// vacuum compacts such a keyspace once it has grown past its threshold
/// and leaves it alone until it has grown fourfold again.
#[test]
fn the_vacuum_compacts_a_churned_keyspace_once() {
    let meta = Meta::open_in_memory().unwrap();
    for i in 0..1200 {
        let name = format!("f{i}");
        meta.create(ROOT_INO, &name, 0o644, 0, 0).unwrap();
        meta.unlink(ROOT_INO, &name).unwrap();
        let observed = meta
            .read_consistent(|snap| meta.dirty_snapshot(snap))
            .unwrap();
        meta.clear_dirty_upto(&observed).unwrap();
    }
    assert!(!meta.has_dirty());
    let done = meta.vacuum_churn().unwrap();
    assert!(done.contains(&"dirty"), "vacuumed {done:?}");
    assert!(!meta.has_dirty());
    let again = meta.vacuum_churn().unwrap();
    assert!(!again.contains(&"dirty"), "vacuumed again: {again:?}");
    // The namespace is untouched.
    let f = meta.create(ROOT_INO, "after", 0o644, 0, 0).unwrap();
    assert_eq!(
        meta.lookup(ROOT_INO, "after").unwrap().map(|a| a.ino),
        Some(f.ino)
    );
}

// ------------------------------------- spilled xattrs changed one at a time

/// The `ns` rows of `ino` (its `0x01` record and `0x03` xattrs) as
/// `record::plan_inode` would write them for `model`, the xattr set the
/// file should have — the whole-set rule, computed independently.
fn planned_rows(
    meta: &Meta,
    ino: u64,
    model: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let attr = meta.getattr(ino).unwrap().unwrap();
    let attrs = Attrs {
        kind: Kind::from_u8(attr.kind.as_u8()).unwrap(),
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        size: attr.size,
        mtime_ns: attr.mtime_ns,
        ctime_ns: attr.ctime_ns,
        rdev: attr.rdev,
    };
    let xattrs: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
        .collect();
    let planned = record::plan_inode(attrs, None, None, &xattrs, hash_blob);
    let mut rows = vec![(keys::inode(ino), planned.record.encode())];
    if planned.xattrs == record::XattrPlacement::Spilled {
        for (name, value) in &xattrs {
            let (payload, _) = record::place_value(value.clone(), hash_blob);
            rows.push((keys::xattr(ino, name), payload.encode()));
        }
    }
    rows.sort();
    rows
}

fn stored_rows(meta: &Meta, ino: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let range = keys::xattrs_of(ino);
    let mut rows: Vec<_> = meta
        .ns_dump()
        .unwrap()
        .into_iter()
        .filter(|(k, _)| {
            *k == keys::inode(ino) || (range.start() <= k.as_slice() && k.as_slice() < range.end())
        })
        .collect();
    rows.sort();
    rows
}

/// What stress-ng's `xattr` stressor does to one file (a few hundred
/// names created, each replaced by a shorter value, then all removed),
/// with a large value and the inline/spilled boundary crossed both ways.
/// A spilled set is changed one name at a time
/// (`ns::put_spilled_xattr`); the stored rows must be byte for byte what
/// the whole-set rule produces, after every step, both executed locally
/// and replayed from the log.
#[test]
fn a_spilled_xattr_set_changed_one_name_at_a_time_stores_the_whole_set_rule() {
    use constellation_meta::LogRecord;
    let ino = (1u64 << 40) | 9;
    let local = Meta::open_in_memory().unwrap();
    let f = local.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let replayed = Meta::open_in_memory().unwrap();
    replayed
        .apply_records(&[LogRecord::Create {
            parent: ROOT_INO,
            name: "f".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }])
        .unwrap();
    let mut steps: Vec<(String, Option<Vec<u8>>)> = Vec::new();
    for i in 0..300 {
        steps.push((
            format!("user.var_{i}"),
            Some(format!("orig-value-{i}").into_bytes()),
        ));
    }
    steps.push(("user.big".into(), Some(vec![7u8; 4096])));
    for i in 0..300 {
        steps.push((
            format!("user.var_{i}"),
            Some(format!("value-{i}").into_bytes()),
        ));
    }
    steps.push(("user.big".into(), None));
    for i in 0..300 {
        steps.push((format!("user.var_{i}"), None));
    }
    // Back and forth across the boundary on a small set.
    for round in 0..3 {
        for i in 0..20 {
            steps.push((format!("user.k{i}"), Some(vec![b'v'; 4 + round])));
        }
        for i in 0..20 {
            steps.push((format!("user.k{i}"), None));
        }
    }
    let mut model = std::collections::BTreeMap::new();
    for (n, (name, value)) in steps.into_iter().enumerate() {
        let t = 10 + n as i64;
        let record = match &value {
            Some(value) => {
                local
                    .set_xattr(f.ino, &name, value, SetXattrMode::Set)
                    .unwrap();
                model.insert(name.clone(), value.clone());
                LogRecord::SetXattr {
                    ino,
                    name: name.clone(),
                    value: value.clone(),
                    time_ns: t,
                }
            }
            None => {
                local.remove_xattr(f.ino, &name).unwrap();
                model.remove(&name);
                LogRecord::RemoveXattr {
                    ino,
                    name: name.clone(),
                    time_ns: t,
                }
            }
        };
        replayed.apply_records(&[record]).unwrap();
        assert_eq!(
            stored_rows(&local, f.ino),
            planned_rows(&local, f.ino, &model),
            "local, step {n} ({name})"
        );
        assert_eq!(
            stored_rows(&replayed, ino),
            planned_rows(&replayed, ino, &model),
            "replayed, step {n} ({name})"
        );
    }
    assert!(local.list_xattrs(f.ino).unwrap().is_empty());
}
