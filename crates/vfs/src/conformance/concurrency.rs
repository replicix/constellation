//! `concurrency`: many threads, each with handles of its own, on the same
//! and on different inodes and directories; results are checked against
//! invariants or a model, never an order (the interleaving is the
//! target's).

use super::oracle::Driver;
use super::{fail, joined, must, refused, Env, Rng, TestResult};
use crate::types::Ino;
use constellation_types::Code;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Barrier;

const THREADS: usize = 8;

pub(super) fn creates_in_one_directory_are_all_visible(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    let per_thread = 25;
    let mut rng = env.rng();
    let seeds: Vec<u64> = (0..THREADS)
        .map(|i| rng.fork(i as u64).next_u64())
        .collect();
    let done = AtomicBool::new(false);
    let contents: Vec<Vec<(String, Vec<u8>)>> = std::thread::scope(|s| {
        // A lister running alongside must never see a name twice.
        let lister = s.spawn(|| {
            let c = fx.client();
            let mut listings = 0;
            while !done.load(Ordering::Acquire) {
                let names = must("readdir", c.readdir_all(d, 16));
                let unique: BTreeSet<_> = names.iter().map(|e| e.name.clone()).collect();
                assert_eq!(unique.len(), names.len(), "a name was listed twice");
                listings += 1;
            }
            listings
        });
        let workers: Vec<_> = seeds
            .iter()
            .enumerate()
            .map(|(t, &seed)| {
                let fx = &fx;
                s.spawn(move || {
                    let c = fx.client();
                    let mut rng = Rng::new(seed);
                    (0..per_thread)
                        .map(|j| {
                            let name = format!("t{t}-{j}");
                            let len = rng.below(3000) as usize;
                            let data = rng.bytes(len);
                            must("put", c.put(d, &name, &data));
                            (name, data)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        // Every worker joined (panicked or not) before the lister is told
        // to stop, and the lister stopped before a worker's panic is
        // re-raised: the scope would otherwise wait on it forever.
        let out: Vec<_> = workers.into_iter().map(|w| w.join()).collect();
        done.store(true, Ordering::Release);
        let listings = joined(lister.join());
        let out = out.into_iter().map(joined).collect();
        assert!(listings >= 1);
        out
    });
    let mut want: Vec<String> = contents.iter().flatten().map(|(n, _)| n.clone()).collect();
    want.sort();
    assert_eq!(
        must("names", c.names(d)),
        want,
        "every created name, exactly once"
    );
    for (name, data) in contents.iter().flatten() {
        let e = must("lookup", c.lookup(d, name));
        assert_eq!(e.attr.size, data.len() as u64, "{name}");
        assert!(
            &must("slurp", c.slurp(e.attr.ino)) == data,
            "{name}: content"
        );
    }
    Ok(())
}

pub(super) fn exclusive_create_has_one_winner(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let root = fx.client().root();
    for round in 0..12 {
        let name = format!("race-{round}");
        let barrier = Barrier::new(THREADS);
        let results: Vec<_> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..THREADS)
                .map(|_| {
                    let (fx, barrier, name) = (&fx, &barrier, &name);
                    s.spawn(move || {
                        let c = fx.client();
                        barrier.wait();
                        match c.create_excl(root, name) {
                            Ok((e, o)) => {
                                must("close", c.close(e.attr.ino, o.fh));
                                Ok(e.attr.ino)
                            }
                            Err(e) => Err(e.code()),
                        }
                    })
                })
                .collect();
            workers.into_iter().map(|w| joined(w.join())).collect()
        });
        let winners: Vec<Ino> = results
            .iter()
            .filter_map(|r| r.as_ref().ok().copied())
            .collect();
        assert_eq!(
            winners.len(),
            1,
            "round {round}: exactly one create wins, got {results:?}"
        );
        for r in &results {
            if let Err(code) = r {
                assert_eq!(*code, Code::Exists, "round {round}: the losers see EEXIST");
            }
        }
        let c = fx.client();
        assert_eq!(must("lookup", c.lookup(root, &name)).attr.ino, winners[0]);
    }
    Ok(())
}

pub(super) fn unlink_has_one_winner(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let root = fx.client().root();
    for round in 0..12 {
        let name = format!("victim-{round}");
        must("put", fx.client().put(root, &name, b"x"));
        let barrier = Barrier::new(THREADS);
        let results: Vec<Result<(), Code>> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..THREADS)
                .map(|_| {
                    let (fx, barrier, name) = (&fx, &barrier, &name);
                    s.spawn(move || {
                        let c = fx.client();
                        barrier.wait();
                        c.unlink(root, name).map_err(|e| e.code())
                    })
                })
                .collect();
            workers.into_iter().map(|w| joined(w.join())).collect()
        });
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "round {round}: exactly one unlink wins, got {results:?}"
        );
        assert!(
            results
                .iter()
                .all(|r| matches!(r, Ok(()) | Err(Code::NotFound))),
            "{results:?}"
        );
        refused("lookup", fx.client().lookup(root, &name), Code::NotFound);
    }
    Ok(())
}

pub(super) fn renames_keep_every_inode_named_once(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let dirs = [
        must("mkdir a", c.mkdir(root, "a")).attr.ino,
        must("mkdir b", c.mkdir(root, "b")).attr.ino,
        must("mkdir c", c.mkdir(root, "c")).attr.ino,
    ];
    let per_thread = 3;
    let mut rng = env.rng();
    // Each thread owns its files: (name, dir index, inode); the rest of the
    // world only ever sees them move.
    let owned: Vec<Vec<(String, usize, Ino)>> = (0..THREADS)
        .map(|t| {
            (0..per_thread)
                .map(|j| {
                    let name = format!("f{t}-{j}-0");
                    let e = must("put", c.put(dirs[0], &name, name.as_bytes()));
                    (name, 0, e.attr.ino)
                })
                .collect()
        })
        .collect();
    let seeds: Vec<u64> = (0..THREADS)
        .map(|i| rng.fork(i as u64).next_u64())
        .collect();
    let finals: Vec<Vec<(String, usize, Ino)>> = std::thread::scope(|s| {
        let workers: Vec<_> = owned
            .into_iter()
            .zip(seeds)
            .map(|(mut mine, seed)| {
                let fx = &fx;
                s.spawn(move || {
                    let c = fx.client();
                    let mut rng = Rng::new(seed);
                    for round in 1..=30 {
                        let i = rng.below(mine.len() as u64) as usize;
                        let (name, dir, _) = &mine[i];
                        let to = rng.below(3) as usize;
                        // A new unique name every time: no thread ever
                        // renames onto another's file.
                        let stem = name.rsplit_once('-').expect("stem").0.to_string();
                        let new_name = format!("{stem}-{round}");
                        must("rename", c.rename(dirs[*dir], name, dirs[to], &new_name));
                        mine[i].0 = new_name;
                        mine[i].1 = to;
                    }
                    mine
                })
            })
            .collect();
        workers.into_iter().map(|w| joined(w.join())).collect()
    });
    // Every inode is named exactly once, where its owner left it.
    let mut listed: BTreeMap<String, (usize, Ino)> = BTreeMap::new();
    for (di, dir) in dirs.iter().enumerate() {
        for e in must("readdir", c.readdir_all(*dir, 20)) {
            let n = String::from_utf8_lossy(e.name.as_bytes()).into_owned();
            if n == "." || n == ".." {
                continue;
            }
            assert!(
                listed.insert(n.clone(), (di, e.ino)).is_none(),
                "{n} is listed in two directories"
            );
        }
    }
    let want: BTreeMap<String, (usize, Ino)> = finals
        .into_iter()
        .flatten()
        .map(|(n, d, i)| (n, (d, i)))
        .collect();
    assert_eq!(
        listed, want,
        "every inode is where its owner renamed it to, once"
    );
    for (name, (_, ino)) in &want {
        assert!(
            !must("slurp", c.slurp(*ino)).is_empty(),
            "{name} lost its content"
        );
    }
    Ok(())
}

/// A file with a model of its content, driven through two handles.
fn drive_file(fx: &super::Fx, dir: Ino, name: &str, seed: u64, steps: usize) -> (Ino, Vec<u8>) {
    let c = fx.client();
    let mut rng = Rng::new(seed);
    let (e, a) = must("create", c.create(dir, name));
    let ino = e.attr.ino;
    let b = must("open second", c.open_rw(ino));
    let mut model: Vec<u8> = Vec::new();
    for _ in 0..steps {
        let fh = if rng.chance(50) { a.fh } else { b.fh };
        match rng.below(10) {
            0..=5 => {
                let off = rng.below(20_000) as usize;
                let len = rng.range(1, 5000) as usize;
                let data = rng.bytes(len);
                must("write", c.write(ino, fh, off as u64, &data));
                if model.len() < off + data.len() {
                    model.resize(off + data.len(), 0);
                }
                model[off..off + data.len()].copy_from_slice(&data);
            }
            6 => {
                let size = rng.below(25_000) as usize;
                must("truncate", c.truncate(ino, Some(fh), size as u64));
                model.resize(size, 0);
            }
            _ => {
                let off = rng.below(25_000) as usize;
                let len = rng.range(1, 6000) as u32;
                let got = must("read", c.read(ino, fh, off as u64, len));
                let end = (off + len as usize).min(model.len());
                let want: &[u8] = if off < model.len() {
                    &model[off..end]
                } else {
                    &[]
                };
                assert!(
                    got == want,
                    "{name}: read {len} at {off} differs from the model"
                );
            }
        }
        assert_eq!(
            must("getattr", c.getattr(ino)).size,
            model.len() as u64,
            "{name}: size"
        );
    }
    must("close a", c.close(ino, a.fh));
    must("close b", c.close(ino, b.fh));
    (ino, model)
}

pub(super) fn disjoint_files_match_the_model(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    let mut rng = env.rng();
    let seeds: Vec<u64> = (0..THREADS)
        .map(|i| rng.fork(i as u64).next_u64())
        .collect();
    let files: Vec<(Ino, Vec<u8>)> = std::thread::scope(|s| {
        let workers: Vec<_> = seeds
            .iter()
            .enumerate()
            .map(|(t, &seed)| {
                let fx = &fx;
                s.spawn(move || drive_file(fx, d, &format!("file-{t}"), seed, 60))
            })
            .collect();
        workers.into_iter().map(|w| joined(w.join())).collect()
    });
    for (t, (ino, model)) in files.iter().enumerate() {
        let got = must("slurp", c.slurp(*ino));
        assert!(
            &got == model,
            "file-{t}: final content ({} bytes) differs from the model's ({})",
            got.len(),
            model.len()
        );
    }
    Ok(())
}

pub(super) fn disjoint_ranges_of_one_file_match_the_model(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, first) = must("create", c.create(c.root(), "shared"));
    let ino = e.attr.ino;
    const BLOCK: usize = 4096;
    let blocks_per_thread = 16;
    let block = |t: usize, k: usize, round: usize| -> Vec<u8> {
        (0..BLOCK)
            .map(|i| ((t * 31 + k * 7 + round * 13 + i) % 251) as u8)
            .collect()
    };
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let fx = &fx;
            s.spawn(move || {
                let c = fx.client();
                let o = must("open", c.open_rw(ino));
                for round in 0..3 {
                    for k in 0..blocks_per_thread {
                        let idx = k * THREADS + t;
                        must(
                            "write",
                            c.write(ino, o.fh, (idx * BLOCK) as u64, &block(t, k, round)),
                        );
                        // Read the last round's block back through this
                        // handle at once.
                        let got = must(
                            "read",
                            c.read(ino, o.fh, (idx * BLOCK) as u64, BLOCK as u32),
                        );
                        assert!(got == block(t, k, round), "block {idx} read back differs");
                    }
                }
                must("close", c.close(ino, o.fh));
            });
        }
    });
    must("close first", c.close(ino, first.fh));
    let total = THREADS * blocks_per_thread * BLOCK;
    assert_eq!(must("getattr", c.getattr(ino)).size, total as u64);
    let all = must("slurp", c.slurp(ino));
    assert_eq!(all.len(), total);
    for t in 0..THREADS {
        for k in 0..blocks_per_thread {
            let idx = k * THREADS + t;
            assert!(
                all[idx * BLOCK..(idx + 1) * BLOCK] == block(t, k, 2)[..],
                "block {idx} (thread {t}) holds the wrong bytes"
            );
        }
    }
    Ok(())
}

pub(super) fn overlapping_writes_are_not_torn(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, first) = must("create", c.create(c.root(), "hot"));
    let ino = e.attr.ino;
    const BLOCK: usize = 8192;
    const BLOCKS: usize = 4;
    must(
        "preload",
        c.write(ino, first.fh, 0, &vec![0u8; BLOCK * BLOCKS]),
    );
    let stop = AtomicBool::new(false);
    let torn = AtomicUsize::new(0);
    std::thread::scope(|s| {
        // Writers overwrite whole blocks with one byte value each.
        let writers: Vec<_> = (1..=4u8)
            .map(|v| {
                let (fx, stop) = (&fx, &stop);
                s.spawn(move || {
                    let c = fx.client();
                    let o = must("open", c.open_rw(ino));
                    let data = vec![v; BLOCK];
                    let mut n = 0;
                    while !stop.load(Ordering::Acquire) && n < 200 {
                        for b in 0..BLOCKS {
                            must("write", c.write(ino, o.fh, (b * BLOCK) as u64, &data));
                        }
                        n += 1;
                    }
                    must("close", c.close(ino, o.fh));
                })
            })
            .collect();
        // Readers must see each block as one writer's value.
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (fx, stop, torn) = (&fx, &stop, &torn);
                s.spawn(move || {
                    let c = fx.client();
                    let o = must("open", c.open(ino, crate::OpenFlags::READ));
                    let mut reads = 0;
                    while !stop.load(Ordering::Acquire) && reads < 400 {
                        for b in 0..BLOCKS {
                            let got =
                                must("read", c.read(ino, o.fh, (b * BLOCK) as u64, BLOCK as u32));
                            if got.len() != BLOCK || got.iter().any(|x| *x != got[0]) {
                                torn.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        reads += 1;
                    }
                    must("release", c.release(ino, o.fh));
                })
            })
            .collect();
        let written: Vec<_> = writers.into_iter().map(|w| w.join()).collect();
        stop.store(true, Ordering::Release);
        for r in readers {
            joined(r.join());
        }
        written.into_iter().for_each(joined);
    });
    assert_eq!(
        torn.load(Ordering::Relaxed),
        0,
        "a read saw a block that mixed two writers' bytes"
    );
    must("close first", c.close(ino, first.fh));
    let all = must("slurp", c.slurp(ino));
    assert_eq!(all.len(), BLOCK * BLOCKS);
    for (b, chunk) in all.chunks(BLOCK).enumerate() {
        assert!(
            chunk.iter().all(|x| *x == chunk[0]),
            "block {b} ended up torn"
        );
    }
    Ok(())
}

pub(super) fn model_replay_concurrent(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let mut rng = env.rng();
    let workers = 4;
    let bases: Vec<Ino> = (0..workers)
        .map(|t| must("mkdir", c.mkdir(root, &format!("w{t}"))).attr.ino)
        .collect();
    let seeds: Vec<u64> = (0..workers)
        .map(|i| rng.fork(i as u64).next_u64())
        .collect();
    let hard_links = env.caps().hard_links;
    let stop = AtomicBool::new(false);
    let failures: Vec<Result<(), String>> = std::thread::scope(|s| {
        // Pressure from outside the workers' subtrees.
        let churn = s.spawn(|| {
            let c = fx.client();
            let mut i = 0;
            while !stop.load(Ordering::Acquire) {
                let name = format!("churn-{}", i % 20);
                let _ = c.put(root, &name, b"churn");
                let _ = c.unlink(root, &name);
                let _ = c.readdir_all(root, 8);
                i += 1;
            }
        });
        let handles: Vec<_> = bases
            .iter()
            .zip(&seeds)
            .map(|(&base, &seed)| {
                let fx = &fx;
                s.spawn(move || {
                    let c = fx.client();
                    let mut rng = Rng::new(seed);
                    let mut driver = Driver::new(&c, base, hard_links);
                    for i in 0..150 {
                        driver
                            .step(&mut rng)
                            .map_err(|e| format!("step {i}: {e}"))?;
                    }
                    driver.verify_tree()
                })
            })
            .collect();
        // As in `creates_in_one_directory_are_all_visible`: stop the churn
        // before re-raising a worker's panic.
        let out: Vec<_> = handles.into_iter().map(|h| h.join()).collect();
        stop.store(true, Ordering::Release);
        joined(churn.join());
        out.into_iter().map(joined).collect()
    });
    for (t, r) in failures.into_iter().enumerate() {
        if let Err(e) = r {
            fail!(
                "worker {t} (seed {:#x}) diverged from the model: {e}",
                env.seed()
            );
        }
    }
    Ok(())
}
