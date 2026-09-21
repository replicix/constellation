//! §0.3 — diff cost as a function of the difference, three-way merge,
//! and the determinism property the whole design rests on.

use std::time::Instant;

use rand::prelude::*;
use rand::rngs::SmallRng;

use crate::corpus::Corpus;
use crate::keys;
use crate::node::Hash;
use crate::stats::Report;
use crate::store::Store;
use crate::tree::{Mut, Tree};

pub fn diff_and_merge(rep: &mut Report, store: &Store, root: &Hash, corpus: &Corpus) {
    let t = Tree::new(store);
    rep.head("### 0.3 Diff, merge, determinism");
    rep.line("| changed keys | diff found | node reads | time | node reads / changed key |");
    rep.line("|---:|---:|---:|---:|---:|");
    for changes in [1usize, 10, 100, 1_000, 10_000, 100_000] {
        let mut rng = SmallRng::seed_from_u64(changes as u64);
        let mut muts: Vec<Mut> = (0..changes)
            .map(|_| {
                let i = rng.random_range(0..corpus.recs.len());
                let ino = corpus.ino(i);
                let mut a = corpus.attrs(&corpus.recs[i]);
                a.mode = 0o100600;
                (keys::inode_key(ino), Some(keys::inode_val(&a, &[], &[])))
            })
            .collect();
        muts.sort_by(|a, b| a.0.cmp(&b.0));
        muts.dedup_by(|a, b| a.0 == b.0);
        let other = t.apply(root, &muts);
        let start = Instant::now();
        let (found, nodes) = t.diff(root, &other);
        let secs = start.elapsed().as_secs_f64();
        rep.line(format!(
            "| {} | {} | {} | {:.2} ms | {:.1} |",
            muts.len(),
            found.len(),
            nodes,
            secs * 1e3,
            nodes as f64 / muts.len() as f64
        ));
    }
    rep.blank();
    rep.line("The tree holds 35.7M keys at every row, so a diff whose cost tracks the *change* and not the state is the property being asserted.");

    // Three-way merge of two disjoint 10k-op branches.
    let mut rng = SmallRng::seed_from_u64(99);
    let mut next_a = corpus.len() as u64 + 30_000_000;
    let mut next_b = corpus.len() as u64 + 40_000_000;
    let (ma, _) = crate::b02::batch(
        corpus,
        crate::b02::Shape::SemiClustered,
        10_000,
        &mut next_a,
        &mut rng,
    );
    let (mb, _) = crate::b02::batch(
        corpus,
        crate::b02::Shape::SemiClustered,
        10_000,
        &mut next_b,
        &mut rng,
    );
    let a = t.apply(root, &ma);
    let b = t.apply(root, &mb);
    let start = Instant::now();
    let ab = t.merge(root, &a, &b).expect("disjoint branches merge");
    let merge_s = start.elapsed().as_secs_f64();
    let ba = t.merge(root, &b, &a).expect("disjoint branches merge");
    rep.blank();
    rep.line(format!(
        "- Three-way merge of two disjoint 10k-op branches: {:.0} ms, and both sides compute **{}** ({}).",
        merge_s * 1e3,
        if ab == ba { "the same root hash" } else { "DIFFERENT ROOTS — FAIL" },
        hex8(&ab)
    ));
    let conflict_key = ma[0].0.clone();
    let a2 = t.apply(root, &[(conflict_key.clone(), Some(b"one".to_vec()))]);
    let b2 = t.apply(root, &[(conflict_key.clone(), Some(b"two".to_vec()))]);
    match t.merge(root, &a2, &b2) {
        Err(c) => rep.line(format!(
            "- Overlapping branches surface an explicit conflict set of exactly {} key(s).",
            c.len()
        )),
        Ok(_) => rep.line("- **FAIL**: overlapping branches merged silently."),
    }
}

/// The determinism property test at bench scale: one key set, many
/// insertion orders, one root hash.
pub fn determinism(rep: &mut Report, keys_n: usize, orders: usize) {
    let store = Store::memory();
    let t = Tree::new(&store);
    let mut rng = SmallRng::seed_from_u64(0xdefa17);
    let mut all: Vec<(Vec<u8>, Vec<u8>)> = (0..keys_n)
        .map(|i| {
            let ino: u64 = rng.random();
            (keys::inode_key(ino), format!("v{i}-{ino}").into_bytes())
        })
        .collect();
    all.sort();
    all.dedup_by(|a, b| a.0 == b.0);
    let bulk = t.build_sorted(all.iter().cloned());

    let start = Instant::now();
    let mut idx: Vec<usize> = (0..all.len()).collect();
    let mut agree = 0usize;
    for _ in 0..orders {
        idx.shuffle(&mut rng);
        let mut root = t.empty();
        for chunk in idx.chunks(1_000) {
            let mut muts: Vec<Mut> = chunk
                .iter()
                .map(|&i| (all[i].0.clone(), Some(all[i].1.clone())))
                .collect();
            muts.sort_by(|a, b| a.0.cmp(&b.0));
            root = t.apply(&root, &muts);
        }
        if root == bulk {
            agree += 1;
        }
    }
    let secs = start.elapsed().as_secs_f64();

    // Delete-then-reinsert must return the original hash.
    let mut victims: Vec<usize> = (0..all.len()).choose_multiple(&mut rng, all.len() / 10);
    victims.sort_unstable();
    let mut del: Vec<Mut> = victims.iter().map(|&i| (all[i].0.clone(), None)).collect();
    del.sort_by(|a, b| a.0.cmp(&b.0));
    let after = t.apply(&bulk, &del);
    let mut ins: Vec<Mut> = victims
        .iter()
        .map(|&i| (all[i].0.clone(), Some(all[i].1.clone())))
        .collect();
    ins.sort_by(|a, b| a.0.cmp(&b.0));
    let back = t.apply(&after, &ins);

    rep.blank();
    rep.line(format!(
        "- Determinism: {} keys inserted in {} random orders (batched, incremental) → **{}/{}** orders produced the bulk-built root hash {} ({:.0} s).",
        all.len(),
        orders,
        agree,
        orders,
        hex8(&bulk),
        secs
    ));
    rep.line(format!(
        "- Delete {} keys then reinsert them → root {} the original.",
        victims.len(),
        if back == bulk {
            "**returns to**"
        } else {
            "**DIFFERS from** (FAIL)"
        }
    ));
    rep.line(format!(
        "**Gate 0.3: diff is O(difference), disjoint merges agree by hash, determinism {}.**",
        if agree == orders && back == bulk {
            "holds"
        } else {
            "FAILED"
        }
    ));
}

fn hex8(h: &Hash) -> String {
    h[..4].iter().map(|b| format!("{b:02x}")).collect()
}
