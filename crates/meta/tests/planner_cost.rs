//! The ship planner's cost with a long unshipped journal behind a held
//! seed (ADR-59 changed what a create in the seed's directory is: held
//! before, shipped now). Run with `--ignored --nocapture`; prints the
//! nanoseconds of one planning round (`take_journal_grouped`) for 1,000 and
//! 10,000 unshipped transactions.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::{ChunkInfo, Manifest};
use constellation_meta::{Meta, MetaStore};
use replicix_chunkfmt::{ChunkHash, ChunkLayout};
use std::collections::BTreeMap;
use std::time::Instant;

fn plan_ns(unshipped: usize) -> (u128, usize) {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let lost = ChunkHash::of(b"bytes that are gone");
    let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let broken = meta.create(d, "broken", 0o644, 0, 0).unwrap().ino;
    let rows: Vec<u64> = meta
        .take_journal_grouped(usize::MAX)
        .unwrap()
        .into_iter()
        .flat_map(|(_, b)| b)
        .map(|(s, _)| s)
        .collect();
    meta.ack_journal_rows_at(&rows, 1).unwrap();
    let manifest = Manifest {
        layout: ChunkLayout::new(4096),
        file_len: 19,
        chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, lost)])),
    }
    .encode();
    meta.set_manifest_dirty(broken, None, &manifest, 19, None, None, &[lost])
        .unwrap();
    meta.note_unrecoverable_chunks(&[(lost, broken)], true)
        .unwrap();
    meta.rename(d, "broken", d, "moved").unwrap();
    for i in 0..unshipped {
        meta.create(d, &format!("f{i}"), 0o644, 0, 0).unwrap();
    }
    let shipped = meta.take_journal_grouped(usize::MAX).unwrap().len();
    let rounds = 20u32;
    let t = Instant::now();
    for _ in 0..rounds {
        let _ = meta.take_journal_grouped(usize::MAX).unwrap();
    }
    (t.elapsed().as_nanos() / rounds as u128, shipped)
}

#[test]
#[ignore]
fn the_planner_with_a_held_seed_and_a_long_unshipped_journal() {
    for n in [1_000usize, 10_000] {
        let (ns, shipped) = plan_ns(n);
        println!("PLANNER unshipped={n} ns/round={ns} shipped_txs={shipped}");
    }
}
