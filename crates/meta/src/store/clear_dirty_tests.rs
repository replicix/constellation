//! `Meta::clear_dirty_upto`: the writer lock is held for a bounded chunk of
//! keys at a time, never for the whole publish's read set, and a key
//! re-dirtied between two chunks keeps its mark.

use crate::store::{Meta, CLEAR_DIRTY_CHUNK};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

fn key(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// Puts `n` keys into `dirty`, each with counter 1, and returns what a
/// publish's snapshot of them would hold.
fn dirty_keys(meta: &Meta, n: u64) -> Vec<(Vec<u8>, u64)> {
    let mut tx = meta.db.write_tx();
    for i in 0..n {
        tx.insert(&meta.dirty, key(i), 1u64.to_be_bytes());
    }
    tx.commit().unwrap();
    (0..n).map(|i| (key(i), 1)).collect()
}

/// The test's own keys (8 bytes) still in `dirty`; the store marks a few
/// namespace keys of its own (the root inode) that are not the test's.
fn remaining(meta: &Meta) -> Vec<Vec<u8>> {
    use fjall::Readable;
    meta.db
        .read_tx()
        .iter(&meta.dirty)
        .map(|g| g.into_inner().unwrap().0.to_vec())
        .filter(|k| k.len() == 8)
        .collect()
}

/// Fail-before: the clear removed every observed key in one write
/// transaction, holding the writer lock for as long as the read set is long
/// (13-226 ms at 25k-52k keys). Counted by the writer-lock acquisitions: one
/// for the whole set before, one per chunk of `CLEAR_DIRTY_CHUNK` keys now.
#[test]
fn the_clear_takes_the_writer_lock_for_a_bounded_chunk_at_a_time() {
    let meta = Meta::open_in_memory().unwrap();
    let n = 5000u64;
    let observed = dirty_keys(&meta, n);
    let acquisitions = Arc::new(AtomicU64::new(0));
    {
        let acquisitions = acquisitions.clone();
        meta.set_writer_wait_hook_for_tests(Some(Arc::new(move || {
            acquisitions.fetch_add(1, Relaxed);
        })));
    }
    meta.clear_dirty_upto(&observed).unwrap();
    meta.set_writer_wait_hook_for_tests(None);
    let chunks = acquisitions.load(Relaxed);
    assert!(
        chunks >= n.div_ceil(CLEAR_DIRTY_CHUNK as u64),
        "{n} keys were cleared in {chunks} write transactions: more than {CLEAR_DIRTY_CHUNK} keys under one hold of the writer lock"
    );
    assert!(remaining(&meta).is_empty());
}

/// A key of a later chunk that is re-dirtied after an earlier chunk
/// committed holds a higher counter by the time its chunk runs, and
/// survives; every other key is cleared.
#[test]
fn a_key_re_dirtied_between_chunks_survives_the_clear() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let n = 3 * CLEAR_DIRTY_CHUNK as u64 + 10;
    let observed = dirty_keys(&meta, n);
    let victim = 2 * CLEAR_DIRTY_CHUNK as u64 + 5;
    let calls = Arc::new(AtomicU64::new(0));
    let inside = Arc::new(AtomicBool::new(false));
    {
        let (calls, inside) = (calls.clone(), inside.clone());
        let m = meta.clone();
        meta.set_writer_wait_hook_for_tests(Some(Arc::new(move || {
            if inside.load(Relaxed) {
                return;
            }
            // The second chunk is about to open its transaction: the first
            // has committed, the victim's chunk has not run.
            if calls.fetch_add(1, Relaxed) == 1 {
                inside.store(true, Relaxed);
                let mut tx = m.db.write_tx();
                tx.insert(&m.dirty, key(victim), 2u64.to_be_bytes());
                tx.commit().unwrap();
                inside.store(false, Relaxed);
            }
        })));
    }
    meta.clear_dirty_upto(&observed).unwrap();
    meta.set_writer_wait_hook_for_tests(None);
    assert!(
        calls.load(Relaxed) >= 3,
        "the clear ran in fewer than 3 chunks"
    );
    let left = remaining(&meta);
    assert_eq!(left, vec![key(victim)]);
}
