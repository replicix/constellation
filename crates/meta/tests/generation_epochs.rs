//! ADR-43: a delegation generation number is `epoch << 32 | n`, minted by
//! the tenure that grants it from a persisted counter, in its own epoch's
//! block only. These are the meta-level pins: the mint's arithmetic, its
//! refusals, its counter across a restart, and the session's seeding of
//! sparse numbers (a generation the table ended without any key left here
//! is voided lazily, when a dependency names it).

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::delegation::{gen_epoch, GEN_EPOCH_SHIFT, GEN_MAX_EPOCH};
use constellation_meta::{LogRecord, Meta, Position, Streams, TouchSet};

const fn gen(epoch: u64, n: u64) -> u64 {
    (epoch << GEN_EPOCH_SHIFT) | n
}

fn refused(meta: &Meta, epoch: u64, at_least: u64) -> String {
    meta.next_delegation_gen(epoch, at_least)
        .expect_err("refused")
        .to_string()
}

#[test]
fn a_generation_number_carries_the_epoch_that_minted_it() {
    let meta = Meta::open_in_memory().unwrap();
    assert_eq!(meta.next_delegation_gen(1, 1).unwrap(), gen(1, 1));
    assert_eq!(meta.next_delegation_gen(1, 1).unwrap(), gen(1, 2));
    assert_eq!(gen_epoch(gen(1, 2)), 1);
    // A floor inside the block is honoured (the log's highest number plus
    // one, for a store that lost its counter); one below the block is not
    // needed, the block's own start is the floor.
    assert_eq!(meta.next_delegation_gen(1, gen(1, 9)).unwrap(), gen(1, 9));
    assert_eq!(meta.next_delegation_gen(1, 1).unwrap(), gen(1, 10));
    // The next tenure starts its own block, whatever the log's floor
    // below it says.
    assert_eq!(meta.next_delegation_gen(2, gen(1, 11)).unwrap(), gen(2, 1));
    assert_eq!(meta.next_delegation_gen(2, 1).unwrap(), gen(2, 2));
    assert_eq!(gen_epoch(gen(2, 1)), 2);
}

#[test]
fn a_number_outside_the_roots_own_block_is_refused_from_any_source() {
    let meta = Meta::open_in_memory().unwrap();
    // The floor names a later tenure's grant: this root is deposed.
    assert!(refused(&meta, 1, gen(2, 1)).contains("outside epoch 1's block"));
    // The persisted counter is in a later block (this node minted in
    // epoch 2, and an old epoch-1 caller asks again).
    assert_eq!(meta.next_delegation_gen(2, 1).unwrap(), gen(2, 1));
    assert!(refused(&meta, 1, 1).contains("outside epoch 1's block"));
    // Nothing was minted by a refusal: the counter is where it was.
    assert_eq!(meta.next_delegation_gen(2, 1).unwrap(), gen(2, 2));
    // Epochs that cannot mint.
    assert!(refused(&meta, 0, 1).contains("cannot mint"));
    assert!(refused(&meta, GEN_MAX_EPOCH + 1, 1).contains("cannot mint"));
    // The last number of a block is kept free.
    assert!(refused(&meta, 3, gen(3, u64::from(u32::MAX))).contains("exhausted"));
    assert_eq!(
        meta.next_delegation_gen(3, gen(3, u64::from(u32::MAX) - 1))
            .unwrap(),
        gen(3, u64::from(u32::MAX) - 1)
    );
    assert!(refused(&meta, 3, 1).contains("exhausted"));
}

#[test]
fn the_counter_survives_a_restart_and_a_lost_counter_is_floored_by_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let meta = Meta::open(&path).unwrap();
        assert_eq!(meta.next_delegation_gen(5, 1).unwrap(), gen(5, 1));
        assert_eq!(meta.next_delegation_gen(5, 1).unwrap(), gen(5, 2));
        // The grant leaves only after the counter is synced, so a restart
        // cannot mint 2 again.
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    assert_eq!(meta.next_delegation_gen(5, 1).unwrap(), gen(5, 3));
    meta.close().unwrap();
    drop(meta);
    // The same epoch re-adopted keeps its counter across another restart.
    let meta = Meta::open(&path).unwrap();
    assert_eq!(meta.next_delegation_gen(5, 1).unwrap(), gen(5, 4));
    // A store that lost its counter (a new state directory) re-adopting the
    // epoch is protected only by the log's floor.
    let fresh = Meta::open_in_memory().unwrap();
    assert_eq!(fresh.next_delegation_gen(5, gen(5, 5)).unwrap(), gen(5, 5));
}

/// ADR-43 (review M2): a restart seeds the session from the generations the
/// store keeps a key of. One the table named and ended before this replica
/// applied anything of it (a root rebuilt from a side replica: the rebuild
/// replaces `ns`, so a generation ended inside its window has no key here)
/// is not seeded; a dependency on it is voided when the root's check of it
/// says no, at cut 0, and is then lost, not waited for.
#[test]
fn a_dependency_on_a_generation_that_ended_without_a_key_is_voided_when_checked() {
    let (g5, g6) = (gen(5, 3), gen(6, 1));
    let rebuilt = Meta::open_in_memory().unwrap();
    let records: Vec<LogRecord> = [g5, g6]
        .into_iter()
        .flat_map(|g| {
            [
                LogRecord::Delegate {
                    dir: ROOT_INO,
                    node: 4,
                    gen: g,
                    designated: false,
                    range: (0, 0),
                },
                LogRecord::RecallCut {
                    dir: ROOT_INO,
                    gen: g,
                    through: 0,
                },
            ]
        })
        .collect();
    rebuilt
        .apply_segment(1, 1, &records, &TouchSet::default())
        .unwrap();
    assert_eq!(rebuilt.delegation_table().max_gen(), g6);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let meta = Meta::open(&path).unwrap();
        meta.replace_ns_from_rebuilt(&rebuilt, &Default::default())
            .unwrap();
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    let table = meta.delegation_table();
    assert_eq!(table.max_gen(), g6);
    assert!(table.iter().next().is_none(), "nothing is live");

    let mut streams = Streams::NONE;
    assert!(streams.raise(g5, 2));
    let deps = Position {
        seq: 0,
        pending: None,
        streams,
    };
    // The seeding left it alone (no key), so the session alone says no ...
    assert!(!meta.session().reaches_streams(&deps));
    assert!(!meta.session().is_voided(g5));
    // ... and the root's check voids it lazily, at cut 0.
    assert!(meta.reaches_streams_deps(&deps));
    assert!(meta.session().is_voided(g5));
    assert!(meta.session().deps_lost(&deps), "past the cut: lost");
    assert!(meta.reaches_deps(&deps));
    // A generation above the table's highest (not applied here yet) is not
    // ended: a dependency on it still waits.
    let mut streams = Streams::NONE;
    assert!(streams.raise(gen(7, 1), 1));
    let ahead = Position {
        seq: 0,
        pending: None,
        streams,
    };
    assert!(!meta.reaches_streams_deps(&ahead));
    assert!(!meta.session().is_voided(gen(7, 1)));
}
