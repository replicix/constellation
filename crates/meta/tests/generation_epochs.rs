//! ADR-43: a delegation generation number is `epoch << 32 | n`, minted by
//! the tenure that grants it from a persisted counter, in its own epoch's
//! block only. These are the meta-level pins: the mint's arithmetic, its
//! refusals, its counter across a restart, and the session's seeding of
//! sparse numbers (a generation the table ended without any key left here
//! is voided lazily, when a dependency names it).

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::delegation::{gen_epoch, GEN_EPOCH_SHIFT, GEN_MAX_EPOCH};
use constellation_meta::{LogRecord, Meta, MetaStore, MutateOp, Position, Rid, Streams, TouchSet};

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
/// applied anything of it (a root rebuilt from a side replica whose own
/// window began after the end: no key of it anywhere) is not seeded; a dependency on it is voided when the root's check of it
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
    // Both ended before the rebuilt replica's own window: no key of them.
    for g in [g5, g6] {
        rebuilt.kv_del(&format!("deleg_void_cut:{g}")).unwrap();
    }

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

/// The executors ask `deps_lost` before `reaches_streams` (`holder.rs`, the
/// delegate stream's append and drain): the lazy void must be seen by
/// whichever comes first, or the first arrival of an op whose dependency is
/// past the cut of a key-less generation that ended before the open passes
/// `deps_lost`, is reached by the void and executes ahead of its cause.
#[test]
fn a_lost_dependency_on_a_keyless_ended_generation_is_lost_at_the_first_check() {
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
    // Both ended before the rebuilt replica's own window: no key of them.
    for g in [g5, g6] {
        rebuilt.kv_del(&format!("deleg_void_cut:{g}")).unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let meta = Meta::open(&path).unwrap();
        meta.replace_ns_from_rebuilt(&rebuilt, &Default::default())
            .unwrap();
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    let mut streams = Streams::NONE;
    assert!(streams.raise(g5, 2));
    let deps = Position {
        seq: 0,
        pending: None,
        streams,
    };
    assert!(!meta.session().deps_lost(&deps), "not voided yet");
    // The first check is `deps_lost`: it voids, and says lost.
    assert!(meta.deps_lost_deps(&deps));
    assert!(meta.session().is_voided(g5));
}

/// ADR-43 (review S-3): a deposed root's stranded appends leave
/// `deleg_log_idx:g` behind (`retract_log_idx_tx` keeps the key) while the
/// strand rolls the table back below `g`. The restart holds `g` through that
/// key and does not void it: the table has not reached it, so it has not
/// ended, and a successor may re-ship the grant. A key at or below the
/// table's highest that the table does not list is still voided.
#[test]
fn a_restart_does_not_void_a_generation_above_the_tables_highest() {
    let (ended, stranded) = (gen(5, 3), gen(7, 1));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let meta = Meta::open(&path).unwrap();
        meta.apply_segment(
            1,
            1,
            &[
                LogRecord::Delegate {
                    dir: ROOT_INO,
                    node: 4,
                    gen: ended,
                    designated: false,
                    range: (0, 0),
                },
                LogRecord::RecallCut {
                    dir: ROOT_INO,
                    gen: ended,
                    through: 3,
                },
            ],
            &TouchSet::default(),
        )
        .unwrap();
        meta.kv_set(&format!("deleg_log_idx:{stranded}"), "4")
            .unwrap();
        assert_eq!(meta.delegation_table().max_gen(), ended);
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    assert!(meta.session().is_voided(ended));
    assert!(
        !meta.session().is_voided(stranded),
        "above the table's highest"
    );
    let at = |g: u64, i: u64| {
        let mut streams = Streams::NONE;
        assert!(streams.raise(g, i));
        Position {
            seq: 0,
            pending: None,
            streams,
        }
    };
    assert!(meta.reaches_streams_deps(&at(stranded, 4)), "held");
    assert!(!meta.reaches_streams_deps(&at(stranded, 5)));
    assert!(!meta.session().is_voided(stranded));
    assert!(!meta.deps_lost_deps(&at(stranded, 5)));
}

fn at(g: u64, i: u64) -> Position {
    let mut streams = Streams::NONE;
    assert!(streams.raise(g, i));
    Position {
        seq: 0,
        pending: None,
        streams,
    }
}

/// ADR-43 (review S-1): the rebuild runs in-process (nothing reopens), and
/// the log's keys of a generation that ended inside its window are in the
/// side replica only. `replace_ns_from_rebuilt` copies them (raise-only),
/// seeds the session from them and remembers the rebuilt table as the open
/// one: a dependency on that generation at its real cut is reached and not
/// lost, one past it is lost, and one on a generation the side never held a
/// key of is voided when checked, not waited for; and a restart afterwards
/// finds the same cuts, not cut 0.
#[test]
fn an_in_process_rebuild_carries_the_ends_its_window_holds() {
    let (keyless, ended, live) = (gen(4, 2), gen(5, 3), gen(6, 1));
    let rebuilt = Meta::open_in_memory().unwrap();
    let delegate = |g: u64| LogRecord::Delegate {
        dir: ROOT_INO,
        node: 4,
        gen: g,
        designated: false,
        range: (0, 0),
    };
    rebuilt
        .apply_segment(
            1,
            1,
            &[
                delegate(keyless),
                LogRecord::RecallCut {
                    dir: ROOT_INO,
                    gen: keyless,
                    through: 0,
                },
                delegate(ended),
                LogRecord::RecallCut {
                    dir: ROOT_INO,
                    gen: ended,
                    through: 3,
                },
                delegate(live),
            ],
            &TouchSet::default(),
        )
        .unwrap();
    // The side's window began after `keyless` ended: it holds no key of it.
    rebuilt
        .kv_del(&format!("deleg_void_cut:{keyless}"))
        .unwrap();
    rebuilt
        .kv_set(&format!("deleg_log_idx:{ended}"), "7")
        .unwrap();
    rebuilt
        .kv_set(&format!("deleg_log_idx:{live}"), "4")
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    let check = |meta: &Meta| {
        assert!(meta.session().is_voided(ended));
        // At the real cut: reached, not lost. Past it: lost.
        assert!(meta.reaches_streams_deps(&at(ended, 3)));
        assert!(!meta.deps_lost_deps(&at(ended, 3)));
        assert!(meta.deps_lost_deps(&at(ended, 4)));
        // The live one is held through the highest index it has here.
        assert!(!meta.session().is_voided(live));
        assert!(meta.reaches_streams_deps(&at(live, 9)));
        assert!(!meta.reaches_streams_deps(&at(live, 10)));
        // No key: voided when checked (cut 0), not waited for.
        assert!(meta.deps_lost_deps(&at(keyless, 2)));
        assert!(meta.reaches_streams_deps(&at(keyless, 2)));
    };
    {
        let meta = Meta::open(&path).unwrap();
        // Raise-only: what this store holds above the log's stays.
        meta.kv_set(&format!("deleg_log_idx:{ended}"), "2").unwrap();
        meta.kv_set(&format!("deleg_log_idx:{live}"), "9").unwrap();
        meta.replace_ns_from_rebuilt(&rebuilt, &Default::default())
            .unwrap();
        // In-process: nothing reopened the store.
        check(&meta);
        for (key, want) in [
            (format!("deleg_log_idx:{ended}"), "7"),
            (format!("deleg_log_idx:{live}"), "9"),
            (format!("deleg_void_cut:{ended}"), "3"),
        ] {
            assert_eq!(meta.kv_get(&key).unwrap().as_deref(), Some(want), "{key}");
        }
        meta.close().unwrap();
    }
    // A restart finds the same cuts (not cut 0).
    let meta = Meta::open(&path).unwrap();
    check(&meta);
}

fn delegate(g: u64) -> LogRecord {
    LogRecord::Delegate {
        dir: ROOT_INO,
        node: 4,
        gen: g,
        designated: false,
        range: (0, 0),
    }
}

/// ADR-43 (review, round 3): the session is seeded after an in-process
/// rebuild from the side replica's table (the durable log's), not from the
/// table this store holds once the reset has re-applied its kept committed
/// entries: a streamed-ahead `RecallCut` among them writes the table row
/// without the void (only the durable log voids), so reading the table back
/// would void that end here and count it as ended before the open, which
/// the lazy void's restriction keeps out; were the entry to strand, nothing
/// would lift the void.
#[test]
fn a_rebuild_does_not_void_an_end_a_kept_streamed_entry_carries() {
    let h = gen(5, 3);
    let rid = Rid {
        node: 2,
        incarnation: 1,
        seq: 1,
    };
    let side = Meta::open_in_memory().unwrap();
    side.apply_segment(1, 1, &[delegate(h)], &TouchSet::default())
        .unwrap();
    side.kv_set(&format!("deleg_log_idx:{h}"), "7").unwrap();

    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(h)], &TouchSet::default())
        .unwrap();
    // A committed answer whose records carry the generation's end, installed
    // ahead of the log (durable = false: the table row, no void).
    let ino = (1u64 << 40) | 1;
    let records = [
        LogRecord::Create {
            parent: ROOT_INO,
            name: "a".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 10,
        },
        LogRecord::RecallCut {
            dir: ROOT_INO,
            gen: h,
            through: 9,
        },
        LogRecord::Completed { rid },
    ];
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: "a".into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    assert!(meta
        .install_shadow_committed(rid, 1, 0, &op, &Default::default(), &records, true)
        .unwrap());
    assert!(!meta.session().is_voided(h), "streamed-ahead: no void");
    assert!(
        meta.delegation_table().iter().next().is_none(),
        "end applied"
    );

    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    // The kept entry is on the rebuilt namespace, with the end it carries.
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    assert!(
        meta.delegation_table().iter().next().is_none(),
        "the kept entry's end is on the table"
    );
    // The side's table lists `h` live: it is neither voided nor ended
    // before the open.
    assert!(!meta.session().is_voided(h));
    assert!(!meta.session().ended_before_open(h));
    assert!(!meta.deps_lost_deps(&at(h, 8)));
}

/// ADR-43 (review, round 3): the rebuild sets the cut of an ended
/// generation from the store (the log's), not the larger of it and what the
/// session holds: a deposed root's own cut of the generation (its journaled
/// end, stranded since) above the log's would count a dependency between the
/// two as not lost.
#[test]
fn a_rebuild_takes_the_log_s_cut_over_a_deposed_roots_own() {
    let g = gen(5, 3);
    let side = Meta::open_in_memory().unwrap();
    side.apply_segment(
        1,
        1,
        &[
            delegate(g),
            LogRecord::RecallCut {
                dir: ROOT_INO,
                gen: g,
                through: 5,
            },
        ],
        &TouchSet::default(),
    )
    .unwrap();
    let meta = Meta::open_in_memory().unwrap();
    // The deposed root's own end of the generation, at 9.
    meta.session().void_stream(g, 9);
    meta.session().note_void_cut(g, 9);
    assert!(!meta.deps_lost_deps(&at(g, 9)));
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert_eq!(meta.session().void_cut(g), Some(5));
    assert!(!meta.deps_lost_deps(&at(g, 5)));
    assert!(meta.deps_lost_deps(&at(g, 6)));
}

/// A store holding the table row of `h` (granted in the log, `h`'s log
/// index at 7) and an outstanding streamed-ahead `RecallCut { h, 9 }`
/// (`committed`: gated or not), closed: the restart's input.
fn store_with_a_streamed_ahead_end(path: &std::path::Path, h: u64, committed: bool) {
    let meta = Meta::open(path).unwrap();
    meta.apply_segment(1, 1, &[delegate(h)], &TouchSet::default())
        .unwrap();
    meta.kv_set(&format!("deleg_log_idx:{h}"), "7").unwrap();
    meta.install_streamed_committed(
        1,
        2,
        1,
        1,
        &[LogRecord::RecallCut {
            dir: ROOT_INO,
            gen: h,
            through: 9,
        }],
        committed,
        (0, 0),
    )
    .unwrap();
    assert!(
        meta.delegation_table().iter().next().is_none(),
        "end applied"
    );
    assert!(!meta.session().is_voided(h), "streamed-ahead: no void");
    meta.close().unwrap();
}

/// ADR-43 (the restart path's exposure, PROGRESS round 3's open item): the
/// open seeds the session from the store's table plus the speculative set, and an
/// outstanding streamed-ahead `RecallCut` wrote its row without the void
/// (only the durable log voids). The open must not void that end nor count
/// it as ended before the open.
#[test]
fn a_restart_does_not_void_an_end_only_a_streamed_entry_carries() {
    let h = gen(5, 3);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    store_with_a_streamed_ahead_end(&path, h, true);
    let meta = Meta::open(&path).unwrap();
    assert!(!meta.session().is_voided(h), "only the stream ended it");
    assert!(!meta.session().ended_before_open(h));
    assert_eq!(
        meta.session().stream_applied(h),
        7,
        "held at the log's index"
    );
    assert!(!meta.deps_lost_deps(&at(h, 8)));
    assert!(!meta.reaches_streams_deps(&at(h, 8)), "waits for the log");
    assert!(!meta.session().is_voided(h));
}

/// The same, and the entry then strands (an uncommitted streamed entry,
/// a segment of a later epoch): the table lists `h` again, and nothing
/// lifts a void the open set (only a `Local` strand stages an unvoid), so a
/// dependency on `h` past what the log holds must still wait.
#[test]
fn a_restart_then_a_strand_leaves_the_generation_live_and_unvoided() {
    let h = gen(5, 3);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    store_with_a_streamed_ahead_end(&path, h, false);
    let meta = Meta::open(&path).unwrap();
    let ino = (1u64 << 40) | 1;
    meta.apply_segment(
        2,
        2,
        &[LogRecord::Create {
            parent: ROOT_INO,
            name: "a".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 10,
        }],
        &TouchSet::default(),
    )
    .unwrap();
    assert!(
        meta.delegation_table().iter().any(|d| d.gen == h),
        "the strand rolled the end back"
    );
    assert!(!meta.session().is_voided(h), "live, not voided");
    assert!(!meta.reaches_streams_deps(&at(h, 8)));
    assert!(!meta.deps_lost_deps(&at(h, 8)));
}

/// The in-process twin (the read path's `void_ended_generations`, which
/// any unreached watermark triggers): an end only an outstanding streamed
/// entry carries is not voided by a read's wait either; were it, the
/// entry's strand would leave the live generation voided for good.
#[test]
fn a_read_does_not_void_an_end_only_a_streamed_entry_carries() {
    let h = gen(5, 3);
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(h)], &TouchSet::default())
        .unwrap();
    meta.session().note_stream(h, 7);
    meta.install_streamed(
        1,
        2,
        1,
        1,
        &[LogRecord::RecallCut {
            dir: ROOT_INO,
            gen: h,
            through: 9,
        }],
    )
    .unwrap();
    meta.session().set_budget_ms(1);
    meta.session().raise_observed(at(h, 8));
    let _ = meta.session_wait(&[]);
    assert!(!meta.session().is_voided(h), "only the stream ended it");
}

/// The `persist_void_cut` crash window (ADR-43 round 4, open item 2): the
/// segment that carries a `RecallCut` the root's stream installed here first
/// confirms that entry and skips its row, so nothing applies the record
/// again; the cut it carries must be kept in the segment's own transaction.
/// Written after the commit (the replica's `persist_void_cut`, which a
/// meta-only apply does not run: the crash between the two), the restart
/// voids at the log's index of the generation (7 here: rows of it the
/// replica took from a checkpoint have no index), and a dependency on a row
/// the log holds (8, 9) is refused as lost on every re-send.
#[test]
fn a_crash_after_a_segment_confirms_a_streamed_end_keeps_the_record_s_cut() {
    let h = gen(5, 3);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    store_with_a_streamed_ahead_end(&path, h, true);
    {
        let meta = Meta::open(&path).unwrap();
        meta.apply_segment_rows(
            2,
            1,
            1,
            &[1],
            &[(0, 0)],
            &[LogRecord::RecallCut {
                dir: ROOT_INO,
                gen: h,
                through: 9,
            }],
            &TouchSet::default(),
        )
        .unwrap();
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    assert_eq!(
        meta.void_cut(h).unwrap(),
        Some(9),
        "the record's cut, kept with the segment"
    );
    assert!(meta.session().is_voided(h));
    assert!(!meta.deps_lost_deps(&at(h, 9)), "a row the log holds");
    assert!(meta.deps_lost_deps(&at(h, 10)));
}

fn delegate_create(meta: &Meta, g: u64, rid: Rid, name: &str, ino: u64) {
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    meta.delegate_execute(
        &op,
        Some(rid),
        g,
        Default::default(),
        &Default::default(),
        constellation_types::clock::Now::same(0),
    )
    .unwrap();
}

/// ADR-43's orphan-liveness item (`deleg_gens_awaiting_end`): the root of
/// epoch 1 streams its grant of `g` (held by its group) to this node, which
/// installs it and executes as the delegate. The operator then abandons that
/// group (epoch 2's marker): the grant strands with the speculation no
/// successor re-ships, and the log never carries it, nor any row of `g`, nor
/// an end of it. The delegate's own rows of `g` stayed in its journal with no
/// end applied, so `deleg_gens_awaiting_end` named `g` for good (across
/// restarts) and `delegation_sync` installed no later grant here; the rows'
/// effects stayed in this replica, which the log never carries. The abandon
/// ends `g` as the log's `Recall` would, at what the log holds of it (0): the
/// rows are rolled back and queued for replay by rid, and the cut is kept.
#[test]
fn an_abandon_ends_a_generation_whose_grant_never_reached_the_log() {
    let g = gen(1, 1);
    let rid = Rid {
        node: 9,
        incarnation: 1,
        seq: 1,
    };
    let ino = (2u64 << 40) | 20;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let meta = Meta::open(&path).unwrap();
        meta.install_streamed_committed(1, 1, 1, 1, &[delegate(g)], true, (0, 0))
            .unwrap();
        assert!(meta.delegation_table().iter().any(|d| d.gen == g));
        delegate_create(&meta, g, rid, "x", ino);
        let live = std::collections::BTreeSet::from([g]);
        assert!(meta.deleg_gens_awaiting_end(&live).unwrap().is_empty());
        meta.apply_segment_rows(
            1,
            2,
            0,
            &[],
            &[],
            &[LogRecord::GroupAbandoned {
                epoch: 1,
                gen: 0,
                through_known: 0,
            }],
            &TouchSet::default(),
        )
        .unwrap();
        assert!(
            meta.delegation_table().iter().all(|d| d.gen != g),
            "the grant stranded"
        );
        let awaiting = meta.deleg_gens_awaiting_end(&Default::default()).unwrap();
        assert!(
            awaiting.is_empty(),
            "the orphan's rows block this node's installs: {awaiting:?}"
        );
        assert!(
            meta.lookup(ROOT_INO, "x").unwrap().is_none(),
            "the orphan's row is rolled back"
        );
        assert!(
            meta.pending_replays()
                .unwrap()
                .iter()
                .any(|op| op.rid == rid),
            "and queued for replay by rid"
        );
        assert!(meta.session().is_voided(g));
        assert!(meta.deps_lost_deps(&at(g, 1)));
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    assert!(meta
        .deleg_gens_awaiting_end(&Default::default())
        .unwrap()
        .is_empty());
    assert_eq!(meta.void_cut(g).unwrap(), Some(0));
    assert!(meta.deps_lost_deps(&at(g, 1)));
}

/// The integrity side of the same rule: a grant the log carries is not an
/// orphan, whatever the abandon strands. The generation stays live and
/// unvoided, and the delegate's rows stay (they leave by the log's rows or
/// its recall).
#[test]
fn an_abandon_leaves_a_generation_the_log_granted_alone() {
    let g = gen(1, 1);
    let rid = Rid {
        node: 9,
        incarnation: 1,
        seq: 1,
    };
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(g)], &TouchSet::default())
        .unwrap();
    delegate_create(&meta, g, rid, "x", (2u64 << 40) | 20);
    meta.apply_segment_rows(
        2,
        2,
        0,
        &[],
        &[],
        &[LogRecord::GroupAbandoned {
            epoch: 1,
            gen: 0,
            through_known: 0,
        }],
        &TouchSet::default(),
    )
    .unwrap();
    assert!(meta.delegation_table().iter().any(|d| d.gen == g));
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_some());
    assert!(!meta.session().is_voided(g));
    assert_eq!(meta.void_cut(g).unwrap(), None);
    assert!(!meta.deps_lost_deps(&at(g, 1)));
}

/// The guard for an end only the successor's stream carries so far: the
/// log granted `g` (epoch 1), this node executed as its delegate, and the
/// claimant of epoch 2 (which abandoned epoch 1's group) streamed its
/// `RecallCut` of `g` ahead of its marker. The table no longer lists `g`,
/// no cut is kept, and the delegate's row is still held, yet the log WILL
/// carry the end (and, below its cut, rows of `g`): the abandon must leave
/// `g` alone, or the row would be replayed by rid while the log may carry
/// it, and a dependency the log holds would be refused as lost.
#[test]
fn an_abandon_leaves_a_generation_a_later_stream_ends_alone() {
    let g = gen(1, 1);
    let rid = Rid {
        node: 9,
        incarnation: 1,
        seq: 1,
    };
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(g)], &TouchSet::default())
        .unwrap();
    delegate_create(&meta, g, rid, "x", (2u64 << 40) | 20);
    meta.install_streamed_committed(
        2,
        2,
        1,
        1,
        &[LogRecord::RecallCut {
            dir: ROOT_INO,
            gen: g,
            through: 1,
        }],
        true,
        (0, 0),
    )
    .unwrap();
    assert!(meta.delegation_table().iter().all(|d| d.gen != g));
    meta.apply_segment_rows(
        2,
        2,
        0,
        &[],
        &[],
        &[LogRecord::GroupAbandoned {
            epoch: 1,
            gen: 0,
            through_known: 0,
        }],
        &TouchSet::default(),
    )
    .unwrap();
    assert_eq!(meta.void_cut(g).unwrap(), None, "not ended by the abandon");
    assert!(!meta.session().is_voided(g));
    assert!(
        !meta
            .pending_replays()
            .unwrap()
            .iter()
            .any(|op| op.rid == rid),
        "the delegate's row is not replayed"
    );
}

/// A requester that installed a delegate's reply (a shadow of `g`) without
/// ever listing `g` (it never saw the grant) holds that entry for good unless
/// the abandon, which proves the log never grants `g`, ends the generation:
/// the recall strand then takes the shadow and queues its rid for replay.
#[test]
fn an_abandon_ends_a_generation_only_a_requester_s_shadow_names() {
    for committed in [false, true] {
        a_requester_s_shadow_alone_names(committed);
    }
}

fn a_requester_s_shadow_alone_names(committed: bool) {
    let g = gen(1, 1);
    let rid = Rid {
        node: 9,
        incarnation: 1,
        seq: 1,
    };
    let meta = Meta::open_in_memory().unwrap();
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: "x".into(),
        ino: (2u64 << 40) | 20,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    let records = [LogRecord::Create {
        parent: ROOT_INO,
        name: "x".into(),
        ino: (2u64 << 40) | 20,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: 10,
    }];
    assert!(meta
        .install_shadow_committed(rid, 1, g, &op, &Default::default(), &records, committed)
        .unwrap());
    assert!(meta.delegation_table().iter().all(|d| d.gen != g));
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_some());
    meta.apply_segment_rows(
        2,
        2,
        0,
        &[],
        &[],
        &[LogRecord::GroupAbandoned {
            epoch: 1,
            gen: 0,
            through_known: 0,
        }],
        &TouchSet::default(),
    )
    .unwrap();
    assert_eq!(meta.void_cut(g).unwrap(), Some(0), "ended by the abandon");
    assert!(
        meta.lookup(ROOT_INO, "x").unwrap().is_none(),
        "the shadow is rolled back"
    );
    assert!(meta
        .pending_replays()
        .unwrap()
        .iter()
        .any(|op| op.rid == rid));
}

fn no_tail(prev_epoch: u64, through: u64) -> LogRecord {
    LogRecord::TailFollowsThrough {
        prev_epoch,
        through,
    }
}

const RID: Rid = Rid {
    node: 9,
    incarnation: 1,
    seq: 1,
};

/// A replica that installed epoch 1's streamed grant of `g` (`committed`:
/// held by the root's group or not) and executed a row of `g` as its
/// delegate (`x` created, its rid [`RID`]).
fn delegate_of_a_streamed_grant(g: u64, committed: bool) -> Meta {
    let meta = Meta::open_in_memory().unwrap();
    meta.install_streamed_committed(1, 1, 1, 1, &[delegate(g)], committed, (0, 0))
        .unwrap();
    assert!(meta.delegation_table().iter().any(|d| d.gen == g));
    delegate_create(&meta, g, RID, "x", (2u64 << 40) | 20);
    meta
}

/// The rows the journal would ship next, by their generation's rows: none
/// may be a row of `g`.
fn ships_a_row_of(meta: &Meta, g: u64) -> bool {
    meta.journal_own_gens().unwrap().contains(&g)
        || !meta.take_journal_grouped(1000).unwrap().is_empty()
}

fn ended_and_replayed(meta: &Meta, g: u64) {
    assert_eq!(meta.void_cut(g).unwrap(), Some(0), "ended at 0");
    assert!(meta.session().is_voided(g));
    assert!(meta.deps_lost_deps(&at(g, 1)));
    assert!(
        meta.deleg_gens_awaiting_end(&Default::default())
            .unwrap()
            .is_empty(),
        "nothing of g left to wait for"
    );
    assert!(
        meta.lookup(ROOT_INO, "x").unwrap().is_none(),
        "the row is rolled back"
    );
    assert!(
        meta.pending_replays()
            .unwrap()
            .iter()
            .any(|op| op.rid == RID),
        "and queued for replay by rid"
    );
}

/// ADR-43 review SHOULD-3: the claimant of epoch 2, which abandoned epoch
/// 1's group, was the delegate of `g`, a grant of epoch 1 the log never
/// carried. Its own marker never runs through the segment path, so the
/// abandon's orphan rule did not run on it: its row of `g` stayed in its
/// journal and was the first row it shipped as root, a row of a generation
/// with no `Delegate` in the log. The claim's strand ends `g` as every other
/// replica does: the row is rolled back and replayed by rid as a root row.
#[test]
fn an_abandoning_claimant_that_was_the_orphan_s_delegate_ships_none_of_its_rows() {
    let g = gen(1, 1);
    let meta = delegate_of_a_streamed_grant(g, true);
    assert!(ships_a_row_of(&meta, g));
    meta.strand_for_takeover(2, &[], Some(1)).unwrap();
    assert!(
        meta.delegation_table().iter().all(|d| d.gen != g),
        "the grant stranded"
    );
    assert!(!ships_a_row_of(&meta, g), "its row would ship ungranted");
    ended_and_replayed(&meta, g);
    assert!(
        meta.delegate_stranded(g).unwrap(),
        "never executed in again"
    );
}

/// The lone-root takeover (no abandon): the lone root of epoch 1 (no group,
/// its gate off) streamed its grant of `g` uncommitted, this node executed
/// as the delegate, and the root died before shipping. The claimant of
/// epoch 2 carries no tail (`TailFollowsThrough { 1, 0 }`): nothing of
/// epoch 1 the log does not hold ever ships. Before, the strand took the
/// grant and left the rows: `deleg_gens_awaiting_end` named `g` for good
/// (no later grant installed here) and the rows' effects stayed in this
/// replica only.
#[test]
fn a_takeover_without_a_tail_ends_a_lone_root_s_unshipped_grant() {
    let g = gen(1, 1);
    let meta = delegate_of_a_streamed_grant(g, false);
    meta.apply_segment_rows(1, 2, 0, &[], &[], &[no_tail(1, 0)], &TouchSet::default())
        .unwrap();
    assert!(meta.delegation_table().iter().all(|d| d.gen != g));
    ended_and_replayed(&meta, g);
}

/// The same on the claimant, which was the delegate: its strand ends `g`,
/// and its row of it never ships.
#[test]
fn a_claimant_without_a_tail_ships_none_of_its_rows_of_an_unshipped_grant() {
    let g = gen(1, 1);
    let meta = delegate_of_a_streamed_grant(g, false);
    let proof = constellation_meta::OrphanProof::of(&[no_tail(1, 0)]);
    meta.strand_for_takeover_carrying(2, &[], None, &Default::default(), &proof)
        .unwrap();
    assert!(!ships_a_row_of(&meta, g), "its row would ship ungranted");
    ended_and_replayed(&meta, g);
}

/// The integrity guards of the takeover rule: a marker that carries a tail
/// (the grant may still ship with it), a marker of an older epoch's
/// takeover (another epoch's rows may ride a tail a later claim fetches),
/// and a grant the log carries are left alone: the generation stays live,
/// its row stays.
#[test]
fn a_takeover_leaves_a_grant_that_may_still_ship_alone() {
    let g = gen(1, 1);
    let unended = |meta: &Meta| {
        assert_eq!(meta.void_cut(g).unwrap(), None);
        assert!(!meta.session().is_voided(g));
        assert!(meta.lookup(ROOT_INO, "x").unwrap().is_some());
    };
    // A tail follows (through 5): `g` may be in it.
    let meta = delegate_of_a_streamed_grant(g, true);
    meta.apply_segment_rows(1, 2, 0, &[], &[], &[no_tail(1, 5)], &TouchSet::default())
        .unwrap();
    unended(&meta);
    let proof = constellation_meta::OrphanProof::of(&[no_tail(1, 5)]);
    assert!(proof.is_empty());
    // The marker of epoch 3 says nothing of epoch 1.
    let meta = delegate_of_a_streamed_grant(g, false);
    meta.apply_segment_rows(1, 3, 0, &[], &[], &[no_tail(2, 0)], &TouchSet::default())
        .unwrap();
    unended(&meta);
    // The log granted it.
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(g)], &TouchSet::default())
        .unwrap();
    delegate_create(&meta, g, RID, "x", (2u64 << 40) | 20);
    meta.apply_segment_rows(2, 2, 0, &[], &[], &[no_tail(1, 0)], &TouchSet::default())
        .unwrap();
    assert!(meta.delegation_table().iter().any(|d| d.gen == g));
    unended(&meta);
    // And on a claimant that holds it: its row stays to ship.
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(g)], &TouchSet::default())
        .unwrap();
    delegate_create(&meta, g, RID, "x", (2u64 << 40) | 20);
    let proof = constellation_meta::OrphanProof::of(&[no_tail(1, 0)]);
    meta.strand_for_takeover_carrying(2, &[], None, &Default::default(), &proof)
        .unwrap();
    unended(&meta);
    assert!(ships_a_row_of(&meta, g));
}

/// N4 (the restart dependency wait): the lone root of epoch 1 streamed its
/// grant of `g` and its end, both uncommitted and both outstanding at this
/// replica's restart, and died. The claimant carries no tail; its marker
/// strands both. The end's install had taken `g` out of the table, so `g`
/// was named nowhere and nothing ended it: a dependency on `g` waited for
/// the rest of the process. The marker proves the grant never ships: `g`
/// ends at 0 and the dependency is lost (refused, re-sent after its cause's
/// replay) instead of waiting.
#[test]
fn a_takeover_without_a_tail_ends_a_streamed_grant_and_end_after_a_restart() {
    let g = gen(1, 1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.db");
    {
        let meta = Meta::open(&path).unwrap();
        meta.install_streamed_committed(1, 1, 1, 1, &[delegate(g)], false, (0, 0))
            .unwrap();
        meta.install_streamed_committed(
            1,
            1,
            2,
            2,
            &[LogRecord::RecallCut {
                dir: ROOT_INO,
                gen: g,
                through: 1,
            }],
            false,
            (0, 0),
        )
        .unwrap();
        assert!(meta.delegation_table().iter().all(|d| d.gen != g));
        meta.close().unwrap();
    }
    let meta = Meta::open(&path).unwrap();
    assert!(!meta.deps_lost_deps(&at(g, 1)));
    meta.apply_segment_rows(1, 2, 0, &[], &[], &[no_tail(1, 0)], &TouchSet::default())
        .unwrap();
    assert_eq!(meta.void_cut(g).unwrap(), Some(0));
    assert!(meta.deps_lost_deps(&at(g, 1)), "lost, not waited for");
}

/// Review (orph): a root that re-adopts its own lease after a restart keeps
/// its epoch and ships a second marker of it, which says no tail of the
/// epoch before follows (`marker_records`: a takeover gate without a backup
/// tail). But this tenure took epoch 1 over with its tail (the first marker,
/// through 5) and its journal may still hold that tail, `Delegate(g)` among
/// it, unshipped: it ships after the second marker. A replica that holds rows
/// of `g` as its delegate, whose streamed grant the first marker stranded,
/// must not end `g` at 0 on the second marker.
#[test]
fn a_re_adoption_marker_proves_nothing_of_the_tail_its_tenure_re_ships() {
    let g = gen(1, 1);
    let meta = delegate_of_a_streamed_grant(g, false);
    // Epoch 2's claim re-ships epoch 1's tail through 5.
    meta.apply_segment_rows(1, 2, 0, &[], &[], &[no_tail(1, 5)], &TouchSet::default())
        .unwrap();
    assert!(meta.delegation_table().iter().all(|d| d.gen != g));
    // Its holder restarts and re-adopts epoch 2: a second marker of epoch 2.
    meta.apply_segment_rows(2, 2, 0, &[], &[], &[no_tail(1, 0)], &TouchSet::default())
        .unwrap();
    assert_eq!(
        meta.void_cut(g).unwrap(),
        None,
        "the re-shipped tail may still grant g"
    );
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_some());
}

/// Plan 46 M0.4 review MUST-1 (the two-takeover chain). The root of epoch 1
/// streamed its grant of `g` (not yet held by its group) to this node, which executed
/// as the delegate. The backup of epoch 2 sealed that root with a tail
/// through a sequence below the grant (`TailFollowsThrough { 1, 5 }`: no
/// proof, a replica cannot tell where the grant sat): every replica unlisted
/// `g` and ended it nowhere, and this node's row of `g` stayed in its
/// journal. Epoch 2 then settled the row's rid (`Refused { ABANDONED }`).
/// This node claims epoch 3 with no tail: its proof covers epoch 2 only, the
/// strand was handed no own generations (the gate passes them only with a
/// tail), and the row shipped as a root row of a generation the log never
/// granted, completing a rid the log had refused. The claim's strand must
/// take it out and queue it for replay by rid, whose dedup then meets the
/// refusal: nothing of it ever executes again.
#[test]
fn a_claimant_without_a_tail_ships_none_of_its_rows_of_a_generation_the_log_never_listed() {
    let g = gen(1, 1);
    // Streamed uncommitted: the group (the backup that sealed) never held
    // the grant, which sat past the sealed `through`.
    let meta = delegate_of_a_streamed_grant(g, false);
    meta.apply_segment_rows(1, 2, 0, &[], &[], &[no_tail(1, 5)], &TouchSet::default())
        .unwrap();
    assert!(
        meta.delegation_table().iter().all(|d| d.gen != g),
        "the seal takeover's marker strands the streamed grant"
    );
    assert_eq!(meta.void_cut(g).unwrap(), None, "and ends g nowhere");
    assert!(ships_a_row_of(&meta, g), "the delegate's row is still held");
    let abandoned = constellation_types::Code::Canceled;
    meta.apply_segment_rows(
        2,
        2,
        0,
        &[],
        &[],
        &[LogRecord::Refused {
            rid: RID,
            code: abandoned,
        }],
        &TouchSet::default(),
    )
    .unwrap();
    // Epoch 3's claim by this node, without a tail (its predecessor had no
    // backup, or released): what the gate passes (no own generations).
    let proof = constellation_meta::OrphanProof::of(&[no_tail(2, 0)]);
    assert!(!proof.covers(1));
    meta.strand_for_takeover_carrying(3, &[], None, &Default::default(), &proof)
        .unwrap();
    assert!(
        !ships_a_row_of(&meta, g),
        "its row would ship under an origin the log never granted"
    );
    assert!(
        meta.lookup(ROOT_INO, "x").unwrap().is_none(),
        "the row is rolled back"
    );
    assert!(
        meta.pending_replays()
            .unwrap()
            .iter()
            .any(|op| op.rid == RID),
        "and queued for replay by rid"
    );
    assert_eq!(
        meta.completed_outcome(RID).unwrap(),
        Some(constellation_meta::CompletedOutcome::Refused { code: abandoned }),
        "the replay finds the log's settled refusal, and runs nothing"
    );
    assert!(
        meta.delegate_stranded(g).unwrap(),
        "never executed in again"
    );
    assert_eq!(
        meta.void_cut(g).unwrap(),
        Some(0),
        "ended at what the log holds"
    );
    assert!(meta.deps_lost_deps(&at(g, 1)));
    assert!(meta
        .deleg_gens_awaiting_end(&Default::default())
        .unwrap()
        .is_empty());
}

/// The guard of the same rule: a claimant without a tail keeps its rows of a
/// generation the log lists (they ship under the log's grant), and the
/// generation stays live.
#[test]
fn a_claimant_without_a_tail_keeps_its_rows_of_a_listed_generation() {
    let g = gen(1, 1);
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment(1, 1, &[delegate(g)], &TouchSet::default())
        .unwrap();
    delegate_create(&meta, g, RID, "x", (2u64 << 40) | 20);
    let proof = constellation_meta::OrphanProof::of(&[no_tail(2, 0)]);
    meta.strand_for_takeover_carrying(3, &[], None, &Default::default(), &proof)
        .unwrap();
    assert!(ships_a_row_of(&meta, g));
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_some());
    assert_eq!(meta.void_cut(g).unwrap(), None);
    assert!(!meta.delegate_stranded(g).unwrap());
}

/// Plan 46 M0.4 review SHOULD-1 (pin, ignored: L2b, deferred to plan 46
/// M1.2). ADR-43 says an orphan-ended generation's rows "roll back and replay
/// by rid", but the orphan pass strands them as a recall does, and C2 §3.8
/// drops a delegate's gated row there (`strand_local_tx`: `recall && gated &&
/// gen != 0 && epoch == 0`), on the premise that a committed row is at or
/// below the cut. At an orphan cut of 0 that premise fails: a `DelegGroup`
/// row may be answered (ADR-41: a lone root's journal counts as the second
/// copy), and the delegate keeps nothing of it. What rescues it today is the
/// requester's persisted shadow and the deposed root's queued append, both
/// replays by rid: the L2b pair. Queuing it here as well adds a third copy
/// of the rid beside them, which is L2b's own question (the member copies of
/// M1.2 remove both decision sites).
#[test]
#[ignore = "L2b: plan 46 M1.2 (review M0.4 SHOULD-1)"]
fn an_orphan_end_queues_a_delegates_answered_gated_row() {
    let g = gen(1, 1);
    let meta = Meta::open_in_memory().unwrap();
    meta.install_streamed_committed(1, 1, 1, 1, &[delegate(g)], false, (0, 0))
        .unwrap();
    {
        let _class =
            constellation_meta::PendingClass::set(constellation_meta::RowClass::DelegGroup);
        delegate_create(&meta, g, RID, "x", (2u64 << 40) | 20);
    }
    meta.apply_segment_rows(1, 2, 0, &[], &[], &[no_tail(1, 0)], &TouchSet::default())
        .unwrap();
    assert_eq!(meta.void_cut(g).unwrap(), Some(0), "ended at 0");
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_none());
    assert!(
        meta.pending_replays()
            .unwrap()
            .iter()
            .any(|op| op.rid == RID),
        "the answered row is queued for replay by rid, never dropped"
    );
}
