//! Plan 30 §M3a: the speculation log (`store::spec`).
//!
//! The unit tests pin down one rule each (retirement by rid, stranding,
//! rollback + redo with overlapping keys, hints, the takeover gate). The
//! property test drives a replica through random interleavings of
//! shadows, hints, foreign segments, retirements, strandings and
//! takeovers — and, plan 30 §M3b, of tenures as holder: local
//! transactions, own ships, a late segment inserted before local work,
//! and deposition — and checks that its namespace always equals a
//! reference rebuilt from scratch (the durable log plus the speculation
//! that survived, in the order this replica applied them), and that its
//! publish view is always the log prefix when only local speculation is
//! outstanding.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{
    execute_mutate, LogRecord, Meta, MetaStore, MutateOp, PublishBasis, Rid, TouchSet,
};

const HOLDER: u64 = 7;

fn rid(seq: u64) -> Rid {
    Rid {
        node: 2,
        incarnation: 1,
        seq,
    }
}

fn ino(n: u64) -> u64 {
    (HOLDER << 40) | n
}

fn create(name: &str, ino: u64, t: i64) -> LogRecord {
    LogRecord::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: t,
    }
}

fn create_op(name: &str, ino: u64) -> MutateOp {
    MutateOp::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    }
}

fn unlink(name: &str, t: i64) -> LogRecord {
    LogRecord::Unlink {
        parent: ROOT_INO,
        name: name.into(),
        time_ns: t,
    }
}

fn chmod(ino: u64, mode: u32, t: i64) -> LogRecord {
    LogRecord::Setattr {
        ino,
        mode: Some(mode),
        uid: None,
        gid: None,
        size: None,
        atime_ns: None,
        mtime_ns: None,
        time_ns: t,
    }
}

fn completed(r: Rid) -> LogRecord {
    LogRecord::Completed { rid: r }
}

/// Every `ns` key and value, byte for byte (timestamps included).
fn raw_ns(meta: &Meta) -> Vec<(Vec<u8>, Vec<u8>)> {
    meta.read_consistent(|snap| meta.ns_dump_at(snap)).unwrap()
}

fn apply(meta: &Meta, seq: u64, epoch: u64, records: &[LogRecord]) {
    meta.apply_segment(seq, epoch, records, &TouchSet::default())
        .unwrap();
}

#[test]
fn a_shadow_retires_by_rid_and_its_row_is_compacted() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    assert!(meta
        .install_shadow(rid(1), 1, &create_op("a", ino(1)), &recs)
        .unwrap());
    assert!(meta.has_outstanding_speculation());
    assert_eq!(
        meta.completed_position(rid(1)).unwrap(),
        None,
        "a shadow must not claim its rid completed: only the log may"
    );
    assert!(
        meta.lookup(ROOT_INO, "a").unwrap().is_some(),
        "read-your-write"
    );

    // A segment from the same holder that does not carry the rid leaves
    // it outstanding; the one that does retires it.
    let applied = meta
        .apply_segment(1, 1, &[create("b", ino(2), 11)], &TouchSet::default())
        .unwrap();
    assert_eq!(applied.retired, 0);
    assert!(meta.has_outstanding_speculation());
    let applied = meta
        .apply_segment(2, 1, &recs, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.retired, 1);
    assert!(!applied.stranded.any());
    assert!(!meta.has_outstanding_speculation());
    let counts = meta.speculation_counts().unwrap();
    assert_eq!((counts.outstanding, counts.pending_replay), (0, 0));
    assert!(meta.completed_position(rid(1)).unwrap().is_some());
    assert_eq!(meta.applied_seq().unwrap(), 2);
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
}

#[test]
fn a_shadow_for_an_already_completed_rid_is_not_installed() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    apply(&meta, 1, 1, &recs);
    // The reply arrives after this replica already tailed the segment.
    assert!(!meta
        .install_shadow(rid(1), 1, &create_op("a", ino(1)), &recs)
        .unwrap());
    assert!(!meta.has_outstanding_speculation());
}

/// The overlapping-keys case plan 30 §M3a calls out: a stranded shadow
/// create and a foreign segment applied after it both touch the parent
/// directory's attributes. Rollback must restore the parent to its
/// pre-shadow bytes and the redo must then re-apply the foreign
/// segment's own change, so the result is byte-for-byte the log-only
/// state.
#[test]
fn stranding_rolls_back_a_shadow_under_a_foreign_segment_touching_the_same_parent() {
    let meta = Meta::open_in_memory().unwrap();
    let reference = Meta::open_in_memory().unwrap();
    // Plan 30 §M12: a parent's times merge by `max`, so the stamps must
    // be above the root directory's own (its creation, the wall clock)
    // for the two stores to end byte-identical.
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    let seg1 = vec![create("other", ino(2), t0 + 20)];
    let seg2 = vec![chmod(ino(2), 0o600, t0 + 30)];

    let phantom = vec![create("phantom", ino(1), t0 + 10), completed(rid(1))];
    meta.install_shadow(rid(1), 1, &create_op("phantom", ino(1)), &phantom)
        .unwrap();
    // Tailed while the shadow is outstanding: captured for redo.
    let applied = meta
        .apply_segment(1, 1, &seg1, &TouchSet::default())
        .unwrap();
    assert!(!applied.stranded.any());
    assert!(meta.lookup(ROOT_INO, "phantom").unwrap().is_some());

    // The next holder's first segment strands the shadow.
    let applied = meta
        .apply_segment(2, 2, &seg2, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.stranded.shadows, 1);
    assert!(meta.lookup(ROOT_INO, "phantom").unwrap().is_none());
    assert!(!meta.has_outstanding_speculation());

    reference.apply_records(&seg1).unwrap();
    reference.apply_records(&seg2).unwrap();
    assert_eq!(raw_ns(&meta), raw_ns(&reference));
    assert_eq!(meta.usage_bytes_files(), reference.usage_bytes_files());

    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].rid, rid(1));
    assert_eq!(queued[0].op, create_op("phantom", ino(1)));
    assert_eq!(meta.completed_position(rid(1)).unwrap(), None);
    meta.forget_replay(queued[0].queue_seq).unwrap();
    assert!(meta.pending_replays().unwrap().is_empty());
}

/// A shadow that retired *after* an older, still-outstanding one is not
/// redone itself when the older one strands: its effect comes back
/// through the captured segment that retired it.
#[test]
fn a_retired_shadow_behind_a_stranded_one_survives_through_its_segment() {
    let meta = Meta::open_in_memory().unwrap();
    let reference = Meta::open_in_memory().unwrap();
    let s1 = vec![create("s1", ino(1), 10), completed(rid(1))];
    let s2 = vec![create("s2", ino(2), 11), completed(rid(2))];
    meta.install_shadow(rid(1), 2, &create_op("s1", ino(1)), &s1)
        .unwrap();
    meta.install_shadow(rid(2), 2, &create_op("s2", ino(2)), &s2)
        .unwrap();
    // Epoch 2's holder ships only s2 (s1 stays in its journal).
    let applied = meta.apply_segment(1, 2, &s2, &TouchSet::default()).unwrap();
    assert_eq!(applied.retired, 1);
    assert_eq!(meta.speculation_counts().unwrap().outstanding, 1);
    // Epoch 3 takes over: s1 strands.
    let seg2 = vec![create("later", ino(3), 30)];
    let applied = meta
        .apply_segment(2, 3, &seg2, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.stranded.shadows, 1);

    reference.apply_records(&s2).unwrap();
    reference.apply_records(&seg2).unwrap();
    assert_eq!(
        meta.dump_replicated().unwrap(),
        reference.dump_replicated().unwrap()
    );
    assert!(meta.lookup(ROOT_INO, "s2").unwrap().is_some());
    assert!(meta.lookup(ROOT_INO, "s1").unwrap().is_none());
    assert_eq!(meta.pending_replays().unwrap().len(), 1);
}

/// A shadow the stranding segment itself completes (the new holder
/// replayed or deduped it) retires instead of being queued again.
#[test]
fn a_segment_that_completes_a_lower_epoch_shadow_retires_it() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    meta.install_shadow(rid(1), 1, &create_op("a", ino(1)), &recs)
        .unwrap();
    let applied = meta
        .apply_segment(1, 2, &recs, &TouchSet::default())
        .unwrap();
    assert!(!applied.stranded.any());
    assert_eq!(applied.retired, 1);
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
}

#[test]
fn a_hint_retires_at_its_floor_and_strands_on_a_later_epoch() {
    let meta = Meta::open_in_memory().unwrap();
    // Retired: the floor segment carries the entry.
    meta.install_hint(&[create("h", ino(1), 10)], 1, 1).unwrap();
    assert!(meta.has_outstanding_speculation());
    let applied = meta
        .apply_segment(1, 1, &[create("h", ino(1), 10)], &TouchSet::default())
        .unwrap();
    assert_eq!(applied.retired, 1);
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.lookup(ROOT_INO, "h").unwrap().is_some());

    // Stranded: the answering holder's unshipped entry never lands, and a
    // later epoch's segment arrives before the floor.
    meta.install_hint(&[create("ghost", ino(2), 20)], 3, 1)
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "ghost").unwrap().is_some());
    let applied = meta
        .apply_segment(2, 2, &[create("real", ino(3), 30)], &TouchSet::default())
        .unwrap();
    assert_eq!(applied.stranded.hints, 1);
    assert!(meta.lookup(ROOT_INO, "ghost").unwrap().is_none());
    assert!(meta.lookup(ROOT_INO, "real").unwrap().is_some());
    assert!(
        meta.pending_replays().unwrap().is_empty(),
        "a hint has nothing to replay"
    );
}

/// Plan 30 §M6/§M9 (backup sim seed 600396): an `Exists` reply whose
/// refusal the holder's pre-S3 stream carried here first. The holder
/// refused `create f3` (journal row 2: `Refused`) and read `f3 -> B` for
/// the hint; it then renamed `f2 -> f3` (rows 3–4) and streamed all
/// three before the (acknowledgement-held) reply arrived. The hint is
/// older than the streamed state: installed on top, it put `f3 -> B`
/// back over the rename, and the segment — skipping the streamed rows,
/// retiring the hint — left the replica on `f3 -> B` while every other
/// replica had `f3 -> A`. The mirror of `install_shadow`'s streamed rule:
/// a hint whose refusal is already streamed is not installed.
#[test]
fn a_hint_whose_refusal_was_streamed_first_is_not_installed() {
    let (a, b) = (ino(1), ino(2));
    let requester = rid(9);
    let other = Rid {
        node: 3,
        incarnation: 1,
        seq: 1,
    };
    // (Stamps above the root's own creation time: parent times merge by
    // `max`, plan 30 §M12.)
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    let seg1 = [create("f2", a, t0 + 10), create("f3", b, t0 + 11)];
    let refused = LogRecord::Refused {
        rid: requester,
        errno: 17,
    };
    let rename = LogRecord::Rename {
        parent: ROOT_INO,
        name: "f2".into(),
        new_parent: ROOT_INO,
        new_name: "f3".into(),
        time_ns: t0 + 12,
    };
    let seg2 = [refused.clone(), rename.clone(), completed(other)];

    let reference = Meta::open_in_memory().unwrap();
    apply(&reference, 1, 1, &seg1);
    apply(&reference, 2, 1, &seg2);

    let meta = Meta::open_in_memory().unwrap();
    apply(&meta, 1, 1, &seg1);
    meta.install_streamed(1, 2, 2, std::slice::from_ref(&refused))
        .unwrap();
    meta.install_streamed(1, 3, 4, &[rename, completed(other)])
        .unwrap();
    let f3 = |m: &Meta| m.lookup(ROOT_INO, "f3").unwrap().map(|e| e.ino);
    assert_eq!(f3(&meta), Some(a));
    let installed = meta
        .install_hint_from(Some(requester), &[create("f3", b, t0 + 11)], 2, 1, 0)
        .unwrap();
    assert!(
        !installed,
        "the hint was read before state this replica streamed already"
    );
    assert_eq!(
        f3(&meta),
        Some(a),
        "the stale hint moved f3 back over the streamed rename"
    );
    meta.apply_segment_rows(2, 1, 4, &[2, 3, 4], &[], &seg2, &TouchSet::default())
        .unwrap();
    assert!(!meta.has_outstanding_speculation());
    assert_eq!(f3(&reference), Some(a));
    assert_eq!(raw_ns(&meta), raw_ns(&reference));
}

/// The takeover gate's rollback: every shadow below the new epoch is
/// rolled back and queued, in acceptance order.
#[test]
fn strand_below_epoch_queues_stranded_shadows_in_order() {
    let meta = Meta::open_in_memory().unwrap();
    let base = raw_ns(&meta);
    for (i, name) in ["x", "y", "z"].iter().enumerate() {
        let n = i as u64 + 1;
        meta.install_shadow(
            rid(n),
            1,
            &create_op(name, ino(n)),
            &[create(name, ino(n), 10 + n as i64), completed(rid(n))],
        )
        .unwrap();
    }
    assert_eq!(meta.usage_bytes_files().1, 3);
    let stranded = meta.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.shadows, 3);
    assert_eq!(raw_ns(&meta), base, "every shadow rolled back");
    assert_eq!(meta.usage_bytes_files().1, 0);
    let queued: Vec<u64> = meta
        .pending_replays()
        .unwrap()
        .iter()
        .map(|op| op.rid.seq)
        .collect();
    assert_eq!(queued, vec![1, 2, 3]);
    // Nothing below epoch 1 is outstanding any more: a second pass is a
    // no-op.
    assert!(!meta.strand_below_epoch(2).unwrap().any());
}

/// A shadow unlink of a durable file is rolled back: the file (and its
/// dentry, reverse dentry and parent attributes) come back byte for byte.
#[test]
fn a_stranded_unlink_restores_the_file() {
    let meta = Meta::open_in_memory().unwrap();
    apply(&meta, 1, 1, &[create("f", ino(1), 10)]);
    let before = raw_ns(&meta);
    let op = MutateOp::Unlink {
        parent: ROOT_INO,
        name: "f".into(),
    };
    meta.install_shadow(rid(1), 1, &op, &[unlink("f", 20), completed(rid(1))])
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "f").unwrap().is_none());
    assert!(!meta.orphans().unwrap().is_empty() || meta.getattr(ino(1)).unwrap().is_none());
    meta.strand_below_epoch(2).unwrap();
    assert_eq!(raw_ns(&meta), before);
    assert!(
        meta.orphans().unwrap().is_empty(),
        "the restored inode is linked again, not an orphan"
    );
}

// ------------------------------------------------------ property test

/// SplitMix64: a dependency-free, seedable generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

const NAMES: [&str; 5] = ["n0", "n1", "n2", "n3", "n4"];

/// A holder's view of the namespace, name -> ino: what it validates new
/// ops against. `durable` is the log-derived one; the holder's is that
/// plus its unshipped journal.
#[derive(Clone, Default)]
struct View {
    entries: std::collections::BTreeMap<&'static str, u64>,
}

impl View {
    fn apply(&mut self, rec: &LogRecord) {
        match rec {
            // Replay semantics: a create of a name another inode already
            // holds evicts it and wins — the log has no other rule for two
            // creates of the same name (`same_name_conflict_converges_
            // last_wins`), and it applies here too: a late segment ordered
            // before local work validated without it does not make the
            // local create's name claim void, it just means the local
            // create is (in real log position) the later of the two.
            LogRecord::Create { name, ino, .. } => {
                let name: &'static str =
                    NAMES.iter().copied().find(|n| *n == name.as_str()).unwrap();
                self.entries.insert(name, *ino);
            }
            LogRecord::Unlink { name, .. } => {
                self.entries.retain(|n, _| *n != name.as_str());
            }
            _ => {}
        }
    }
}

/// A valid op against `view` (and applied to it): create a free name,
/// unlink a present one, or chmod a present one.
fn gen_op(
    rng: &mut Rng,
    view: &mut View,
    next_ino: &mut u64,
    t: &mut i64,
) -> (LogRecord, MutateOp) {
    *t += 1;
    let name = NAMES[rng.below(NAMES.len() as u64) as usize];
    let (rec, op) = match view.entries.get(name).copied() {
        None => {
            let i = ino(*next_ino);
            *next_ino += 1;
            (create(name, i, *t), create_op(name, i))
        }
        Some(_) if rng.chance(50) => (
            unlink(name, *t),
            MutateOp::Unlink {
                parent: ROOT_INO,
                name: name.into(),
            },
        ),
        Some(i) => {
            let mode = 0o600 | (rng.below(8) as u32);
            (
                chmod(i, mode, *t),
                MutateOp::Setattr {
                    ino: i,
                    mode: Some(mode),
                    uid: None,
                    gid: None,
                    size: None,
                    atime_ns: None,
                    mtime_ns: None,
                },
            )
        }
    };
    view.apply(&rec);
    (rec, op)
}

/// One application this replica made, in order, for the reference.
enum Event {
    Segment(Vec<LogRecord>),
    Speculation(usize, Vec<LogRecord>),
    /// Plan 30 §M3b: a transaction this replica executed as holder.
    Local(usize, Vec<LogRecord>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fate {
    Outstanding,
    Retired,
    Stranded,
}

struct Spec {
    epoch: u64,
    /// `Some` for a shadow or a local transaction, `None` for a hint.
    rid: Option<Rid>,
    /// Hints only.
    floor: u64,
    /// Local transactions only: how many journal rows it wrote.
    local_rows: Option<usize>,
    fate: Fate,
}

impl Spec {
    fn is_local(&self) -> bool {
        self.local_rows.is_some()
    }
}

/// One random run. Returns a description of the failure, if any.
///
/// The simulated cluster: one holder at a time (epoch `epoch`) with an
/// unshipped journal, other requesters whose ops land in that journal,
/// and the replica under test, which forwards ops (shadows), receives
/// `Exists` answers (hints), tails every shipped segment immediately, and
/// sometimes takes the lease itself (the takeover gate). A takeover by
/// anyone strands the old holder's unshipped journal.
///
/// Plan 30 §M3b: while the replica under test holds the lease, its own
/// ops and every other requester's execute *on it* (`Local` speculation,
/// captured through `execute_mutate`), it ships prefixes of its own
/// journal (retiring them), and before its first ship of the tenure the
/// previous holder's leftover journal may still land as a late, unfenced
/// segment (applied *before* the local work — the insert-before rule).
/// Another node taking over deposes it: the new holder's first segment
/// (an empty epoch marker, as `shipper::acquire_lease_for` ships) strands
/// every unshipped local transaction.
///
/// Segments may carry any prefix of the journal — a split can separate
/// an op's records from its `Completed` — except while a hint is
/// outstanding: a hint's floor is only exact when the segment at the
/// floor carries everything the holder had when it answered (see
/// `Meta::install_hint`; plan 30 §M6 replaces floors with positions).
fn run_case(seed: u64, steps: usize) -> Result<(), String> {
    let mut rng = Rng(seed);
    let meta = Meta::open_in_memory().unwrap();
    let mut events: Vec<Event> = Vec::new();
    let mut specs: Vec<Spec> = Vec::new();

    let mut durable = View::default();
    let mut holder = View::default();
    let mut journal: Vec<LogRecord> = Vec::new();
    let mut epoch = 1u64;
    let mut next_seq = 1u64;
    let mut next_ino = 1u64;
    let mut next_rid = 1u64;
    let mut t = 1i64;
    let mut trace: Vec<String> = Vec::new();
    // Plan 30 §M3b: whether the replica under test holds the lease, and
    // whether it has shipped anything this tenure (after that, fencing
    // forbids a late segment of the previous epoch).
    let mut holding = false;
    let mut shipped_this_tenure = false;
    let mut leftover: Vec<LogRecord> = Vec::new();
    let mut leftover_epoch = 0u64;

    for _ in 0..steps {
        let roll = rng.below(100);
        if holding {
            match roll {
                // An op executes on this replica as holder: its own, or a
                // peer's forwarded one (only the rid's node differs).
                0..=44 => {
                    let (_, op) = gen_op(&mut rng, &mut holder, &mut next_ino, &mut t);
                    let r = rid(next_rid);
                    next_rid += 1;
                    match execute_mutate(&meta, &op, Some(r)) {
                        Ok(recs) => {
                            events.push(Event::Local(specs.len(), recs.clone()));
                            specs.push(Spec {
                                epoch,
                                rid: Some(r),
                                floor: 0,
                                local_rows: Some(recs.len()),
                                fate: Fate::Outstanding,
                            });
                            trace.push(format!("local {r:?} epoch {epoch}: {op:?}"));
                        }
                        // A late segment redone under our work can make
                        // the generator's view stale; a refused op simply
                        // did not happen.
                        Err(error) => {
                            holder = durable.clone();
                            trace.push(format!("local op refused ({error}): {op:?}"));
                        }
                    }
                }
                // This replica ships a prefix (whole transactions) of its
                // own journal.
                45..=74 => {
                    let want = 1 + rng.below(4) as usize;
                    let Some((_, batch)) = meta.take_journal_grouped(want).unwrap().pop() else {
                        continue;
                    };
                    let seqs: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
                    let mut rows = batch.len();
                    meta.ack_journal_rows_at(&seqs, next_seq).unwrap();
                    for s in specs
                        .iter_mut()
                        .filter(|s| s.is_local() && s.fate == Fate::Outstanding)
                    {
                        let n = s.local_rows.unwrap();
                        if n > rows {
                            break;
                        }
                        rows -= n;
                        s.fate = Fate::Retired;
                    }
                    for (_, rec) in &batch {
                        durable.apply(rec);
                    }
                    trace.push(format!(
                        "own segment {next_seq} epoch {epoch}: {} records",
                        batch.len()
                    ));
                    next_seq += 1;
                    shipped_this_tenure = true;
                }
                // The previous holder's leftover journal lands late,
                // before this tenure's first segment.
                75..=84 => {
                    if shipped_this_tenure || leftover.is_empty() {
                        continue;
                    }
                    let take = 1 + rng.below(leftover.len() as u64) as usize;
                    let recs: Vec<LogRecord> = leftover.drain(..take).collect();
                    let applied = meta
                        .apply_segment(next_seq, leftover_epoch, &recs, &TouchSet::default())
                        .unwrap();
                    if applied.stranded.any() {
                        return Err(format!(
                            "seed {seed}: a late lower-epoch segment stranded something\n{}",
                            trace.join("\n")
                        ));
                    }
                    for s in specs.iter_mut().filter(|s| s.fate == Fate::Outstanding) {
                        if s.rid.is_some_and(|r| {
                            recs.iter()
                                .any(|rec| matches!(rec, LogRecord::Completed { rid } if *rid == r))
                        }) && !s.is_local()
                        {
                            s.fate = Fate::Retired;
                        }
                    }
                    // Log order: the late segment precedes every local
                    // transaction still unshipped.
                    let at = events
                        .iter()
                        .position(|e| {
                            matches!(e, Event::Local(i, _) if specs[*i].fate == Fate::Outstanding)
                        })
                        .unwrap_or(events.len());
                    for rec in &recs {
                        durable.apply(rec);
                    }
                    trace.push(format!(
                        "late segment {next_seq} epoch {leftover_epoch}: {} records (inserted \
                         before local work: {})",
                        recs.len(),
                        applied.inserted_before_local
                    ));
                    events.insert(at, Event::Segment(recs));
                    next_seq += 1;
                    holder = durable.clone();
                }
                // Another node takes over: this replica is deposed. The new
                // holder's first segment is its (empty) epoch marker.
                85..=94 => {
                    epoch += 1;
                    holding = false;
                    meta.set_holder_epoch(0);
                    leftover.clear();
                    journal.clear();
                    for s in specs.iter_mut().filter(|s| s.fate == Fate::Outstanding) {
                        if s.epoch < epoch {
                            s.fate = Fate::Stranded;
                        }
                    }
                    let applied = meta
                        .apply_segment(next_seq, epoch, &[], &TouchSet::default())
                        .unwrap();
                    trace.push(format!(
                        "deposed: marker segment {next_seq} epoch {epoch} stranded {} local",
                        applied.stranded.locals
                    ));
                    events.push(Event::Segment(Vec::new()));
                    next_seq += 1;
                    holder = durable.clone();
                }
                _ => {}
            }
        } else {
            match roll {
                // This replica forwards an op; the holder accepts it.
                0..=29 => {
                    let (rec, op) = gen_op(&mut rng, &mut holder, &mut next_ino, &mut t);
                    let r = rid(next_rid);
                    next_rid += 1;
                    let recs = vec![rec, completed(r)];
                    journal.extend(recs.iter().cloned());
                    let installed = meta.install_shadow(r, epoch, &op, &recs).unwrap();
                    assert!(installed, "a fresh rid is never already completed");
                    events.push(Event::Speculation(specs.len(), recs));
                    specs.push(Spec {
                        epoch,
                        rid: Some(r),
                        floor: 0,
                        local_rows: None,
                        fate: Fate::Outstanding,
                    });
                    trace.push(format!("shadow {r:?} epoch {epoch}"));
                }
                // Another requester's op lands in the holder's journal.
                30..=44 => {
                    let (rec, _) = gen_op(&mut rng, &mut holder, &mut next_ino, &mut t);
                    journal.push(rec);
                    trace.push("foreign op".into());
                }
                // An `Exists` answer: an entry the holder has, installed early
                // (always below the floor: this replica has applied every
                // shipped segment).
                45..=54 => {
                    let pick = rng.below(NAMES.len() as u64) as usize;
                    let Some((&name, &i)) = holder.entries.iter().nth(pick) else {
                        continue;
                    };
                    let recs = vec![create(name, i, 0)];
                    if !meta.install_hint(&recs, next_seq, epoch).unwrap() {
                        // Live speculation touches the name: not installed.
                        trace.push(format!("hint {name} refused"));
                        continue;
                    }
                    events.push(Event::Speculation(specs.len(), recs));
                    specs.push(Spec {
                        epoch,
                        rid: None,
                        floor: next_seq,
                        local_rows: None,
                        fate: Fate::Outstanding,
                    });
                    trace.push(format!("hint {name} floor {next_seq} epoch {epoch}"));
                }
                // The holder ships (a prefix of) its journal; this replica
                // tails it.
                55..=79 => {
                    if journal.is_empty() {
                        continue;
                    }
                    let hint_outstanding = specs
                        .iter()
                        .any(|s| s.rid.is_none() && s.fate == Fate::Outstanding);
                    let take = if hint_outstanding {
                        journal.len()
                    } else {
                        1 + rng.below(journal.len() as u64) as usize
                    };
                    let recs: Vec<LogRecord> = journal.drain(..take).collect();
                    let seq = next_seq;
                    next_seq += 1;
                    let completes: Vec<Rid> = recs
                        .iter()
                        .filter_map(|r| match r {
                            LogRecord::Completed { rid } => Some(*rid),
                            _ => None,
                        })
                        .collect();
                    // The rules the replica must follow, mirrored: strand
                    // first, then retire.
                    for s in specs.iter_mut().filter(|s| s.fate == Fate::Outstanding) {
                        let completed_here = s.rid.is_some_and(|r| completes.contains(&r));
                        if s.epoch < epoch && !completed_here {
                            s.fate = Fate::Stranded;
                        }
                    }
                    for s in specs.iter_mut().filter(|s| s.fate == Fate::Outstanding) {
                        let done = match s.rid {
                            Some(r) => completes.contains(&r),
                            None => seq >= s.floor,
                        };
                        if done {
                            s.fate = Fate::Retired;
                        }
                    }
                    meta.apply_segment(seq, epoch, &recs, &TouchSet::default())
                        .unwrap();
                    for rec in &recs {
                        durable.apply(rec);
                    }
                    trace.push(format!(
                        "segment {seq} epoch {epoch}: {} of {} journal records",
                        recs.len(),
                        recs.len() + journal.len()
                    ));
                    // Log order (EC2 campaign 4 B-2): the segment precedes
                    // every op of this replica still outstanding — none of
                    // them is in the log yet. (Non-overlapping ones commute;
                    // the replica rewinds under the overlapping ones.)
                    let at = events
                        .iter()
                        .position(|e| match e {
                            Event::Speculation(i, _) | Event::Local(i, _) => {
                                specs[*i].fate == Fate::Outstanding
                            }
                            Event::Segment(_) => false,
                        })
                        .unwrap_or(events.len());
                    events.insert(at, Event::Segment(recs));
                }
                // Another node takes over: the holder's unshipped journal is
                // stranded with it.
                80..=91 => {
                    epoch += 1;
                    journal.clear();
                    holder = durable.clone();
                    trace.push(format!("another node takes over, epoch {epoch}"));
                }
                // This replica takes over (the takeover gate), and holds.
                _ => {
                    leftover_epoch = epoch;
                    leftover = std::mem::take(&mut journal);
                    epoch += 1;
                    holder = durable.clone();
                    for s in specs.iter_mut().filter(|s| s.fate == Fate::Outstanding) {
                        if s.epoch < epoch {
                            s.fate = Fate::Stranded;
                        }
                    }
                    meta.set_holder_epoch(epoch);
                    meta.strand_below_epoch(epoch).unwrap();
                    holding = true;
                    shipped_this_tenure = false;
                    trace.push(format!("this node takes over, epoch {epoch}"));
                }
            }
        }

        let outstanding = specs
            .iter()
            .filter(|s| !s.is_local() && s.fate == Fate::Outstanding)
            .count() as u64;
        let local = specs
            .iter()
            .filter(|s| s.is_local() && s.fate == Fate::Outstanding)
            .count() as u64;
        let queued = specs
            .iter()
            .filter(|s| s.fate == Fate::Stranded && s.rid.is_some())
            .count() as u64;
        let counts = meta.speculation_counts().unwrap();
        if counts.outstanding != outstanding
            || counts.pending_replay != queued
            || counts.local != local
        {
            return Err(format!(
                "seed {seed}: counts {counts:?}, expected outstanding {outstanding}, \
                 local {local}, queued {queued}\n{}",
                trace.join("\n")
            ));
        }
        if meta.has_outstanding_speculation() != (outstanding > 0) {
            return Err(format!(
                "seed {seed}: has_outstanding_speculation disagrees\n{}",
                trace.join("\n")
            ));
        }
        // Plan 30 §M3b: whenever only local speculation is outstanding,
        // the publish view is exactly the log prefix — the durable events
        // alone.
        if outstanding == 0 {
            let basis = meta
                .read_consistent(|snap| meta.publish_basis_at(snap))
                .unwrap();
            let published = match &basis {
                PublishBasis::Defer => {
                    return Err(format!(
                        "seed {seed}: a publish deferred with only local speculation\n{}",
                        trace.join("\n")
                    ))
                }
                other => published_names(&meta, other),
            };
            let expected: std::collections::BTreeMap<String, u64> = durable
                .entries
                .iter()
                .map(|(n, i)| (n.to_string(), *i))
                .collect();
            if published != expected {
                return Err(format!(
                    "seed {seed}: publish view {published:?} is not the log prefix \
                     {expected:?}\n{}",
                    trace.join("\n")
                ));
            }
        }
    }

    // The reference: the durable log plus the speculation that survived,
    // in log order — a segment before whatever of this replica was still
    // outstanding when it arrived (a late segment before the local work
    // it was inserted under, a tailed one before the shadows and hints). Retired requester
    // speculation is left out — the segment that retired it carries the
    // same records — and so is stranded speculation. A local transaction
    // stays unless stranded: shipping it made it durable in place.
    let reference = Meta::open_in_memory().unwrap();
    for event in &events {
        match event {
            Event::Segment(recs) => reference.apply_records(recs).unwrap(),
            Event::Speculation(i, recs) => {
                if specs[*i].fate == Fate::Outstanding {
                    reference.apply_records(recs).unwrap();
                }
            }
            Event::Local(i, recs) => {
                if specs[*i].fate != Fate::Stranded {
                    reference.apply_records(recs).unwrap();
                }
            }
        }
    }
    let got = meta.dump_replicated().unwrap();
    let want = reference.dump_replicated().unwrap();
    if got != want {
        return Err(format!(
            "seed {seed}: replica diverged from log + surviving speculation\n\
             got:  {got:#?}\nwant: {want:#?}\ntrace:\n{}",
            trace.join("\n")
        ));
    }
    Ok(())
}

/// The root directory's entries as a publish through `basis` would show
/// them, name -> ino.
fn published_names(meta: &Meta, basis: &PublishBasis) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    for name in NAMES {
        let key = constellation_mtree::keys::dentry(ROOT_INO, name.as_bytes());
        let value = match basis {
            PublishBasis::Substituted(view) => meta
                .read_consistent(|snap| meta.ns_get_via_at(snap, view, &key))
                .unwrap(),
            _ => meta
                .read_consistent(|snap| meta.ns_get_at(snap, &key))
                .unwrap(),
        };
        if let Some(bytes) = value {
            let (ino, _) = constellation_mtree::record::DentryRecord::ino_and_kind(&bytes).unwrap();
            out.insert(name.to_string(), ino);
        }
    }
    out
}

#[test]
fn speculation_matches_log_plus_surviving_speculation() {
    for seed in 0..64 {
        if let Err(e) = run_case(seed, 60) {
            panic!("{e}");
        }
    }
}

/// The same property over many more seeds and longer runs.
#[test]
#[ignore]
fn speculation_matches_log_plus_surviving_speculation_long() {
    for seed in 0..2_000 {
        if let Err(e) = run_case(seed, 200) {
            panic!("{e}");
        }
    }
}

/// Plan 30 §M9 (backup seed 607661): a sealed backup re-applies its
/// predecessor's tail with `apply_adopted_records`. The tail's outcome
/// rows — a journaled refusal, an inbox acknowledgement — touch no key,
/// and were applied but never journaled, so the log never carried them:
/// a later execution of the refused rid was no longer deduplicated
/// anywhere but here. They ride this tenure's journal now.
#[test]
fn an_adopted_tails_refusal_and_inbox_ack_are_journaled() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(2);
    let refused = rid(40);
    let before = meta.journal_len().unwrap();
    meta.apply_adopted_records(
        &[LogRecord::Refused {
            rid: refused,
            errno: 17,
        }],
        None,
    )
    .unwrap();
    let ack = LogRecord::InboxAck {
        epoch: 1,
        node: 3,
        n: 0,
        i: 0,
    };
    meta.apply_adopted_records(
        &[create("x", ino(41), 10), completed(rid(41)), ack.clone()],
        Some(rid(41)),
    )
    .unwrap();
    let txs = meta.journal_txs_from(0, 1000).unwrap();
    let rows: Vec<LogRecord> = txs.iter().flat_map(|t| t.records.clone()).collect();
    assert_eq!(
        meta.journal_len().unwrap() - before,
        4,
        "refusal, create, completion, inbox ack: {rows:?}"
    );
    assert!(rows.contains(&LogRecord::Refused {
        rid: refused,
        errno: 17
    }));
    assert!(rows.contains(&ack));
    assert!(matches!(
        meta.completed_outcome(refused).unwrap(),
        Some(constellation_meta::CompletedOutcome::Refused { errno: 17 })
    ));
}

/// backup-crash-slow seed 601075: replies overtake each other. The
/// `Exists` reply to `create f0` (read before `rename f0 f1` on the
/// holder) arrived after that rename's shadow was installed here; the
/// hint put `f0` back on the renamed inode — `f0` and `f1` on one inode,
/// which no segment ever undid. A hint whose keys live speculation
/// touches is not installed (the refusal waits for the log instead).
#[test]
fn a_hint_is_not_installed_over_live_speculation_on_its_keys() {
    let meta = Meta::open_in_memory().unwrap();
    let a = ino(50);
    apply(&meta, 1, 1, &[create("f0", a, 10)]);
    let rename = LogRecord::Rename {
        parent: ROOT_INO,
        name: "f0".into(),
        new_parent: ROOT_INO,
        new_name: "f1".into(),
        time_ns: 11,
    };
    let op = MutateOp::Rename {
        parent: ROOT_INO,
        name: "f0".into(),
        new_parent: ROOT_INO,
        new_name: "f1".into(),
    };
    assert!(meta
        .install_shadow(rid(51), 1, &op, &[rename, completed(rid(51))])
        .unwrap());
    let installed = meta
        .install_hint_from(Some(rid(52)), &[create("f0", a, 10)], 3, 1, 0)
        .unwrap();
    assert!(!installed, "the stale hint went in over the shadow");
    assert!(meta.lookup(ROOT_INO, "f0").unwrap().is_none());
    assert_eq!(meta.lookup(ROOT_INO, "f1").unwrap().map(|e| e.ino), Some(a));
}

/// `chaos-soak-4` seed 42, `write_full_duel:wf293`: a requester's own
/// op whose accepted reply overtook the holder's pre-S3 stream. The
/// holder journaled another node's write to the inode (row 2–3) and
/// then this node's (rows 4–5); both became backup-durable together,
/// and the reply to this node was installed as a shadow before the
/// stream delivered row 2. The streamed transaction was then applied on
/// top of the shadow, and the stream's copy of this node's own
/// transaction was adopted in place, without re-applying it: the replica
/// kept the other node's write while the log (and every other replica)
/// ends on this node's. The segment skips streamed rows, so nothing ever
/// corrected it. A streamed transaction belongs *before* every shadow
/// the stream has not reached yet, and an adopted shadow at its stream
/// position.
#[test]
fn a_streamed_transaction_goes_under_a_shadow_whose_reply_overtook_it() {
    let a = ino(60);
    let mine = rid(61);
    let other = Rid {
        node: 3,
        incarnation: 1,
        seq: 1,
    };
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    let seg1 = [create("wf", a, t0)];
    let theirs = [chmod(a, 0o600, t0 + 1), completed(other)];
    let ours = [chmod(a, 0o640, t0 + 2), completed(mine)];
    let seg2: Vec<LogRecord> = theirs.iter().chain(ours.iter()).cloned().collect();
    let op = MutateOp::Setattr {
        ino: a,
        mode: Some(0o640),
        uid: None,
        gid: None,
        size: None,
        atime_ns: None,
        mtime_ns: None,
    };

    let reference = Meta::open_in_memory().unwrap();
    apply(&reference, 1, 1, &seg1);
    apply(&reference, 2, 1, &seg2);
    let mode = |m: &Meta| m.getattr(a).unwrap().unwrap().mode & 0o7777;
    assert_eq!(mode(&reference), 0o640);

    let meta = Meta::open_in_memory().unwrap();
    apply(&meta, 1, 1, &seg1);
    assert!(meta.install_shadow(mine, 1, &op, &ours).unwrap());
    assert_eq!(mode(&meta), 0o640, "read-your-writes");
    meta.install_streamed(1, 2, 3, &theirs).unwrap();
    assert_eq!(
        mode(&meta),
        0o640,
        "the streamed earlier write went over this node's later one"
    );
    meta.install_streamed(1, 4, 5, &ours).unwrap();
    assert_eq!(mode(&meta), 0o640, "the adopted shadow is not on top");
    meta.apply_segment_rows(2, 1, 5, &[2, 3, 4, 5], &[], &seg2, &TouchSet::default())
        .unwrap();
    assert!(!meta.has_outstanding_speculation());
    assert_eq!(mode(&meta), 0o640);
    assert_eq!(raw_ns(&meta), raw_ns(&reference));
}

/// EC2 campaign 4 B-2: git's loose object on a requester — `create tmp`,
/// `link tmp obj`, `unlink tmp`, each installed as a shadow before the
/// segment carrying the `create` arrives (the holder's ship is slower than
/// git's three syscalls on real S3). Applied on top of the shadows, the
/// segment re-created the inode with one link (the tmp name was gone),
/// its `link` found `obj` in place and added none, and its `unlink tmp`
/// dropped the inode: `obj` dangled on this replica, and on every fresh
/// node once this one published. The segment now goes in under the
/// shadows it overlaps (rolled back, then redone on top). Both as one
/// segment and as one per op.
#[test]
fn a_segment_under_a_git_objects_shadows_keeps_the_object() {
    for split in [false, true] {
        let meta = Meta::open_in_memory().unwrap();
        let x = ino(1);
        let c = vec![create("tmp", x, 10), completed(rid(1))];
        let l = vec![
            LogRecord::Link {
                ino: x,
                parent: ROOT_INO,
                name: "obj".into(),
                time_ns: 11,
            },
            completed(rid(2)),
        ];
        let u = vec![unlink("tmp", 12), completed(rid(3))];
        assert!(meta
            .install_shadow(rid(1), 1, &create_op("tmp", x), &c)
            .unwrap());
        assert!(meta
            .install_shadow(
                rid(2),
                1,
                &MutateOp::Link {
                    ino: x,
                    parent: ROOT_INO,
                    name: "obj".into()
                },
                &l
            )
            .unwrap());
        assert!(meta
            .install_shadow(
                rid(3),
                1,
                &MutateOp::Unlink {
                    parent: ROOT_INO,
                    name: "tmp".into()
                },
                &u
            )
            .unwrap());
        let a = meta.getattr(x).unwrap().expect("inode after shadows");
        assert_eq!(a.nlink, 1);
        if split {
            apply(&meta, 1, 1, &c);
            apply(&meta, 2, 1, &l);
            apply(&meta, 3, 1, &u);
        } else {
            let all: Vec<LogRecord> = c.iter().chain(&l).chain(&u).cloned().collect();
            apply(&meta, 1, 1, &all);
        }
        assert!(
            meta.lookup(ROOT_INO, "obj").unwrap().is_some(),
            "split {split}"
        );
        let a = meta
            .getattr(x)
            .unwrap()
            .unwrap_or_else(|| panic!("split {split}: inode gone"));
        assert_eq!(a.nlink, 1, "split {split}");
    }
}

/// EC2 campaign 4 B-2, on the root: its own git client's ops in a
/// delegated subtree are shadows here (the delegate executed them), and
/// the delegate's stream then brings the same transactions for the root
/// to append. Appending the `create` over the shadows of the `link` and
/// the `unlink` that followed it dropped the object's inode the same way.
#[test]
fn a_root_appending_a_delegates_git_object_keeps_the_object() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let gen = 5;
    let x = ino(1);
    let ops: Vec<(Rid, MutateOp, Vec<LogRecord>)> = vec![
        (
            rid(1),
            create_op("tmp", x),
            vec![create("tmp", x, 10), completed(rid(1))],
        ),
        (
            rid(2),
            MutateOp::Link {
                ino: x,
                parent: ROOT_INO,
                name: "obj".into(),
            },
            vec![
                LogRecord::Link {
                    ino: x,
                    parent: ROOT_INO,
                    name: "obj".into(),
                    time_ns: 11,
                },
                completed(rid(2)),
            ],
        ),
        (
            rid(3),
            MutateOp::Unlink {
                parent: ROOT_INO,
                name: "tmp".into(),
            },
            vec![unlink("tmp", 12), completed(rid(3))],
        ),
    ];
    for (r, op, recs) in &ops {
        assert!(meta.install_shadow_from(*r, 1, gen, op, recs).unwrap());
    }
    for (idx, (r, _, recs)) in ops.iter().enumerate() {
        let body: Vec<LogRecord> = recs
            .iter()
            .filter(|rec| !matches!(rec, LogRecord::Completed { .. }))
            .cloned()
            .collect();
        assert!(meta
            .apply_delegate_tx(
                &body,
                Some(*r),
                gen,
                idx as u64 + 1,
                constellation_meta::Position::ZERO
            )
            .unwrap());
        assert!(meta.lookup(ROOT_INO, "obj").unwrap().is_some() || idx == 0);
    }
    assert!(meta.lookup(ROOT_INO, "tmp").unwrap().is_none());
    assert!(meta.lookup(ROOT_INO, "obj").unwrap().is_some());
    let a = meta.getattr(x).unwrap().expect("the object's inode");
    assert_eq!(a.nlink, 1);
}

/// Plan 30 §M10 × §M9 (members follow a continuation epoch's stream): a
/// member installs the hold owner's streamed transactions ahead of the
/// log, including a manifest whose chunk is on another member only (in
/// an epoch nothing reaches S3 before the close). That manifest must stay
/// speculation until the segment carrying it lands — and it lands only
/// once its chunk is in S3 (the holder's ship plan defers it and stops
/// `through` below it; `store::held`). Meanwhile:
/// - the member publishes nothing (its outstanding speculation defers
///   every commit);
/// - a later transaction the holder shipped past it retires alone: the
///   out-of-order segment's `through` stops below the deferred one;
/// - if the chunk's owner never returns and a later epoch's segment comes
///   first (a takeover), the manifest is stranded and rolled back: the
///   member's namespace is the log's again.
#[test]
fn a_streamed_manifest_held_back_by_its_chunk_stays_speculation_until_it_ships() {
    let (f, g) = (ino(70), ino(71));
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    let seg1 = [create("f", f, t0), create("g", g, t0 + 1)];
    let r_manifest = Rid {
        node: 3,
        incarnation: 1,
        seq: 1,
    };
    let r_chmod = Rid {
        node: 2,
        incarnation: 1,
        seq: 1,
    };
    let manifest_tx = [
        LogRecord::WriteManifest {
            ino: f,
            base_manifest: None,
            manifest: vec![0xAB; 24],
            size: 4096,
            time_ns: t0 + 2,
        },
        completed(r_manifest),
    ];
    let chmod_tx = [chmod(g, 0o600, t0 + 3), completed(r_chmod)];
    let basis = |m: &Meta| m.read_consistent(|snap| m.publish_basis_at(snap)).unwrap();

    // The log as it will read once the chunk is up: seq 2 is the chmod
    // the holder shipped past the deferred manifest, seq 3 the manifest.
    let shipped = Meta::open_in_memory().unwrap();
    apply(&shipped, 1, 1, &seg1);
    let only_chmod = Meta::open_in_memory().unwrap();
    apply(&only_chmod, 1, 1, &seg1);
    apply(&only_chmod, 2, 1, &chmod_tx);
    shipped
        .apply_segment_rows(2, 1, 0, &[4, 5], &[], &chmod_tx, &TouchSet::default())
        .unwrap();
    shipped
        .apply_segment_rows(3, 1, 5, &[2, 3], &[], &manifest_tx, &TouchSet::default())
        .unwrap();

    let member = |then_ship: bool| {
        let meta = Meta::open_in_memory().unwrap();
        apply(&meta, 1, 1, &seg1);
        // The holder's epoch journal: jseq 2–3 the manifest, 4–5 the chmod.
        meta.install_streamed(1, 2, 3, &manifest_tx).unwrap();
        meta.install_streamed(1, 4, 5, &chmod_tx).unwrap();
        assert!(meta.has_outstanding_speculation());
        assert_eq!(basis(&meta), PublishBasis::Defer, "nothing published");
        // The holder ships the chmod ahead of the deferred manifest: its
        // `through` stops below jseq 2 (`journal_through_after`).
        meta.apply_segment_rows(2, 1, 1, &[4, 5], &[], &chmod_tx, &TouchSet::default())
            .unwrap();
        assert!(
            meta.has_outstanding_speculation(),
            "the deferred manifest was retired by a segment that did not carry it"
        );
        assert_eq!(basis(&meta), PublishBasis::Defer, "nothing published");
        assert!(meta.completed_position(r_manifest).unwrap().is_none());
        if then_ship {
            meta.apply_segment_rows(3, 1, 5, &[2, 3], &[], &manifest_tx, &TouchSet::default())
                .unwrap();
        } else {
            // Its chunk's owner never came back; another node took the
            // lease over (epoch 2) and its marker strands the epoch-1
            // speculation.
            meta.strand_below_epoch(2).unwrap();
        }
        meta
    };

    let meta = member(true);
    assert!(!meta.has_outstanding_speculation());
    assert_eq!(raw_ns(&meta), raw_ns(&shipped));

    let meta = member(false);
    assert!(!meta.has_outstanding_speculation());
    assert_eq!(
        raw_ns(&meta),
        raw_ns(&only_chmod),
        "the stranded manifest was not rolled back"
    );
}

/// Fix "capture under an epoch hold", `repair drop-held --remote`: the
/// chunk's owner never came back and the operator dropped the deferred
/// manifest on the holder (a conflict copy there; the rows leave its
/// journal). The holder's next segment, same epoch, ships *past* the
/// dropped rows — `through` beyond them, its rows not named — and the
/// member must roll its streamed copy back, not keep it as confirmed
/// (M9's first rule retired it by `through` alone, and the dropped write
/// stayed in the member's namespace for good). A later streamed
/// transaction on the same inode (its chmod) is rolled back with it and
/// comes back only through the log.
#[test]
fn a_streamed_transaction_the_tenure_ships_past_is_rolled_back() {
    let (f, g) = (ino(72), ino(73));
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    let seg1 = [create("f", f, t0), create("g", g, t0 + 1)];
    let rid = |node: u64| Rid {
        node,
        incarnation: 1,
        seq: 1,
    };
    let manifest_tx = [
        LogRecord::WriteManifest {
            ino: f,
            base_manifest: None,
            manifest: vec![0xAB; 24],
            size: 4096,
            time_ns: t0 + 2,
        },
        completed(rid(3)),
    ];
    // Depends on the manifest (the same inode); requeued by rid on the
    // holder when the manifest is dropped, and re-journaled after.
    let chmod_f_tx = [chmod(f, 0o600, t0 + 3), completed(rid(2))];
    let chmod_g_tx = [chmod(g, 0o640, t0 + 4), completed(rid(4))];
    // What the holder ships after the drop: the unrelated chmod (its
    // rows 6–7, through 7: past the dropped 2–3 and the requeued 4–5).
    let replayed_chmod_f = [chmod(f, 0o600, t0 + 5), completed(rid(2))];

    let member = Meta::open_in_memory().unwrap();
    apply(&member, 1, 1, &seg1);
    member.install_streamed(1, 2, 3, &manifest_tx).unwrap();
    member.install_streamed(1, 4, 5, &chmod_f_tx).unwrap();
    member.install_streamed(1, 6, 7, &chmod_g_tx).unwrap();
    assert_eq!(member.speculation_counts().unwrap().outstanding, 3);
    assert!(member.manifest(f).unwrap().is_some());

    member
        .apply_segment_rows(2, 1, 7, &[6, 7], &[], &chmod_g_tx, &TouchSet::default())
        .unwrap();
    assert!(
        member.manifest(f).unwrap().is_none(),
        "the dropped manifest stayed in the member's namespace"
    );
    assert_eq!(
        member.getattr(f).unwrap().unwrap().mode & 0o7777,
        0o644,
        "the chmod streamed after the dropped manifest was kept"
    );
    assert_eq!(member.getattr(g).unwrap().unwrap().mode & 0o7777, 0o640);
    assert!(
        !member.has_outstanding_speculation(),
        "{:?}",
        member.speculation_counts()
    );

    // The requeued chmod comes back through the log.
    member
        .apply_segment_rows(
            3,
            1,
            9,
            &[8, 9],
            &[],
            &replayed_chmod_f,
            &TouchSet::default(),
        )
        .unwrap();
    let expected = Meta::open_in_memory().unwrap();
    apply(&expected, 1, 1, &seg1);
    apply(&expected, 2, 1, &chmod_g_tx);
    apply(&expected, 3, 1, &replayed_chmod_f);
    assert_eq!(raw_ns(&member), raw_ns(&expected));
}

/// Fix "capture under an epoch hold": installing a streamed transaction
/// this replica already holds (the holder streams from its start again;
/// a restarted subscriber lost the cursor that skipped it) changes
/// nothing — flex-backup seed 1230's member re-applied `rename f2 f3`
/// after a later create had reused `f2`.
#[test]
fn a_streamed_transaction_installed_twice_is_installed_once() {
    let (f2, f3) = (ino(80), ino(81));
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    let meta = Meta::open_in_memory().unwrap();
    apply(&meta, 1, 1, &[create("f2", f2, t0)]);
    let rename = [
        LogRecord::Rename {
            parent: ROOT_INO,
            name: "f2".into(),
            new_parent: ROOT_INO,
            new_name: "f3".into(),
            time_ns: t0 + 1,
        },
        completed(rid(1)),
    ];
    let recreate = [create("f2", f3, t0 + 2), completed(rid(2))];
    meta.install_streamed(1, 2, 3, &rename).unwrap();
    meta.install_streamed(1, 4, 5, &recreate).unwrap();
    assert_eq!(meta.streamed_tip(1).unwrap(), Some(5));
    let before = raw_ns(&meta);
    meta.install_streamed(1, 2, 3, &rename).unwrap();
    meta.install_streamed(1, 4, 5, &recreate).unwrap();
    assert_eq!(raw_ns(&meta), before, "installed twice");
    assert_eq!(meta.speculation_counts().unwrap().outstanding, 2);
    assert_eq!(
        MetaStore::lookup(&meta, ROOT_INO, "f3")
            .unwrap()
            .map(|e| e.ino),
        Some(f2),
        "the rename kept the inode it moved"
    );
    assert_eq!(meta.streamed_tip(2).unwrap(), None);
}

/// Fix "capture under an epoch hold": the log refuses a rid this node
/// holds a shadow of (the holder dropped the op: `repair drop-held`).
/// The shadow is rolled back and nothing is queued for replay — the
/// outcome is final and the holder's conflict copy is the artifact.
#[test]
fn a_refused_rid_in_the_log_rolls_its_shadow_back_without_a_replay() {
    let meta = Meta::open_in_memory().unwrap();
    let t0 = constellation_fs_core::types::now_ns() + 1_000_000_000;
    apply(&meta, 1, 1, &[create("a", ino(90), t0)]);
    let r = rid(31);
    let records = [create("b", ino(91), t0 + 1), completed(r)];
    assert!(meta
        .install_shadow(r, 1, &create_op("b", ino(91)), &records)
        .unwrap());
    assert!(MetaStore::lookup(&meta, ROOT_INO, "b").unwrap().is_some());
    apply(&meta, 2, 1, &[LogRecord::Refused { rid: r, errno: 5 }]);
    assert!(
        MetaStore::lookup(&meta, ROOT_INO, "b").unwrap().is_none(),
        "the shadow's effect was kept although the log refused its rid"
    );
    let counts = meta.speculation_counts().unwrap();
    assert_eq!(
        (counts.outstanding, counts.pending_replay),
        (0, 0),
        "{counts:?}"
    );
    assert!(meta.pending_replays().unwrap().is_empty());
}
