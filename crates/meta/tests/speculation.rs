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
    let seg1 = vec![create("other", ino(2), 20)];
    let seg2 = vec![chmod(ino(2), 0o600, 30)];

    let phantom = vec![create("phantom", ino(1), 10), completed(rid(1))];
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
                    meta.install_hint(&recs, next_seq, epoch).unwrap();
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
                    events.push(Event::Segment(recs));
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
    // in the order this replica applied them (a late segment already moved
    // before the local work it was inserted under). Retired requester
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
