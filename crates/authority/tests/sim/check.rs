//! The end-of-run checks, mirroring `constellation-model`'s properties:
//! `converged_at_quiescence`, `commits_are_log_prefixes`, exactly-once
//! over the log's `Completed { rid }` records, and (in `history.rs`)
//! linearizability.
//!
//! The oracle is the bucket's log replayed from scratch onto a fresh
//! `Meta` through the very same `Replica::apply_segment` the nodes use,
//! with the same epoch fencing — so "the state derived from the durable
//! log" is computed by production code, not by a second implementation.

use super::history::NsRet;
use super::node::CommitRecord;
use constellation_authority::{segment, Replica, Seq};
use constellation_meta::{LogRecord, Meta, Rid};
use constellation_store_s3::LogStore;
use object_store::ObjectStore;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// A raw `ns` dump: key/value pairs in key order.
pub type Dump = Vec<(Vec<u8>, Vec<u8>)>;

pub struct Oracle {
    pub head: Seq,
    pub final_dump: Dump,
    /// `ns` dumps at the applied positions the commits claim.
    pub at: BTreeMap<Seq, Dump>,
    /// How many applied (unfenced) segments completed each rid.
    pub completed: HashMap<Rid, usize>,
    /// Where in the log each rid completed (segment, record index): the
    /// linearization order of every op that took effect.
    pub completed_at: HashMap<Rid, (u64, usize)>,
    pub segments: usize,
    pub fenced: usize,
    /// One line per segment, for failure reports.
    pub describe: Vec<String>,
}

pub async fn replay_log(raw: Arc<dyn ObjectStore>, snapshots_at: &BTreeSet<Seq>) -> Oracle {
    let log = LogStore::new(raw);
    let meta = Meta::open_in_memory().expect("oracle meta");
    meta.set_node_prefix(4_000_000).expect("prefix");
    let mut seqs = log.list_segments().await.expect("list segments");
    seqs.sort_unstable();
    let mut max_epoch = 0u64;
    let mut completed: HashMap<Rid, usize> = HashMap::new();
    let mut completed_at: HashMap<Rid, (u64, usize)> = HashMap::new();
    let mut at = BTreeMap::new();
    let mut fenced = 0;
    let mut head = 0;
    let mut describe = Vec::new();
    let mut expected = seqs.first().copied().unwrap_or(1);
    if snapshots_at.contains(&0) {
        at.insert(0, meta.ns_dump().expect("dump"));
    }
    for seq in &seqs {
        assert_eq!(*seq, expected, "the log has a gap before {seq}");
        expected = seq + 1;
        let payload = log.get_segment(*seq).await.expect("segment");
        let seg = segment::decode(&payload).expect("decode");
        describe.push(format!(
            "seq {seq} epoch {} node {} through {} rows {:?} records {:?}",
            seg.epoch,
            seg.node,
            seg.through,
            seg.rows,
            seg.records
                .iter()
                .map(|r| match r {
                    LogRecord::Completed { rid } =>
                        format!("Completed({},{},{})", rid.node, rid.incarnation, rid.seq),
                    LogRecord::Create {
                        parent, name, ino, ..
                    } => format!("Create({parent:#x}/{name},{ino:#x})"),
                    LogRecord::Unlink { parent, name, .. } => format!("Unlink({parent:#x}/{name})"),
                    LogRecord::Rename {
                        parent,
                        name,
                        new_parent,
                        new_name,
                        ..
                    } => format!("Rename({parent:#x}/{name}->{new_parent:#x}/{new_name})"),
                    LogRecord::Refused { rid, code } => format!(
                        "Refused({},{},{};{})",
                        rid.node,
                        rid.incarnation,
                        rid.seq,
                        code.posix_name()
                    ),
                    LogRecord::Recall { gen, .. } => format!("Recall(g{gen})"),
                    other => format!("{other:?}")
                        .split_whitespace()
                        .next()
                        .unwrap_or("?")
                        .to_string(),
                })
                .collect::<Vec<_>>()
        ));
        if seg.epoch > 0 && seg.epoch < max_epoch {
            fenced += 1;
            Replica::skip_segment(&meta, *seq).expect("skip");
        } else {
            for (i, rec) in seg.records.iter().enumerate() {
                if let LogRecord::Completed { rid } = rec {
                    *completed.entry(*rid).or_default() += 1;
                    completed_at.entry(*rid).or_insert((*seq, i));
                }
            }
            Replica::apply_segment(
                &meta,
                *seq,
                seg.epoch,
                seg.through,
                &seg.rows,
                &seg.origins,
                &seg.records,
            )
            .expect("apply");
            max_epoch = max_epoch.max(seg.epoch);
        }
        head = *seq;
        if snapshots_at.contains(seq) {
            at.insert(*seq, meta.ns_dump().expect("dump"));
        }
    }
    Oracle {
        head,
        final_dump: meta.ns_dump().expect("dump"),
        at,
        completed,
        completed_at,
        segments: seqs.len(),
        fenced,
        describe,
    }
}

/// Directory and file timestamps are set, not merged, by replay
/// (`touch_times_tx`), so a shadow applied ahead of an older segment
/// leaves a different mtime/ctime than the plain replay does — a known
/// relaxation plan 30 §M12 turns into a max-merge. The checks compare
/// namespace content with times zeroed.
fn mask_times(k: &[u8], v: &[u8]) -> Vec<u8> {
    use constellation_mtree::record::{DentryRecord, InodeRecord};
    match k.first() {
        Some(1) => {
            if let Ok(mut rec) = InodeRecord::decode(v) {
                rec.attrs.mtime_ns = 0;
                rec.attrs.ctime_ns = 0;
                return rec.encode();
            }
        }
        Some(2) => {
            if let Ok(mut rec) = DentryRecord::decode(v) {
                rec.attrs.mtime_ns = 0;
                rec.attrs.ctime_ns = 0;
                return rec.encode();
            }
        }
        _ => {}
    }
    v.to_vec()
}

fn describe_key(k: &[u8]) -> String {
    match k.first() {
        Some(1) if k.len() == 9 => format!(
            "inode {:#x}",
            u64::from_be_bytes(k[1..9].try_into().unwrap())
        ),
        Some(2) if k.len() > 9 => format!(
            "dentry {:#x}/{}",
            u64::from_be_bytes(k[1..9].try_into().unwrap()),
            String::from_utf8_lossy(&k[9..])
        ),
        _ => format!("key {k:?}"),
    }
}

fn first_difference(a: &[(Vec<u8>, Vec<u8>)], b: &[(Vec<u8>, Vec<u8>)]) -> Option<String> {
    let am: BTreeMap<_, _> = a
        .iter()
        .map(|(k, v)| (k.clone(), mask_times(k, v)))
        .collect();
    let bm: BTreeMap<_, _> = b
        .iter()
        .map(|(k, v)| (k.clone(), mask_times(k, v)))
        .collect();
    let mut diffs = Vec::new();
    for (k, v) in &am {
        match bm.get(k) {
            None => diffs.push(format!(
                "{} only on the left (value {:?})",
                describe_key(k),
                v
            )),
            Some(w) if w != v => diffs.push(format!(
                "{} differs (left {:?}, right {:?})",
                describe_key(k),
                v,
                w
            )),
            _ => {}
        }
    }
    for (k, w) in &bm {
        if !am.contains_key(k) {
            diffs.push(format!(
                "{} only on the right (value {:?})",
                describe_key(k),
                w
            ));
        }
    }
    if diffs.is_empty() {
        None
    } else {
        Some(diffs.join("; "))
    }
}

/// Every quiescent live replica equals the log-derived state.
pub fn check_convergence(replicas: &[(u64, Dump)], oracle: &Oracle) -> Result<(), String> {
    for (node, dump) in replicas {
        if let Some(diff) = first_difference(dump, &oracle.final_dump) {
            return Err(format!(
                "node {node} did not converge to the log-derived state at head {}: {diff}",
                oracle.head
            ));
        }
    }
    Ok(())
}

/// Every published state equals the log-derived state at its claimed
/// applied position.
pub fn check_commits(commits: &[CommitRecord], oracle: &Oracle) -> Result<(), String> {
    for c in commits {
        let Some(expected) = oracle.at.get(&c.applied) else {
            return Err(format!(
                "commit {} by node {} claims applied {} which the oracle did not snapshot",
                c.seq, c.node, c.applied
            ));
        };
        if let Some(diff) = first_difference(&c.dump, expected) {
            return Err(format!(
                "commit {} by node {} is not the log prefix at {}: {diff}",
                c.seq, c.node, c.applied
            ));
        }
    }
    Ok(())
}

/// No rid completes twice in the log; an op that returned success
/// completed exactly once; an op refused with an errno never completed.
/// Tentative ops are exempt from the "exactly" part (their replay may
/// still be pending, or became a conflict copy) but never from "at most
/// once".
pub fn check_exactly_once(
    returned: &[(Rid, NsRet)],
    tentative: &std::collections::HashSet<Rid>,
    oracle: &Oracle,
    converged: bool,
) -> Result<(), String> {
    for (rid, n) in &oracle.completed {
        if *n > 1 {
            return Err(format!("rid {rid:?} completed {n} times in the log"));
        }
    }
    if !converged {
        return Ok(());
    }
    for (rid, ret) in returned {
        let n = oracle.completed.get(rid).copied().unwrap_or(0);
        if tentative.contains(rid) {
            continue;
        }
        match ret {
            NsRet::Ok if n != 1 => {
                return Err(format!(
                    "rid {rid:?} returned success but completed {n} times in the log"
                ))
            }
            NsRet::Eexist | NsRet::Enoent if n != 0 => {
                return Err(format!(
                    "rid {rid:?} was refused ({ret:?}) but completed {n} times in the log"
                ))
            }
            _ => {}
        }
    }
    Ok(())
}

/// No client is told "in doubt" (`EIO`) for a rid whose outcome the
/// answering node already had: its completion was in an unfenced log
/// segment at or below the log sequence that node's replica had applied
/// when it answered. (An op accepted by a holder that then stalled, and
/// whose requester took the lease over, used to wait out its deadline
/// for a log that already carried it — or carried it through the
/// successor's own ship — and answer in doubt.)
pub fn check_in_doubt_answers(answers: &[(u64, Rid, Seq)], oracle: &Oracle) -> Result<(), String> {
    for (node, rid, applied) in answers {
        if let Some((seq, _)) = oracle.completed_at.get(rid) {
            if *seq <= *applied {
                return Err(format!(
                    "node {node} answered rid {rid:?} in doubt with its completion already \
                     applied (log seq {seq} <= applied {applied}): a client was told EIO for \
                     a write that had landed"
                ));
            }
        }
    }
    Ok(())
}
