//! Fresh-chunk hints: which node wrote the chunks of a manifest this
//! node just applied (campaign 6 D2-OVH).
//!
//! A reader on another node learns of a new file's manifest over the
//! log stream (or the pre-S3 stream) within milliseconds of the close,
//! but the writer's cooperative-cache delta that advertises the chunk
//! is gossiped on a 250 ms tick and usually arrives later. Until it
//! does, no mirror lists a holder, and the read went to S3: one GET on
//! the visibility path of every freshly written file (0.2 s from
//! us-west-2 to a bucket in Milan, 300 ms under the harness's injected
//! latency), although the writer has the chunk in its cache.
//!
//! So every manifest applied from another node leaves a hint: its chunk
//! hashes, and the node that wrote it — the node of the request id in
//! the transaction's `Completed { rid }` (the requester whose close
//! uploaded the chunks, whoever sequenced it). A demand fetch whose
//! chunk no mirror lists asks that node first; S3 stays the hedge and
//! the last resort. A hint is only ever a guess about where bytes are:
//! a wrong one costs a declined request on the P2P link, never
//! correctness (chunks are verified by hash).

use constellation_fs_core::manifest::{ChunkInfo, Manifest};
use constellation_fs_core::ChunkHash;
use constellation_meta::LogRecord;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long a hint stays usable: by then the writer's delta (or a
/// reconciliation round) has long put the chunk into the mirrors.
pub(super) const FRESH_HINT_TTL: Duration = Duration::from_secs(30);
/// At most this many hints are kept (32 bytes of hash, a node id and a
/// timestamp each); past it the expired ones go, then the oldest half.
pub(super) const FRESH_HINT_CAP: usize = 16_384;

#[derive(Default)]
pub(super) struct FreshHints {
    map: HashMap<[u8; 32], (u64, Instant)>,
}

impl FreshHints {
    pub(super) fn note(&mut self, origin: u64, hash: &ChunkHash, now: Instant) {
        if self.map.len() >= FRESH_HINT_CAP && !self.map.contains_key(&hash.0) {
            self.map
                .retain(|_, (_, at)| now.saturating_duration_since(*at) < FRESH_HINT_TTL);
            if self.map.len() >= FRESH_HINT_CAP {
                let mut ages: Vec<Instant> = self.map.values().map(|(_, at)| *at).collect();
                ages.sort_unstable();
                let cut = ages[ages.len() / 2];
                self.map.retain(|_, (_, at)| *at > cut);
            }
        }
        self.map.insert(hash.0, (origin, now));
    }

    /// The node that wrote `hash`, if a manifest naming it was applied
    /// here within [`FRESH_HINT_TTL`].
    pub(super) fn origin(&self, hash: &ChunkHash, now: Instant) -> Option<u64> {
        self.map
            .get(&hash.0)
            .filter(|(_, at)| now.saturating_duration_since(*at) < FRESH_HINT_TTL)
            .map(|(origin, _)| *origin)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.map.len()
    }
}

/// The chunks (and spilled chunk lists) every `WriteManifest` in
/// `records` names, each with the node whose request wrote it.
///
/// A transaction's records are contiguous, and its `Completed { rid }`
/// follows its *first* record (`meta::store::journal::append_tx`), so a
/// record directly followed by a `Completed` opens a transaction whose
/// origin is that rid's node, and the records up to the next such pair
/// belong to it. A transaction without a rid (the sequencer's own
/// bookkeeping) is attributed to the one before it; a manifest in one
/// before any rid is skipped. Either way the result is a hint.
pub(super) fn written_chunks(records: &[LogRecord]) -> Vec<(u64, ChunkHash)> {
    let mut out = Vec::new();
    let mut origin: Option<u64> = None;
    for (i, rec) in records.iter().enumerate() {
        if let Some(LogRecord::Completed { rid }) = records.get(i + 1) {
            if !matches!(rec, LogRecord::Completed { .. }) {
                origin = Some(rid.node);
            }
        }
        let LogRecord::WriteManifest { manifest, .. } = rec else {
            continue;
        };
        let Some(node) = origin else {
            continue;
        };
        let Ok(m) = Manifest::decode(manifest) else {
            continue;
        };
        match &m.chunks {
            ChunkInfo::Inline(chunks) => {
                out.extend(chunks.values().map(|h| (node, *h)));
            }
            ChunkInfo::Spilled(h) => out.push((node, *h)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_meta::Rid;

    fn manifest(hashes: &[ChunkHash]) -> Vec<u8> {
        let mut m = Manifest::empty(4096);
        let mut chunks = std::collections::BTreeMap::new();
        for (i, h) in hashes.iter().enumerate() {
            chunks.insert(i as u64, *h);
        }
        m.chunks = ChunkInfo::Inline(chunks);
        m.file_len = 4096 * hashes.len() as u64;
        m.encode()
    }

    fn write(ino: u64, hashes: &[ChunkHash]) -> LogRecord {
        LogRecord::WriteManifest {
            ino,
            base_manifest: None,
            manifest: manifest(hashes),
            size: 4096,
            time_ns: 1,
            mtime_ns: 1,
        }
    }

    fn done(node: u64, seq: u64) -> LogRecord {
        LogRecord::Completed {
            rid: Rid {
                node,
                incarnation: 1,
                seq,
            },
        }
    }

    fn setattr(ino: u64) -> LogRecord {
        LogRecord::Setattr {
            ino,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
            time_ns: 1,
        }
    }

    #[test]
    fn each_manifest_is_attributed_to_its_transactions_requester() {
        let (h1, h2, h3) = (ChunkHash([1; 32]), ChunkHash([2; 32]), ChunkHash([3; 32]));
        let got = written_chunks(&[
            // Node 7's close: the manifest opens its transaction.
            write(10, &[h1]),
            done(7, 1),
            // Node 9's op: a setattr first, its manifest later.
            setattr(11),
            done(9, 4),
            write(11, &[h2, h3]),
        ]);
        assert_eq!(got, vec![(7, h1), (9, h2), (9, h3)]);
    }

    #[test]
    fn a_manifest_before_any_request_id_is_no_hint() {
        let h = ChunkHash([5; 32]);
        assert!(written_chunks(&[write(10, &[h])]).is_empty());
        assert!(written_chunks(&[setattr(3), write(10, &[h])]).is_empty());
    }

    #[test]
    fn a_spilled_manifest_hints_its_chunk_list() {
        let list = ChunkHash([8; 32]);
        let mut m = Manifest::empty(4096);
        m.chunks = ChunkInfo::Spilled(list);
        let rec = LogRecord::WriteManifest {
            ino: 3,
            base_manifest: None,
            manifest: m.encode(),
            size: 1,
            time_ns: 1,
            mtime_ns: 1,
        };
        assert_eq!(written_chunks(&[rec, done(2, 1)]), vec![(2, list)]);
    }

    #[test]
    fn hints_expire_and_stay_bounded() {
        let mut hints = FreshHints::default();
        let t0 = Instant::now();
        let h = ChunkHash([1; 32]);
        hints.note(4, &h, t0);
        assert_eq!(hints.origin(&h, t0), Some(4));
        assert_eq!(hints.origin(&h, t0 + FRESH_HINT_TTL), None);
        for i in 0..(FRESH_HINT_CAP as u64 + 10) {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&i.to_le_bytes());
            k[31] = 0xff;
            hints.note(1, &ChunkHash(k), t0 + Duration::from_millis(i));
        }
        assert!(hints.len() <= FRESH_HINT_CAP);
    }
}
