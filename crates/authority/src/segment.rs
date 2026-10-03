//! The S3 log segment envelope, byte-identical to `cli::shipper`'s
//! private `SegmentEnvelope` (postcard, version 2). Phase 2 of plan 30 M5
//! makes the shipper use this module instead of its own copy; until then
//! the two must stay in step, which `tests` in this module pins with a
//! fixed encoding.

use constellation_meta::LogRecord;
use serde::{Deserialize, Serialize};

const SEGMENT_VERSION: u32 = 3;

#[derive(Serialize, Deserialize)]
struct SegmentEnvelope {
    v: u32,
    node: u64,
    epoch: u64,
    records: Vec<LogRecord>,
    /// Plan 30 §M6: the shipping holder's journal seq every row at or
    /// below which has now shipped (0: none). Appended last, so a reader
    /// that decodes only the first four fields (the harness's log
    /// checks) still reads the envelope.
    through: u64,
    /// Plan 30 §M9: the shipping holder's journal seq of every journal
    /// row in `records` (atime ride-along rows have none), in record
    /// order. A backup trims its `backup_tail` by them, and a subscriber
    /// retires the pre-S3 streamed speculation they confirm — exactly,
    /// even when M4/M7 ship transactions out of journal order.
    rows: Vec<u64>,
    /// Plan 30 §M11: the delegation origin `(gen, idx)` of each row in
    /// `rows` (`(0, 0)`: the shipper's own). A delegate retires its own
    /// transactions by it; every replica keeps its per-generation applied
    /// index from it.
    origins: Vec<(u64, u64)>,
}

/// A decoded segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub node: u64,
    pub epoch: u64,
    pub records: Vec<LogRecord>,
    /// Plan 30 §M6: see `SegmentEnvelope::through`. A replica that applied
    /// this segment has the shipping tenure's journal through
    /// `(epoch, through)`.
    pub through: u64,
    /// Plan 30 §M9: see `SegmentEnvelope::rows`.
    pub rows: Vec<u64>,
    /// Plan 30 §M11: see `SegmentEnvelope::origins`.
    pub origins: Vec<(u64, u64)>,
}

impl Segment {
    /// The journal position applying this segment reaches.
    pub fn journal_pos(&self) -> constellation_meta::JournalPos {
        constellation_meta::JournalPos {
            epoch: self.epoch,
            jseq: self.through,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("log segment postcard decode: {0}")]
    Decode(#[from] postcard::Error),
    #[error("unsupported log segment version {0}")]
    Version(u32),
}

pub fn encode(
    node: u64,
    epoch: u64,
    through: u64,
    rows: &[u64],
    origins: &[(u64, u64)],
    records: &[LogRecord],
) -> Result<Vec<u8>, SegmentError> {
    Ok(postcard::to_allocvec(&SegmentEnvelope {
        v: SEGMENT_VERSION,
        node,
        epoch,
        records: records.to_vec(),
        through,
        rows: rows.to_vec(),
        origins: origins.to_vec(),
    })?)
}

pub fn decode(payload: &[u8]) -> Result<Segment, SegmentError> {
    let env: SegmentEnvelope = postcard::from_bytes(payload)?;
    if env.v != SEGMENT_VERSION {
        return Err(SegmentError::Version(env.v));
    }
    Ok(Segment {
        node: env.node,
        epoch: env.epoch,
        records: env.records,
        through: env.through,
        rows: env.rows,
        origins: env.origins,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_meta::Rid;

    #[test]
    fn round_trips_and_matches_the_shipper_encoding() {
        let records = vec![
            LogRecord::Unlink {
                parent: 1,
                name: "x".into(),
                time_ns: 3,
            },
            LogRecord::Completed {
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: 9,
                },
            },
        ];
        let bytes = encode(5, 3, 17, &[16, 17], &[(0, 0), (2, 5)], &records).unwrap();
        // `v=3, node=5, epoch=3` then the record vector, all postcard
        // varints: the prefix the shipper writes for the same envelope.
        assert_eq!(&bytes[..3], &[3, 5, 3]);
        let seg = decode(&bytes).unwrap();
        assert_eq!(seg.node, 5);
        assert_eq!(seg.epoch, 3);
        assert_eq!(seg.records, records);
        assert_eq!(seg.through, 17);
        assert_eq!(seg.rows, vec![16, 17]);
        assert_eq!(seg.origins, vec![(0, 0), (2, 5)]);
        assert!(matches!(
            decode(&[1, 0, 0, 0, 0, 0, 0]),
            Err(SegmentError::Version(1))
        ));
    }
}
