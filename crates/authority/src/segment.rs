//! The S3 log segment envelope, byte-identical to `cli::shipper`'s
//! private `SegmentEnvelope` (postcard, version 2). Phase 2 of plan 30 M5
//! makes the shipper use this module instead of its own copy; until then
//! the two must stay in step, which `tests` in this module pins with a
//! fixed encoding.

use constellation_meta::LogRecord;
use serde::{Deserialize, Serialize};

const SEGMENT_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
struct SegmentEnvelope {
    v: u32,
    node: u64,
    #[serde(default)]
    epoch: u64,
    records: Vec<LogRecord>,
}

/// A decoded segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub node: u64,
    pub epoch: u64,
    pub records: Vec<LogRecord>,
}

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("log segment postcard decode: {0}")]
    Decode(#[from] postcard::Error),
    #[error("unsupported log segment version {0}")]
    Version(u32),
}

pub fn encode(node: u64, epoch: u64, records: &[LogRecord]) -> Result<Vec<u8>, SegmentError> {
    Ok(postcard::to_allocvec(&SegmentEnvelope {
        v: SEGMENT_VERSION,
        node,
        epoch,
        records: records.to_vec(),
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
        let bytes = encode(5, 3, &records).unwrap();
        // `v=2, node=5, epoch=3` then the record vector, all postcard
        // varints: the prefix the shipper writes for the same envelope.
        assert_eq!(&bytes[..3], &[2, 5, 3]);
        let seg = decode(&bytes).unwrap();
        assert_eq!(seg.node, 5);
        assert_eq!(seg.epoch, 3);
        assert_eq!(seg.records, records);
        assert!(matches!(
            decode(&[1, 0, 0, 0]),
            Err(SegmentError::Version(1))
        ));
    }
}
