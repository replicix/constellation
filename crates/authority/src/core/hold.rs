#![allow(dead_code)] // FX-5 C2a: the record types are used by the next commit
//! FX-5: the epoch hold's durable record and its transfer numbers.
//!
//! A continuation epoch carries one lease (the *key*: [`HoldKey`]); one
//! node at a time holds that epoch's hold. Every hold of a key has a
//! transfer number: 0 at the carrier's adoption, plus one per give. A node
//! persists exactly one [`HoldRecord`] (`local["epoch_hold_rec"]`), and
//! every transition writes the whole record durably before the act that
//! depends on it:
//!
//! - `Held { key, n, at }`: this node holds `key`'s hold, taken as transfer
//!   `n` (0: the carrier's adoption); `at` is the epoch it journals under,
//!   what a restart re-adopts at;
//! - `Given { key, n, to }`: it gave the hold as transfer `n` to `to`
//!   (persisted with the let-go, before the answer leaves);
//! - `Ended { key, n, reclaim }`: it let the hold go without a transfer
//!   (the close, a deposition, a vanish, a release in a frozen epoch). No
//!   recipient exists by construction.
//!
//! The record is the hold: in memory the node never holds without a
//! `Held` record of it, and a restart re-adopts exactly the record.
//! Uniqueness of the hold follows from the numbers (`docs`: ADR text of
//! FX-5): gives are totally ordered by `n`, an offer of `n` is takeable
//! only by its recipient, and only until that node has passed `n`.
//!
//! The record is read once per process (`HoldRecState`); a value that
//! cannot be read or parsed is [`HoldRecState::Unreadable`] and blocks
//! every transition (fail closed), never "no record".

use crate::event::{Carrier, XferNo};
use crate::ids::{Epoch, NodeId};
use std::fmt;
use std::str::FromStr;

/// The carried lease an epoch hold stands on (node, epoch, expiry): the
/// key compared everywhere is those three fields.
pub type HoldKey = Carrier;

/// Whether a closer still owes the carried lease's re-claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reclaim {
    /// The close made the re-claim this node's to do.
    Owed,
    /// The re-claim is done or moot, or the hold went without a close.
    Settled,
}

/// A node's one persisted record of an epoch hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldRecord {
    /// Holds `key`'s hold, taken as transfer `n`; `at` is the epoch it
    /// journals under (`Core::epoch_hold_epoch_for`).
    Held { key: HoldKey, n: XferNo, at: Epoch },
    /// Gave `key`'s hold as transfer `n` to `to`.
    Given { key: HoldKey, n: XferNo, to: NodeId },
    /// Let `key`'s hold go without a transfer; `n` is the number it held
    /// at.
    Ended {
        key: HoldKey,
        n: XferNo,
        reclaim: Reclaim,
    },
}

impl HoldRecord {
    /// The carried lease the record is about.
    pub fn key(&self) -> HoldKey {
        match self {
            HoldRecord::Held { key, .. }
            | HoldRecord::Given { key, .. }
            | HoldRecord::Ended { key, .. } => *key,
        }
    }

    /// The transfer number the record carries.
    pub fn n(&self) -> XferNo {
        match self {
            HoldRecord::Held { n, .. }
            | HoldRecord::Given { n, .. }
            | HoldRecord::Ended { n, .. } => *n,
        }
    }
}

/// Why a stored record could not be read back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HoldRecordError {
    /// The stored value is not one of the three encodings.
    #[error("malformed epoch hold record {0:?}")]
    Malformed(String),
}

impl fmt::Display for HoldRecord {
    /// `"h:<node>:<epoch>:<expires>:<n>:<at>"`,
    /// `"g:<node>:<epoch>:<expires>:<n>:<to>"`,
    /// `"e:<node>:<epoch>:<expires>:<n>:<o|s>"`: colon-separated, every
    /// field always present.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let k = self.key();
        match self {
            HoldRecord::Held { n, at, .. } => write!(
                f,
                "h:{}:{}:{}:{}:{}",
                k.node, k.epoch, k.expires_unix_ms, n.0, at
            ),
            HoldRecord::Given { n, to, .. } => write!(
                f,
                "g:{}:{}:{}:{}:{}",
                k.node, k.epoch, k.expires_unix_ms, n.0, to
            ),
            HoldRecord::Ended { n, reclaim, .. } => write!(
                f,
                "e:{}:{}:{}:{}:{}",
                k.node,
                k.epoch,
                k.expires_unix_ms,
                n.0,
                match reclaim {
                    Reclaim::Owed => 'o',
                    Reclaim::Settled => 's',
                }
            ),
        }
    }
}

impl FromStr for HoldRecord {
    type Err = HoldRecordError;

    /// Anything but the three encodings is an error (never "no record":
    /// the empty string is the caller's `None`).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || HoldRecordError::Malformed(s.to_string());
        let parts: Vec<&str> = s.split(':').collect();
        let [tag, node, epoch, expires, n, last] = parts.as_slice() else {
            return Err(bad());
        };
        let key = Carrier {
            node: node.parse().map_err(|_| bad())?,
            epoch: epoch.parse().map_err(|_| bad())?,
            expires_unix_ms: expires.parse().map_err(|_| bad())?,
        };
        let n = XferNo(n.parse().map_err(|_| bad())?);
        match *tag {
            "h" => Ok(HoldRecord::Held {
                key,
                n,
                at: last.parse().map_err(|_| bad())?,
            }),
            "g" => Ok(HoldRecord::Given {
                key,
                n,
                to: last.parse().map_err(|_| bad())?,
            }),
            "e" => Ok(HoldRecord::Ended {
                key,
                n,
                reclaim: match *last {
                    "o" => Reclaim::Owed,
                    "s" => Reclaim::Settled,
                    _ => return Err(bad()),
                },
            }),
            _ => Err(bad()),
        }
    }
}

/// What this process knows of the record. Fail closed: `Unreadable`
/// blocks every hold transition (adoption, give, re-offer, take, nudge,
/// restore) until a later read succeeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HoldRecState {
    /// The read failed, or the value did not parse.
    Unreadable,
    /// What the store holds (`None`: no record).
    Known(Option<HoldRecord>),
}

impl HoldRecState {
    /// The highest transfer number of `key` this node ever took or gave:
    /// the record's own `n` when it is about `key`; 0 when there is no
    /// record of `key` (nothing taken or given); `None` when the record is
    /// unreadable (every caller declines).
    pub(crate) fn highest(&self, key: HoldKey) -> Option<XferNo> {
        match self {
            HoldRecState::Unreadable => None,
            HoldRecState::Known(Some(r)) if r.key() == key => Some(r.n()),
            HoldRecState::Known(_) => Some(XferNo(0)),
        }
    }

    /// The record, when readable.
    pub(crate) fn record(&self) -> Option<&HoldRecord> {
        match self {
            HoldRecState::Known(r) => r.as_ref(),
            HoldRecState::Unreadable => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> HoldKey {
        Carrier {
            node: 1,
            epoch: 4,
            expires_unix_ms: 1_700_000_123,
        }
    }

    #[test]
    fn every_record_round_trips() {
        let records = [
            HoldRecord::Held {
                key: key(),
                n: XferNo(0),
                at: 4,
            },
            HoldRecord::Held {
                key: key(),
                n: XferNo(u32::MAX),
                at: 5,
            },
            HoldRecord::Given {
                key: key(),
                n: XferNo(3),
                to: 2,
            },
            HoldRecord::Ended {
                key: key(),
                n: XferNo(2),
                reclaim: Reclaim::Owed,
            },
            HoldRecord::Ended {
                key: key(),
                n: XferNo(2),
                reclaim: Reclaim::Settled,
            },
            HoldRecord::Held {
                key: Carrier {
                    node: 3,
                    epoch: 1,
                    expires_unix_ms: -5,
                },
                n: XferNo(7),
                at: 2,
            },
        ];
        for r in records {
            let text = r.to_string();
            assert_eq!(text.parse::<HoldRecord>(), Ok(r), "{text}");
        }
        assert_eq!(
            HoldRecord::Given {
                key: key(),
                n: XferNo(3),
                to: 2
            }
            .to_string(),
            "g:1:4:1700000123:3:2"
        );
    }

    #[test]
    fn every_malformed_form_is_an_error_never_no_record() {
        for bad in [
            "",
            "0",
            "4",
            "h",
            "h:1:4:5:0",
            "h:1:4:5:0:4:9",
            "x:1:4:5:0:4",
            "H:1:4:5:0:4",
            "h:a:4:5:0:4",
            "h:1:-4:5:0:4",
            "h:1:4:x:0:4",
            "h:1:4:5:-1:4",
            "h:1:4:5:4294967296:4",
            "h:1:4:5:0:",
            "g:1:4:5:0:two",
            "e:1:4:5:0:x",
            "e:1:4:5:0:",
            "e:1:4:5:0:os",
            " h:1:4:5:0:4",
            "h:1:4:5:0:4 ",
        ] {
            assert!(bad.parse::<HoldRecord>().is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn highest_is_the_records_own_number_for_its_key_only() {
        let held = HoldRecord::Held {
            key: key(),
            n: XferNo(5),
            at: 4,
        };
        let known = HoldRecState::Known(Some(held));
        assert_eq!(known.highest(key()), Some(XferNo(5)));
        let other = Carrier {
            expires_unix_ms: key().expires_unix_ms + 1,
            ..key()
        };
        assert_eq!(known.highest(other), Some(XferNo(0)));
        assert_eq!(HoldRecState::Known(None).highest(key()), Some(XferNo(0)));
        assert_eq!(HoldRecState::Unreadable.highest(key()), None);
    }

    #[test]
    fn the_number_after_the_last_is_none() {
        assert_eq!(XferNo(u32::MAX).next(), None);
        assert_eq!(XferNo(1).next(), Some(XferNo(2)));
    }
}
