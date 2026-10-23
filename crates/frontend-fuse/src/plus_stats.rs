//! What a connection's `READDIRPLUS` pages handed the kernel (plan 40
//! §4.11): pages, entries with attributes, and the entries that went out
//! name-only, by the reason they did.
//!
//! An entry with attributes is a kernel lookup of its name; a name-only
//! one costs the kernel a `LOOKUP` when it needs the name. The split is
//! what tells an operator whether `ls -l` is being served from the page
//! (few `lookup` ops per listed name) or is falling back to the lookups
//! plan 40 exists to avoid.
//!
//! Counted when a page **succeeds**, from what the kernel's buffer took
//! (`DirPlusReply::done`): an entry the buffer refused, or a page that
//! failed, reached the kernel as nothing and is counted as nothing.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Why an entry of a plus page went out name-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameOnly {
    /// `CONSTELLATION_FUSE_READDIRPLUS=off` in this image while the kernel
    /// still asks for plus pages (a connection negotiated by an earlier
    /// image): every entry.
    Knob,
    /// `.` and `..` (and a node id the kernel would refuse: the root's, 0).
    Dots,
    /// The view handed no attributes: it could not vouch for them (a
    /// strict view, a rename or removal between the batch and the
    /// attribute read, a remote change not yet applied).
    View,
    /// The table of attribute reply marks was full (a pid-0 lister of a
    /// huge directory, plan 40 Q2): the kernel looks the name up when it
    /// needs it.
    Watch,
}

impl NameOnly {
    /// Every reason, in the order [`PlusCounts::name_only`] lists them.
    pub const ALL: [NameOnly; 4] = [Self::Knob, Self::Dots, Self::View, Self::Watch];

    /// The label `node.status` and `/metrics` carry.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Knob => "knob_off",
            Self::Dots => "dots",
            Self::View => "view",
            Self::Watch => "watch_full",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// One page's tallies, added to the connection's when the page succeeds.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PageTally {
    pub attr: u64,
    pub name_only: [u64; 4],
}

impl PageTally {
    pub(crate) fn name_only(&mut self, why: NameOnly) {
        self.name_only[why.index()] += 1;
    }
}

/// The connection's counters ([`SessionStats`](crate::SessionStats)
/// reports them).
#[derive(Debug, Default)]
pub struct PlusCounters {
    pages: AtomicU64,
    attr: AtomicU64,
    name_only: [AtomicU64; 4],
}

/// A snapshot of [`PlusCounters`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlusCounts {
    /// `READDIRPLUS` pages answered.
    pub pages: u64,
    /// Entries handed with attributes (each a kernel lookup reference).
    pub entries: u64,
    /// Entries handed name-only, by [`NameOnly::as_str`]; every reason is
    /// listed, in [`NameOnly::ALL`] order.
    pub name_only: Vec<(&'static str, u64)>,
}

impl PlusCounts {
    /// Entries handed name-only, whatever the reason.
    pub fn name_only_total(&self) -> u64 {
        self.name_only.iter().map(|(_, n)| n).sum()
    }
}

impl PlusCounters {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// A page that succeeded.
    pub(crate) fn page(&self, t: PageTally) {
        self.pages.fetch_add(1, Ordering::Relaxed);
        self.attr.fetch_add(t.attr, Ordering::Relaxed);
        for (c, n) in self.name_only.iter().zip(t.name_only) {
            c.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn counts(&self) -> PlusCounts {
        PlusCounts {
            pages: self.pages.load(Ordering::Relaxed),
            entries: self.attr.load(Ordering::Relaxed),
            name_only: NameOnly::ALL
                .iter()
                .map(|w| {
                    (
                        w.as_str(),
                        self.name_only[w.index()].load(Ordering::Relaxed),
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_adds_its_tally_by_reason() {
        let c = PlusCounters::new();
        assert_eq!(c.counts().pages, 0);
        let mut t = PageTally {
            attr: 5,
            ..Default::default()
        };
        t.name_only(NameOnly::Dots);
        t.name_only(NameOnly::Dots);
        t.name_only(NameOnly::Watch);
        c.page(t);
        c.page(PageTally::default());
        let n = c.counts();
        assert_eq!((n.pages, n.entries, n.name_only_total()), (2, 5, 3));
        assert_eq!(
            n.name_only,
            vec![("knob_off", 0), ("dots", 2), ("view", 0), ("watch_full", 1)]
        );
    }
}
