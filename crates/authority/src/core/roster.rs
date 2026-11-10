//! The node's one membership object (plan 46 M1.3 step 10, ADR-50).
//!
//! Who could take this lease over, who may hold a read delegation of this
//! node's delegates, who the inbox polls, who a promise is asked of, who a
//! backup could be: every question about the other nodes of the filesystem is
//! answered here, from one set of inputs the core received: registry reads
//! (`Event::Roster`), directory snapshots (`Event::Peers`), the messages of
//! nodes (`note_heard`, at the one door every peer message passes) and the
//! node's own lapses (`note_lapse`). Only the core writes it, and only
//! through those methods; a reader uses the predicates. The driver produces
//! reads and never decides membership.
//!
//! What it keeps, and what each input does to it:
//!
//! - **The write-eligible roster.** The nodes of the last complete registry
//!   read, plus the nodes any newer incomplete read listed. Reads are
//!   numbered by the driver in the order it issued them (`seq`): a read not
//!   newer than the installed one is a stale arrival and is ignored. A
//!   complete read replaces the installed lists. A read that could not be
//!   completed only adds the nodes it did read, and marks the peer set
//!   unknown (unknown counts as peers) until a complete read newer than it.
//!   So a record that is corrupt, or vanishes between the LIST and the GET,
//!   or a read that arrives late, never shrinks what the node knows.
//! - **The nodes heard from** (`heard`): a node that sent this one a request
//!   since the directory last showed it, with the wait clock of its latest
//!   message. A read or snapshot drops it only if the read was issued after
//!   that message, for an older read may predate the node's registration.
//! - **The links** (the peer directory): reachability only. A snapshot also
//!   counts the lapses the driver had handed this core before it took it.
//! - **The audience**: every node that was on the roster or in the directory
//!   since the first complete snapshot, the readers that may hold a read
//!   delegation of this node's delegates. It only grows.
//!
//! The singleton test (`singleton`) is this object's too: a node is a
//! singleton only if it knows nobody but itself could write. Unknown never
//! means alone.

use super::*;

/// One registry read as the core receives it (`Event::Roster`).
#[derive(Debug, Clone)]
pub(crate) struct RosterRead {
    pub(crate) seq: u64,
    pub(crate) write_eligible: Vec<NodeId>,
    pub(crate) read_only: Vec<NodeId>,
    pub(crate) complete: bool,
    pub(crate) lapses: u64,
    pub(crate) issued: WaitMs,
}

/// The stamp of a read the object keeps.
#[derive(Debug, Clone, Copy)]
struct ReadStamp {
    seq: u64,
    lapses: u64,
    /// The wait clock the read was issued at (before its LIST).
    issued: WaitMs,
}

/// See the module doc.
#[derive(Debug)]
pub(crate) struct Roster {
    /// The write-eligible nodes of the last complete read.
    listed: BTreeSet<NodeId>,
    /// The nodes an incomplete read newer than that one listed.
    added: BTreeSet<NodeId>,
    /// `listed` and `added`, sorted: what the readers see.
    write_eligible: Vec<NodeId>,
    /// The last complete read's read-only members.
    read_only: BTreeSet<NodeId>,
    /// The last complete read installed: `None` until one has arrived.
    installed: Option<ReadStamp>,
    /// An incomplete read newer than `installed`: who the peers are is
    /// unknown.
    last_incomplete: Option<ReadStamp>,
    /// Nodes that sent this node a request (a forward, a settle, an inbox
    /// batch) since the directory last showed them, each with the wait clock
    /// of its latest request.
    heard: BTreeMap<NodeId, WaitMs>,
    /// The directory's view of the peers.
    links: BTreeMap<NodeId, PeerLink>,
    /// `Event::OwnLag` events this core has handled.
    lapses_seen: u64,
    /// The highest lapse count the driver had handed when it took a
    /// directory snapshot (`Event::Peers::lapses`).
    peers_lapses: u64,
    /// A directory snapshot has arrived at all.
    peers_seen: bool,
    /// A snapshot has arrived whose directory a complete registry scan
    /// filled; it stays set.
    peers_complete: bool,
    /// Every node that was on the roster or in the directory since the first
    /// complete snapshot. It only grows.
    audience: BTreeSet<NodeId>,
}

impl Roster {
    pub(crate) fn new() -> Self {
        Roster {
            listed: BTreeSet::new(),
            added: BTreeSet::new(),
            write_eligible: Vec::new(),
            read_only: BTreeSet::new(),
            installed: None,
            last_incomplete: None,
            heard: BTreeMap::new(),
            links: BTreeMap::new(),
            lapses_seen: 0,
            peers_lapses: 0,
            peers_seen: false,
            peers_complete: false,
            audience: BTreeSet::new(),
        }
    }

    // ------------------------------------------------------------ inputs

    /// A registry read. `false`: a stale arrival (not newer than the
    /// installed complete read, or, for an incomplete one, not newer than
    /// the last incomplete either), which changes nothing.
    ///
    /// A complete read replaces the installed lists; it ends the unknown
    /// peer set only if it is newer than the last incomplete read. An
    /// incomplete read only adds the nodes it listed: it removes nothing.
    pub(crate) fn install_read(&mut self, read: RosterRead) -> bool {
        let stamp = ReadStamp {
            seq: read.seq,
            lapses: read.lapses,
            issued: read.issued,
        };
        if self.installed.is_some_and(|i| read.seq <= i.seq) {
            return false;
        }
        if read.complete {
            self.listed = read.write_eligible.into_iter().collect();
            self.read_only = read.read_only.into_iter().collect();
            self.installed = Some(stamp);
            if self.last_incomplete.is_some_and(|l| l.seq < read.seq) {
                self.last_incomplete = None;
                self.added.clear();
            }
        } else {
            if self.last_incomplete.is_some_and(|l| read.seq <= l.seq) {
                return false;
            }
            self.added.extend(read.write_eligible);
            self.last_incomplete = Some(stamp);
        }
        self.write_eligible = self.listed.union(&self.added).copied().collect();
        self.note_audience();
        if read.complete {
            // The read is the authority on who is a peer: a node it does
            // not list is none, unless it asked after the read was issued.
            let (listed, issued) = (&self.write_eligible, read.issued);
            self.heard
                .retain(|n, at| listed.contains(n) || issued <= *at);
        }
        true
    }

    /// A directory snapshot taken at `issued`, with the lapse count the
    /// driver had handed before it, and whether a complete registry scan
    /// filled the directory behind it.
    pub(crate) fn install_links(
        &mut self,
        links: Vec<PeerLink>,
        registry_complete: bool,
        lapses: u64,
        issued: WaitMs,
    ) {
        self.links = links.into_iter().map(|l| (l.node, l)).collect();
        // The directory now shows every requester it knows: the links are
        // the truth from here on.
        let shown = &self.links;
        self.heard
            .retain(|n, at| !shown.contains_key(n) || issued <= *at);
        self.peers_seen = true;
        self.peers_complete |= registry_complete;
        self.note_audience();
        self.peers_lapses = self.peers_lapses.max(lapses);
    }

    /// A request from `node` reached this node (the one door every peer
    /// message passes). `true` if the node was not heard from since the
    /// directory last showed it. A node the installed read lists as
    /// read-only is not evidence: it cannot take the lease over.
    pub(crate) fn note_heard(&mut self, node: NodeId, at: WaitMs) -> bool {
        if self.read_only.contains(&node) {
            return false;
        }
        self.heard.insert(node, at).is_none()
    }

    /// This node handled an `Event::OwnLag`.
    pub(crate) fn note_lapse(&mut self) {
        self.lapses_seen += 1;
    }

    /// Grow the audience by the roster and the directory's links, once a
    /// complete snapshot has arrived. The one place it grows; it never
    /// shrinks.
    fn note_audience(&mut self) {
        if self.peers_seen && self.peers_complete {
            self.audience.extend(self.write_eligible.iter().copied());
            self.audience.extend(self.read_only.iter().copied());
            self.audience.extend(self.links.keys().copied());
        }
    }

    // -------------------------------------------------------- predicates

    /// The write-eligible nodes known: the last complete read's, and those
    /// a newer incomplete read listed (this node included when it is
    /// registered).
    pub(crate) fn write_eligible(&self) -> &[NodeId] {
        &self.write_eligible
    }

    /// The write-eligible nodes other than `me`.
    pub(crate) fn write_peers(&self, me: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.write_eligible
            .iter()
            .copied()
            .filter(move |n| *n != me)
    }

    /// `node` is known to be write-eligible.
    pub(crate) fn lists(&self, node: NodeId) -> bool {
        self.write_eligible.contains(&node)
    }

    /// Who the peers are is not known: no directory snapshot yet, none taken
    /// after this node's last lapse, or the newest registry read could not
    /// be completed. Unknown counts as peers.
    pub(crate) fn peers_unknown(&self) -> bool {
        !self.peers_seen || self.peers_lapses < self.lapses_seen || self.last_incomplete.is_some()
    }

    /// A node asked this one for something since the directory last showed
    /// it: a peer, whatever the reads in hand say.
    pub(crate) fn has_heard(&self) -> bool {
        !self.heard.is_empty()
    }

    /// The directory's links, by node.
    pub(crate) fn links(&self) -> &BTreeMap<NodeId, PeerLink> {
        &self.links
    }

    /// The nodes that may hold a read delegation of this node's delegates,
    /// or `None` while that set is not known: the roster and a complete
    /// directory snapshot must have arrived (an empty set would read as
    /// "nobody holds any").
    pub(crate) fn audience(&self, me: NodeId) -> Option<BTreeSet<NodeId>> {
        if !(self.installed.is_some() && self.peers_seen && self.peers_complete) {
            return None;
        }
        let mut peers = self.audience.clone();
        peers.extend(self.write_eligible.iter().copied());
        peers.extend(self.read_only.iter().copied());
        peers.extend(self.links.keys().copied());
        peers.remove(&me);
        Some(peers)
    }

    /// Whether `l` is a link to a peer of `me`, for the questions of who
    /// could back it or take over from it: a connected link to another node
    /// that the last complete read lists as write-eligible, or that the read
    /// cannot speak for. A read speaks only for the links up before it was
    /// issued (a node registers before it links), so a node linked since
    /// after the read (or whose link has no start), or any link when no
    /// read is installed, counts as a peer, listed or not: the next read
    /// may fail (S3 cut) and the node never enter the roster in hand. A
    /// node the read lists as read-only, or omits although it was linked
    /// before the read was issued, is none (it cannot take the lease, or it
    /// left).
    pub(crate) fn link_is_peer(&self, me: NodeId, l: &PeerLink) -> bool {
        if l.node == me || !l.connected || self.read_only.contains(&l.node) {
            return false;
        }
        if self.lists(l.node) {
            return true;
        }
        let issued = self.installed.map_or(WaitMs(i64::MIN), |i| i.issued);
        l.since.is_none_or(|since| issued < since)
    }

    /// Whether `me` knows that no other node could write (ADR-50, S10-I1
    /// and S10-I3): a complete read is installed, no incomplete read is
    /// newer, no lapse of this node's is newer than the installed read, the
    /// read lists nobody but `me` as write-eligible, nobody has messaged
    /// this node since the directory last showed them, and no link is a
    /// peer's that the read cannot speak for ([`Self::link_is_peer`]: a
    /// node linked since after the read was issued, or with no known start,
    /// that the read does not list as read-only).
    ///
    /// Read by the root's singleton test (`Core::singleton_now`).
    pub(crate) fn singleton(&self, me: NodeId) -> bool {
        let Some(installed) = self.installed else {
            return false;
        };
        self.last_incomplete.is_none()
            && installed.lapses >= self.lapses_seen
            && self.write_eligible.iter().all(|n| *n == me)
            && self.heard.is_empty()
            && !self.links.values().any(|l| self.link_is_peer(me, l))
    }

    // ------------------------------------------------------- test hooks

    #[cfg(test)]
    pub(crate) fn mark_seen_for_test(&mut self, seen: bool) {
        self.installed = seen.then_some(ReadStamp {
            seq: 0,
            lapses: 0,
            issued: WaitMs(0),
        });
    }

    #[cfg(test)]
    pub(crate) fn mark_peers_seen_for_test(&mut self, seen: bool) {
        self.peers_seen = seen;
    }

    #[cfg(test)]
    pub(crate) fn mark_peers_complete_for_test(&mut self, complete: bool) {
        self.peers_complete = complete;
    }

    #[cfg(test)]
    pub(crate) fn set_write_eligible_for_test(&mut self, nodes: Vec<NodeId>) {
        self.read_only.retain(|n| !nodes.contains(n));
        self.listed = nodes.iter().copied().collect();
        self.write_eligible = nodes;
    }

    /// Forget every read: the next one is the first.
    #[cfg(test)]
    pub(crate) fn forget_reads_for_test(&mut self) {
        self.installed = None;
        self.last_incomplete = None;
    }

    /// Make `me` a singleton as a complete read would: only `me` listed,
    /// nobody heard from, nothing incomplete, no lapse newer than the
    /// read. The links stay as they are (a test that links a node sets it
    /// up after, or clears them with `clear_links_for_test`).
    #[cfg(test)]
    pub(crate) fn make_singleton_for_test(&mut self, me: NodeId) {
        self.listed = BTreeSet::from([me]);
        self.added.clear();
        self.write_eligible = vec![me];
        self.heard.clear();
        self.last_incomplete = None;
        self.installed = Some(ReadStamp {
            seq: self.installed.map_or(0, |i| i.seq),
            lapses: self.lapses_seen,
            issued: WaitMs(i64::MAX),
        });
    }

    #[cfg(test)]
    pub(crate) fn set_read_only_for_test(&mut self, nodes: BTreeSet<NodeId>) {
        self.read_only = nodes;
    }

    #[cfg(test)]
    pub(crate) fn clear_heard_for_test(&mut self) {
        self.heard.clear();
    }

    /// Forget the audience and build it again from the roster and the links.
    #[cfg(test)]
    pub(crate) fn rebuild_audience_for_test(&mut self) {
        self.audience.clear();
        self.note_audience();
    }

    #[cfg(test)]
    pub(crate) fn incomplete_for_test(&self) -> bool {
        self.last_incomplete.is_some()
    }

    #[cfg(test)]
    pub(crate) fn link_mut_for_test(&mut self, node: NodeId) -> Option<&mut PeerLink> {
        self.links.get_mut(&node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(node: NodeId) -> PeerLink {
        PeerLink {
            node,
            connected: true,
            last_seen: None,
            rtt_ms: Some(5),
            since: None,
        }
    }

    fn w(ms: i64) -> WaitMs {
        WaitMs(ms)
    }

    /// A read numbered `seq`, issued at wait clock `seq`.
    fn read(seq: u64, write_eligible: &[NodeId], complete: bool) -> RosterRead {
        RosterRead {
            seq,
            write_eligible: write_eligible.to_vec(),
            read_only: Vec::new(),
            complete,
            lapses: 0,
            issued: w(seq as i64),
        }
    }

    #[test]
    fn nothing_is_known_before_the_first_snapshot() {
        let r = Roster::new();
        assert!(r.peers_unknown());
        assert!(r.audience(1).is_none());
        assert!(!r.has_heard());
        assert!(r.write_eligible().is_empty());
        assert!(!r.singleton(1), "unknown is not alone");
    }

    #[test]
    fn a_read_not_newer_than_the_installed_one_is_a_stale_arrival() {
        let mut r = Roster::new();
        assert!(r.install_read(read(10, &[1, 2, 3], true)));
        assert!(!r.install_read(read(9, &[1], true)), "older: ignored");
        assert!(!r.install_read(read(10, &[1], true)), "the same read twice");
        assert!(!r.install_read(read(5, &[1, 2, 3, 4], false)));
        assert_eq!(r.write_eligible(), [1, 2, 3]);
        assert!(
            !r.incomplete_for_test(),
            "an older incomplete read is stale too"
        );
        assert!(r.install_read(read(11, &[1, 2], true)));
        assert_eq!(r.write_eligible(), [1, 2]);
    }

    #[test]
    fn a_read_that_could_not_be_completed_only_adds() {
        let mut r = Roster::new();
        r.install_links(vec![], true, 0, w(1));
        assert!(r.install_read(read(2, &[1, 2], true)));
        assert!(!r.peers_unknown());
        // The scan lost the record of node 2 and found a new node 3.
        assert!(r.install_read(read(3, &[1, 3], false)));
        assert_eq!(r.write_eligible(), [1, 2, 3], "nothing is removed");
        assert!(r.peers_unknown(), "who the peers are is not known");
        // An empty incomplete read (every record unreadable) removes nothing.
        assert!(r.install_read(read(4, &[], false)));
        assert_eq!(r.write_eligible(), [1, 2, 3]);
        // A complete read newer than the incomplete ones is the truth again.
        assert!(r.install_read(read(5, &[1, 2], true)));
        assert_eq!(r.write_eligible(), [1, 2]);
        assert!(!r.peers_unknown());
    }

    #[test]
    fn a_complete_read_older_than_an_incomplete_one_does_not_end_the_unknown() {
        let mut r = Roster::new();
        r.install_links(vec![], true, 0, w(1));
        assert!(r.install_read(read(2, &[1], true)));
        assert!(r.install_read(read(4, &[1, 3], false)));
        // Reordered: the complete read numbered 3 arrives after the
        // incomplete one numbered 4.
        assert!(r.install_read(read(3, &[1, 2], true)));
        assert_eq!(
            r.write_eligible(),
            [1, 2, 3],
            "its lists, plus the newer adds"
        );
        assert!(r.peers_unknown(), "the newer incomplete read still stands");
        assert!(r.install_read(read(5, &[1, 2], true)));
        assert_eq!(r.write_eligible(), [1, 2]);
        assert!(!r.peers_unknown());
    }

    #[test]
    fn a_lapse_makes_the_peers_unknown_until_a_snapshot_taken_after_it() {
        let mut r = Roster::new();
        r.install_links(vec![], true, 0, w(1));
        assert!(!r.peers_unknown());
        r.note_lapse();
        assert!(r.peers_unknown());
        r.install_links(vec![], true, 0, w(2));
        assert!(
            r.peers_unknown(),
            "a snapshot read before the lapse was handed"
        );
        r.install_links(vec![], true, 1, w(3));
        assert!(!r.peers_unknown());
    }

    #[test]
    fn a_node_heard_from_is_dropped_only_by_a_read_issued_after_its_message() {
        let mut r = Roster::new();
        assert!(r.note_heard(7, w(20)));
        assert!(!r.note_heard(7, w(21)), "already heard");
        // (A read numbered `seq` is issued at wait clock `seq`.)
        r.install_read(read(1, &[1], true));
        assert!(r.has_heard(), "a read issued before its latest message");
        r.install_read(read(22, &[1], true));
        assert!(!r.has_heard(), "a read issued after it, not listing it");
        r.note_heard(8, w(30));
        r.install_read(read(40, &[1, 8], true));
        assert!(
            r.has_heard(),
            "a listed node stays heard until the directory shows it"
        );
        r.install_links(vec![link(8)], false, 0, w(41));
        assert!(!r.has_heard(), "the directory shows it");
        r.note_heard(9, w(50));
        r.install_links(vec![link(9)], false, 0, w(49));
        assert!(r.has_heard(), "a snapshot taken before its message");
    }

    #[test]
    fn the_audience_only_grows_and_needs_a_complete_snapshot() {
        let mut r = Roster::new();
        r.install_read(read(1, &[1, 2], true));
        assert!(r.audience(1).is_none(), "no snapshot yet");
        r.install_links(vec![link(3)], false, 0, w(2));
        assert!(r.audience(1).is_none(), "the snapshot was not complete");
        r.install_links(vec![link(3), link(4)], true, 0, w(3));
        assert_eq!(r.audience(1).unwrap(), BTreeSet::from([2, 3, 4]));
        // A later scan that lost a record cannot shrink it.
        r.install_read(read(4, &[1], true));
        r.install_links(vec![link(3)], false, 0, w(5));
        assert_eq!(r.audience(1).unwrap(), BTreeSet::from([2, 3, 4]));
        r.install_links(vec![link(3), link(5)], true, 0, w(6));
        assert_eq!(r.audience(1).unwrap(), BTreeSet::from([2, 3, 4, 5]));
    }

    #[test]
    fn write_peers_leave_out_the_node_itself() {
        let mut r = Roster::new();
        r.install_read(read(1, &[1, 2, 3], true));
        assert_eq!(r.write_peers(2).collect::<Vec<_>>(), vec![1, 3]);
        assert!(r.lists(3));
        assert!(!r.lists(4));
    }

    /// ADR-50 S10-I1 and S10-I3: alone only when a complete read lists
    /// nobody else and nothing contradicts it.
    #[test]
    fn a_node_is_a_singleton_only_when_every_input_says_alone() {
        let mut r = Roster::new();
        r.install_links(vec![], true, 0, w(1));
        assert!(!r.singleton(1), "no read yet");
        assert!(r.install_read(read(2, &[1], true)));
        assert!(r.singleton(1));
        // A peer listed.
        assert!(r.install_read(read(3, &[1, 2], true)));
        assert!(!r.singleton(1));
        assert!(r.install_read(read(4, &[1], true)));
        assert!(r.singleton(1));
        // A read that could not be completed.
        assert!(r.install_read(read(5, &[1], false)));
        assert!(!r.singleton(1));
        assert!(r.install_read(read(6, &[1], true)));
        assert!(r.singleton(1));
        // A node that messaged this one.
        r.note_heard(4, w(100));
        assert!(!r.singleton(1));
        r.clear_heard_for_test();
        assert!(r.singleton(1));
        // A connected link to a node the read does not list as read-only.
        r.install_links(vec![link(5)], true, 0, w(7));
        assert!(!r.singleton(1));
        assert!(r.install_read(RosterRead {
            read_only: vec![5],
            ..read(8, &[1], true)
        }));
        assert!(r.singleton(1), "a read-only node cannot take the lease");
        // A lapse of this node's own, newer than the installed read.
        r.note_lapse();
        assert!(!r.singleton(1));
        assert!(r.install_read(RosterRead {
            lapses: 1,
            read_only: vec![5],
            ..read(9, &[1], true)
        }));
        assert!(r.singleton(1));
    }
}
