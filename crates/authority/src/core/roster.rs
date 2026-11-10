//! The node's one membership object (plan 46 M1.3 step 10, ADR-50).
//!
//! Who could take this lease over, who may hold a read delegation of this
//! node's delegates, who the inbox polls, who a promise is asked of, who a
//! backup could be: every question about the other nodes of the filesystem is
//! answered here, from one set of inputs the core received: registry reads
//! (`Event::Roster`, `Event::RosterUnread`), directory snapshots
//! (`Event::Peers`), the messages of nodes (`note_heard`, at the one door
//! every peer message passes) and the node's own lapses (`note_lapse`).
//! Only the core writes it, and only through those methods; a reader uses the
//! predicates. The driver produces reads and never decides membership.
//!
//! What it keeps, and what each input does to it:
//!
//! - **The write-eligible roster.** The last complete registry read. A read
//!   issued before the newest one taken is a stale arrival and is ignored. A
//!   read that could not be completed (`install_unread`) keeps the last
//!   complete roster and marks the peer set unknown (unknown counts as
//!   peers).
//! - **The nodes heard from** (`heard`): a node that sent this one a request
//!   since the directory last showed it, with the wait clock of its latest
//!   message. A read or snapshot drops it only if the read was issued after
//!   that message, for an older read may predate the node's registration.
//! - **The links** (the peer directory): reachability only. A snapshot also
//!   counts the lapses the driver had handed this core before it took it.
//! - **The audience**: every node that was on the roster or in the directory
//!   since the first complete snapshot, the readers that may hold a read
//!   delegation of this node's delegates. It only grows.

use super::*;

/// See the module doc.
#[derive(Debug)]
pub(crate) struct Roster {
    /// The last complete read's write-eligible nodes.
    write_eligible: Vec<NodeId>,
    /// An `Event::Roster` has arrived: `write_eligible` is the registry's,
    /// not the empty start value.
    seen: bool,
    /// The wait clock of the newest read taken (`Event::Roster` or
    /// `Event::RosterUnread`); a read issued before it is ignored.
    issued: WaitMs,
    /// The newest read could not be completed: `write_eligible` is the last
    /// complete one, and the peer set is unknown.
    unread: bool,
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
            write_eligible: Vec::new(),
            seen: false,
            issued: WaitMs(i64::MIN),
            unread: false,
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

    /// A complete registry read, issued at `issued`. `false`: it was issued
    /// before the newest read taken, a stale arrival that changes nothing.
    pub(crate) fn install_read(&mut self, write_eligible: Vec<NodeId>, issued: WaitMs) -> bool {
        if issued < self.issued {
            return false;
        }
        self.issued = issued;
        self.unread = false;
        self.write_eligible = write_eligible;
        self.seen = true;
        self.note_audience();
        // The read is the authority on who is a peer: a node it does not
        // list is none, unless it asked after the read was issued.
        let listed = &self.write_eligible;
        self.heard
            .retain(|n, at| listed.contains(n) || issued <= *at);
        true
    }

    /// A registry read that could not be completed, issued at `issued`.
    /// `false`: a stale arrival.
    pub(crate) fn install_unread(&mut self, issued: WaitMs) -> bool {
        if issued < self.issued {
            return false;
        }
        self.issued = issued;
        self.unread = true;
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
    /// directory last showed it.
    pub(crate) fn note_heard(&mut self, node: NodeId, at: WaitMs) -> bool {
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
            self.audience.extend(self.links.keys().copied());
        }
    }

    // -------------------------------------------------------- predicates

    /// The write-eligible nodes of the last complete read (this node
    /// included when it is registered).
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

    /// The last complete read lists `node` as write-eligible.
    pub(crate) fn lists(&self, node: NodeId) -> bool {
        self.write_eligible.contains(&node)
    }

    /// Who the peers are is not known: no directory snapshot yet, none taken
    /// after this node's last lapse, or the newest registry read could not
    /// be completed. Unknown counts as peers.
    pub(crate) fn peers_unknown(&self) -> bool {
        !self.peers_seen || self.peers_lapses < self.lapses_seen || self.unread
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
        if !(self.seen && self.peers_seen && self.peers_complete) {
            return None;
        }
        let mut peers = self.audience.clone();
        peers.extend(self.write_eligible.iter().copied());
        peers.extend(self.links.keys().copied());
        peers.remove(&me);
        Some(peers)
    }

    // ------------------------------------------------------- test hooks

    #[cfg(test)]
    pub(crate) fn mark_seen_for_test(&mut self, seen: bool) {
        self.seen = seen;
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
        self.write_eligible = nodes;
    }

    #[cfg(test)]
    pub(crate) fn forget_issued_for_test(&mut self) {
        self.issued = WaitMs(i64::MIN);
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
    pub(crate) fn unread_for_test(&self) -> bool {
        self.unread
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

    #[test]
    fn nothing_is_known_before_the_first_snapshot() {
        let r = Roster::new();
        assert!(r.peers_unknown());
        assert!(r.audience(1).is_none());
        assert!(!r.has_heard());
        assert!(r.write_eligible().is_empty());
    }

    #[test]
    fn a_read_issued_before_the_newest_is_a_stale_arrival() {
        let mut r = Roster::new();
        assert!(r.install_read(vec![1, 2, 3], w(10)));
        assert!(!r.install_read(vec![1], w(9)), "older: ignored");
        assert_eq!(r.write_eligible(), [1, 2, 3]);
        assert!(!r.install_unread(w(9)), "an older unread is stale too");
        assert!(!r.unread_for_test());
        assert!(r.install_read(vec![1, 2], w(10)), "same instant: installed");
        assert_eq!(r.write_eligible(), [1, 2]);
    }

    #[test]
    fn an_unread_scan_keeps_the_roster_and_marks_the_peers_unknown() {
        let mut r = Roster::new();
        r.install_links(vec![], true, 0, w(1));
        assert!(!r.peers_unknown());
        assert!(r.install_read(vec![1, 2], w(2)));
        assert!(r.install_unread(w(3)));
        assert_eq!(r.write_eligible(), [1, 2], "the last complete roster stays");
        assert!(r.peers_unknown());
        assert!(r.install_read(vec![1, 2], w(4)));
        assert!(!r.peers_unknown(), "a complete read ends it");
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
        r.install_read(vec![1], w(20));
        assert!(r.has_heard(), "a read issued before its latest message");
        r.install_read(vec![1], w(22));
        assert!(!r.has_heard(), "a read issued after it, not listing it");
        r.note_heard(8, w(30));
        r.install_read(vec![1, 8], w(40));
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
        r.install_read(vec![1, 2], w(1));
        assert!(r.audience(1).is_none(), "no snapshot yet");
        r.install_links(vec![link(3)], false, 0, w(2));
        assert!(r.audience(1).is_none(), "the snapshot was not complete");
        r.install_links(vec![link(3), link(4)], true, 0, w(3));
        assert_eq!(r.audience(1).unwrap(), BTreeSet::from([2, 3, 4]));
        // A later scan that lost a record cannot shrink it.
        r.install_read(vec![1], w(4));
        r.install_links(vec![link(3)], false, 0, w(5));
        assert_eq!(r.audience(1).unwrap(), BTreeSet::from([2, 3, 4]));
        r.install_links(vec![link(3), link(5)], true, 0, w(6));
        assert_eq!(r.audience(1).unwrap(), BTreeSet::from([2, 3, 4, 5]));
    }

    #[test]
    fn write_peers_leave_out_the_node_itself() {
        let mut r = Roster::new();
        r.install_read(vec![1, 2, 3], w(1));
        assert_eq!(r.write_peers(2).collect::<Vec<_>>(), vec![1, 3]);
        assert!(r.lists(3));
        assert!(!r.lists(4));
    }
}
