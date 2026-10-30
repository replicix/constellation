# Delegations

Delegated sub-sequencers (plan 30 M11–M12). The root lease holder can
hand a subtree, or one name-hash range of a busy directory, to another
node. That node then sequences every mutation there at local speed and
streams the results to the root, which appends them to the one log.
There is still one lease in S3 and one log; delegations are a table
inside the root's authority, maintained over P2P.

## Table of Contents

- [Terminology](#terminology)
- [Details](#details)
  - [The delegation table](#the-delegation-table)
  - [Ownership](#ownership)
  - [How a delegate executes](#how-a-delegate-executes)
  - [The append path and dependencies](#the-append-path-and-dependencies)
  - [Grants, renewal and expiry](#grants-renewal-and-expiry)
  - [Cross-subtree operations and recall](#cross-subtree-operations-and-recall)
  - [Automatic placement](#automatic-placement)
  - [Hot shared directories](#hot-shared-directories)
  - [Offline designations](#offline-designations)
  - [Failures](#failures)
  - [What stays with the root](#what-stays-with-the-root)
- [Commands](#commands)
- [Configuration](#configuration)
- [Status](#status)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Root**: the node holding the root lease in S3. It is the only node
  that appends to the log.
- **Delegate**: a node that sequences one subtree (or one hash range of
  a directory) for the root.
- **Generation (`gen`)**: one delegation's lifetime, numbered globally.
  A recall ends a generation; a re-delegation starts a new one.
- **Owner**: the node that sequences a key: the delegate whose
  delegation contains it, else the root.
- **Cross-subtree op**: an operation whose keys have two owners, such
  as a rename between two delegated subtrees.
- **Recall**: the root taking a delegation back. The delegate drains its
  stream and stops, or the root waits out its grant.
- **`deps`**: the delegate stream positions a requester has observed,
  carried with its forwarded ops so the root never appends an effect
  before its causes.
- **Placement**: the root's automatic choice of what to delegate to
  whom.

## Details

### The delegation table

`Delegate { dir, node, gen }` and `RecallCut { dir, gen, through }` log
records maintain a replicated delegation table (a `Recall { dir, gen }` in
a log written before the cut was in the record is still read: see
[The generation's end](#the-generations-end-and-its-cut)). It is stored in the namespace
(so a bootstrapped replica learns it from the head commit), and every
node resolves ownership against its own copy.

- Delegations never overlap: a directory under a delegation, or one
  with a delegated descendant, cannot be delegated. There is no
  sub-delegation.
- A node holds at most one whole-subtree delegation (plus designations).
- The root directory itself is never delegated.
- No delegation is made inside a continuation epoch. When an epoch
  opens, the root recalls every live generation; delegation resumes
  after the epoch closes.
- Delegation needs P2P. With P2P off, or `CONSTELLATION_DELEGATION=off`,
  no `Delegate` record is ever written.

### Ownership

- A dentry `(parent, name)` belongs to the innermost delegation that
  contains `parent`.
- An inode's own keys (attributes, xattrs, reverse links) belong to the
  delegation that contains its **primary link's** parent.
- An operation whose keys resolve to two owners is cross-subtree. An
  exclusive hold on a directory resolves to the directory itself, so an
  `rmdir` or rename of a delegated root is cross-subtree too.
- Ownership is a local ancestor walk; there is no lookup service.

A forward goes to the owner. A node with a stale table that sends an op
to the wrong node gets `NotHolder` with the right owner, and follows up
to two redirects (a delegate names the root, the root names a delegate).

Routing uses the links the authority core sees. A node with no link to
the delegate sends the op to the root. The root redirects to the
delegate only while its own link to it is up; otherwise it recalls the
generation (see [Cross-subtree operations and recall](#cross-subtree-operations-and-recall)).
The driver sends the core its links every second, and again as soon as
a peer enrols or leaves or a link goes up or down. A newly enrolled peer
is probed at once, so its link shows within a round trip. Before, a
peer could be invisible to the core for up to a second after it
enrolled, and a write into a delegation made to it in that second was
recalled (`startup-link-lag`). `p2p.peers[].core_connected` in
`constellation status` shows the core's view of each link.

### How a delegate executes

A delegate validates each op against its own replica. That replica is
authoritative for the subtree because every mutation under it goes
through the delegate. It then:

1. journals the transaction as speculation (see
   [Forwarded mutations](forwarded-mutations.md#speculation-and-stranded-op-recovery)),
   with the generation's next stream index and the requester's `deps`;
2. acknowledges once its **group** holds the row (plan 43 C2,
   [ADR-33](../../explanation/DECISIONS.md#adr-33-commit-is-the-configured-groups-acknowledgement-vertical-paxos-over-s3)):
   - With no backup recorded for the generation the group is the delegate
     and the root: the answer leaves when the root has *journaled its
     append* of the row — the root's `DelegateStreamAck.through` reaches the
     row's index — so the delegate sends the row at once rather than on the
     stream tick, and a node's op costs one round trip to the root.
   - With a backup recorded (a `DelegGroup` record naming it) the group is
     the delegate and that backup, chosen by RTT to the delegate, exactly
     like the root's (see [Durability and failover](durability-and-failover.md)).
     The root is never the delegate's backup. The delegate counts the backup
     only once *its own replica's table* records it — the record is
     committed in the log — never from a message; until a change it asked
     for is recorded, an acknowledgement needs the members of both versions;
     and a backup leaves the record only once every index answered through it
     is at or below the root's cursor or in the log.
   - A **lone** delegate — its table records no backup, none is requested or
     appended to, and no peer, the root included, is a backup candidate
     (linked, within `CONSTELLATION_BACKUP_RTT_BUDGET_MS`, stable for
     `CONSTELLATION_BACKUP_STABLE_MS`) — answers its own node's client from
     its disk (Layer A, the one speculation the design keeps); another node's
     forward still waits for the root's append. Its own node's rows are
     answered from its disk only while its journal is fewer transactions
     past the root's cursor (or the log) than a stream batch carries records
     (`delegation_stream_rows`, 256: two batches of creates); past that they
     wait for the root's append too (`Core::deleg_lone_held`). The
     stream carries one batch per round trip to the root, so without the
     bound a busy delegate's own node ran its journal ahead of the stream
     without limit and every forward, answered behind every earlier row,
     waited for all of it. The lead is judged for each row by its own stream
     index against the horizon the core publishes (the committed prefix plus
     the lead), so the FUSE fast path cannot journal past it between two
     steps of the core. A row is also not answered from the disk while an
     earlier row of its own node's clients still waits for the root (it was
     journaled past the lead, or before the delegate turned lone): were the
     root to crash, the earlier row would be dropped and the later one
     replayed without the effect it rests on (a create, then a rename of it).
     Another node's forward is not such a row.
   - A designation's rows are non-stealable and answer at once; `ack=s3` waits
     for the log.

   The class an answer waits for is fixed when the row is journaled. A
   retry of a rid whose earlier row is still uncommitted waits for that row
   exactly as the first answer did. An acknowledgement that waits for the log
   (`ack=s3`, or a backup that lags) counts the log by its index of the
   generation — the highest row a segment this replica applied carries —
   never by "the row left this journal", which a strand does too. A row that
   has left the journal without the log carrying it (a recall's or a
   takeover's strand, a rebuild) was dropped: its acknowledgement is aborted
   (`Busy`, the requester retries by rid; `deleg_acks_dropped`), and so is
   every park for a row past the cut of a generation that ends here;
3. streams its transactions to the root in index order
   (`DelegateStream`, one batch in flight per generation).

The delegate's own FUSE writes under its delegation take the local fast
path whatever the delegation's group is (plan 43 C2d): the write runs on the
FUSE thread and journals its row with the class the core has published for
the generation by the time the row is journaled, under the store's writer
lock, not when the op was admitted (`Core::report_deleg_durable`: `Lone`
while the delegate is lone,
`NonStealable` for a designation, otherwise `DelegGroup`, or `Archive` under
`ack=s3`). A `Lone` or `NonStealable` row is answered at once; a row that
waits is answered once the core's commit point for the generation covers its
stream index (`Meta::session().wait_deleg_durable`: the log carries it, or
every member of the group, and of a requested change, holds it — the same
predicate the core path's parks wait on; a row once covered stays durable
after the generation is withdrawn). If the generation is stopped,
lapsed, lost its backup or ended before that, or the wait runs out
(the 30 s durability wait budget), the op is in doubt: it goes back
through the core under its rid, which answers from the row's commit or
re-executes it at the new owner. A delegate that turns lone after journaling
a `DelegGroup` row still waits for that row. The indexes the group has
committed, whatever the class is now, count as answered for the group's
reconfiguration (`acked_max`): a backup leaves the record only once the
root's cursor or the log covers them. Its journal rows retire when a root
segment carries them. If
the generation ends first, the rows past the cut are stranded: rolled back,
and — a lone delegate's rows, answered on its disk — replayed by rid through
the new owner, or — rows that waited for a group, whose clients were never
answered — dropped, their clients retrying by rid
(`speculation.deleg_dropped`). A delegate refusal is journaled into the
stream like any other outcome, and waits for its commit like an
acceptance.

### The append path and dependencies

The root appends a delegate's stream in index order after checking that
the generation is live and was granted to the sender. It does not
re-validate the operations. It journals them with the delegate's origin,
so the segment carries it and every replica, the delegate included,
learns the generation's applied index.

Stream batches (`DelegateStream`) and renewals (`DelegRenew`) are one-way
messages, and so are the root's answers (`DelegateStreamAck`,
`DelegRenewed`): the receiving node acknowledges a message once it is
queued for its authority core, so no request slot waits for the root's
core and no answer is lost to a request timeout. The stream
acknowledgement is cumulative (`through`: the root holds the
generation's stream through that index, whichever batch it answers), so
an answer to a batch the delegate already gave up on and re-sent still
counts, answers that overtake each other never move the delegate's
cursor back, and duplicates change nothing. One batch is in flight per
generation; one not answered within the request timeout is sent again
from the acknowledged cursor after a backoff (4 ticks, doubling to
2 s), and the root, which skips the rows it holds, answers it with the
same cursor. Every batch and renewal carries the delegate's stream
round: its incarnation and a fresh id at the install and at every change
of root. An answer counts only from the root the generation streams to,
in the round it answers, so an old root's answer (or one from an
earlier tenure of the same node, or one to the delegate's previous
incarnation, whose request ids the new one reuses) never credits the
new root with rows it does not have. The root counts a batch from the
generation's delegate as a sign of life, as it counts a renewal: while
the delegate streams, re-sends included, the root neither outwaits the
grant nor seals the delegate's backup. Before, a root that answered
later than the 2 s batch timeout (1 s for a renewal) had every answer
thrown away; the delegate re-sent a batch the root already held every
2.5 s, and the root sealed the generation as silent at the very cursor
the delegate was streaming from (`stress-ng-fs-nodes`: the delegate's
writes waited 100–220 s). Most of that lateness was the delegate's own
core falling behind the holder's log: every tailed segment rewound its
whole unappended stream. A segment now goes under a delegate's
transactions only where it overlaps them (see
[Forwarded mutations](forwarded-mutations.md#speculation-and-stranded-op-recovery)).

A forward carries `deps`: for each delegate stream, the highest
acknowledged position the requester has observed. The root appends a
transaction only once everything in its `deps` is in its replica, and a
delegate executes an op only once its `deps` are in *its* replica. So
no replica ever contains a record whose causes are missing: data
written in `D1` and then a marker written in `D2` are never seen in the
wrong order (`marker-order`). A requester tracks at most 8 streams; if
its observed set overflows, it sends the op to the root instead. A
dependency on a generation that has ended is void, except one past the
generation's cut (the transaction was acknowledged but never appended,
so it is being replayed by rid): an op with such a dependency is not
executed (a delegate or the root answers `Held`; an inbox op stays in
the inbox and its requester withdraws it), and the requester re-sends
it with fresh `deps` once its own replays have landed. A node holds its
new writes while it has stranded ops of its own to replay, so its
writes stay in the order it issued them. An op that is itself one of
the stranded ops (a fast-path write a deposition rolled back after its
acknowledgement wait had already ended in doubt, resubmitted by rid) is
held only for the replays queued ahead of it, and it carries the
delegate rows its own entry recorded (as its replay would): held for the
ones behind, it and the replay drain would wait for each other until the
client deadline. `status` counts the checks that held an op for replays
(`deps_lost_holds`) and the drain ticks that stopped at a head that is such
an op (`replays_waiting_on_client`); both stay small. Which generations a replica
holds, and which have ended, is rebuilt when it reopens its store
(from the delegation table's highest generation and live rows, and the
log's per-generation index): a watermark naming a generation that
ended before a restart — a lock grant's floor carries the releaser's
frontier — is reached at once, instead of every read waiting out the
session budget for the rest of the process.

Replies from a delegate carry `(gen, idx)` in their position, so the
session guarantees of [Close-to-open modes](cto-modes.md) cover
delegated writes too.

A reply the requester cannot install at once (the delegate evaluated
the op behind its own transactions the log does not have yet, such as
a close right after the requester's create) waits for that
transaction. When the root has a backup, the root streams its journal,
its appends of the delegate streams included, to every node ahead of
S3 (see [Durability and failover](durability-and-failover.md)); the
requester's op is answered as soon as that stream installs the root's
append of it, one LAN round trip or two, not an S3 round trip. A node's
dependency on the root's journal (the `deps` of its next forward) is
likewise reached through that stream, so the root's own writes into a
delegated directory do not wait for its ship either. Status:
`ack.awaited_log_streamed_deleg`.

### Grants, renewal and expiry

- A grant lasts `CONSTELLATION_DELEGATION_TTL_MS` (by default the lock
  grant TTL, 20 s, and at least 5 s: lock grants under a delegation never
  outlive it, and at 5 s a lock in a delegated subtree was honoured for
  under 3 s and lapsed on any slow step). The delegate
  renews it at half its TTL (`DelegRenew`); the root answers with a new
  grant capped by the root's own lease (`expires − margin`), so a grant
  never outlives the lease it came from. A renewal not answered within
  the request timeout is sent again; a grant that comes later still
  counts, from its own renewal's send, for up to a minute.
- A delegate's renewal is a step of its authority core, and a core stuck
  in one step (a priority metadata write waiting for the store's writer
  lock behind a FUSE thread's slow journal write, a blocking log write)
  sends none. Past the renewal slack (about `ttl/2 + margin`) the root
  reclaimed a delegation whose node was alive. So while the driver has
  made no progress for a quarter of the renewal period (500 ms at most,
  100 ms at least) and for no longer than `CONSTELLATION_HOLDER_STALL_MS`
  (15 s: past that it is hung, not slow), the heartbeat thread (the
  holder's off-core liveness thread, with its own runtime and connection)
  sends each renewal that is due, from a certificate the core publishes
  when it changes (the generation, its root and round, when a renewal is
  next due, the period; per step the driver compares it in place under
  one lock, allocating nothing unless it changed), counting a renewal the
  core dispatched as sent only once the driver dispatched it. The lane
  sends its own message (`DelegRenewOffCore`), which carries no backup and
  no stream head. It registers each request with the core first, on the
  urgent lane, and the core counts the answer as it counts any late one:
  honoured from the lane's send. The lane never sets a grant's honoured
  end, its lapse, the parked ops or the execution, and it logs nothing per
  send. `CONSTELLATION_DELEG_LANE=off` turns it off (for A/B runs). Why
  this is safe:
  1. The lane stamps `sent` before it hands the request to the transport,
     the root grants from its receipt (`granted ≥ sent`) and reclaims no
     earlier than `granted + ttl + margin`, and the core honours `sent +
     ttl − margin`: the delegate stops at least two margins before the
     root's earliest reclaim, the same inequality as an in-time renewal.
  2. Only the core decides: the answer goes through `on_deleg_renewed` and
     its root and round checks; a refusal on a lane request is dropped, as
     any late refusal is, and the log ends the generation.
  3. The root grants by the same rules (no grant once a recall or a
     reclaim has begun, its freshness hold, the cap of its own lease), but
     it learns less from the lane's renewal than from the core's: it
     extends the grant and answers, and notes **no stream head and no
     backup**. Both need the core. The head is what the delegate executed
     by the renewal's stamp; the core settles its tagged writes before it
     reports it, and the FUSE fast path keeps executing rows while the core
     is stuck, so a lane could only report a head that is too old. The root
     takes the as-of time of its lock cut, and a new tenure's wait for a
     head, from the core's renewals alone; a lock barrier under the
     subtree therefore settles when the core renews, not on the lane's
     word (liveness only: a holder outwaited meanwhile is granted on
     after). The backup is the node the root seals when the delegate goes
     silent and whose acknowledgements clients were given; a certificate
     one step old must not unname the one the core just chose.
  4. The certificate is the state after the last finished step. A step in
     progress that stops the generation (a refusal, a strand) can be
     followed by at most one lane renewal per period from the old
     certificate: the root's `until` moves one period later for a
     generation the delegate no longer executes in. That costs liveness
     only; the generation still ends through the log or a recall.
  5. Bound: no lane renewal once the driver is stuck past the hung bound,
     so a hung delegate's subtree is reclaimed at most that bound plus
     `ttl + margin` after the hang began.
  The lane follows the grant the root named: the answers reach the node's
  P2P service off the core, which notes the granted ttl per generation and
  round with the node it came from, and the lane renews at a quarter of it
  (not below 250 ms) where that is shorter than the core's period, counting
  only the answers of the root its certificate names. The root caps every
  grant by what is left of its own lease (`expires − margin`) outside a
  continuation-epoch hold: a lease renewal that runs late (a slow or
  throttled S3 lease PUT, the root's own core stuck behind the same slow
  disk, which is correlated with the delegate's stall) or a grant inherited
  from an old root after a failover leaves a grant of a second or so, and a
  period of half the previous full grant would come after the root's
  reclaim. (With a 60 s lease, a 20 s grant and a 1 s margin this needs the
  lease renewal about 9 s late; rare, not never.) Only the lane's period
  follows it; the core still decides everything. A lane request that fails at
  the transport is reported to the core (on the urgent lane, behind its
  registration), which drops it from its pending renewals, as it does a core
  renewal's failure.
  **The root's side: its answer off the core.** The root answers every
  renewal from its core too (`DelegRenewed`), and a root core stuck in a
  step answers nothing: the delegate lapsed at `sent + ttl − margin` and
  the root reclaimed at `until`, though both nodes were alive
  (`delegate-store-stall` with the root's store stalled, which reported
  `KNOWN-GAP root core stuck`). While the root's driver has been stuck
  for the same time as the delegate's lane waits (a quarter of half the
  grant, 100 to 500 ms) and no longer than `CONSTELLATION_HOLDER_STALL_MS`,
  the root's P2P service, which receives the renewal on the shared
  runtime, answers it itself: as the RPC reply of the request (no send
  waits on the root's runtime), `DelegRenewedOffCore { ttl_ms }`, after
  extending the generation's window in the **shared window table**
  (`DelegWindows`, the one place a generation's `until` lives; the core
  reads it through `gen_until`). The request is still queued to the core
  unchanged, which answers it in full when it runs (a second extension is
  a `max`) and does the bookkeeping the off-core answer skipped
  (`Event::DelegRenewAnsweredOffCore`: statistics, the lease's activity
  stamp, the expiry timer, the lease renewal a short grant calls for). The
  delegate applies the reply only as an **extension of a grant it already
  serves**, honoured from the request's own send, never as a first grant,
  and with no lock content: no handoff, grace, floor, cut or barrier. For
  its own request that was in flight it also arms the next renewal from the
  send (the full answer, later, is a late one: its lock content is
  installed as ever). A renewal that reached the root just before its core
  got stuck was queued to that core (nothing looked stuck yet) and nobody
  answers it off the core; so a delegate sends a renewal the root has not
  answered within a quarter of a grant period plus 250 ms once more
  (`Core::deleg_resend_ms`; the first request is kept for a late answer),
  and that second one arrives while the core is stuck. The service that
  answers is a task of the shared runtime, so the core's driver runs on a
  thread of its own (`cst-authority`, polled by `Handle::block_on`): a step
  waiting behind a slow journal write used to block a worker of that
  runtime and the tasks queued on it, the service's among them, and a
  renewal that reached the root during the stall was read only when the
  stall ended (about half of the `delegate-store-stall` runs with the root
  stalled). The window table and the certificate are what make it the
  root's own answer:
  1. *The window is one critical section.* Extensions (the core's and the
     answerer's) are a `max` and commute. Whatever ends a window, or
     after which the core would grant no more, **closes the generation's
     slot inside the core's step**, at the transition: a recall begins, a
     seal is sent, the generation ends, an epoch opens, the lease is gone
     (`DelegWindows::close*`; a closed slot is extended by the core only,
     which reopens it). The expiry decision (`close_if_expired`) reads the
     clock inside the table's guard and closes in the same section, so a
     window the answerer extended after the step took its `now` (a step
     that waited in the store) is re-armed, never sealed or reclaimed on
     the stale read.
  2. *The root rule is the core's.* The answerer decides from the lease
     certificate the core publishes at the end of every step, before the
     driver dispatches its actions (`RootLease`: the lease epoch and its
     end on the honour clock, the margin, the grant, the freshness facts
     of `renew_hold`: whether the tenure may be taken over early, the
     send of the latest S3 request that proved it live, the window, the
     lease mark): usable (held, not lost, no gate or release in flight, no
     continuation epoch), the cap (a grant never outlives the lease's end
     less the margin: `ttl = min(grant, cap)`, nothing at or below 0) and
     the freshness rule (a tenure that can be taken over before its lease
     expires answers only within `backup_takeover_ms` of a proof of
     liveness, and only with a marked lease), each judged against the
     honour clock at the answer. A release, a takeover gate or an epoch
     learned in a running step is published as no certificate before the
     action that makes it effective goes out; the rest the freshness window
     (fast tenures) or the cap (`Local`) bounds, exactly as for the lock
     lane. A root with a backup, the usual delegation root, answers within
     1.5 s of a proof of liveness, and a core that is stuck makes no S3
     request to prove it: see *The root proves its own liveness* below.
  3. *The lock side is settled.* The generation is answered only if the
     core sent it a granting answer in this tenure and since then no lock
     handoff is pending, no outwait barrier is out and the subtree's floor
     is what that answer carried (`Window::settled`), and the delegate
     applies the reply only while it is serving and the root's core has
     granted it in the current round (a root change starts a new round:
     the new root's core grants first); so no answer without lock content
     is ever the delegate's first grant or leaves a handoff behind. A
     refusal by the core after an off-core grant of the same request still
     ends the generation at the delegate.
  4. *The delegate honours from its own send*, which is no later than the
     answer: it stops at `sent + ttl − margin`, the root's window runs to
     `answer + ttl + (margin)`.
  `CONSTELLATION_DELEG_ROOT_LANE=off` turns the answerer off (A/B runs).
  **The root proves its own liveness** (plan 41 D3). A tenure that can be
  taken over before its lease expires (it lists a backup, or acks at
  `ack=s3`) answers only within `backup_takeover_ms` (1.5 s) of the send of
  an S3 request that proved it live, and a core stuck in a step makes none:
  the answerer found its proof stale 1.5 s into every stall and answered
  nothing more (the backed pair of `delegate-store-stall` lost its
  delegation to stalls past the grant's slack, `KNOWN-GAP root core
  stuck`). Now a renewal refused **only** for stale liveness (every other
  rule holds: slot open and settled, lease marked, lease left below the
  margin; and the driver is stuck and not hung, so never while the core
  runs and never past `CONSTELLATION_HOLDER_STALL_MS`) asks the root's
  `RootProbe` for one read of the lease object, which the P2P service
  performs on a task of its own through the driver's own `LeaseStore` (so
  the own-S3 watch counts it). At most one read is in flight (renewals that
  arrive meanwhile wait for it, at most one freshness window) and at most
  one is started per freshness window, whatever the last one found: a read
  serves every renewal, of every delegation and of the lock lane's own
  grants, that arrives in its round. The read is judged as the core judges
  a `LeaseGet` of its own (`note_s3_liveness`, `on_renew_probe`): holder
  and epoch only, never an expiry against a clock (plan 43 C7). Naming this
  node at the certificate's epoch, not released, it is a proof of liveness
  **as of its send** (the honour reading taken before the request left),
  and the unchanged freshness rule judges it at the answer: a read slower
  than the window justifies nothing. Another holder, a later epoch or no
  lease is a deposition: the epoch's earlier proof is withdrawn, the
  answerer refuses it from then on (`deposed`) and no more reads are made
  for it. A failed or timed-out read, an older body or a released lease
  prove nothing. The core is told of every read on the urgent lane
  (`Event::RootProbed`) and treats it as its own: a proof extends its
  `last_s3_fresh`, a deposition deposes it when it runs. Why a read is a
  proof: a successor takes the tenure only by a lease CAS (a TTL claim, a
  seal, an `ack=s3` takeover) and quarantines what the root may have
  answered from its marker on, `marker + backup_takeover_ms + ...`; a read
  served at or after its send that still names this root precedes that CAS,
  so an answer decided within the window of the send ends inside the
  quarantine: the argument of the core's own proof, with the read's send
  where the request's send stood. A takeover that lands after the read is
  covered by the quarantine; one that landed before it shows in it. The
  proof only feeds the freshness rule: a certificate the core withdrew (a
  release, an epoch, a gate) is not revived by it. `CONSTELLATION_ROOT_PROBE=off`
  turns the read off (A/B runs). Status: `root_probes` (reads),
  `root_probes_live`, `root_probes_deposed`, `root_probes_withheld`
  (renewals refused a read of their own: one was sent within the window, or
  the epoch is deposed).
  Status: `delegation.root_answers_off_core`, `root_answers_deferred` (by
  reason: `hung`, `lease`, `generation`, `node`, `closed`, `unsettled`,
  `stale_liveness`, `unmarked`, `cap`, `deposed`) on the root,
  `off_core_answers_applied` on the delegate. A receive path that is a task
  of the root's shared runtime is held up with it: a runtime blocked for
  seconds answers nothing off the core either (the harness's runtime-hog
  arm measures it; not fixed here).
  Not covered, and told apart by the core when it gives a grant up (a lapse, a
  refusal after the grant ran out, or the log ending the generation after it
  ran out; each is also counted in `grants_lost`):
  - A freeze of the whole process, no thread of which runs, from a renewal's
    due time to the grant's loss. The heartbeat thread stamps when its next
    tick is due; the driver, waking first after a freeze, reads the missed
    tick, and hands the core the freeze only if the shared runtime's own lag
    probe missed the same interval (the whole process, not the heartbeat
    thread held up alone, in a log line or on a mutex, which is a failure of
    the lane's thread, not a freeze). If it began no later than a second
    after the renewal fell due and lasted to within a round trip of the
    grant's end, the loss is counted in `lapses_during_freeze` (with
    `last_freeze_ms` and `last_freeze_gen`). A freeze that ended in time to
    renew in, or began after the lane had its chance, or is far from the
    loss, explains nothing. The root cannot see the delegate's freeze: a
    reclaim that follows one is explained by the delegate's counter for the
    same grant (the reclaim point is the delegate's lapse point).
  - A root that did not answer: a renewal the delegate had sent by the time the
    lane would have acted (a due time plus a second, the freeze's own rule,
    so that a late lane's renewal is never taken for one sent in time) stayed
    unanswered until the grant ran out. The root answers renewals from its
    core, which this lane does not cover; counted in `lapses_root_silent`
    (with `last_silent_gen`) as a diagnosis, not an excuse. The one such case
    measured was the root's core, and the delegate's, blocked in the same
    instant in a shared log sink (plan 44's class), not a slow S3 put.
  A delegate that sent nothing in time, or whose lane's request never got to
  the root, is counted in neither: that is the lane's or the transport's failure.
  The heartbeat thread can still block in a log write (plan 44's non-blocking
  writer is to address it); that is the second case, or a failure.
- The delegate honours a grant until `sent + ttl − margin` on its own
  clock; the root considers it live until `granted + ttl + margin` on
  its clock (the same discipline as read delegations and lock grants).
- The root reclaims a grant nobody renewed (a crashed delegate) by
  itself, and grants no renewal once a recall or reclaim has begun.
- The delegate's grant lapses when it cannot get it renewed by the
  root's earliest reclaim: at `until + 2 × margin` (`until` is `sent +
  ttl − margin`, the root reclaims at `granted + ttl + margin` with
  `granted ≥ sent`, so the delegate lapses first, by the request's
  one-way delay), or the reclaim horizon (`ttl + margin`) after the
  install for a grant it never got renewed. The root is then dead or
  cut off. Lapsed, the generation executes and parks nothing (it still
  streams what it holds), and its ops, the parked ones first, take the
  ordinary route: to a live root, which recalls the generation first,
  else the inbox and the lease path. It keeps asking for a renewal
  every request timeout: a link that recovers before the root reclaims
  gets the grant back, and only a refusal or the log ends the
  generation. Status: `lapsed`. A lapse is always safe: it only gives up
  execution while the grant is not honoured anyway.
- An acknowledgement from a stream that ended before the root appended
  it is tentative: the row is stranded and the requester replays the op
  by rid.
- Every renewal carries the stream index the delegate has executed and
  when it sent the renewal, on its clock. The root keeps the latest of
  each generation: a lock owner that waited a holder out learns from
  them how far every stream went, and a granting answer carries the
  root's resulting cut to the delegate (see [Cluster
  locks](cluster-locks.md#coherence-from-one-holder-to-the-next)).

### Cross-subtree operations and recall

Cross-subtree operations go to the root: renames and hard links across
delegations, and `rmdir` or rename of a delegated root. So does any op
the root receives for a key under a live delegation (for example, from a
node with a stale table, or from the S3 inbox).

Before executing such an op, the root **recalls** the involved
generations:

1. it sends `DelegRecall`;
2. the delegate answers `DelegRecalled { through }` and stops executing;
3. the root waits for the stream to catch up to `through`, or, if the
   delegate is unreachable, for the grant to expire (if the delegate
   recorded a backup in its `DelegGroup` record, the root first asks the
   backup to seal and does not end the generation without its answer);
4. the root journals `RecallCut { dir, gen, through }`, which ends the
   generation and carries its cut (below); later stream records of it are
   refused.

The root then executes the op itself. There is no two-phase commit.
After a cross-subtree op the root delegates the same subtrees again,
under new generations.

#### The generation's end and its cut

`through` is the highest stream index of the generation the root had
appended to its journal before the record (its cursor; further when the
root held the generation itself, inherited at a takeover, for its own rows
ship after the record). Every row of the generation past it is void: never
in the log, never to be acknowledged. Every replica reads the cut from the
record when it applies it from the log, instead of inferring it from what
it holds of the generation (a delegate's own head holds its unappended
rows too, which the record voids): it voids the generation there, lowers
what it observed of it to the cut, and keeps the cut (`deleg_void_cut`, a
node-local key written with the record) so that a restart seeds the same
cut. A root that journaled the record and is deposed before it ships drops
the kept cut with the stranded row, since the log never carries that
record. A delegate that applies the record ends the generation here, and the
acknowledgements it parked for a row past the cut are aborted (`Busy`: the
requester retries by rid at the generation's new owner); a park at or below
the cut releases once the log's index of the generation reaches it — the
archive's, never "the row left this journal", which a recall's strand does
too. A `Recall { dir, gen }` of an older log has no cut in it: each replica
infers it from what it holds (as before this change).

The record reaches the other tables later than it ends the
generation at the root, under an S3 cut only after the cut. Until then
the recalled delegate answers lock requests and renewals for the subtree
`NotOwner` naming the root that recalled it, and a node that hears it
asks that root for the subtree's locks (see [Cluster
locks](cluster-locks.md#delegations)).

### Automatic placement

Placement is on by default (`CONSTELLATION_DELEGATION_PLACEMENT`). The
root sees every op's origin (its rid) and directory, keeps a sliding
window (`CONSTELLATION_DELEGATION_WINDOW_MS`, 30 s) of ops per directory
and node, and evaluates it every tenth of the window:

- **Delegate** the *topmost* directory `D` below `/` such that one node
  wrote at least `CONSTELLATION_DELEGATION_DOMINANCE` (70%) of the ops
  under `D`, the subtree saw at least `CONSTELLATION_DELEGATION_MIN_OPS`
  (200) ops in the window, and the node is connected, write-eligible,
  not the root and holds no other whole delegation. Walking up while the
  condition holds gives one delegation over a writer's whole working
  set, not a swarm of leaves. This follows Ceph's finding (Mantle,
  SC'15): move metadata to its dominant writer rather than spreading it.
- **Recall** a placed delegation when, for a whole dwell
  (`CONSTELLATION_DELEGATION_DWELL_MS`, 60 s), the delegate's share
  stayed below `CONSTELLATION_DELEGATION_LEAVE` (50%) or the subtree's
  rate below half the floor. Entering at 70% and leaving at 50% is the
  hysteresis.
- **Cool down** a recalled directory for
  `CONSTELLATION_DELEGATION_COOLDOWN_MS` (30 s) before it can be placed
  again, so it never ping-pongs.

Manual delegations and designations are never recalled by placement.
This placement is separate from the root lease's own placement (see
[Lease placement](lease-placement.md)), which moves the root lease
itself.

### Hot shared directories

Several writers creating files in one directory (plan 30 M12):

- **Commutative parent attributes.** Each node runs a hybrid logical
  clock (HLC); inode timestamps are HLC stamps, and a parent's `mtime`
  and `ctime` merge by `max`. A parent's `nlink` changes by deltas
  implied by the record (mkdir and rmdir ±1). Timestamps still increase
  (pjdfstest passes). The record format did not change.
- **Shared parent holds.** A create, unlink or link holds `(parent,
  name)` exclusively and the parent *shared*; `rmdir`, renaming the
  parent and `setattr` on it hold the parent exclusively. So creates of
  different names in one directory no longer serialize on the parent,
  on the requester or at the sequencer.
- **Name-hash ranges (GIGA+), by hand or by an opt-in placement.** A
  directory can be split into 2, 4, 8 or 16 ranges of its names' hashes, each delegated as its own generation.
  A range covers the directory's own names only; the contents of a
  subdirectory resolve past the split directory. A rename across ranges
  is cross-subtree (both ranges recalled, then re-delegated). `readdir`
  is unaffected, since there is one log. Ranges nobody holds stay with
  the root.

**Automatic splits are off by default** ([ADR-38](../../explanation/DECISIONS.md#adr-38-automatic-directory-hash-range-splits-are-off-by-default)). They are an
expert setting: `CONSTELLATION_DELEGATION_SPLIT=20` turns them on, and the
root's placement then splits a directory that is hot (at least the rate
floor of ops in the directory itself), that no node dominates, and that
several nodes each write at least that share (here 20%) of. It uses the
smallest power of two that gives every such node a range. Each range goes
only to a node that dominates **that range** (70%). A directory whose
ranges have all been recalled is merged back. `constellation delegate
--range` splits a directory by hand, and stays available whatever the
setting.

*When it can help.* A split pays off only when each writer's names fall
into the writer's own hash range, so that its creates are local to the
range's delegate. Ordinary applications do not name files by their hash, so
this is a property of a special workload, not of a use case. In the
[directory-split benchmark](../../../bench/dirsplit/RESULTS.md), a
synthetic workload of that kind ran at 1.2× to 3.3× the throughput of no
split within one AZ, and up to 60× with the writers far from the root.

*What it costs otherwise.* With random names (creates, create and unlink,
renames) a split directory ran at 0.01× to 0.91× the throughput of no
split, in every topology, at every writer count (2, 4, 8) and at every
root round trip measured (up to 60 ms), usually below 0.2×:

| 4 writers, creates of random names | hand split / no split |
|---|---|
| one AZ, S3 20 ms | 0.06 |
| one AZ, S3 80 ms | 0.02 |
| half the writers 60 ms from the root | 0.05 |
| every node in its own region | 0.34 |
| writers together, root 60 ms away | 0.72 |

The gap narrows as the root moves away (writers together, S3 20 ms: 0.13
at a root round trip of 2 ms, 0.44 at 20 ms, 0.70 at 60 ms) but does not
close, and a slower S3 widens it (the same sweep at S3 80 ms: 0.04, 0.15,
0.38). Two things cost: a create of a random name lands in the writer's
own range with probability 1/N, so most creates are forwarded to the range's
delegate, as they would be to the root; and an op in one range waits for
the other ranges' streams through the root's S3 log, a fixed wait of
about 145 ms at S3 20 ms and about 500 ms at S3 80 ms. A split adds this
ordering cost and removes nothing. The figures are those of a split in
place (a hand split in the benchmark): with the automatic split enabled, the
placement never split a directory of random names (it gives a range only to
a node that dominates it), and measured the same as no split there. The
listing of a split directory is also answered by one sequencer (see
"Known gaps" in [Close-to-open modes](cto-modes.md#known-gaps)), so a strict
listing could miss a name another range's delegate just created. For that
reason **splits are not available with `--cto strict`**: on a node that has
a strict mount, `CONSTELLATION_DELEGATION_SPLIT` is ignored (one warning
at mount) and `constellation delegate --range` is refused; delegating the
directory whole stays allowed. The rule is the node's own mounts', so on a
file system with strict mounts anywhere, leave the setting off on every
node that can hold the root lease.

*What to use instead.* Delegating the directory **whole** (or its subtree)
to the writer that works in it is the tool for writers far from the root:
in the benchmark it ran at 12× to 39× (writers together, root far away)
and 26× to 209× (every node in its own region) the throughput of no
delegation, and 0.9× to 3.6× within one AZ. The placement does this by
itself when one node dominates the directory (70%), and
`constellation delegate` does it by hand.

### Offline designations

An offline designation (`constellation offline <path>`) is a
non-stealable delegation to the designee: the same machinery, without
TTL expiry. While the designee is reachable, other nodes' writes under
the path are forwarded to it. An op under a designation that reaches the
root is refused with `EROFS`, and a cross-subtree op involving one with
`EXDEV`; a designation is never recalled to execute one. `--ro`
designations create no delegation.

### Failures

- **Delegate crashes, no backup recorded**: the root reclaims the grant after
  `ttl + margin` and ends the generation. Every row the delegate answered to
  another node is in the root's journal (the answer waited for the append);
  the clients of the rows it had not answered retry by rid through the root.
  A lone delegate's own node's ops, answered on its disk, are replayed by
  their rid.
- **Delegate crashes, with a recorded backup**: the root has the backup seal,
  drains what it holds, and continues (`delegate-crash-backup`). The
  generation is not ended before the backup answers (rows acknowledged through
  the group may be nowhere else): the seal is asked again every window
  (`deleg_seal_waits`, a warning once per generation) until it does.
- **The delegate's backup is new or lost rows**: the delegate re-sends
  only what it has not seen in the log. The backup's acknowledgement
  covers every row it has from the log (its applied stream index), plus
  the rows it holds contiguously above that. When the backup is behind
  the log, its answer is short; the delegate then appends again on the
  stream tick, not on every answer. A node that stops backing the root
  discards only the root's tail, never the delegate streams it backs.
- **Delegate partitioned from the root**: it stops at `sent + ttl −
  margin`, before the root may reclaim; its unstreamed rows past the cut are
  stranded: the ones nobody was answered for are dropped (their clients retry
  by rid), a lone delegate's own are replayed.
- **Delegate's core stalled behind a slow disk** (a store write that holds
  the writer lock for seconds on a loaded btrfs or network volume, a log
  write blocked the same way): the delegate keeps its grant for as long as
  the stall is below `CONSTELLATION_HOLDER_STALL_MS` (15 s), because the
  off-core lane renews it meanwhile (above); its writes wait for the stall
  and nothing else, and the delegation is not lost. Before, a stall longer
  than the renewal slack (about `ttl/2 + margin`: 3.5 s at a 5 s ttl, 11 s
  at the 20 s default) lost it: the root reclaimed, the delegate lapsed,
  and every later write in the subtree went through the root, 4 to 12 times
  slower, until something delegated it again (`delegate-store-stall`). A
  stall past the hung bound is the delegate being hung, not slow: the lane
  stops there and the root reclaims, as for a dead delegate.
- **Root's core stalled behind a slow disk**: the root's P2P service
  answers the delegates' renewals itself while the core is stuck below the
  same bound ("The root's side" above), so the generations are neither
  reclaimed nor lapsed; a root hung past the bound is a dead one, and a
  lease that ends during the stall ends the grants at its cap.
- **Delegate restarts or is handed off** (a crash and remount, `daemon
  --upgrade`, a K5 engine-pod handoff: the same node identity and
  journal): it re-adopts the generations the table names for it when
  it starts, renews them at once, and re-streams its unappended rows
  from the log's index. Its writes wait for that first renewal (one
  round trip), not for the root to reclaim the grant, which used to
  hold them for about the grant's ttl (`delegate-handoff-renewal`). For
  that, a planned handoff (`daemon --upgrade`, the local handoff of an
  engine pod) closes every P2P connection with a close frame after its
  drain and before the `exec` or exit, so the root's answer travels over a
  fresh connection to the successor; before, the root answered into its
  pooled connection to the dead incarnation until its 2 s restart probe
  evicted it, and the first write after every resume waited 2.0-2.7 s.
  After a crash nothing says goodbye, and the probe still bounds the
  wait. A generation that ended meanwhile is refused at the renewal and
  dropped when the log says so.
- **Root dies and the delegate is no backup** (no backup in budget, or
  the root dropped it before it died): only a TTL takeover replaces the
  root. The delegate's grant lapses unrenewed (above), its writes take
  the lease path, and it takes the root over once the lease is
  claimable, then ends the generation itself: within the lease TTL plus
  the 3 s a non-backup waits (`delegate-root-loss-ttl`). Before, its
  writes parked on renewals the dead root never answered, and nothing
  started the takeover (`fsync`s stuck for minutes on kind). A third
  node whose op a delegate accepted waits for the root's append; it
  takes the root over the same way (see
  [Durability and failover](durability-and-failover.md#layer-b-a-backup-within-the-rtt-budget)).
  With the delegate a listed backup, the seal-based failover applies
  (`delegate-root-loss`, `delegate-root-blackhole`).
- **Root fails over**: generations of the old root are inherited by the
  new one. The initial grant is capped by the old root's lease, so an
  inherited grant is already dead at a TTL takeover and the successor
  may reclaim at once. After a fast failover the delegates re-stream to
  the new root from the index the log already has
  (`root-failover-with-delegates`). The backup that takes over
  re-journals the predecessor's appends of delegate streams under their
  delegation origin, in the predecessor's order; if it was itself a
  delegate, its own unappended transactions are rolled back and
  replayed by rid after that tail, never shipped ahead of it. Every
  generation the node holds is marked in that same local transaction (a
  holed one: its stream now has a hole, the replayed rows are root
  rows), so the node never executes in it again. The mark is also the
  gate's fence: the strand, the tail and the replay are separate store
  transactions, and the node's own writes in its subtree execute on the
  store directly, admitted by a mirror of the delegation table that
  lags the core by an event. A write admitted before the strand and
  reaching the store's writer lock after it is refused there (the
  store's `NotAdmitted`, nothing written) and goes through the core,
  whose gate parks it until the replay is done: it lands after the
  stranded rows, never before them under their names. The mark is never
  cleared (generations are not reused). It ends the generation itself once its gate
  opens; if it crashes or loses the lease first, it comes back as a
  stopped delegate that answers a recall at the log's index of the
  stream, and the next root ends it there. A generation the successor inherits is learned when its takeover gate
  opens, whichever round that is in.
  A deposed root's appends of a delegate's stream that never shipped are
  not the log's: when they leave its journal (the rebuild, a strand) the
  index it holds of the generation goes back below them, so if it takes
  the root over again it inherits the generation at the log's index and
  the delegate re-streams them.
- While any generation is live, the root does not release its lease when
  idle and declines a cooperative handoff.

### What stays with the root

- Cross-subtree operations, and everything not under a delegation.
- Appending to the log, publishing commits, GC.
- Writes from a requester whose observed stream set is full.

## Commands

| Command | Meaning |
|---|---|
| `constellation delegate TARGET --to NODE` | delegate the directory `TARGET` to node `NODE`. Run it on the root lease holder. Refused if the directory overlaps a live delegation, is the root, or `NODE` already holds a whole delegation |
| `constellation delegate TARGET --to NODE --range IDX/COUNT` | delegate one of `COUNT` (2, 4, 8 or 16) name-hash ranges of the directory. Refused on a node with a `--cto strict` mount |
| `constellation undelegate TARGET` | recall the delegation (all ranges) on a directory. Refused for a designation, which only `constellation online` releases |
| `constellation delegations TARGET` | list the live table: directory, path, node, generation, designated, range |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CONSTELLATION_DELEGATION` | on | delegate and accept delegations |
| `CONSTELLATION_DELEGATION_TTL_MS` | lock TTL (20 s), at least `5000` | grant lifetime |
| `CONSTELLATION_DELEG_LANE` | on | the off-core renewal lane for a stuck core; `off` for A/B runs |
| `CONSTELLATION_DELEG_ROOT_LANE` | on | the root's off-core answer to a delegate's renewal while its core is stuck; `off` for A/B runs |
| `CONSTELLATION_ROOT_PROBE` | on | the root's own read of its lease object when its proof of liveness is stale (a root with a backup); `off` for A/B runs |
| `CONSTELLATION_DELEGATION_PLACEMENT` | on | automatic placement of dominated subtrees (and of splits, if enabled) |
| `CONSTELLATION_DELEGATION_WINDOW_MS` | `30000` | placement window |
| `CONSTELLATION_DELEGATION_MIN_OPS` | `200` | rate floor per window |
| `CONSTELLATION_DELEGATION_DOMINANCE` | `70` | percent to be given a subtree or a range |
| `CONSTELLATION_DELEGATION_LEAVE` | `50` | percent below which a placed delegation is recalled |
| `CONSTELLATION_DELEGATION_SPLIT` | `0` (off) | expert setting: percent per writer that makes a hot directory a split candidate; `20` is the value it was tuned with (see "Hot shared directories"); ignored on a node with a `--cto strict` mount |
| `CONSTELLATION_DELEGATION_DWELL_MS` | `60000` | recall dwell |
| `CONSTELLATION_DELEGATION_COOLDOWN_MS` | `30000` | cool-down after a recall |

For the millisecond knobs `0` means the default. For `_MIN_OPS`,
`_DOMINANCE`, `_LEAVE` and `_SPLIT` it is a value: `SPLIT=0` turns hash
range splits off, `LEAVE=0` never recalls a placed delegation for its
share (only for its rate), `DOMINANCE=0` gives a subtree to its top
writer whatever the share, and `MIN_OPS=0` drops the rate floor. See
[Configuration](../configuration.md#delegation-and-placement).

## Status

`constellation status` (JSON, under `delegation`) shows `enabled`, the
table, `mine` (this node's generations), `gens`, `kinds`, `backups`, and
`placement`: the busiest subtrees as `(dir, node, node_ops,
subtree_ops)`. Counters include `executed`, `fast_path_executed`, `lapsed`,
`fast_path_routed` (ops the root's fast path sent through the core because
a live delegation owned their keys), `fast_path_fenced` (ops the delegate's
fast path sent through the core because its generation ended at this
node's takeover gate while the op waited for the store's writer lock),
`forwarded_to_delegate`, `deps_waits`,
`streamed_txs`, `appended_txs`, `cross_subtree`, `recalls_sent`,
`recalls_drained`, `recalls_expired`, `reclaimed`, `ended`,
`renewals_off_core` (renewals the off-core lane sent while the core was
stuck in a step), `root_answers_off_core` and `root_answers_deferred` (the
root's own answers off its stuck core, and why it declined), `off_core_answers_applied`,
`grants_lost` (every grant the node gave up), of which
`lapses_during_freeze` with `last_freeze_ms` and `last_freeze_gen` (the
node's whole process was frozen) and `lapses_root_silent` with
`last_silent_gen` (a renewal sent in time was never answered by the root)
are classified: see "Grants, renewal and expiry",
`redelegated`, `deps_overflow_to_root`, `refused_designated`, and the
placement's `place_evaluations`, `place_delegated`, `place_recalled`,
`place_splits`, `place_range_recalls`, `place_skipped_cooldown` and
`place_skipped_unreachable`.

## Troubleshooting

### A writer's subtree is never delegated

Check `delegation.placement`: the writer needs 70% of the subtree's ops
and the subtree at least 200 ops per 30 s window. It must be connected
to the root over P2P and write-eligible, and must not already hold a
whole delegation. A recently recalled directory waits out the cool-down
(`place_skipped_cooldown`).

### A rename between two directories is slow

It is cross-subtree: the root recalls both delegations first. If one
delegate is unreachable, the rename waits for its grant to expire
(`recalls_expired`).

### Writes under a subtree fail with `EROFS` or `EXDEV`

The subtree is designated offline to another node that is unreachable
(`EROFS`), or the op crosses into a designated subtree (`EXDEV`). See
`constellation designations`.

### The root lease never moves

A live delegation (or a lock grant) keeps the root lease where it is.
Recall the delegations, or turn placement off, to let it move.

## FAQ

- **Does delegation change S3 traffic?** No. There is still one log;
  delegates make no S3 request for their writes, and the root's log
  PUTs carry them. Measured request counts per file were unchanged.
- **Does a single node pay anything?** No: with no peers nothing is ever
  delegated.
- **How much faster is it?** On one host, two writers in two
  directories went from about 610 files/s with a single sequencer to up
  to 950 delegated. The gain is largest when the root is far away:
  delegated writes stop paying a round trip to it.

## References

- Plan 30 §M11–§M12 ([plan](../../plans/v1/done/30-write-path-resilience-and-scale-out.md))
- [ADR-23](../../explanation/DECISIONS.md#adr-23-delegated-sub-sequencers-over-one-log)
- [`crates/authority/src/core/delegate.rs`](../../../crates/authority/src/core/delegate.rs),
  [`crates/authority/src/core/placement.rs`](../../../crates/authority/src/core/placement.rs),
  [`crates/meta/src/delegation.rs`](../../../crates/meta/src/delegation.rs),
  [`crates/meta/src/hlc.rs`](../../../crates/meta/src/hlc.rs)
- Models: `crates/model/src/delegation.rs`, `crates/model/src/hotdir.rs`;
  harness: `delegated-subtrees`, `cross-subtree-rename`,
  `delegate-crash`, `marker-order`, `root-failover-with-delegates`,
  `auto-placement`, `shared-dir-multi-writer`, `delegate-root-loss`,
  `delegate-root-blackhole`, `delegate-root-loss-ttl`,
  `delegate-handoff-renewal`, `delegate-store-stall`
- [Lease placement](lease-placement.md), [Forwarded mutations](forwarded-mutations.md),
  [Cluster locks](cluster-locks.md)
