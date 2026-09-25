//! A [Stateright](https://docs.rs/stateright) model of the metadata
//! authority protocol described in plan 30 §3, built to reproduce the two
//! correctness bugs in plan 30 §1.1 as counterexamples (M1). No product
//! crate depends on this one, and this crate depends on none of them —
//! it is a from-scratch abstraction, kept faithful to the real code by
//! the mapping table below, not a shared-code simulation.
//!
//! # Running it
//!
//! ```text
//! cd crates/model
//! cargo test -p constellation-model --release -- --nocapture
//! ```
//!
//! Every test today fits the ~60s-each budget at its current bounds
//! (`max_tick`/`max_seq`/workload size); a future milestone that needs a
//! bigger configuration to reproduce something should mark it
//! `#[ignore]` per plan 30 §M1, rather than growing the default bounds
//! for everyone.
//!
//! [`protocol::AuthorityModel`] is a `stateright::Model`. Build one with
//! [`protocol::AuthorityModel::new`] and the `with_*` builder methods,
//! then `.checker().spawn_bfs().join()` it (see the tests under
//! `tests/` for worked examples, including how to print a discovered
//! counterexample path with `Checker::discovery`).
//!
//! [`protocol::Protocol::ExactlyOnce`] (plan 30 §M2) adds `Rid`-keyed
//! exactly-once forwarding on top of `Today`; see the "Action → code
//! mapping" and "Simplifications" sections below for what changed.
//!
//! [`protocol::Protocol::Recovery`] (plan 30 §M3a) adds the requester
//! half of the speculation log on top of `ExactlyOnce`: shadows retire by
//! rid, a segment or takeover at a higher epoch strands them (rolled back
//! and queued for replay by rid), `ReplayStranded` replays them through
//! the current holder, the takeover gate replays them locally before the
//! new holder validates anything, and a node with an outstanding shadow
//! does not publish. `Recovery` also bounds authority in time the way
//! `LeaseView::usable` does (see `protocol::Protocol`'s doc).
//!
//! Plan 30 §M3b adds the holder half to the same `Recovery` variant: a
//! holder's journal entries carry their epoch and a captured
//! before-image (`protocol::JournalEntry`), a node publishes while its
//! journal is non-empty by substituting those before-images (so the
//! commit is the log prefix at its applied position), a deposed holder
//! rolls its journal back and replays it by rid instead of shipping it
//! under a lost epoch, a takeover ships an empty epoch-marker segment
//! before its gate runs, and a forward reply from an epoch the receiving
//! node has already superseded is queued for replay instead of installed.
//!
//! Plan 30 §M13 (`AuthorityModel::with_inbox(true)`, module [`inbox`])
//! is an orthogonal "P2P is unavailable" knob on top of `Recovery`, not
//! a protocol variant: non-holders submit ops as CAS-created batch
//! objects in the bucket, the holder polls and executes them, outcomes
//! (`Completed { rid }` and the new `Refused { rid, errno }`) ride the
//! log, a takeover drains every older epoch's batches inside its gate,
//! and no P2P message exists at all. It adds the property
//! `no_rid_executes_twice`, and two naive-variant knobs the tests use
//! to show the model finds the bugs in the obvious design.
//!
//! [`protocol::Protocol::Positions`] (plan 30 §M6, module [`positions`])
//! is layered on `Recovery`: clients also *read* (`AuthorityModel::
//! with_lookup`/`with_readdir`, `protocol::Action::Read`), replies carry
//! the answering holder's position and M5's `base`, an accepted record on
//! a base the requester has not applied waits for the log instead of
//! being installed, and a read waits until the node's applied position
//! reaches its `observed` watermark unless a shadow at least that new
//! covers the name (and never while a queued replay touches it). It adds
//! the properties `read_your_writes` and `monotonic_reads` (registered
//! only for a workload that reads), defined on write sets per name; today's
//! system (`Recovery` with reads) violates both, as does `Positions` with
//! either rule switched off (`with_session_wait(false)`,
//! `with_stale_base_shadows(true)`), and `tests/positions.rs` keeps those
//! counterexamples.
//!
//! Plan 30 §M9 (module [`backup`]) is a third focused model: the
//! synchronous backup, the seal, reconfiguration by CAS and `ack=s3`,
//! with "no acknowledged write is lost under a single failure" and the
//! acknowledgement order against the log; and [`cto`] gains the fast
//! takeover (a seal, or `ack=s3`) with the successor's delegation
//! horizon and the old holder's liveness probe.
//!
//! Plan 30 §M8 (module [`cto`]) is a second, focused model: `cto=strict`
//! reads (ReadIndex, read delegations with recall, lease-capped grants,
//! recall before release) over per-node clocks with bounded drift, and
//! the `close_to_open` property. It is separate because clocks are what
//! it is about and what this model deliberately leaves out; see its
//! module doc for the abstraction and the drift-margin argument.
//!
//! Plan 30 §M10 (module [`flex`], [`flex::FlexEpochs`]) is a focused
//! model of its own, like M8's and M9's: continuation epochs that may
//! form with up to `f` roster nodes missing, `heartbeat/<node>` promises
//! read across clocks with bounded drift, the S3 takeover's promise
//! check, and the interaction with M9's fast takeovers. Its property is
//! `single_authority` (never an epoch and an S3 holder, or two of either,
//! able to acknowledge at once), next to `linearizable` and
//! `converged_at_quiescence`; `tests/flex_epochs.rs` has the naive
//! variants' counterexamples and the full rule clean.
//!
//! Plan 30 §M11 (module [`delegation`]) is a third focused model:
//! delegated sub-sequencers over one log — a root and delegates for
//! disjoint subtrees, `Delegate`/`Recall` records, ownership by an
//! ancestor walk, delegate execution as speculation streamed to the root
//! with `deps`, cross-subtree ops recalled to the root, recall of an
//! unreachable delegate by TTL and margin under bounded drift, delegate
//! crashes with and without a backup, and root failover with live
//! delegates. Its properties are per-key linearizability, the causal
//! cut, `marker_order`, recall safety, log-record validity,
//! exactly-once, convergence, read-your-writes, and two stability
//! properties (nothing stranded without a fault; a backup-acknowledged
//! op never lost). `tests/delegation.rs` keeps the hand-built
//! counterexample paths for the naive variants as the primary checks.
//!
//! # What is modeled
//!
//! - **Actors.** `N` `Node`s (2–3 across the tests) plus one implicit
//!   `S3`: rather than a separate actor exchanging its own messages, S3
//!   is modeled as a handful of fields directly in [`protocol::State`]
//!   (the lease register, the log slots, the last-published commit) that
//!   any node's action may read or CAS-update atomically. This is a
//!   deliberate simplification from the "S3 requests are messages to the
//!   S3 actor" phrasing in plan 30 §M1 — see "Simplifications" below for
//!   the justification.
//! - **Namespace.** [`namespace`] models one flat directory with
//!   [`namespace::N_NAMES`] names and the two ops
//!   [`namespace::NsOp::CreateExcl`]/[`namespace::NsOp::Unlink`], with a
//!   [`namespace::NamespaceSpec`] sequential spec (`EEXIST`/`ENOENT`)
//!   that `stateright::semantics::LinearizabilityTester` checks recorded
//!   client histories against.
//! - **Network.** P2P (forward request/reply, handoff request/reply)
//!   messages travel through `State::network`, a multiset any pending
//!   message can be drawn from in any order (reordering) or discarded
//!   entirely by [`protocol::Action::DropMessage`] (loss) when
//!   `allow_lossy` is set. This is a hand-rolled stand-in for
//!   `stateright::actor::Network`'s "unordered, lossy" configuration —
//!   see "Simplifications" for why.
//! - **Time.** A bounded logical tick ([`protocol::Action::Tick`]) drives
//!   lease expiry; the forward timeout and lease renewal are causal
//!   ("may give up any time before the reply/CAS lands") rather than
//!   tied to specific tick counts, since the safety properties below
//!   only depend on *ordering*, never on real durations.
//! - **Faults.** [`protocol::Action::Crash`] (fail-stop, bounded by
//!   `max_crashes`), [`protocol::Action::Restart`] (fail-stop with the
//!   durable journal/applied position/shadow intact, gated by
//!   `allow_restart`), and [`protocol::Action::Pause`]/`Resume` (the node
//!   takes no steps — including replying to pending P2P requests — while
//!   ticks keep advancing globally).
//!
//! # Action → code mapping
//!
//! | Model action | Code path it abstracts |
//! |---|---|
//! | `ClientInvoke` (node holds) | `crates/cli/src/fusefs.rs::mutate_op_rebasable` (`open_for_new_mutation()` branch → `execute_mutate`) |
//! | `ClientInvoke` (node forwards) | `crates/cli/src/fusefs.rs::mutate_op_rebasable` (`SyncRequest::Forward` send) + `crates/cli/src/forward.rs::request_mutate_with` |
//! | `DeliverForwardRequest` | `crates/cli/src/node_runtime.rs` `SyncRequest::Mutate` arm → `crates/cli/src/forward.rs::holder_execute` |
//! | `DeliverForwardReply` (Accepted) | `crates/cli/src/forward.rs::apply_accepted` → `Meta::install_shadow` (a `spec` row of kind `Shadow`; skipped when `completed` already has the rid) |
//! | `DeliverForwardReply` (replay reply, `Recovery`) | `crates/cli/src/recovery.rs::drain_pending_replays` (accepted → a fresh shadow; refused → `.constellation-conflict/` copy) |
//! | `ReplayStranded` (`Recovery`) | `crates/cli/src/recovery.rs::drain_pending_replays` (`SyncRequest::Forward` with the stranded rid) |
//! | `ForwardTimeout` | `crates/cli/src/forward.rs::request_mutate_with`'s `tokio::time::timeout` → `MutateOutcome::Busy` |
//! | `RetryForward` (`ExactlyOnce` only) | `crates/cli/src/forward.rs::request_mutate_with`'s same-rid retry loop: the same holder if `state.lease` (the model's stand-in for the peer directory's belief) still names it, else the redirected one — plan 30 §M2's "retry the same rid... same holder... then a redirected holder" |
//! | `RequestHandoff` / `DeliverHandoffRequest` / `DeliverHandoffReply` | `crates/cli/src/node_runtime.rs` `SyncRequest::HandOff` arm (`ship.sync_one` + `LeaseKeeper::release`) and the `peers.request_lease` fast-path retry in the `SyncRequest::Acquire` arm |
//! | `AcquireLease` | `crates/cli/src/lease.rs::LeaseKeeper::classify`/`commit` (`Plan::Create`/`Plan::Claim`, `TailedToHead`) + `crates/cli/src/shipper.rs::acquire_lease_for`/`tail_to_head`; under `Recovery` also the takeover gate (`LeaseKeeper::commit_gated` → `recovery::takeover_gate`: `Meta::strand_below_epoch` + `recovery::replay_locally`) |
//! | `AcquireLease` (takeover, `Recovery`: epoch marker) | `crates/cli/src/shipper.rs::Shipper::ship_epoch_marker`, called from `shipper::acquire_lease_for` before `recovery::takeover_gate` (modeled inside the same atomic step; see the comment at its call site) |
//! | Holder-side capture (`ClientInvoke`/`DeliverForwardRequest`/`AcquireLease`/local replay journaling, `Recovery`) | `crates/meta/src/store/local.rs` (`Meta::begin_local`/`finish_local`) and `crates/meta/src/store/spec.rs` (`SpecKind::Local`): `protocol::JournalEntry`'s epoch and before-image |
//! | `Renew` (success / deposition) | `crates/cli/src/lease.rs::LeaseKeeper::renew_now` → `diagnose_lost_renew` → `mark_lost`; under `Recovery` the deposition then runs `crates/cli/src/recovery.rs::recover_deposed` (Local entries rolled back, queued for replay by rid) |
//! | `Ship` (incl. collision absorption) | `crates/cli/src/shipper.rs::ship_part` (`put_segment` create-if-absent; `Err(AlreadyExists) => tail_part`); shipping retires the shipped Local entries (`Recovery`) |
//! | `Tail` (fencing + shadow retirement) | `crates/cli/src/shipper.rs::apply_decoded_segment` (epoch fencing) → `Meta::apply_segment` (stranding rollback/redo, apply, retirement by rid under `Recovery`; record equality before M3a); under `Recovery` also `Meta::apply_segment`'s epoch stranding of Local entries → `recovery::recover_deposed`, and re-capture of the surviving Local entries' before-images |
//! | `DeliverForwardReply` (Accepted below this node's held epoch, `Recovery`) | `Meta::install_shadow` refusing below `Meta::holder_epoch`; the op is queued for replay by rid instead |
//! | `Publish` | `crates/cli/src/mtree_publish.rs::TreePublisher::publish`/`publish_batch` (deferred while a shadow is outstanding, `Meta::has_outstanding_speculation`, `Recovery`); under `Recovery` the holder publishes through `plan_from_dirty` with `Meta::publish_basis_at` before-image substitution (`protocol::log_prefix_view`) |
//! | `Crash` | fail-stop (harness `kill9`; `LeaseKeeper`'s "deposition is terminal" module doc) |
//! | `Restart` | fail-stop-then-rejoin with the durable journal intact (`crates/cli/src/shipper.rs::bootstrap`, minus lease authority) |
//! | `Pause` / `Resume` | a stopped node whose timers keep running (the shape of `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS` in `node_runtime.rs`'s `SyncRequest::Mutate` task, generalized to the whole node) |
//! | `DropMessage` | WAN loss/reordering of iroh P2P messages (`crates/net`) |
//! | `ClientInvoke` (node submits through the inbox, `inbox`) | `crates/cli/src/fusefs.rs::mutate_op_rebasable`'s P2P-unavailable branch → `store_s3::inbox::InboxSubmitter::submit` (one CAS-created `inbox/<epoch>/<node>/<n>` batch) |
//! | `PollInbox` (`inbox`) | the holder's sync round → `store_s3::inbox::InboxPoller::poll` (GET-next with idle backoff) → `forward::holder_execute` per op, outcome journaled (`Completed`/`Refused`), no reply |
//! | `Tail` (outcome for a pending inbox op, `inbox`) | `Meta::apply_segment` notifying the inbox waiter keyed by rid: `Completed { rid }` → success, `Refused { rid, errno }` → errno; a higher-epoch segment with neither strands the op |
//! | `ResubmitInbox` (`inbox`) | the stranded requester's re-submission of the same rid under the new epoch (after resolving against its own `completed` first, and deleting its stale batch) |
//! | `AcquireLease` (drain, `inbox`) | `store_s3::inbox::InboxStore::drain_below` executed inside `shipper::complete_gate`, after the stranded-op replays and before the view opens |
//! | `GcInbox` (`inbox`) | `store_s3::inbox::InboxPoller::delete` after the outcome's segment shipped, keeping each requester's newest consumed batch (`gc_keep_newest`) |
//! | `Read` (plan 30 §M6) | the FUSE read paths (`fusefs_ops.rs`: `lookup`, `getattr`, `readdir`, `open`, `readlink`, `getxattr`, `listxattr`); under `Positions` gated by the session wait (phase 2: a `Replica`/core check before the local `MetaStore` read, bounded by `CONSTELLATION_SESSION_WAIT_MS`) |
//! | `DeliverForwardRequest` reply position (`Positions`) | `Core::on_mutate_request` → `PeerMsg::MutateReply { position, base }` (M5's `reply_base` generalized; `positions::reply_position`) |
//! | `DeliverForwardReply` → `Phase::AwaitingLog` (`Positions`) | `Core::on_mutate_reply`'s stale-base branch → `ClientPhase::AwaitingLog`, answered by `answer_awaiting_log` (`positions::on_tailed`) |
//!
//! # Simplifications
//!
//! These are deliberate scope cuts, not oversights — each is either
//! orthogonal to the bugs M1 must reproduce, or would blow up the state
//! space without exercising a new code path:
//!
//! 1. Only `create_excl`/`unlink` on a flat, 2-name directory are
//!    modeled. `mkdir`/`rmdir`/`link`/`rename` share the same
//!    create-or-remove shape at this level of abstraction.
//! 2. Each node models exactly one concurrent FUSE-calling client thread
//!    (never two ops in flight from the same node at once) — this is
//!    also what makes a node id a valid linearizability "thread id".
//! 3. **S3 requests are atomic actions, not network messages.** Unlike
//!    P2P, an S3 request from a given node is never reordered or
//!    duplicated relative to that node's *other* S3 requests in the real
//!    system (each is a single HTTP round trip the caller awaits); the
//!    races that matter — another node's CAS landing in between — are
//!    already fully captured by the interleaving semantics every
//!    `stateright::Model` gets for free (any enabled action may fire
//!    next). Modeling S3 as a literal second actor with its own message
//!    queue would add bookkeeping without adding a reachable state.
//! 4. No P2P gossip *push* of segments is modeled (`shipper.rs`'s
//!    `announce_segment`) — the code itself treats it as best-effort
//!    ("peers still converge via the S3 poll"), so omitting it only
//!    removes a latency optimization, never a correctness path.
//! 5. Sticky-lease idle release (`LEASE_MIN_DWELL_MS`,
//!    `LEASE_WANTED_GRACE_MS`, `HANDOFF_PAUSE_MS`, `wanted_by`) is not
//!    modeled: it is a throughput/fairness mechanism layered on top of
//!    the safety rules this model checks, not itself safety-relevant.
//! 6. The plan 29 M6 "an `EEXIST` refusal carries the existing entry"
//!    hint (`causal_wait_target`/`MutateOutcome::Exists`) is not modeled;
//!    a refused create-family op here just returns `EEXIST`.
//! 7. `Rid`/`Completed` exactly-once identity (plan 30 §M2) is present
//!    (`protocol::Rid`, `protocol::Logged`, `protocol::rid_completed_record`)
//!    but only ever *read* under `Protocol::ExactlyOnce` — `Today` still
//!    allocates a rid and tags every record with it (structurally
//!    harmless plumbing shared by both variants), but never consults it,
//!    which is exactly what lets `today_finds_bug_a` still reproduce bug
//!    A. `ExactlyOnce`'s own forward-retry attempts are capped at
//!    `protocol::MAX_FORWARD_RETRIES` (plan 30 §M2's "three attempts") —
//!    not for fidelity alone, but because each additional attempt
//!    revisits the whole tick/renew/ship/crash interleaving space and
//!    multiplied the reachable state count roughly tenfold per attempt
//!    when this was tuned, so a config exercising crashes under
//!    `ExactlyOnce` needs noticeably tighter bounds than the equivalent
//!    `Today` config to stay inside the ~60s budget (see
//!    `exactly_once_is_linearizable`'s doc comment for the measured
//!    numbers). GC/retention (`acked_through`, `CONSTELLATION_COMPLETION_RETENTION_S`)
//!    and the incarnation bump surviving a real restart are product-level
//!    concerns validated by unit/integration tests, not by this model —
//!    `Restart` here already bumps `incarnation`/resets `next_seq` purely
//!    to keep a rid from ever being reallocated, which is the one part of
//!    that machinery a safety property could actually depend on.
//! 8. Log capacity (`max_seq`) and the tick horizon (`max_tick`) are
//!    small, per-test bounds chosen to keep exhaustive BFS within the
//!    ~60s budget; a config that needs to be bigger should be
//!    `#[ignore]`d rather than raised for everyone.
//! 9. `Today` and `ExactlyOnce` only offer `Publish` while the
//!    publishing node's own journal is empty (see the comment at its
//!    `actions()` call site): real `mtree_publish` runs on the same
//!    per-partition cycle that ships first, and without the gate *any*
//!    solitary holder could publish speculative, unshipped local state the
//!    instant after executing it — a real defect (plan 30 §1.1's "the
//!    holder side has the same shape" of bug B) but not the one
//!    `single_writer_is_clean` exists to rule out. `Recovery` no longer
//!    has this gate (plan 30 §M3b): a node with a non-empty journal
//!    publishes its replica with every name an unshipped entry touches
//!    replaced by that name's earliest captured before-image, at its
//!    `applied_seq`, which `commits_are_log_prefixes` then checks
//!    against the log (`with_raw_holder_publish(true)` publishes the raw
//!    replica instead, to show the property is not vacuous).
//! 10. `Recovery` does not require convergence while the lease still
//!     names a crashed node (`failover_pending`): nobody can tell a dead
//!     holder from a slow one, so a shadow it accepted is in doubt, not
//!     stranded, until some node takes over — which is what the next write
//!     anywhere does. A refused replay of a stranded *shadow* is dropped
//!     rather than materialized (the model has no conflict files; the
//!     acked-before-durable gap is L2/L3, closed by M9's `ack=s3`), and
//!     a replay may be re-sent at most `MAX_REPLAY_ATTEMPTS` times (the
//!     real drain retries forever; each retry only mints a fresh message
//!     id). A *deposed holder's own* acknowledged-but-unshipped ops (plan
//!     30 §M3b, `ReplayEntry::deposed`) are the acked-before-durable gap
//!     made explicit (model round 3a): from the takeover CAS their
//!     recorded returns become `NsRet::Tentative`, and a refused replay
//!     makes one `NsRet::Conflicted` (the conflict copy the real code
//!     materializes, `protocol::mark_conflicted`). `prop_linearizable`
//!     feeds both to the checker as ops still in flight on threads of
//!     their own — free to linearize when the replay lands, or never —
//!     while every other op, including the same node's later ones, is
//!     checked strictly. M9's `ack=s3` is what removes the gap itself.
//!     Under `inbox`, replays still travel as `MutateReq` messages; the
//!     real drain submits them through the inbox when P2P is unavailable,
//!     which at this abstraction is the same exchange.
//! 11. Holder-side before-images are one bit per journal entry (every
//!     modeled record touches exactly one name), and the redo after a
//!     tail or a stranded shadow is modeled as re-capturing those bits,
//!     since the model's replica is a fold of log, shadows and journal
//!     computed on demand rather than a store that needs physically
//!     undoing (`protocol::recapture_before_images`). The fold always
//!     layers shadows beneath the journal, where the real `spec` log
//!     orders all speculation by `spec_seq`; this only matters while a
//!     shadow is outstanding, when `Publish` is not offered anyway.
//!     Given exclusive, time-bounded authority and the atomic marker
//!     (simplification 12), no reachable state should tail a foreign
//!     segment under Local entries that survive it (a foreign segment is
//!     always a newer tenure's, which strands them), so the redo path is
//!     exercised mainly by shadows retired or stranded beneath the
//!     journal; it is modeled for every tail anyway.
//! 12. The takeover epoch marker is shipped inside the atomic
//!     `AcquireLease` step, together with the CAS, the tail-to-head check
//!     and the gate (see the comment at its call site for why no other
//!     node's action could interleave distinguishably). Its "slot already
//!     taken by a late segment" retry is kept but unreachable here. Like
//!     `Ship`, a takeover is not offered when the log is full, so every
//!     `Recovery` config needs one more `max_seq` slot per takeover it
//!     wants to see complete.
//! 13. Under `Recovery`, dedup (`protocol::rid_completed_record`) also
//!     consults the node's own unshipped journal, standing in for the
//!     real journal writing `completed` in the op's own transaction. The
//!     other variants keep M2's log-plus-`recent` lookup unchanged.
//! 14. The continuation-epoch path (`adopt_epoch_hold`) is not modeled,
//!     so it cannot bypass the takeover gate here; plan 30 §M3b lists
//!     that bypass as a product-code gap.
//! 15. The reply-racing-takeover refusal (`protocol::Protocol`'s M3b
//!     list) is modeled on both reply paths, but one client op per node
//!     (simplification 2) makes its client path hard to reach: a node only
//!     acquires the lease for its own `NeedsLease` op, so its forward
//!     never overlaps its own takeover the way two FUSE threads' ops can.
//! 16. (`inbox`) One op per batch: with one client op per node
//!     (simplification 2) a requester never has two ops to batch. The
//!     real batch is a group-commit window; its ops are mutually
//!     non-conflicting by the requester's keygate and execute in order,
//!     each acked in its own transaction, so per-op reasoning is the
//!     whole story. The requester's numbering is derived by LIST-last
//!     from the bucket state rather than stored, which is also what a
//!     restarted requester does. Retention pruning of `completed` is not
//!     modeled (the model's `completed` is the whole log), so the
//!     `InboxAck` position watermark the design adds for drains older
//!     than the retention window has no model counterpart. A requester
//!     submits under the epoch the register shows at that instant; a
//!     stale lease read is not modeled, so re-submission is reached
//!     through the takeover epoch marker (simplification 12): the
//!     stranded requester tails the empty higher-epoch segment before
//!     the drain's outcome ships (`inbox_marker_strands_and_resubmits`).
//! 17. (`Positions`) A position is a log slot plus a journal row count
//!     (`positions::Pos`), standing for the real `(epoch, journal_seq)`:
//!     a tenure ships its whole journal as one segment at the next slot,
//!     so the two orders agree (see `positions`' module doc). The read
//!     wait is unbounded in the model (a disabled action); the real one
//!     is bounded and then answers degraded, which is a liveness choice.
//!     `EEXIST` hints are not modeled (simplification 6), so a refusal
//!     is only ever followed by the wait, never by a covering hint; M6's
//!     hint is a latency optimization of the same rule. Reads record no
//!     history events and are not linearizability-checked.

pub mod backup;
pub mod cto;
pub mod delegation;
pub mod flex;
pub mod hotdir;
pub mod inbox;
pub mod locks;
pub mod namespace;
pub mod positions;
pub mod protocol;

pub use namespace::{Errno, NamespaceSpec, NsOp, NsRet, N_NAMES};
pub use protocol::{Action, AuthorityModel, Protocol, State};
