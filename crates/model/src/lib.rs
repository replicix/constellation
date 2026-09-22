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
//! | `DeliverForwardReply` (Accepted) | `crates/cli/src/forward.rs::apply_accepted` (`shadow_insert` + `apply_foreign`) |
//! | `ForwardTimeout` | `crates/cli/src/forward.rs::request_mutate_with`'s `tokio::time::timeout` → `MutateOutcome::Busy` |
//! | `RequestHandoff` / `DeliverHandoffRequest` / `DeliverHandoffReply` | `crates/cli/src/node_runtime.rs` `SyncRequest::HandOff` arm (`ship.sync_one` + `LeaseKeeper::release`) and the `peers.request_lease` fast-path retry in the `SyncRequest::Acquire` arm |
//! | `AcquireLease` | `crates/cli/src/lease.rs::LeaseKeeper::classify`/`commit` (`Plan::Create`/`Plan::Claim`, `TailedToHead`) + `crates/cli/src/shipper.rs::acquire_lease_for`/`tail_to_head` |
//! | `Renew` (success / deposition) | `crates/cli/src/lease.rs::LeaseKeeper::renew_now` → `diagnose_lost_renew` → `mark_lost` |
//! | `Ship` (incl. collision absorption) | `crates/cli/src/shipper.rs::ship_part` (`put_segment` create-if-absent; `Err(AlreadyExists) => tail_part`) |
//! | `Tail` (fencing + shadow retirement) | `crates/cli/src/shipper.rs::apply_decoded_segment` (epoch fencing, `apply_foreign`, `shadow_retire_matching`) |
//! | `Publish` | `crates/cli/src/mtree_publish.rs::TreePublisher::publish`/`publish_batch` |
//! | `Crash` | fail-stop (harness `kill9`; `LeaseKeeper`'s "deposition is terminal" module doc) |
//! | `Restart` | fail-stop-then-rejoin with the durable journal intact (`crates/cli/src/shipper.rs::bootstrap`, minus lease authority) |
//! | `Pause` / `Resume` | a stopped node whose timers keep running (the shape of `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS` in `node_runtime.rs`'s `SyncRequest::Mutate` task, generalized to the whole node) |
//! | `DropMessage` | WAN loss/reordering of iroh P2P messages (`crates/net`) |
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
//! 7. `Rid`/`Completed` exactly-once identity (plan 30 M2) is
//!    *deliberately absent* — that omission is exactly what lets
//!    `today_finds_bug_a` reproduce bug A. `Protocol::Today` is the only
//!    variant this milestone implements; see [`protocol::Protocol`]'s
//!    doc for where later milestones extend it.
//! 8. Log capacity (`max_seq`) and the tick horizon (`max_tick`) are
//!    small, per-test bounds chosen to keep exhaustive BFS within the
//!    ~60s budget; a config that needs to be bigger should be
//!    `#[ignore]`d rather than raised for everyone.
//! 9. `Publish` is only offered while the publishing node's own journal
//!    is empty (see the comment at its `actions()` call site): real
//!    `mtree_publish` runs on the same per-partition cycle that ships
//!    first, so a live, still-acting node's journal is already drained
//!    by the time it publishes. Without this, *any* solitary holder
//!    could publish speculative, unshipped local state the instant after
//!    executing it — a real defect (plan 30 §1.1 also describes "the
//!    holder side has the same shape" of bug B) but a different one from
//!    what `single_writer_is_clean` exists to rule out. A lingering
//!    *shadow* (a forwarded op the log will never confirm — the actual
//!    bug B shape) is unaffected by this gate.

pub mod namespace;
pub mod protocol;

pub use namespace::{Errno, NamespaceSpec, NsOp, NsRet, N_NAMES};
pub use protocol::{Action, AuthorityModel, Protocol, State};
