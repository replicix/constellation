//! Multi-node filesystem consistency stress tool.
//!
//! Shared ops, generators, and checkers drive both a fast local CI cluster
//! ([`LocalCluster`]) and a multi-node TCP soak ([`TcpCluster`] + `chaos worker`).
//!
//! # Primary entry points
//!
//! - [`Coordinator::run`] — execute a [`Profile`] against any [`Cluster`]
//! - [`LocalCluster`] — in-process mounts (used by harness `chaos-ci`)
//! - [`TcpCluster`] — remote workers for multi-node soak
//! - [`check_history`] — pure offline checkers over a [`History`]:
//!   the per-step invariants, exactly-once, Elle-style cycles, and plan 30
//!   §M6's per-node session guarantees ([`sessions`], enforced since M6
//!   phase 2: [`sessions::ENFORCE_SESSION_GUARANTEES`])
//! - [`check_convergence`] / [`check_log_completions`] — plan 30 §M4's
//!   whole-cluster checks the harness runs after a run (every replica's
//!   tree, a fresh one's included; every rid completes once in the log)
//!
//! Generators and checkers never know whether workers are threads or remote hosts.

#![deny(clippy::unwrap_used)]

pub mod check;
pub mod cluster;
pub mod converge;
pub mod coord;
pub mod elle;
pub mod exactly_once;
pub mod gen;
pub mod history;
pub mod op;
pub mod proto;
pub mod sessions;
pub mod store;
pub mod worker;

pub use check::{check_history, CheckFailure};
pub use cluster::{Cluster, LocalCluster, TcpCluster};
pub use converge::{check_convergence, snapshot_tree, TreeSnapshot};
pub use coord::Coordinator;
pub use exactly_once::{check_log_completions, LoggedCompletion};
pub use gen::{Profile, ScenarioSet};
pub use history::{Event, EventKind, History};
pub use op::{execute_op, hash_bytes, Complete, Op, Outcome};
