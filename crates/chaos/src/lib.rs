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
//! - [`check_history`] — pure offline checkers over a [`History`]
//!
//! Generators and checkers never know whether workers are threads or remote hosts.

#![deny(clippy::unwrap_used)]

pub mod check;
pub mod cluster;
pub mod coord;
pub mod gen;
pub mod history;
pub mod op;
pub mod proto;
pub mod store;
pub mod worker;

pub use check::{check_history, CheckFailure};
pub use cluster::{Cluster, LocalCluster, TcpCluster};
pub use coord::Coordinator;
pub use gen::{Profile, ScenarioSet};
pub use history::{Event, EventKind, History};
pub use op::{execute_op, hash_bytes, Complete, Op, Outcome};
