//! `constellation-control`: the control protocol (plan 31 §9).
//!
//! One typed request/response protocol, with streaming and cancellation,
//! that every management surface speaks: the CLI, the harness, the web
//! adapter and (later) the SPA, the CSI driver and remote devices. It
//! replaces the old line-JSON `crates/api`.
//!
//! ## Map of the crate
//!
//! | module | what it is |
//! |---|---|
//! | [`proto`] | frames, handshake, envelope, [`ControlError`], the parameter/result types |
//! | [`methods`] | the typed method table: one type per method, [`METHODS`] |
//! | [`authz`] | [`Role`], [`Principal`], [`Policy`] (owner = admin, allowlist grants) |
//! | [`audit`] | append-only JSONL log of mutating calls (params digest only) |
//! | [`transport`] | [`Transport`](transport::Transport): unix socket (peer creds, fd passing), in-process, generic stream, named-pipe stub |
//! | [`server`] | [`Router`] of typed handlers, [`serve`], [`dispatch_in_process`] |
//! | [`client`] | [`Client`]: typed calls, subscriptions, chunked results, cancel, timeouts |
//! | [`schema`] | JSON Schema generation and its committed copy |
//! | `web` | (feature `web`) the localhost HTTP adapter and embedded UI, over [`dispatch_in_process`] |
//!
//! ## Design in one page
//!
//! - **Frames**: `u32 length | u8 kind | payload`; 8 MiB max; kinds Hello,
//!   Welcome, Request, Response, Event, Cancel, Chunk. Hello/Welcome are
//!   always JSON and negotiate the encoding (JSON or postcard) and features;
//!   there is deliberately no protocol version (§9.3).
//! - **Typed methods, opaque envelope**: the envelope carries params and
//!   results as a [`Blob`](proto::Blob) — inline JSON, or postcard bytes — so
//!   one envelope type serves both encodings and JSON stays readable.
//! - **One dispatch path**: sockets, in-process clients and the web adapter
//!   all reach handlers through the same authorize → audit → decode → run
//!   sequence, which is what the C5 parity test relies on.
//! - **Authorization from the transport**: the peer's kernel credentials
//!   become a [`Principal`]; the [`Policy`] gives roles; every method
//!   declares its minimum.
//! - **File descriptors ride on frames**: a flag bit on the frame plus
//!   `SCM_RIGHTS` on the unix socket, or a direct move in-process.
//!
//! What is *not* here: the method implementations (the engine binds them:
//! `constellation_engine::control`, with the host's own pieces from the
//! daemon), TypeScript generation (plan 33) and the named-pipe transport
//! (plan 35).

pub mod audit;
pub mod authz;
pub mod client;
pub mod fd;
#[cfg(unix)]
pub mod handoff_wire;
pub mod methods;
pub mod proto;
pub mod schema;
pub mod server;
pub mod transport;
#[cfg(feature = "web")]
pub mod web;

pub use audit::{AuditRecord, AuditSink, FileAuditSink, MemoryAuditSink};
pub use authz::{Policy, Principal, Role};
pub use client::{Client, ClientOptions};
pub use methods::{Method, MethodInfo, StreamKind, METHODS};
pub use proto::{ControlError, Encoding, ErrorKind};
pub use server::{
    dispatch_in_process, dispatch_stream_in_process, serve, CallCtx, DispatchOptions, Router,
    ServeOptions, ServerHandle,
};
#[cfg(test)]
mod tests;
