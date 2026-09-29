//! `constellation-vfs`: the frontend contract (plan 31 §6).
//!
//! Everything from this crate up is where OS- and kernel-specific
//! translation happens; everything below it is the one engine. A frontend
//! (Linux FUSE today; NFS, WinFsp, Android SAF in plans 34-36) decodes its
//! protocol into [`OpCtx`] + a [`Vfs`] call and turns each [`Responder`]
//! completion back into its protocol's reply. No frontend, and no engine
//! internal, is FUSE-shaped past this crate.
//!
//! - [`Vfs`]: the ops. [`OpCtx`] (op id and kind, [`Caller`], deadline,
//!   [`CancelToken`], tracing span) goes with each.
//! - [`Responder`]: completion, exactly once, with a drop fail-safe;
//!   [`Blocking`] parks a synchronous caller, [`FnResponder`] calls a
//!   function, [`DirSink`] is `readdir`'s buffer.
//! - [`FrontendEvents`]/[`Invalidation`]: what the engine tells the
//!   frontend (its cache is stale), delivered from one dedicated thread.
//! - [`FrontendCaps`]: what the frontend can do, declared once.
//! - Policies ([`NamePolicy`], [`XattrPolicy`], [`IdentityMap`],
//!   [`PolicyStack`]): how frontend names/xattrs/principals become the
//!   engine's, applied beneath the trait so no frontend repeats them.
//! - [`OpWatch`]: the stalled-op watchdog every op registers with.
//! - [`VfsError`]: a portable [`constellation_types::Code`].

pub mod caps;
pub mod ctx;
pub mod error;
pub mod events;
pub mod name;
pub mod policy;
pub mod responder;
pub mod types;
mod vfs;
pub mod watch;

pub use caps::{CasePolicy, FrontendCaps, OpenUnlinked, PushInval, XattrSupport};
pub use ctx::{Caller, CancelToken, OpCtx, OpId, OpKind, OpKindSet, Principal};
pub use error::{VfsError, VfsResult};
pub use events::{FrontendEvents, Invalidation};
pub use name::{Name, NameBuf, XattrName, XattrNameBuf};
pub use policy::{IdentityMap, NamePolicy, PolicyStack, XattrPolicy, NAME_MAX};
pub use responder::{
    Blocking, BlockingWait, CollectDir, DirEntry, DirSink, FnResponder, Responder,
};
pub use types::{
    Attr, Durability, Entry, FallocateMode, Fh, FileKind, Ino, LockKind, LockOwner, LockRange,
    LockSpec, LockStatus, OpenFlags, OpenOwner, Opened, ReadData, RenameFlags, SeekWhence, SetAttr,
    SetXattrFlags, SetXattrMode, StatFs, TimeSet, WriteData, ROOT_INO,
};
pub use vfs::Vfs;
pub use watch::{OpWatch, WatchKey, Watched};
