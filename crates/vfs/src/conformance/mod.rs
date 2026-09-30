//! The conformance kit (plan 31 §8): a kernel-free suite that drives any
//! [`Vfs`] through the same contract and reports per test.
//!
//! # Running it
//!
//! - **Against the reference filesystem** (`MockVfs` in reference mode,
//!   [`reference::RefTarget`]): `cargo test -p constellation-vfs --features
//!   conformance` — what the `conformance` CI job runs. This proves the
//!   kit itself is self-consistent and gives every frontend crate a
//!   correct stand-in to compare against.
//! - **Against a real target**: implement [`ConformanceTarget`] (a factory
//!   for fresh [`Fixture`]s) and call [`run_all`]. The engine's instance is
//!   `ENGINE_TARGET.md` next to this file, ready to drop into
//!   `crates/engine/tests/conformance.rs`. A new frontend plugs in the
//!   same way: its target builds a view with that frontend's
//!   [`FrontendCaps`] and [`crate::PolicyStack`] and hands the kit the
//!   `Vfs` — the kit needs no kernel, so a WinFsp or NFS target runs on any
//!   host.
//!
//! # Shape
//!
//! - [`TESTS`] lists every test: name, [`Group`], the [`Cap`]s it needs.
//!   [`run_all`] runs the ones the frontend's [`FrontendCaps`] allow and
//!   returns a [`Report`]: passed, failed (with the message) or skipped
//!   (with a reason that names the capability as a whole word,
//!   `requires capability ClusterLocks`, the spelling
//!   `tests/parity.py`'s capability matcher expects). A test that needs
//!   something only the fixture can say — a snapshot hook, a second view,
//!   a target that supports cancelling waits — skips itself with a reason
//!   naming that. Nothing is silently green.
//! - Every test is **seeded** ([`RunOptions::seed`], mixed with the test's
//!   name) and deterministic in what it *does*; threads make the
//!   interleaving nondeterministic, so what concurrent tests *check* are
//!   invariants and a model's answer, never an order.
//! - Groups: `namespace`, `io`, `xattr`, `readdir`, `concurrency`,
//!   `deferral`, `cancellation`, `invalidation`, `confinement` (§6.12).
//!
//! # The oracle
//!
//! `crates/model` is a Stateright model of the *authority protocol* (one
//! flat directory of two names, create/unlink only): it checks a
//! replication algorithm, not a POSIX namespace, and cannot judge a `Vfs`.
//! The kit's oracle is its own (`oracle`): a small path-based model of
//! the namespace and content that the sequential and concurrent model
//! tests replay a seeded workload against, comparing every op's outcome
//! and the final tree.
//!
//! # What the kit deliberately does not test
//!
//! - `OpenFlags::APPEND` on `write`: a kernel frontend computes the append
//!   offset itself (the engine ignores the flag); the contract carries it
//!   for frontends that do not.
//! - Permission bits: the kernel checks modes above the trait.
//! - Timing and durability levels beyond "every level succeeds or is
//!   `NotSupported`, and the data is there afterwards".
//! - **Cancellation on Linux FUSE.** fuser 0.18 delivers no
//!   `FUSE_INTERRUPT`, so a Linux FUSE frontend's `CancelToken` is never
//!   set by the kernel (plan 31 §6.3's known gap). The `cancellation`
//!   group tests the token where a target says its waits honour it
//!   ([`Declared::cancellable_waits`]) and skips, naming the gap, where it
//!   does not.

mod cancellation;
mod client;
mod concurrency;
mod confinement;
mod deferral;
pub mod facade;
mod invalidation;
mod io;
mod namespace;
mod oracle;
mod readdir;
pub mod reference;
mod report;
mod xattr;

pub use client::{Client, Completed, Pending, HANG};
pub use facade::{Done, DynVfs, Probe};
pub use report::{Outcome, Report, TestReport};

use crate::caps::{Cap, FrontendCaps};
use crate::events::{FrontendEvents, Invalidation};
use crate::types::Ino;
use crate::vfs::Vfs;
use std::any::Any;
use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Instant;

/// Mirrors `constellation_engine::LINK_DOMAIN_XATTR`: the root-only marker
/// that starts a link domain inside a view (§6.12).
pub const LINK_DOMAIN_XATTR: &str = crate::mock::LINK_DOMAIN_XATTR;

// ------------------------------------------------------------------ target

/// What a test asks a target for.
#[derive(Debug, Clone, Default)]
pub struct TargetOpts {
    /// The test's seed, for a target that has randomness of its own.
    pub seed: u64,
}

/// A factory of fresh, empty filesystems behind a [`Vfs`].
pub trait ConformanceTarget: Send + Sync {
    type V: Vfs;

    /// A fresh filesystem and one view of its whole tree, behaving as a
    /// frontend with `caps` sees it (the target builds its
    /// [`crate::PolicyStack`] with [`crate::PolicyStack::for_caps`]).
    fn fresh(&self, caps: &FrontendCaps, opts: TargetOpts) -> Fixture<Self::V>;

    /// A name for reports.
    fn name(&self) -> String {
        "target".to_string()
    }
}

/// What a target says about itself that is not a [`FrontendCaps`] bit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declared {
    /// `rename` honours `RENAME_NOREPLACE` and `RENAME_EXCHANGE`. Default
    /// `true`, the contract (the engine's `View` honours both since the
    /// plan 31 C4 follow-ups); a target that does not declares `false` and
    /// the rename-flag tests skip, naming `rename-flags`.
    pub rename_flags: bool,
    /// A `CancelToken` set on a waiting op makes it complete `Code::Intr`
    /// (see the module doc: never true for a Linux FUSE *mount*, which no
    /// kernel interrupt reaches; a `Vfs` driven directly may).
    pub cancellable_waits: bool,
}

impl Default for Declared {
    fn default() -> Self {
        Self {
            rename_flags: true,
            cancellable_waits: true,
        }
    }
}

type SnapshotHook = Arc<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>;

/// [`Hooks::evict`].
pub type EvictHook = Arc<dyn Fn(Ino) -> Result<(), String> + Send + Sync>;

/// The optional abilities of a fixture beyond its one view. A target that
/// cannot offer one leaves it `None`, and the tests that need it skip
/// naming it.
pub struct Hooks<V: Vfs> {
    /// `snapshot(path, name)`: freeze the directory at `path` (relative to
    /// the view's root) as snapshot `name`.
    pub snapshot: Option<SnapshotHook>,
    /// `subtree_view(path, confine_links)`: another view of the same
    /// filesystem rooted at the directory `path` (relative to this view's
    /// root), with `ViewSpec::confine_links` as given.
    #[allow(clippy::type_complexity)]
    pub subtree_view: Option<Arc<dyn Fn(&str, bool) -> Result<Fixture<V>, String> + Send + Sync>>,
    /// `second_view()`: another view of the whole tree whose mutations
    /// reach this fixture's [`Hooks::events`] the way a remote node's
    /// would.
    #[allow(clippy::type_complexity)]
    pub second_view: Option<Arc<dyn Fn() -> Result<Fixture<V>, String> + Send + Sync>>,
    /// Where this fixture's frontend events go.
    pub events: Option<Arc<RecordingEvents>>,
    /// Wait until every event queued so far reached [`Hooks::events`].
    pub settle: Option<Arc<dyn Fn() + Send + Sync>>,
    /// `evict(ino)`: drop the file's data from every local cache, so that
    /// the next read of it must wait for the backing store (a *cold*
    /// read, which the deferral group checks is answered off the calling
    /// thread when the frontend allows it).
    pub evict: Option<EvictHook>,
}

impl<V: Vfs> Default for Hooks<V> {
    fn default() -> Self {
        Self {
            snapshot: None,
            subtree_view: None,
            second_view: None,
            events: None,
            settle: None,
            evict: None,
        }
    }
}

/// One fresh filesystem and a view of it.
pub struct Fixture<V: Vfs> {
    pub vfs: Arc<V>,
    /// The view's root inode ([`crate::ROOT_INO`] for every target so far).
    pub root: Ino,
    pub caps: FrontendCaps,
    pub declared: Declared,
    pub hooks: Hooks<V>,
    /// Kept alive as long as the fixture (temp dirs, runtimes, the
    /// engine).
    pub keepalive: Vec<Box<dyn Any + Send + Sync>>,
}

impl<V: Vfs> Fixture<V> {
    /// A fixture with no hooks and default declarations.
    pub fn new(vfs: Arc<V>, caps: FrontendCaps) -> Self {
        Self {
            vfs,
            root: crate::types::ROOT_INO,
            caps,
            declared: Declared::default(),
            hooks: Hooks::default(),
            keepalive: Vec::new(),
        }
    }

    /// The object-safe form the tests run against.
    pub fn erase(self) -> Fx {
        let Fixture {
            vfs,
            root,
            caps,
            declared,
            hooks,
            keepalive,
        } = self;
        let subtree = hooks.subtree_view.map(|f| {
            Arc::new(move |path: &str, confine: bool| f(path, confine).map(Fixture::erase))
                as Arc<dyn Fn(&str, bool) -> Result<Fx, String> + Send + Sync>
        });
        let second = hooks.second_view.map(|f| {
            Arc::new(move || f().map(Fixture::erase))
                as Arc<dyn Fn() -> Result<Fx, String> + Send + Sync>
        });
        Fx {
            vfs,
            root,
            caps,
            declared,
            hooks: FxHooks {
                snapshot: hooks.snapshot,
                subtree,
                second,
                events: hooks.events,
                settle: hooks.settle,
                evict: hooks.evict,
            },
            _keepalive: keepalive,
        }
    }
}

/// [`Hooks`], erased.
#[derive(Clone)]
#[allow(clippy::type_complexity)]
struct FxHooks {
    snapshot: Option<SnapshotHook>,
    subtree: Option<Arc<dyn Fn(&str, bool) -> Result<Fx, String> + Send + Sync>>,
    second: Option<Arc<dyn Fn() -> Result<Fx, String> + Send + Sync>>,
    events: Option<Arc<RecordingEvents>>,
    settle: Option<Arc<dyn Fn() + Send + Sync>>,
    evict: Option<EvictHook>,
}

/// A [`Fixture`], erased over the target's `Vfs` type: what a test sees.
pub struct Fx {
    vfs: Arc<dyn DynVfs>,
    root: Ino,
    pub caps: FrontendCaps,
    pub declared: Declared,
    hooks: FxHooks,
    _keepalive: Vec<Box<dyn Any + Send + Sync>>,
}

/// Why a test did not pass.
#[derive(Debug)]
pub enum TestErr {
    /// The test cannot run against this target; the reason names what is
    /// missing.
    Skip(String),
    Fail(String),
}

/// A test's result: `Ok` passed.
pub type TestResult = Result<(), TestErr>;

/// The refusal `r` must be, exactly, `want`.
#[track_caller]
pub(crate) fn refused<T: std::fmt::Debug>(
    what: &str,
    r: crate::VfsResult<T>,
    want: constellation_types::Code,
) {
    refused_any(what, r, &[want]);
}

/// The refusal `r` must be one of `want` (for the few places POSIX
/// systems differ: `unlink` of a directory is `EISDIR` on Linux and
/// `EPERM` elsewhere).
#[track_caller]
pub(crate) fn refused_any<T: std::fmt::Debug>(
    what: &str,
    r: crate::VfsResult<T>,
    want: &[constellation_types::Code],
) {
    match r {
        Ok(v) => panic!("{what}: expected a refusal with {want:?}, the op succeeded with {v:?}"),
        Err(e) if want.contains(&e.code()) => {}
        Err(e) => panic!("{what}: expected {want:?}, got {:?}", e.code()),
    }
}

/// The success `r` must be, with a message naming the step.
#[track_caller]
pub(crate) fn must<T>(what: &str, r: crate::VfsResult<T>) -> T {
    r.unwrap_or_else(|e| panic!("{what}: failed with {:?} ({e})", e.code()))
}

/// A joined thread's value; a thread that panicked re-panics here with
/// its own message (an `expect` would report only "Any { .. }").
pub(crate) fn joined<T>(r: std::thread::Result<T>) -> T {
    r.unwrap_or_else(|payload| std::panic::resume_unwind(payload))
}

/// Skip the test (`return skip!(...)`) with a reason.
macro_rules! skip {
    ($($arg:tt)*) => {
        return Err($crate::conformance::TestErr::Skip(format!($($arg)*)))
    };
}
pub(crate) use skip;

/// Fail the test with a message.
macro_rules! fail {
    ($($arg:tt)*) => {
        return Err($crate::conformance::TestErr::Fail(format!($($arg)*)))
    };
}
pub(crate) use fail;

impl Fx {
    /// A caller on this view: uid 1000, gid 1000.
    pub fn client(&self) -> Client {
        Client::new(self.vfs.clone(), self.root, 1000, 1000)
    }

    /// A caller on this view as `root`.
    pub fn root_client(&self) -> Client {
        Client::new(self.vfs.clone(), self.root, 0, 0)
    }

    pub fn snapshot(&self, path: &str, name: &str) -> Result<(), TestErr> {
        let Some(hook) = &self.hooks.snapshot else {
            skip!("target offers no snapshot hook");
        };
        hook(path, name).map_err(|e| TestErr::Fail(format!("snapshot({path}, {name}): {e}")))
    }

    pub fn subtree(&self, path: &str, confine_links: bool) -> Result<Fx, TestErr> {
        let Some(hook) = &self.hooks.subtree else {
            skip!("target offers no subtree-view hook");
        };
        hook(path, confine_links)
            .map_err(|e| TestErr::Fail(format!("subtree_view({path}, {confine_links}): {e}")))
    }

    pub fn second_view(&self) -> Result<Fx, TestErr> {
        let Some(hook) = &self.hooks.second else {
            skip!("target offers no second-view hook");
        };
        hook().map_err(|e| TestErr::Fail(format!("second_view: {e}")))
    }

    pub fn events(&self) -> Result<&Arc<RecordingEvents>, TestErr> {
        match &self.hooks.events {
            Some(events) => Ok(events),
            None => skip!("target delivers no frontend events to this fixture"),
        }
    }

    /// Make `ino`'s next read cold ([`Hooks::evict`]).
    pub fn evict(&self, ino: Ino) -> Result<(), TestErr> {
        let Some(hook) = &self.hooks.evict else {
            skip!("target offers no evict hook (cold reads)");
        };
        hook(ino).map_err(|e| TestErr::Fail(format!("evict({ino}): {e}")))
    }

    pub fn settle(&self) {
        if let Some(settle) = &self.hooks.settle {
            settle();
        }
    }
}

/// A [`FrontendEvents`] that records what it is told and from which
/// thread.
#[derive(Default)]
pub struct RecordingEvents {
    batches: Mutex<Vec<(ThreadId, Vec<Invalidation>)>>,
}

impl RecordingEvents {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Everything delivered so far, flattened.
    pub fn all(&self) -> Vec<Invalidation> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(_, b)| b.iter().cloned())
            .collect()
    }

    /// Take everything delivered so far.
    pub fn take(&self) -> Vec<Invalidation> {
        self.batches
            .lock()
            .unwrap()
            .drain(..)
            .flat_map(|(_, b)| b)
            .collect()
    }

    /// The distinct threads deliveries came from.
    pub fn threads(&self) -> HashSet<ThreadId> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _)| *t)
            .collect()
    }

    pub fn batch_count(&self) -> usize {
        self.batches.lock().unwrap().len()
    }
}

impl FrontendEvents for RecordingEvents {
    fn invalidate(&self, batch: &[Invalidation]) {
        self.batches
            .lock()
            .unwrap()
            .push((std::thread::current().id(), batch.to_vec()));
    }
}

// --------------------------------------------------------------------- rng

/// The seeded generator the tests draw from (splitmix64).
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            out.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        out.truncate(n);
        out
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    /// A child generator for a thread or a sub-workload.
    pub fn fork(&mut self, salt: u64) -> Rng {
        Rng::new(self.next_u64() ^ salt.wrapping_mul(0xa076_1d64_78bd_642f))
    }
}

// ------------------------------------------------------------------- tests

/// What a test is about; also the `group::` prefix of its report name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Group {
    Namespace,
    Io,
    Xattr,
    Readdir,
    Concurrency,
    Deferral,
    Cancellation,
    Invalidation,
    Confinement,
}

impl Group {
    pub const ALL: &'static [Group] = &[
        Group::Namespace,
        Group::Io,
        Group::Xattr,
        Group::Readdir,
        Group::Concurrency,
        Group::Deferral,
        Group::Cancellation,
        Group::Invalidation,
        Group::Confinement,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Group::Namespace => "namespace",
            Group::Io => "io",
            Group::Xattr => "xattr",
            Group::Readdir => "readdir",
            Group::Concurrency => "concurrency",
            Group::Deferral => "deferral",
            Group::Cancellation => "cancellation",
            Group::Invalidation => "invalidation",
            Group::Confinement => "confinement",
        }
    }
}

/// What a test runs with.
pub struct Env<'a> {
    target: &'a dyn DynTarget,
    caps: &'a FrontendCaps,
    seed: u64,
}

impl Env<'_> {
    /// A fresh fixture for the run's capabilities.
    pub fn fresh(&self) -> Fx {
        self.target
            .fresh_dyn(self.caps, TargetOpts { seed: self.seed })
    }

    /// A fresh fixture for other capabilities (e.g. a frontend without
    /// hard links: the refusal is part of the contract).
    pub fn fresh_with(&self, caps: &FrontendCaps) -> Fx {
        self.target.fresh_dyn(caps, TargetOpts { seed: self.seed })
    }

    pub fn caps(&self) -> &FrontendCaps {
        self.caps
    }

    /// The test's seeded generator.
    pub fn rng(&self) -> Rng {
        Rng::new(self.seed)
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }
}

/// One conformance test.
#[derive(Clone, Copy)]
pub struct ConformanceTest {
    pub name: &'static str,
    pub group: Group,
    /// Capabilities the frontend must have for the test to apply.
    pub caps: &'static [Cap],
    pub run: fn(&Env<'_>) -> TestResult,
}

impl ConformanceTest {
    /// `group::name`, as a report spells it.
    pub fn id(&self) -> String {
        format!("{}::{}", self.group.name(), self.name)
    }
}

macro_rules! tests {
    ($($group:ident / $module:ident : $($name:ident [$($cap:ident),*]),* $(,)?;)*) => {
        /// Every conformance test, in run order.
        pub const TESTS: &[ConformanceTest] = &[
            $($(ConformanceTest {
                name: stringify!($name),
                group: Group::$group,
                caps: &[$(Cap::$cap),*],
                run: $module::$name,
            },)*)*
        ];
    };
}

tests! {
    Namespace / namespace:
        create_lookup_getattr [],
        entries_carry_the_callers_identity [],
        mkdir_rmdir_and_their_refusals [],
        unlink_and_its_refusals [],
        lookup_refusals [],
        create_without_excl_opens_what_is_there [],
        rename_within_and_across_directories [],
        rename_over_existing_targets [],
        rename_refusals [],
        rename_noreplace [],
        rename_exchange [],
        rename_exchange_across_directories [],
        hard_links_count_names [HardLinks],
        hard_link_refusals [HardLinks],
        symlink_and_readlink [],
        special_files [SpecialFiles],
        name_length_limits [],
        unlinked_open_file_stays_usable [KeepOpenUnlinked],
        model_replay_sequential [];
    Io / io:
        write_read_roundtrip [],
        read_past_the_end [],
        pending_size_is_visible_before_close [],
        overwrite_and_extend [],
        sparse_files_read_zeros [],
        truncate_shrinks_and_growth_reads_zeros [],
        setattr_mode_owner_and_times [],
        two_handles_see_each_others_writes [],
        data_survives_close_and_reopen [],
        large_multi_chunk_file [],
        fsync_levels [],
        fallocate_modes [Fallocate],
        fallocate_absent_is_not_supported [],
        seek_data_and_hole [SeekHole],
        seek_absent_is_not_supported [],
        statfs_is_sane [];
    Xattr / xattr:
        set_get_list_remove [Xattrs],
        create_and_replace_flags [Xattrs],
        missing_names [Xattrs],
        namespaces_and_name_limits [Xattrs],
        value_size_limit [Xattrs],
        virtual_xattrs_follow_the_listing_capability [Xattrs],
        virtual_xattrs_are_read_only [Xattrs];
    Readdir / readdir:
        lists_dot_dotdot_and_children [],
        cookies_resume_where_they_left_off [],
        plus_lists_the_same_entries [],
        removal_between_pages_never_repeats_or_loses [],
        stable_under_concurrent_create [],
        readdir_of_a_file_is_notdir [];
    Concurrency / concurrency:
        creates_in_one_directory_are_all_visible [],
        exclusive_create_has_one_winner [],
        unlink_has_one_winner [],
        renames_keep_every_inode_named_once [],
        disjoint_files_match_the_model [],
        disjoint_ranges_of_one_file_match_the_model [],
        overlapping_writes_are_not_torn [],
        model_replay_concurrent [];
    Deferral / deferral:
        every_op_completes_exactly_once [],
        a_blocked_lock_completes_from_another_thread [ClusterLocks],
        non_deferrable_waits_park_the_calling_thread [],
        a_cold_read_completes_from_another_thread [],
        a_non_deferrable_cold_read_parks_the_calling_thread [],
        locks_are_refused_without_the_capability [],
        a_panicking_responder_leaves_the_target_usable [],
        the_provided_responders_fail_safe [];
    Cancellation / cancellation:
        cancelled_before_the_wait_is_interrupted [ClusterLocks],
        cancelled_during_the_wait_is_interrupted [ClusterLocks],
        cancel_after_completion_is_harmless [ClusterLocks],
        a_timed_out_wait_leaves_no_waiter_behind [ClusterLocks];
    Invalidation / invalidation:
        remote_create_invalidates_the_name [PushInval],
        remote_unlink_and_rename_invalidate_names [PushInval],
        remote_write_invalidates_pages [PushInval],
        remote_setattr_invalidates_attributes [PushInval],
        events_come_from_one_dedicated_thread [PushInval];
    Confinement / confinement:
        dotdot_at_the_view_root_is_the_root [],
        the_view_root_is_the_root_inode [],
        inodes_outside_the_subtree_are_refused [],
        forged_inodes_and_handles_are_refused [],
        an_open_file_stays_addressable_when_unlinked [KeepOpenUnlinked],
        constellation_snapshots_stay_inside_the_subtree [],
        snapshot_mirrors_are_read_only [],
        links_are_free_without_confine_links [HardLinks],
        confine_links_refuses_links_across_domains [HardLinks, Xattrs],
        confine_links_refuses_renames_of_linked_files_across_domains [HardLinks, Xattrs];
}

// ------------------------------------------------------------------ runner

/// Object-safe [`ConformanceTarget`].
trait DynTarget: Send + Sync {
    fn fresh_dyn(&self, caps: &FrontendCaps, opts: TargetOpts) -> Fx;
}

impl<T: ConformanceTarget> DynTarget for T {
    fn fresh_dyn(&self, caps: &FrontendCaps, opts: TargetOpts) -> Fx {
        ConformanceTarget::fresh(self, caps, opts).erase()
    }
}

/// How to run.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Mixed with each test's name into the test's seed.
    pub seed: u64,
    /// Run only tests whose `group::name` contains this.
    pub filter: Option<String>,
    /// The frontend to run as; default [`FrontendCaps::linux_fuse`] with
    /// cluster locks.
    pub caps: FrontendCaps,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            seed: 0x5eed,
            filter: None,
            caps: FrontendCaps::linux_fuse(true),
        }
    }
}

fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_string(),
            Err(_) => "panicked with a non-string payload".to_string(),
        },
    }
}

/// Run every test whose `group::name` contains `filter` (all when `None`)
/// against `target` as [`RunOptions::default`]'s frontend.
pub fn run_all<T: ConformanceTarget>(target: &T, filter: Option<&str>) -> Report {
    run(
        target,
        &RunOptions {
            filter: filter.map(str::to_string),
            ..RunOptions::default()
        },
    )
}

/// [`run_all`] with options.
pub fn run<T: ConformanceTarget>(target: &T, opts: &RunOptions) -> Report {
    let mut report = Report {
        target: ConformanceTarget::name(target),
        caps: opts.caps.caps(),
        seed: opts.seed,
        tests: Vec::new(),
    };
    for test in TESTS {
        let id = test.id();
        if let Some(f) = &opts.filter {
            if !id.contains(f.as_str()) {
                continue;
            }
        }
        let started = Instant::now();
        let outcome = run_one(target, test, opts);
        report.tests.push(TestReport {
            name: test.name,
            group: test.group,
            outcome,
            seconds: started.elapsed().as_secs_f64(),
        });
    }
    report
}

fn run_one<T: ConformanceTarget>(target: &T, test: &ConformanceTest, opts: &RunOptions) -> Outcome {
    let missing: Vec<&str> = test
        .caps
        .iter()
        .filter(|c| !opts.caps.has(**c))
        .map(|c| c.name())
        .collect();
    if !missing.is_empty() {
        return Outcome::Skipped(format!("requires capability {}", missing.join(", ")));
    }
    let env = Env {
        target: target as &dyn DynTarget,
        caps: &opts.caps,
        seed: opts.seed ^ fnv(&test.id()),
    };
    match catch_unwind(AssertUnwindSafe(|| (test.run)(&env))) {
        Ok(Ok(())) => Outcome::Passed,
        Ok(Err(TestErr::Skip(reason))) => Outcome::Skipped(reason),
        Ok(Err(TestErr::Fail(msg))) => Outcome::Failed(msg),
        Err(payload) => Outcome::Failed(panic_message(payload)),
    }
}

#[cfg(test)]
mod tests {
    use super::reference::RefTarget;
    use super::*;
    use crate::caps::{CasePolicy, OpenUnlinked, PushInval, XattrSupport};
    use crate::OpKindSet;

    /// A frontend that can do very little: the refusals are the contract.
    fn bare_caps() -> FrontendCaps {
        FrontendCaps {
            push_inval: PushInval::None,
            per_close_flush: false,
            cluster_locks: false,
            xattrs: XattrSupport::None,
            virtual_xattrs_listed: false,
            hard_links: false,
            fallocate: false,
            seek_hole: false,
            special_files: false,
            case: CasePolicy::Sensitive,
            max_io: 1 << 20,
            deferrable: OpKindSet::EMPTY,
            open_unlinked: OpenUnlinked::SillyRename,
            abortable: false,
        }
    }

    fn run_as(caps: FrontendCaps) -> Report {
        let report = run(
            &RefTarget,
            &RunOptions {
                caps,
                ..RunOptions::default()
            },
        );
        println!("{}", report.render());
        report
    }

    fn write_results_if_asked(report: &Report, lane: &str) {
        // The parity checker's input, for a CI job that wants it.
        if let Some(dir) = std::env::var_os("CONSTELLATION_CONFORMANCE_RESULTS") {
            let path = std::path::Path::new(&dir).join(format!("conformance-{lane}.json"));
            std::fs::write(path, report.to_json()).expect("write the results file");
        }
    }

    #[test]
    fn the_reference_target_passes_as_linux_fuse_with_cluster_locks() {
        let report = run_as(FrontendCaps::linux_fuse(true));
        write_results_if_asked(&report, "reference-linux-fuse");
        report.assert_ok();
        // Everything applicable ran: the only skips are the ones that name
        // a capability the frontend lacks (none, here).
        assert!(
            report.skipped().is_empty(),
            "skipped on a frontend with every capability: {:?}",
            report
                .skipped()
                .iter()
                .map(|t| (t.id(), &t.outcome))
                .collect::<Vec<_>>()
        );
        assert_eq!(report.passed().len(), TESTS.len());
    }

    #[test]
    fn the_reference_target_passes_without_cluster_locks() {
        let report = run_as(FrontendCaps::linux_fuse(false));
        report.assert_ok();
        for t in report.skipped() {
            match &t.outcome {
                Outcome::Skipped(why) => assert!(
                    why.contains("ClusterLocks"),
                    "{} skipped for another reason: {why}",
                    t.id()
                ),
                _ => unreachable!(),
            }
        }
        // The lock ops are still refused, deliberately.
        assert_eq!(
            report.outcome("deferral::locks_are_refused_without_the_capability"),
            Some(&Outcome::Passed)
        );
    }

    #[test]
    fn the_reference_target_passes_as_a_frontend_that_can_do_very_little() {
        let report = run_as(bare_caps());
        write_results_if_asked(&report, "reference-bare");
        report.assert_ok();
        // Skips name their capability as a whole word, as tests/parity.py
        // expects.
        for (id, cap) in [
            ("namespace::hard_links_count_names", "HardLinks"),
            ("namespace::special_files", "SpecialFiles"),
            ("xattr::set_get_list_remove", "Xattrs"),
            ("io::fallocate_modes", "Fallocate"),
            ("io::seek_data_and_hole", "SeekHole"),
            (
                "invalidation::remote_create_invalidates_the_name",
                "PushInval",
            ),
            (
                "cancellation::cancelled_during_the_wait_is_interrupted",
                "ClusterLocks",
            ),
            (
                "namespace::unlinked_open_file_stays_usable",
                "KeepOpenUnlinked",
            ),
        ] {
            match report.outcome(id) {
                Some(Outcome::Skipped(why)) => {
                    assert_eq!(why, &format!("requires capability {cap}"), "{id}");
                }
                other => panic!("{id}: expected a skip on {cap}, got {other:?}"),
            }
        }
        // The refusals a frontend without a capability is owed still run.
        for id in [
            "io::fallocate_absent_is_not_supported",
            "io::seek_absent_is_not_supported",
            "deferral::locks_are_refused_without_the_capability",
            "deferral::non_deferrable_waits_park_the_calling_thread",
        ] {
            assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}");
        }
    }

    #[test]
    fn a_failing_test_is_reported_not_propagated() {
        // A target whose filesystem loses every write.
        struct Lossy;
        impl ConformanceTarget for Lossy {
            type V = crate::mock::MockVfs;
            fn fresh(&self, caps: &FrontendCaps, opts: TargetOpts) -> Fixture<Self::V> {
                let fx = RefTarget.fresh(caps, opts);
                fx.vfs.always_write(crate::mock::Script::ok(0));
                fx
            }
        }
        let report = run_all(&Lossy, Some("io::write_read_roundtrip"));
        assert_eq!(report.tests.len(), 1);
        assert!(
            matches!(report.tests[0].outcome, Outcome::Failed(_)),
            "{}",
            report.render()
        );
        assert!(!report.is_ok());
        let json = report.to_json();
        assert!(
            json.contains("\"outcome\":\"failed\"")
                && json.contains("conformance/io::write_read_roundtrip")
        );
        assert!(report.render().contains("FAIL"));
    }

    #[test]
    fn filters_select_by_group_and_name() {
        let report = run_all(&RefTarget, Some("confinement::"));
        assert!(!report.tests.is_empty());
        assert!(report.tests.iter().all(|t| t.group == Group::Confinement));
        let one = run_all(&RefTarget, Some("readdir::plus_lists"));
        assert_eq!(one.tests.len(), 1);
        assert_eq!(
            one.outcome("readdir::plus_lists_the_same_entries"),
            Some(&Outcome::Passed)
        );
    }

    #[test]
    fn the_table_is_consistent() {
        let mut ids = HashSet::new();
        for t in TESTS {
            assert!(ids.insert(t.id()), "duplicate test {}", t.id());
        }
        // Every group has tests.
        for g in Group::ALL {
            assert!(TESTS.iter().any(|t| t.group == *g), "{}", g.name());
        }
        // The confinement group of plan 31 §6.12 is all there.
        let confinement: Vec<_> = TESTS
            .iter()
            .filter(|t| t.group == Group::Confinement)
            .map(|t| t.name)
            .collect();
        for needed in [
            "dotdot_at_the_view_root_is_the_root",
            "inodes_outside_the_subtree_are_refused",
            "constellation_snapshots_stay_inside_the_subtree",
            "confine_links_refuses_links_across_domains",
            "links_are_free_without_confine_links",
        ] {
            assert!(confinement.contains(&needed), "{needed}");
        }
    }

    #[test]
    fn the_same_seed_runs_the_same_workload() {
        let a = Rng::new(7).bytes(64);
        let b = Rng::new(7).bytes(64);
        assert_eq!(a, b);
        assert_ne!(a, Rng::new(8).bytes(64));
        let mut r = Rng::new(1);
        let mut f1 = r.fork(3);
        let mut r2 = Rng::new(1);
        let mut f2 = r2.fork(3);
        assert_eq!(f1.next_u64(), f2.next_u64());
    }
}
