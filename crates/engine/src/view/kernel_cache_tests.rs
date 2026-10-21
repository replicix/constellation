//! Plan 42 K2: the cache stamp an open answers and the events that tell
//! the kernels what changed under their pages without changing it
//! (`kernel_cache`'s module doc). Each test of a condition was checked to
//! fail with the condition removed from `View::open_stamp` or
//! `View::retire_writes` (noted per test).

use super::*;
use crate::kernel_inval::KernelInvalidator;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{
    Blocking, CacheStamp, Fh, FrontendEvents, Invalidation, OpCtx, OpKind, OpenOwner, Opened, Vfs,
};
use std::sync::Mutex as StdMutex;

/// A frontend sink that records what it is told.
#[derive(Clone, Default)]
struct Sink(Arc<StdMutex<Vec<String>>>);

impl FrontendEvents for Sink {
    fn invalidate(&self, batch: &[Invalidation]) {
        for inv in batch {
            self.0.lock().unwrap().push(format!("{inv:?}"));
        }
    }
}

impl Sink {
    fn data_of(&self, ino: Ino) -> usize {
        let want = format!("Data {{ ino: {ino}, range: None }}");
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|l| **l == want)
            .count()
    }
}

struct Fixture {
    view: View,
    k: Option<KernelInvalidator>,
    _meta: Arc<Meta>,
    _dir: tempfile::TempDir,
}

/// A view whose ops register with a running invalidator (`live`), as a
/// mounted one does; its engine number is 3. Sinks registered as views 3
/// (this one) and 4 (another view of the node).
fn fixture(live: bool) -> (Fixture, Sink, Sink) {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut view, dir) = super::quota_tests::test_fs(meta.clone());
    let (this, other) = (Sink::default(), Sink::default());
    let k = live.then(|| {
        let k = KernelInvalidator::start();
        k.register(3, Arc::new(this.clone()), ROOT_INO);
        k.register(4, Arc::new(other.clone()), ROOT_INO);
        view.inflight = k.inflight();
        k
    });
    view.id = 3;
    (
        Fixture {
            view,
            k,
            _meta: meta,
            _dir: dir,
        },
        this,
        other,
    )
}

impl Fixture {
    fn cx(&self, kind: OpKind) -> (Caller, OpKind) {
        (Caller::new(0, 0, None), kind)
    }

    fn open(&self, ino: Ino) -> Opened {
        let (caller, kind) = self.cx(OpKind::Open);
        let opened = Blocking::run(|r| {
            self.view.open(
                &OpCtx::new(kind, &caller),
                ino,
                OpenFlags::READ,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        self.release(ino, opened.fh);
        opened
    }

    fn release(&self, ino: Ino, fh: Fh) {
        let (caller, kind) = self.cx(OpKind::Release);
        Blocking::run(|r| {
            self.view.release(
                &OpCtx::new(kind, &caller),
                ino,
                fh,
                OpenFlags::READ,
                None,
                r,
            )
        })
        .unwrap();
    }

    fn file(&self, name: &str) -> Ino {
        self.view
            .meta
            .create(ROOT_INO, name, 0o644, 0, 0)
            .unwrap()
            .ino
    }

    fn settle(&self) {
        if let Some(k) = &self.k {
            assert!(k.wait_idle(Duration::from_secs(10)));
        }
    }
}

/// Unchanged content answers the same stamp at every open; a commit of
/// a write session answers another.
#[test]
fn an_open_answers_the_same_stamp_until_the_content_changes() {
    let (f, _, _) = fixture(true);
    let ino = f.file("a");
    let s1 = f.open(ino).cache;
    assert!(
        s1.is_some(),
        "a regular file, invalidation on, nothing pending"
    );
    assert_eq!(f.open(ino).cache, s1);
    f.view.do_write(ino, 0, b"new bytes").unwrap();
    f.view.flush_inode(ino, false).unwrap();
    f.settle();
    let s2 = f.open(ino).cache;
    assert!(s2.is_some());
    assert_ne!(s2, s1, "the commit changed the row");
}

/// Kernel invalidation off: nothing would ever tell the kernel of another
/// node's change, so no stamp. (Fails with the `enabled()` check removed.)
#[test]
fn no_invalidator_answers_no_stamp() {
    let (f, _, _) = fixture(false);
    let ino = f.file("a");
    assert_eq!(f.open(ino).cache, None);
}

/// A data notification on its way (here: held back for a request in
/// flight on the file) answers no stamp; once delivered, the stamp is
/// back. (Fails with the `data_pending` check removed.)
#[test]
fn a_pending_data_notification_answers_no_stamp() {
    let (f, _, _) = fixture(true);
    let ino = f.file("a");
    let guard = f.view.inflight.enter(&[ino]);
    f.k.as_ref().unwrap().inodes().invalidate(ino);
    assert_eq!(f.open(ino).cache, None, "held back");
    drop(guard);
    f.settle();
    assert!(f.open(ino).cache.is_some(), "delivered");
}

/// `--cto strict` answers no stamp, the lone sequencer included: every
/// strict open drops the pages. (Fails with the strict check removed.)
#[test]
fn strict_answers_no_stamp() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let ino = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap().ino;
    let (mut fs, _dir, _core) = super::durable_ack_tests::holder_fs(meta.clone(), false);
    let k = KernelInvalidator::start();
    fs.inflight = k.inflight();
    let stamp = meta.content_stamp(ino).unwrap();
    assert!(fs.open_stamp(ino, stamp).is_some(), "bounded");
    fs.sync.as_mut().unwrap().cto_strict = true;
    assert_eq!(fs.open_stamp(ino, stamp), None, "strict");
    assert_eq!(fs.frozen_stamp(ino), None, "strict, frozen too");
}

/// A frozen file's stamp is a constant of its node id.
#[test]
fn a_frozen_stamp_is_a_constant_per_node() {
    let (f, _, _) = fixture(true);
    let a = f.view.frozen_stamp(1 << 62);
    assert!(a.is_some());
    assert_eq!(a, f.view.frozen_stamp(1 << 62));
    assert_ne!(a, f.view.frozen_stamp((1 << 62) + 1));
    let _: Option<CacheStamp> = a;
}

/// §5.1.5: a session dropped without a commit tells every view's kernel
/// (it holds bytes the view will never answer), whatever the lock mode —
/// this view has no cluster locks. (Fails with the `drop_writes`
/// invalidation removed.)
#[test]
fn a_session_dropped_without_a_commit_invalidates_every_view() {
    let (f, this, other) = fixture(true);
    let ino = f.file("a");
    f.view.do_write(ino, 0, b"never published").unwrap();
    assert!(f.view.drop_writes(ino));
    f.settle();
    assert_eq!(this.data_of(ino), 1, "{:?}", this.0.lock().unwrap());
    assert_eq!(other.data_of(ino), 1);
}

/// Each retirement says what it was: `Uncommitted` invalidates every view,
/// `Committed` every other view, `NothingChanged` none. (Each arm checked
/// by swapping it with another.)
#[test]
fn each_retire_reason_tells_the_kernels_what_changed() {
    let (f, this, other) = fixture(true);
    let ino = f.file("a");
    f.view.retire_writes(ino, Retired::NothingChanged);
    f.settle();
    assert_eq!((this.data_of(ino), other.data_of(ino)), (0, 0));
    f.view.retire_writes(ino, Retired::Committed);
    f.settle();
    assert_eq!((this.data_of(ino), other.data_of(ino)), (0, 1));
    f.view.retire_writes(ino, Retired::Uncommitted);
    f.settle();
    assert_eq!((this.data_of(ino), other.data_of(ino)), (1, 2));
}

/// A commit through this view is told to the node's other views, and
/// not to its own kernel, which holds what it wrote.
#[test]
fn a_commit_through_one_view_invalidates_the_others() {
    let (f, this, other) = fixture(true);
    let ino = f.file("a");
    f.view.do_write(ino, 0, b"published").unwrap();
    f.view.flush_inode(ino, false).unwrap();
    f.settle();
    assert_eq!(other.data_of(ino), 1);
    assert_eq!(this.data_of(ino), 0);
    // A close with nothing written publishes nothing and tells nobody.
    f.view.flush_inode(ino, false).unwrap();
    f.settle();
    assert_eq!(other.data_of(ino), 1);
}
