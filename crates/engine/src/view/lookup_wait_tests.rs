//! Plan 30 §M6's key table says a `lookup(parent, name)` waits on
//! `Dentry(parent, name)`, then `Ino(child)` for the attributes it
//! returns, exactly as `getattr` waits on `Ino(ino)`. The code waited
//! only on the dentry, so a lookup whose dentry was covered by this
//! node's own speculation (the shadow of a rename or an exchange, which
//! covers the entries and their parents, never the inode a name leads
//! to; an `Exists` hint, by contrast, carries the child's ino) returned the
//! child's attributes from a replica that had not yet applied what this
//! node had observed — a `stat` by path went back in time where an
//! `fstat` would not have. `.` and `..` returned their target's
//! attributes with no wait at all.
//!
//! The fixture is one view over a replica with a recorded observation
//! the replica has not applied (`Meta::session().raise_observed`, the
//! call a forwarded reply makes) and speculation covering the entry but
//! not the inode. The holder's commit that the observation names — a
//! `Setattr` of the inode, as a forwarded `chmod` is — reaches the
//! replica later, through `apply_segment`, as the log delivers it. A
//! full two-node construction (a holder and a non-owner, as
//! `durable_ack_tests` scripts one) would test the same wait through
//! more machinery: the view's side of the channel to the core is
//! scripted there, so the reply handling that raises the watermark and
//! installs the shadow would have to be re-enacted by hand anyway.
//!
//! Synchronisation: the lookup runs on its own thread and the test
//! gives it [`HELD_BACK`] to answer while the commit is unapplied. A
//! lookup that waits does not answer in that time, and releasing it
//! afterwards is correct however far it had got; one that does not
//! wait answers at once, and the test fails there. Nothing is released
//! on a timer: the apply is always the test's own step.

use super::durable_ack_tests::holder_fs;
use super::*;
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{KeySet, LogRecord, Position, TouchSet};
use constellation_vfs::{Blocking, Caller, Entry, Name, OpCtx, OpKind, Vfs};

/// How long a lookup that must wait is given to (wrongly) answer.
const HELD_BACK: Duration = Duration::from_millis(500);

/// Patience for the answer once the commit is applied (never consumed).
const PATIENCE: Duration = Duration::from_secs(10);

/// A node whose replica holds `file` in the root and a directory
/// `dir`, mode `0o644` and `0o755`, and has observed a position (log
/// sequence 1) it has not applied. Speculation covers the dentry of
/// `file` at that position, nothing else.
fn lagging_node() -> (
    Arc<Meta>,
    View,
    tempfile::TempDir,
    tokio::sync::mpsc::UnboundedReceiver<SyncRequest>,
    Ino,
    Ino,
) {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap();
    let dir = meta.mkdir(ROOT_INO, "dir", 0o755, 0, 0).unwrap();
    let (mut fs, tmp, core) = holder_fs(meta.clone(), false);
    // Not the holder: this node's mutations are forwarded.
    fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
    meta.session().set_budget_ms(PATIENCE.as_millis() as u64);
    let observed = Position {
        seq: 1,
        ..Position::ZERO
    };
    meta.session().raise_observed(observed);
    meta.session().note_covering(
        KeySet {
            dentries: vec![(ROOT_INO, "file".to_string())],
            inos: Vec::new(),
        },
        observed,
    );
    (meta, fs, tmp, core, file.ino, dir.ino)
}

/// The holder's commit: `ino` is changed to `mode`.
fn chmod(ino: Ino, mode: u32) -> LogRecord {
    LogRecord::Setattr {
        ino,
        mode: Some(mode),
        uid: None,
        gid: None,
        size: None,
        atime_ns: None,
        mtime_ns: None,
        time_ns: replicix_objcas::lease::now_unix_ms() as i64 * 1_000_000,
    }
}

/// `lookup(parent, name)` on `fs` while `meta`'s replica has not applied
/// the log segment carrying `commit`: the lookup must not answer before
/// the segment is applied, and the entry it answers afterwards is
/// returned.
fn lookup_across_apply(
    fs: &View,
    meta: &Meta,
    parent: Ino,
    name: &str,
    commit: LogRecord,
) -> Entry {
    let caller = Caller::root();
    std::thread::scope(|scope| {
        let (tx, rx) = std::sync::mpsc::channel();
        scope.spawn(move || {
            let entry = Blocking::run(|r| {
                fs.lookup(
                    &OpCtx::new(OpKind::Lookup, &caller),
                    parent,
                    Name::new(name),
                    r,
                )
            });
            let _ = tx.send(entry);
        });
        match rx.recv_timeout(HELD_BACK) {
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Ok(early) => panic!(
                "lookup({parent}, {name:?}) answered before the replica applied the \
                 position this node had observed: {:?}",
                early.map(|e| (e.attr.ino, e.attr.mode))
            ),
            Err(e) => panic!("the lookup thread died: {e}"),
        }
        meta.apply_segment(1, 1, &[commit], &TouchSet::default())
            .unwrap();
        rx.recv_timeout(PATIENCE)
            .expect("the lookup was not released by the apply")
            .unwrap()
    })
}

/// A name's lookup returns the child's attributes only once the replica
/// has applied what this node observed for the child's inode — the
/// dentry being covered does not make the inode current.
#[test]
fn a_lookup_waits_for_the_childs_inode_not_only_its_dentry() {
    let (meta, fs, _tmp, _core, file, _dir) = lagging_node();
    let before = meta.getattr(file).unwrap().unwrap().mode & 0o7777;
    assert_eq!(before, 0o644);
    let entry = lookup_across_apply(&fs, &meta, ROOT_INO, "file", chmod(file, 0o600));
    assert_eq!(entry.attr.ino, file);
    assert_eq!(
        entry.attr.mode & 0o7777,
        0o600,
        "the entry carries the attributes the observed commit made"
    );
}

/// `..` returns the parent directory's attributes through the same
/// wait (before, the `.`/`..` branch read its target's row with no wait
/// at all).
#[test]
fn a_dotdot_lookup_waits_for_the_parents_inode() {
    let (meta, fs, _tmp, _core, _file, dir) = lagging_node();
    let before = meta.getattr(ROOT_INO).unwrap().unwrap().mode & 0o7777;
    assert_ne!(before, 0o700);
    let entry = lookup_across_apply(&fs, &meta, dir, "..", chmod(ROOT_INO, 0o700));
    assert_eq!(entry.attr.ino, ROOT_INO);
    assert_eq!(
        entry.attr.mode & 0o7777,
        0o700,
        "the entry carries the attributes the observed commit made"
    );
}

/// `.` is the directory itself: the same wait, on its own inode.
#[test]
fn a_dot_lookup_waits_for_the_directorys_inode() {
    let (meta, fs, _tmp, _core, _file, dir) = lagging_node();
    let entry = lookup_across_apply(&fs, &meta, dir, ".", chmod(dir, 0o700));
    assert_eq!(entry.attr.ino, dir);
    assert_eq!(
        entry.attr.mode & 0o7777,
        0o700,
        "the entry carries the attributes the observed commit made"
    );
}

// ---- Plan 40 P7: a plus entry waits as `getattr` of the child does -------

use super::plus::testing::{guarded, plus_page, NaiveGuard};
use super::plus::Naive;
use constellation_vfs::DirEntry;

/// A plus page of `dir` from `cookie` on `fs`, while something is owed
/// that `release` settles: the page must not answer before it, and the
/// entries it answers afterwards are returned. (`naive`: the page's
/// thread runs that naive step.)
fn plus_page_held_until(
    fs: &View,
    dir: Ino,
    cookie: u64,
    naive: Option<Naive>,
    release: impl FnOnce(),
) -> Result<Vec<DirEntry>, String> {
    std::thread::scope(|scope| {
        let (tx, rx) = std::sync::mpsc::channel();
        scope.spawn(move || {
            let _naive = naive.map(NaiveGuard::set);
            let _ = tx.send(plus_page(fs, dir, cookie, 100));
        });
        match rx.recv_timeout(HELD_BACK) {
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Ok(early) => {
                return Err(format!(
                    "the plus page answered before what the children are owed was applied: {:?}",
                    early.map(|page| page.len())
                ))
            }
            Err(e) => panic!("the listing thread died: {e}"),
        }
        release();
        Ok(rx
            .recv_timeout(PATIENCE)
            .expect("the plus page was not released")
            .unwrap())
    })
}

fn mode_of(page: &[DirEntry], name: &str) -> u32 {
    let entry = page
        .iter()
        .find(|e| e.name.as_bytes() == name.as_bytes())
        .and_then(|e| e.entry.as_ref())
        .unwrap_or_else(|| panic!("{name:?} has no entry in {page:?}"));
    entry.attr.mode & 0o7777
}

/// Another node's `chmod` that this node observed (and acknowledged to
/// its client) is in the next plus page once the replica applied it, as it
/// is in a `lookup` or a `getattr`. (From cookie 2, past the listing's own
/// `Dir` wait: the children's keys alone hold the page.)
#[test]
fn a_remote_chmod_is_in_the_next_plus_page_once_applied() {
    guarded(Naive::NoChildWait, |naive| {
        let (meta, fs, _tmp, _core, file, _dir) = lagging_node();
        let page = plus_page_held_until(&fs, ROOT_INO, 2, naive, || {
            meta.apply_segment(1, 1, &[chmod(file, 0o600)], &TouchSet::default())
                .unwrap();
        })?;
        match mode_of(&page, "file") {
            0o600 => Ok(()),
            mode => Err(format!("the entry says {mode:o}, the commit made 600")),
        }
    });
}

/// A queued replay (a stranded op rolled back until it lands) that touches
/// only the child's inode holds the plus page as it holds `getattr`: the
/// listing's `Dir(dir)` wait does not match it. (Fails with a
/// `Dir(dir)`-only wait.)
#[test]
fn a_queued_replay_touching_only_the_child_holds_the_plus_page() {
    guarded(Naive::NoChildWait, |naive| {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
        meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap();
        let (fs, _tmp, _core) = holder_fs(meta.clone(), false);
        meta.session().set_budget_ms(PATIENCE.as_millis() as u64);
        meta.queue_replay(
            constellation_meta::Rid {
                node: 1,
                incarnation: 1,
                seq: 1,
            },
            &constellation_meta::MutateOp::Setattr {
                ino: file,
                mode: Some(0o600),
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns: None,
            },
            &Default::default(),
        )
        .unwrap();
        let queued = meta.pending_replays().unwrap()[0].queue_seq;
        let page = plus_page_held_until(&fs, ROOT_INO, 0, naive, || {
            meta.forget_replay(queued).unwrap();
        })?;
        let names: Vec<_> = page.iter().map(|e| e.name.as_bytes().to_vec()).collect();
        assert!(names.contains(&b"file".to_vec()), "{names:?}");
        Ok(())
    });
}

/// A queued replay of this node's own unlink of a name (a stranded op,
/// rolled back until it lands) touches the name and its directory, never
/// the inode the name led to: a plus page past the listing's `Dir` wait
/// (cookie 2) holds as a `lookup` of the name holds on `Dentry`, instead
/// of handing the kernel an entry for a name this node's client already
/// removed. (Fails with only the children's `Ino` keys.)
#[test]
fn a_queued_replay_of_an_unlink_holds_a_later_plus_page() {
    guarded(Naive::NoDentryWait, |naive| {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap();
        meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap();
        let (fs, _tmp, _core) = holder_fs(meta.clone(), false);
        meta.session().set_budget_ms(PATIENCE.as_millis() as u64);
        meta.queue_replay(
            constellation_meta::Rid {
                node: 1,
                incarnation: 1,
                seq: 1,
            },
            &constellation_meta::MutateOp::Unlink {
                parent: ROOT_INO,
                name: "file".into(),
            },
            &Default::default(),
        )
        .unwrap();
        let queued = meta.pending_replays().unwrap()[0].queue_seq;
        let page = plus_page_held_until(&fs, ROOT_INO, 2, naive, || {
            meta.forget_replay(queued).unwrap();
        })?;
        let names: Vec<_> = page.iter().map(|e| e.name.as_bytes().to_vec()).collect();
        assert!(names.contains(&b"other".to_vec()), "{names:?}");
        Ok(())
    });
}

/// ADR-57: a lookup of a name a pending acknowledged row touches (a gated
/// row this node rolled back, the log lacking it) does not answer without
/// the row, whatever the session budget: after the read hold span it fails
/// with `EIO`, and a lookup of another name answers at once.
#[test]
fn a_lookup_of_a_held_name_fails_with_eio_after_the_span() {
    use constellation_meta::{execute_mutate, MutateOp, PendingClass, RowCommit};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap();
    meta.set_holder_epoch(1);
    {
        let _class = PendingClass::set(RowCommit::ROOT_GROUP);
        execute_mutate(
            &meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "held".into(),
                ino: (1 << 40) | 1,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(constellation_meta::Rid {
                node: 2,
                incarnation: 1,
                seq: 1,
            }),
        )
        .unwrap();
    }
    meta.set_holder_epoch(0);
    assert_eq!(meta.strand_below_epoch(2).unwrap().member_copied, 1);
    assert!(meta.lookup(ROOT_INO, "held").unwrap().is_none());
    let (mut fs, _tmp, _core) = holder_fs(meta.clone(), false);
    fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
    // A budget of 0 disables the session wait; the hold does not care.
    meta.session().set_budget_ms(0);
    let span = Duration::from_millis(300);
    meta.set_read_hold_span(span);
    let caller = Caller::root();
    let lookup = |name: &str| {
        Blocking::run(|r| {
            fs.lookup(
                &OpCtx::new(OpKind::Lookup, &caller),
                ROOT_INO,
                Name::new(name),
                r,
            )
        })
    };
    let t = std::time::Instant::now();
    assert!(lookup("other").is_ok());
    assert!(t.elapsed() < span, "an unrelated name does not wait");
    let t = std::time::Instant::now();
    let held = lookup("held");
    assert!(t.elapsed() >= span, "waited the span: {:?}", t.elapsed());
    assert_eq!(
        held.err().map(|e| e.code()),
        Some(constellation_types::Code::Io),
        "refused, not answered absent"
    );
    assert_eq!(meta.read_hold_stats().refused, 1);
}

/// A node whose root holds `other` and a held name `held` (a gated row this
/// node rolled back, the log lacking it), the read hold span `span`.
fn node_with_a_held_name(span: Duration) -> (Arc<Meta>, View, tempfile::TempDir) {
    use constellation_meta::{execute_mutate, MutateOp, PendingClass, RowCommit};
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap();
    meta.set_holder_epoch(1);
    {
        let _class = PendingClass::set(RowCommit::ROOT_GROUP);
        execute_mutate(
            &meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "held".into(),
                ino: (1 << 40) | 1,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(constellation_meta::Rid {
                node: 2,
                incarnation: 1,
                seq: 1,
            }),
        )
        .unwrap();
    }
    meta.set_holder_epoch(0);
    assert_eq!(meta.strand_below_epoch(2).unwrap().member_copied, 1);
    let (mut fs, tmp, _core) = holder_fs(meta.clone(), false);
    fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
    meta.session().set_budget_ms(0);
    meta.set_read_hold_span(span);
    (meta, fs, tmp)
}

/// ADR-57 review SHOULD-7: a lookup held for a row ends with `EINTR` when
/// its caller is interrupted, not after the read hold span, and the dispatch
/// thread is free again.
#[test]
fn a_held_lookup_ends_with_the_callers_interrupt() {
    let (meta, fs, _tmp) = node_with_a_held_name(Duration::from_secs(10));
    let caller = Caller::root();
    let cancel = constellation_vfs::CancelToken::new();
    let t = std::time::Instant::now();
    let got = std::thread::scope(|sc| {
        let c = cancel.clone();
        sc.spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            c.cancel();
        });
        Blocking::run(|r| {
            let mut cx = OpCtx::new(OpKind::Lookup, &caller);
            cx.cancel = Some(&cancel);
            fs.lookup(&cx, ROOT_INO, Name::new("held"), r)
        })
    });
    assert_eq!(
        got.err().map(|e| e.code()),
        Some(constellation_types::Code::Intr)
    );
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert!(
        meta.member_copy_touches(&[constellation_meta::ReadKey::Dentry(ROOT_INO, "held".into())])
    );
}

/// ADR-57 review SHOULD-5: a continuation page of a listing (`cookie != 0`)
/// of a directory a held row touches waits for the row and fails with `EIO`
/// after the span, as the first page does.
#[test]
fn a_continuation_page_of_a_held_directory_fails_with_eio_after_the_span() {
    let span = Duration::from_millis(300);
    let (_meta, fs, _tmp) = node_with_a_held_name(span);
    let caller = Caller::root();
    let t = std::time::Instant::now();
    let (sink, wait) = constellation_vfs::CollectDir::pair(100);
    fs.readdir(
        &OpCtx::new(OpKind::Readdir, &caller),
        ROOT_INO,
        constellation_vfs::Fh(0),
        1,
        false,
        sink,
    );
    let got = wait.wait();
    assert!(t.elapsed() >= span, "waited the span: {:?}", t.elapsed());
    assert_eq!(
        got.err().map(|e| e.code()),
        Some(constellation_types::Code::Io)
    );
}

/// ADR-57 review SHOULD-4: `copy_file_range` writes what a read of its source
/// answers as a new acknowledged write, so a source a pending acknowledged row
/// touches is waited for, and the copy fails with `EIO` after the span without
/// having written anything; a copy from another source goes through.
#[test]
fn a_copy_from_a_held_source_fails_with_eio_after_the_span_and_writes_nothing() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let held = meta.create(ROOT_INO, "held", 0o644, 0, 0).unwrap().ino;
    let free = meta.create(ROOT_INO, "free", 0o644, 0, 0).unwrap().ino;
    let dst = meta.create(ROOT_INO, "dst", 0o644, 0, 0).unwrap().ino;
    let (mut fs, _tmp, _core) = holder_fs(meta.clone(), false);
    fs.sync.as_mut().unwrap().lease = Arc::new(crate::lease::LeaseView::default());
    meta.session().set_budget_ms(0);
    let span = Duration::from_millis(300);
    meta.set_read_hold_span(span);
    let caller = Caller::root();
    let open = |ino: Ino, flags: OpenFlags| {
        Blocking::run(|r| {
            fs.open(
                &OpCtx::new(OpKind::Open, &caller),
                ino,
                flags,
                constellation_vfs::OpenOwner::NONE,
                r,
            )
        })
        .unwrap()
        .fh
    };
    let (held_fh, free_fh, dst_fh) = (
        open(held, OpenFlags::READ),
        open(free, OpenFlags::READ),
        open(dst, OpenFlags::WRITE),
    );
    // The hold comes after the opens (an open of a held inode waits too).
    meta.test_hold_ino_committed(5, 1, held).unwrap();
    let copy = |src: Ino, fh_in: constellation_vfs::Fh| {
        Blocking::run(|r| {
            fs.copy_file_range(
                &OpCtx::new(OpKind::CopyFileRange, &caller),
                src,
                fh_in,
                0,
                dst,
                dst_fh,
                0,
                16,
                constellation_vfs::CopyFlags::empty(),
                r,
            )
        })
    };
    let t = std::time::Instant::now();
    assert!(copy(free, free_fh).is_ok());
    assert!(t.elapsed() < span, "an unheld source does not wait");
    let t = std::time::Instant::now();
    let got = copy(held, held_fh);
    assert!(t.elapsed() >= span, "waited the span: {:?}", t.elapsed());
    assert_eq!(
        got.err().map(|e| e.code()),
        Some(constellation_types::Code::Io),
        "refused, not copied from the state that lacks the row"
    );
    assert_eq!(meta.read_hold_stats().refused, 1);
}
