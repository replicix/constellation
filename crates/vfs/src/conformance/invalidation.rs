//! `invalidation`: what the engine tells a frontend's cache when another
//! view changes the tree ([`crate::FrontendEvents`], plan 31 §6.5).
//!
//! The fixture pairs a view whose events are recorded with a *second view*
//! whose mutations reach it the way another node's would (a target that
//! cannot produce that leaves the hooks out and these tests skip). The
//! assertions are deliberately lenient about *how* a change is told — a
//! name change is an `Entry` or a `Deleted`, a page change a `Data` — and
//! strict that it *is* told, in the recording view's own inode numbering.

use super::{must, skip, Env, Fx, RecordingEvents, TestResult};
use crate::events::Invalidation;
use crate::types::Ino;
use std::sync::Arc;

/// The two views and the recorder, with the recorder emptied.
fn pair(env: &Env<'_>) -> Result<(Fx, Fx, Arc<RecordingEvents>), super::TestErr> {
    let fx = env.fresh();
    let events = fx.events()?.clone();
    let other = fx.second_view()?;
    fx.settle();
    events.take();
    Ok((fx, other, events))
}

fn told_entry(events: &[Invalidation], parent: Ino, name: &str) -> bool {
    events.iter().any(|e| match e {
        Invalidation::Entry { parent: p, name: n } => {
            *p == parent && n.as_bytes() == name.as_bytes()
        }
        Invalidation::Deleted {
            parent: p, name: n, ..
        } => *p == parent && n.as_bytes() == name.as_bytes(),
        _ => false,
    })
}

fn told_data(events: &[Invalidation], ino: Ino) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Invalidation::Data { ino: i, .. } if *i == ino))
}

fn told_attr(events: &[Invalidation], ino: Ino) -> bool {
    events.iter().any(|e| {
        matches!(e, Invalidation::Attr { ino: i } | Invalidation::Data { ino: i, .. } if *i == ino)
    })
}

pub(super) fn remote_create_invalidates_the_name(env: &Env<'_>) -> TestResult {
    let (fx, other, events) = pair(env)?;
    let (a, b) = (fx.client(), other.client());
    let d = must("mkdir", b.mkdir(b.root(), "d")).attr.ino;
    fx.settle();
    events.take();
    // `a` has looked the name up (and cached the miss).
    let _ = a.lookup(d, "new");
    must("remote create", b.put(d, "new", b"x"));
    fx.settle();
    let told = events.all();
    assert!(
        told_entry(&told, d, "new"),
        "no name invalidation for a remote create: {told:?}"
    );
    Ok(())
}

pub(super) fn remote_unlink_and_rename_invalidate_names(env: &Env<'_>) -> TestResult {
    let (fx, other, events) = pair(env)?;
    let b = other.client();
    let d = must("mkdir", b.mkdir(b.root(), "d")).attr.ino;
    must("put f", b.put(d, "f", b"1"));
    must("put g", b.put(d, "g", b"2"));
    fx.settle();
    events.take();
    must("remote unlink", b.unlink(d, "f"));
    fx.settle();
    let told = events.take();
    assert!(
        told_entry(&told, d, "f"),
        "no name invalidation for a remote unlink: {told:?}"
    );
    must("remote rename", b.rename(d, "g", d, "h"));
    fx.settle();
    let told = events.take();
    assert!(
        told_entry(&told, d, "g"),
        "the old name was not invalidated: {told:?}"
    );
    assert!(
        told_entry(&told, d, "h"),
        "the new name was not invalidated: {told:?}"
    );
    Ok(())
}

pub(super) fn remote_write_invalidates_pages(env: &Env<'_>) -> TestResult {
    let (fx, other, events) = pair(env)?;
    let b = other.client();
    let f = must("put f", b.put(b.root(), "f", b"before")).attr.ino;
    fx.settle();
    events.take();
    let o = must("open", b.open_rw(f));
    must("remote write", b.write(f, o.fh, 0, b"AFTER!"));
    must("close", b.close(f, o.fh));
    fx.settle();
    let told = events.all();
    assert!(
        told_data(&told, f),
        "no page invalidation for a remote write: {told:?}"
    );
    Ok(())
}

pub(super) fn remote_setattr_invalidates_attributes(env: &Env<'_>) -> TestResult {
    let (fx, other, events) = pair(env)?;
    let b = other.client();
    let f = must("put f", b.put(b.root(), "f", b"before")).attr.ino;
    fx.settle();
    events.take();
    must(
        "remote chmod",
        b.setattr(
            f,
            None,
            &crate::SetAttr {
                mode: Some(0o600),
                ..crate::SetAttr::default()
            },
        ),
    );
    fx.settle();
    let told = events.take();
    assert!(
        told_attr(&told, f),
        "no attribute invalidation for a remote chmod: {told:?}"
    );
    must("remote truncate", b.truncate(f, None, 2));
    fx.settle();
    let told = events.take();
    assert!(
        told_attr(&told, f),
        "no invalidation for a remote truncate: {told:?}"
    );
    Ok(())
}

pub(super) fn events_come_from_one_dedicated_thread(env: &Env<'_>) -> TestResult {
    let (fx, other, events) = pair(env)?;
    let b = other.client();
    let d = must("mkdir", b.mkdir(b.root(), "d")).attr.ino;
    for i in 0..20 {
        must("put", b.put(d, &format!("f{i}"), b"x"));
    }
    fx.settle();
    assert!(events.batch_count() > 0, "nothing was delivered");
    let threads = events.threads();
    // Delivered from one dedicated notifier thread (never an op's thread,
    // never a lock holder's): the kernel_inval.rs lesson.
    assert_eq!(
        threads.len(),
        1,
        "deliveries came from {} threads",
        threads.len()
    );
    assert!(
        !threads.contains(&std::thread::current().id()),
        "an event was delivered on the thread that ran the op"
    );
    Ok(())
}

// ------------------------------------------------- plan 42: cache stamps

/// Skip unless the target models a kernel page cache
/// ([`super::Declared::cache_stamps`]).
fn needs_cache_stamps(fx: &Fx) -> TestResult {
    if !fx.declared.cache_stamps {
        skip!(
            "the target models no kernel page cache ({})",
            super::CACHE_STAMPS_GAP
        );
    }
    Ok(())
}

/// The stamp a read-only open of `f` answers (`Opened::cache`).
fn stamp_of(c: &super::Client, f: Ino) -> Option<crate::types::CacheStamp> {
    let o = must("open", c.open(f, crate::OpenFlags::READ));
    let stamp = o.cache;
    must("close", c.close(f, o.fh));
    stamp
}

/// Plan 42 §5.1.1: a target that tells a frontend's cache of other
/// nodes' changes answers an open with a cache stamp, the same one for
/// as long as the file's content is unchanged.
pub(super) fn open_answers_the_same_stamp_for_unchanged_content(env: &Env<'_>) -> TestResult {
    let (fx, _other, _events) = pair(env)?;
    needs_cache_stamps(&fx)?;
    let a = fx.client();
    let f = must("put f", a.put(a.root(), "f", b"content")).attr.ino;
    fx.settle();
    let s = stamp_of(&a, f);
    assert!(
        s.is_some(),
        "a target with push invalidation answers a stamp"
    );
    assert_eq!(stamp_of(&a, f), s, "unchanged content, another stamp");
    // Reads and attribute reads change nothing.
    let _ = must("getattr", a.getattr(f));
    let _ = must("slurp", a.slurp(f));
    assert_eq!(stamp_of(&a, f), s, "a read changed the stamp");
    Ok(())
}

/// Plan 42 §5.1.1: another node's commit changes the stamp this view's
/// next open answers, once it has been delivered (`settle`: the change
/// is applied and its notification is out, so nothing is pending).
pub(super) fn a_second_views_commit_changes_the_stamp(env: &Env<'_>) -> TestResult {
    let (fx, other, events) = pair(env)?;
    needs_cache_stamps(&fx)?;
    let (a, b) = (fx.client(), other.client());
    let f = must("put f", b.put(b.root(), "f", b"before")).attr.ino;
    fx.settle();
    let before = stamp_of(&a, f);
    assert!(before.is_some());
    events.take();
    let o = must("open", b.open_rw(f));
    must("remote write", b.write(f, o.fh, 0, b"AFTER!"));
    must("close", b.close(f, o.fh));
    fx.settle();
    let after = stamp_of(&a, f);
    assert!(after.is_some(), "delivered: nothing pending, a stamp again");
    assert_ne!(after, before, "the remote commit left the stamp");
    assert!(told_data(&events.all(), f));
    Ok(())
}

/// Plan 42 §5.1.5: a write session that ends without becoming the row —
/// here written through a descriptor of a file unlinked meanwhile, whose
/// last close drops it — is told to the frontend as a page invalidation:
/// its kernel holds bytes the view will never answer.
pub(super) fn a_session_dropped_without_commit_invalidates(env: &Env<'_>) -> TestResult {
    let (fx, _other, events) = pair(env)?;
    needs_cache_stamps(&fx)?;
    let a = fx.client();
    let f = must("put f", a.put(a.root(), "f", b"committed")).attr.ino;
    let o = must("open", a.open_rw(f));
    must("write", a.write(f, o.fh, 0, b"never published"));
    must("unlink", a.unlink(a.root(), "f"));
    fx.settle();
    events.take();
    must("close", a.close(f, o.fh));
    // A frontend that reports the kernel's `FORGET`s keeps the orphan,
    // session included, until its kernel lets go of it.
    a.forget(f);
    fx.settle();
    let told = events.all();
    assert!(
        told_data(&told, f),
        "the dropped session's pages were not invalidated: {told:?}"
    );
    Ok(())
}

/// Plan 42 §5.1.5 (Q3): a commit through another view of the same target
/// (a second mount of one node) is told to this view: no foreign apply
/// will report it, and this view's kernel may hold pages of the row it
/// replaced.
pub(super) fn a_commit_through_another_view_invalidates_this_one(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    needs_cache_stamps(&fx)?;
    let events = fx.events()?.clone();
    let sub = fx.subtree("", false)?;
    let (a, b) = (fx.client(), sub.client());
    let f = must("put f", a.put(a.root(), "f", b"before")).attr.ino;
    fx.settle();
    let before = stamp_of(&a, f);
    events.take();
    let fb = must("lookup through the other view", b.lookup(b.root(), "f"))
        .attr
        .ino;
    let o = must("open", b.open_rw(fb));
    must("write", b.write(fb, o.fh, 0, b"AFTER!"));
    must("close", b.close(fb, o.fh));
    fx.settle();
    let told = events.all();
    assert!(
        told_data(&told, f),
        "the other view's commit was not told: {told:?}"
    );
    assert_ne!(stamp_of(&a, f), before, "the commit left the stamp");
    Ok(())
}
