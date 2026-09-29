//! `invalidation`: what the engine tells a frontend's cache when another
//! view changes the tree ([`crate::FrontendEvents`], plan 31 §6.5).
//!
//! The fixture pairs a view whose events are recorded with a *second view*
//! whose mutations reach it the way another node's would (a target that
//! cannot produce that leaves the hooks out and these tests skip). The
//! assertions are deliberately lenient about *how* a change is told — a
//! name change is an `Entry` or a `Deleted`, a page change a `Data` — and
//! strict that it *is* told, in the recording view's own inode numbering.

use super::{must, Env, Fx, RecordingEvents, TestResult};
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
