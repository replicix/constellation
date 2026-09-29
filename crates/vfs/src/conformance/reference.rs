//! The kit's reference target: [`MockVfs`] in reference mode (a small,
//! correct in-memory filesystem, see `mock::reffs`), with every hook
//! offered — subtree views, snapshots, a second view whose mutations
//! reach the first one's events, cancellable waits.
//!
//! It is what `cargo test -p constellation-vfs --features conformance`
//! runs the kit against, and the model of a target for anyone writing
//! one: the engine's (`ENGINE_TARGET.md`) and each future frontend's has
//! the same shape.

use super::{ConformanceTarget, Fixture, Hooks, RecordingEvents, TargetOpts};
use crate::caps::FrontendCaps;
use crate::mock::MockVfs;
use std::sync::Arc;

type SubtreeHook = Arc<dyn Fn(&str, bool) -> Result<Fixture<MockVfs>, String> + Send + Sync>;
type SecondHook = Arc<dyn Fn() -> Result<Fixture<MockVfs>, String> + Send + Sync>;

/// A [`ConformanceTarget`] over the reference filesystem.
#[derive(Debug, Default, Clone, Copy)]
pub struct RefTarget;

impl ConformanceTarget for RefTarget {
    type V = MockVfs;

    fn fresh(&self, caps: &FrontendCaps, _opts: TargetOpts) -> Fixture<MockVfs> {
        let mock = MockVfs::reference(caps.clone());
        let events = RecordingEvents::new();
        mock.set_events(events.clone());
        let fs = mock.ref_fs().expect("a reference mock").clone();
        let mut fixture = Fixture::new(Arc::new(mock.clone()), caps.clone());
        let snapshot = {
            let fs = fs.clone();
            Arc::new(move |path: &str, name: &str| {
                fs.snapshot(path, name).map_err(|c| c.to_string())
            })
        };
        let subtree: SubtreeHook = {
            let (fs, caps, snapshot) = (fs.clone(), caps.clone(), snapshot.clone());
            Arc::new(move |path: &str, confine: bool| {
                let view = MockVfs::over(&fs, path, confine).map_err(|c| c.to_string())?;
                let mut sub = Fixture::new(Arc::new(view), caps.clone());
                sub.hooks.snapshot = Some(snapshot.clone());
                Ok(sub)
            })
        };
        let second: SecondHook = {
            let (fs, caps) = (fs.clone(), caps.clone());
            Arc::new(move || {
                let view = MockVfs::over(&fs, "/", false).map_err(|c| c.to_string())?;
                Ok(Fixture::new(Arc::new(view), caps.clone()))
            })
        };
        fixture.hooks = Hooks {
            snapshot: Some(snapshot),
            subtree_view: Some(subtree),
            second_view: Some(second),
            events: Some(events),
            settle: Some(Arc::new(move || mock.settle())),
        };
        fixture
    }

    fn name(&self) -> String {
        "reference (MockVfs over RefFs)".to_string()
    }
}
