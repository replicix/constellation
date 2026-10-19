//! `node.ping` answers for the runtime: off it (a blocked runtime still
//! answers), but not for a runtime that has run nothing for too long.

use super::register_ping;
use crate::authority_driver::{runtime_probe_task, OffCore};
use constellation_control::methods::NodePing;
use constellation_control::proto::{Empty, ErrorKind};
use constellation_control::server::serve_router;
use constellation_control::transport::UnixSocketListener;
use constellation_control::{Client, Policy, Router};
use std::sync::Arc;
use std::time::Duration;

/// A router with only `node.ping`, probed by `off_core` and failing past
/// `fails_after`, served from a socket (so the answer does not depend on
/// the blocked runtime) and a client of its own.
struct Rig {
    engine: tokio::runtime::Runtime,
    path: std::path::PathBuf,
    _dir: tempfile::TempDir,
    handle: constellation_control::server::ServerHandle,
}

fn rig(fails_after: Duration) -> Rig {
    let engine = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let off_core = Arc::new(OffCore::new(1_000));
    let mut router = Router::new().with_policy(Policy::owner_only(
        constellation_platform::native().process.effective_ids().0,
    ));
    let probe = off_core.clone();
    register_ping(&mut router, move || probe.runtime_stalled(), fails_after);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.sock");
    let handle = {
        let _enter = engine.enter();
        let listener = UnixSocketListener::bind(&path).unwrap();
        engine.spawn(runtime_probe_task(off_core.clone()));
        serve_router(listener, Arc::new(router)).unwrap()
    };
    Rig {
        engine,
        path,
        _dir: dir,
        handle,
    }
}

fn ping(path: &std::path::Path) -> Result<(), constellation_control::ControlError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let client = Client::connect_unix(path).await?;
        client.call::<NodePing>(Empty {}).await.map(|_| ())
    })
}

#[test]
fn a_ping_passes_while_the_runtime_is_blocked_less_than_the_bound_and_fails_past_it() {
    let rig = rig(Duration::from_millis(700));
    // Let the probe tick once.
    std::thread::sleep(Duration::from_millis(200));
    ping(&rig.path).expect("a running runtime answers");
    // The only worker blocked for 2 s: first under the bound...
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    rig.engine.spawn(async move {
        started_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(2_000));
    });
    started_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(250));
    ping(&rig.path).expect("blocked less than the bound: answered, off the runtime");
    // ...then past it.
    std::thread::sleep(Duration::from_millis(700));
    let err = ping(&rig.path).expect_err("blocked past the bound: wedged");
    assert_eq!(err.kind, ErrorKind::Failed, "{err:?}");
    assert!(err.message.contains("wedged"), "{err:?}");
    // The runtime is back: the probe ticks and the ping answers again.
    std::thread::sleep(Duration::from_millis(1_300));
    ping(&rig.path).expect("the runtime recovered");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(rig.handle.shutdown());
    rig.engine.shutdown_background();
}

#[test]
fn no_probe_is_not_a_stalled_runtime() {
    let off = OffCore::new(1_000);
    assert_eq!(off.runtime_stalled(), Duration::ZERO);
    off.note_probe(1, 0);
    assert!(off.runtime_stalled() > Duration::from_secs(1));
    off.probe_stopped();
    assert_eq!(off.runtime_stalled(), Duration::ZERO);
}
