//! Every sender on a `netwatch::UdpSocket` must make progress while the
//! socket is being rebound.
//!
//! iroh rebinds its UDP sockets on every "major" link change, and on a
//! host with container networks coming and going that is about once a
//! second. A sender that runs into a rebind (the socket's write lock is
//! held) parks until the rebind is done. Upstream netwatch 0.19.3 parks
//! all of them on one shared `AtomicWaker`, so only the last one to park
//! is woken and every other sender stays parked until something else
//! wakes its task. In iroh, one such sender is the per-peer
//! `RemoteStateActor` sending a dial's Initial packets. Nothing else wakes
//! it until its 60 s idle timer fires. Its inbox fills up meanwhile, and
//! iroh's socket actor, which hands every new dial and every new
//! connection to these actors one at a time, blocks on that full inbox.
//! The whole endpoint can then neither dial nor accept for a minute.
//! That was the stall after `git-under-flock-faults`' whole-cluster
//! restart. The vendored netwatch (`vendor/netwatch`) fixes it.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Concurrent senders on one socket.
const SENDERS: usize = 16;
/// Datagrams each sender sends.
const PER_SENDER: usize = 20_000;
/// Far beyond what the sends take when no wakeup is lost (well under a
/// second), far below the hang the bug produces (forever, here: the
/// senders have no timers to wake them).
const BOUND: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_sender_progresses_through_rebinds() {
    let socket = Arc::new(netwatch::UdpSocket::bind_local_v4(0).unwrap());
    // A sink that drains everything, so the sends never block on a full
    // receive queue: any stall below is a lost wakeup.
    let sink = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sink.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let to: SocketAddr = sink.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let drain = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut buf = [0u8; 2048];
            while !stop.load(Ordering::Relaxed) {
                let _ = sink.recv(&mut buf);
            }
        })
    };
    // A rebind every millisecond: the lock is free almost all the time,
    // so a sender that stays parked was never woken, not starved.
    let rebinds = {
        let (socket, stop) = (socket.clone(), stop.clone());
        let runtime = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            // A rebind registers the new socket with the reactor.
            let _runtime = runtime.enter();
            let (mut n, mut failed) = (0u64, None);
            while !stop.load(Ordering::Relaxed) {
                match socket.rebind() {
                    Ok(()) => n += 1,
                    Err(e) => failed = Some(e.to_string()),
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            (n, failed)
        })
    };
    let mut tasks = tokio::task::JoinSet::new();
    for k in 0..SENDERS {
        let socket = socket.clone();
        tasks.spawn(async move {
            let payload = [k as u8; 512];
            // A send that fails while the socket is being rebound is
            // counted, not fatal: only a parked sender is the bug.
            let mut failed = 0u64;
            for _ in 0..PER_SENDER {
                if socket.send_to(&payload, to).await.is_err() {
                    failed += 1;
                }
            }
            failed
        });
    }
    let finished = tokio::time::timeout(BOUND, async {
        let mut failed = 0;
        while let Some(res) = tasks.join_next().await {
            failed += res.unwrap();
        }
        failed
    })
    .await;
    stop.store(true, Ordering::Relaxed);
    let (rebinds, rebind_error) = rebinds.join().unwrap();
    eprintln!("rebinds: {rebinds}, last error: {rebind_error:?}");
    drain.join().unwrap();
    let left = tasks.len();
    tasks.abort_all();
    assert!(
        finished.is_ok(),
        "{left} of {SENDERS} senders were still parked {BOUND:?} later ({rebinds} rebinds): \
         a rebind lost their wakeups"
    );
    eprintln!(
        "{SENDERS} senders x {PER_SENDER} datagrams through {rebinds} rebinds, {} sends failed",
        finished.unwrap_or(0)
    );
}
