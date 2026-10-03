# `UdpSocket`: a rebind (or close) wakes only one of the parked senders; the rest hang

## Version

- `netwatch` **0.19.3** from crates.io (upstream commit `0030e57fc70981895bb86212ff0f059cc41c8a4e`); no newer release, and `main` still has the same code.
- Fedora Rawhide, Linux 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.98.1 (48a229cea 2026-09-01), tokio 1.x multi-thread runtime.

## Problem

`UdpSocket` keeps one `AtomicWaker` per direction (`send_waker`, `recv_waker`, `src/udp.rs:22-23`). An `AtomicWaker` holds a single waker. When several tasks send on the same socket and meet a rebind (the `RwLock` is write-locked), each calls `waker.register(cx.waker())` in `poll_read_socket` (`src/udp.rs:249-256`) and replaces the waker of the task that parked before it. `rebind()`/`close()` then call `wake_all()` (`src/udp.rs:276-279`), which wakes only the last registered task. The other tasks are never woken and stay parked until something else happens to poll them. The same single-waker pattern is in the `Pending` arms of `poll_send_ready` and the other send/recv futures (`src/udp.rs:317, 373, 418, 654, 704, 1039`).

## Reproducer

`Cargo.toml`:

```toml
[dependencies]
netwatch = "=0.19.3"
tokio = { version = "1", features = ["full"] }
```

`src/main.rs`:

```rust
use std::{sync::Arc, time::Duration};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let socket = Arc::new(netwatch::UdpSocket::bind_local_v4(0).unwrap());
    let sink = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sink.set_nonblocking(true).unwrap();
    let to = sink.local_addr().unwrap();
    // Drain the sink so sends never block on a full receive queue.
    std::thread::spawn(move || {
        let mut buf = [0u8; 2048];
        loop {
            let _ = sink.recv(&mut buf);
            std::thread::sleep(Duration::from_micros(50));
        }
    });
    // Rebind the socket every millisecond (what a link change does).
    let (s, rt) = (socket.clone(), tokio::runtime::Handle::current());
    std::thread::spawn(move || {
        let _g = rt.enter(); // rebind registers the new fd with the reactor
        loop {
            let _ = s.rebind();
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    // 16 tasks each send 20k datagrams on the one socket.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let s = socket.clone();
        tasks.spawn(async move {
            for _ in 0..20_000 {
                let _ = s.send_to(&[0u8; 512], to).await; // errors during a rebind are fine
            }
        });
    }
    let all = async { while tasks.join_next().await.is_some() {} };
    match tokio::time::timeout(Duration::from_secs(10), all).await {
        Ok(()) => println!("ok: all senders finished"),
        Err(_) => {
            println!("BUG REPRODUCED: {} of 16 senders still parked after 10 s", tasks.len());
            std::process::exit(1);
        }
    }
}
```

Run with `cargo run --release`. It fails in 10 s (4 of 4 runs here).

## Expected vs actual

- Expected: all 16 senders finish within a fraction of a second; every parked sender is woken when the rebind completes.
- Actual: `BUG REPRODUCED: 15 of 16 senders still parked after 10 s` (identical in 4 of 4 runs; they never finish, as nothing else wakes them).

With the fix below, the same program prints `ok: all senders finished` (3 of 3 runs).

## Suggested fix

Keep every parked task's waker instead of one. A minimal drop-in for the `AtomicWaker` import in `src/udp.rs` (I ran the reproducer against this: 3 of 3 pass):

```rust
use std::{sync::Mutex, task::Waker};

/// Every parked task's waker, not just the last one's.
#[derive(Debug, Default)]
struct AtomicWaker(Mutex<Vec<Waker>>);

impl AtomicWaker {
    fn register(&self, w: &Waker) {
        let mut v = self.0.lock().unwrap();
        if !v.iter().any(|x| x.will_wake(w)) {
            v.push(w.clone());
        }
    }
    fn wake(&self) {
        for w in std::mem::take(&mut *self.0.lock().unwrap()) {
            w.wake();
        }
    }
    fn take(&self) {} // a spurious wake later is harmless
}
```

(Replace `use atomic_waker::AtomicWaker;`; rename the type if preferred.)

## Notes

- Likelier with many tasks sharing one socket and frequent rebinds. The same single-waker limit applies to writability: tokio's readiness also keeps only one waker per direction, so tasks parked in `poll_send_ready` can be lost the same way. We also wake the remaining parked senders after each successful send for that case; the diff above covers the rebind/close case only.
- Consequence in iroh: `RemoteStateActor`s sending a dial's Initial packets get stuck. Only their 60 s idle timer wakes them, their 16-slot inboxes fill, and the socket actor's awaited `send` to a full inbox blocks the whole endpoint (no new dials, no accepts) for that minute. We saw this on a host where link changes, and so rebinds, happen about once a second.
