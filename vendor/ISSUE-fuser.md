# fuser 0.18.0 upstream issues

Four independently pasteable issues, separated by `---`: two bugs (1, 2), then two feature requests (3, 4).

---

# 1. Root `umount` gives up on `EBUSY`: a dropped or `umount_and_join`ed session stays mounted

**Version**: fuser 0.18.0 from crates.io (upstream commit `9c957f74efe715112049298cdf1d601781829c8d`, tag `v0.18.0`). Reproduced on Fedora Rawhide, kernel 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28).

## Problem

`MountImpl::umount_impl` (`src/mnt/fuse_pure.rs:80-105`) falls back to the lazy detach (`fuse_unmount_pure`, i.e. `fusermount -u -z`) only on `EPERM`, which is what non-root users get. Root's plain `umount2(2)` instead returns `EBUSY` when a caller is still inside the mount (a request in flight), and that arm is `return Err(err.into())`. `Mount::drop` (`src/mnt/mod.rs:165`) only logs a warning, and `BackgroundSession::umount_and_join` (`src/session.rs:571`) returns the error before joining. The mount stays in the namespace, still served by the session thread, and nothing ends the connection.

## Reproducer

Needs root (`sudo cargo run`). `Cargo.toml`:

```toml
[package]
name = "ebusy-repro"
version = "0.0.0"
edition = "2021"

[dependencies]
fuser = "=0.18.0"
```

`src/main.rs`:

```rust
// Run as root: cargo run
use fuser::{Config, Errno, FileHandle, Filesystem, INodeNo, ReplyAttr, Request, Session};
use std::{fs, path::Path, thread, time::Duration};

struct Slow;
impl Filesystem for Slow {
    // A request that is "in flight": the caller sits in stat(2) inside the mount.
    fn getattr(&self, _r: &Request, _i: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        thread::sleep(Duration::from_secs(2));
        reply.error(Errno::EIO);
    }
}

fn mounted(mp: &str) -> bool {
    fs::read_to_string("/proc/self/mountinfo").unwrap().lines().any(|l| l.split(' ').nth(4) == Some(mp))
}

fn main() {
    let mp = "/var/tmp/fuser-ebusy-mnt";
    fs::create_dir_all(mp).unwrap();
    let bg = Session::new(Slow, mp, &Config::default()).unwrap().spawn().unwrap();
    let caller = thread::spawn(move || { let _ = fs::metadata(Path::new(mp)); });
    thread::sleep(Duration::from_millis(300)); // let the getattr reach the filesystem
    let r = bg.umount_and_join(); // documented: "Unmount the filesystem and join the background thread"
    println!("umount_and_join -> {r:?}");
    caller.join().unwrap();
    if mounted(mp) {
        println!("BUG REPRODUCED: {mp} is still mounted after umount_and_join()");
        println!("stat -> {:?}", fs::metadata(mp).map_err(|e| e.to_string()));
        let _ = std::process::Command::new("umount").arg("-l").arg(mp).status(); // cleanup
        std::process::exit(1);
    }
    println!("ok: unmounted");
}
```

## Expected vs actual

Expected: `umount_and_join()` (or dropping the session) unmounts, lazily if the mount is busy, like the non-root path.

Actual (`sudo target/debug/ebusy-repro`):

```
umount_and_join -> Err(Os { code: 16, kind: ResourceBusy, message: "Device or resource busy" })
BUG REPRODUCED: /var/tmp/fuser-ebusy-mnt is still mounted after umount_and_join()
stat -> Err("Input/output error (os error 5)")
```

## Suggested fix

Treat `EBUSY` like `EPERM`:

```diff
-            if err == nix::errno::Errno::EPERM {
-                // Linux always returns EPERM for non-root users.  We have to let the
-                // library go through the setuid-root "fusermount -u" to unmount.
+            if err == nix::errno::Errno::EPERM || err == nix::errno::Errno::EBUSY {
+                // EPERM: non-root users. EBUSY: root, with a request in flight.
+                // Go through "fusermount -u -z" (a lazy detach) in both cases.
                 fuse_unmount_pure(&self.mountpoint);
                 return Ok(());
```

With this applied (`[patch.crates-io]` to a copy of the crate) the same program prints:

```
umount_and_join -> Ok(())
ok: unmounted
```

## Notes

- An idle mount still gets the plain unmount first; only the busy case changes.
- Any root user of `Session::new` + drop is affected whenever a caller is inside the mount at that moment (e.g. during shutdown).

---

# 2. `FUSE_INTERRUPT` is answered `ENOSYS` and never reaches the filesystem: a killed caller of a slow request cannot be cancelled

**Version**: fuser 0.18.0 from crates.io (upstream commit `9c957f74efe715112049298cdf1d601781829c8d`, tag `v0.18.0`). Reproduced on Fedora Rawhide, kernel 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28).

## Problem

`Request::dispatch` (`src/request.rs:117-120`) handles `Operation::Interrupt` with `// TODO: handle FUSE_INTERRUPT` and `return Err(Errno::ENOSYS)`. The filesystem is never told, and `Filesystem` has no callback for it. On `ENOSYS` the kernel sets `fc->no_interrupt` (`fs/fuse/dev.c`, `fuse_dev_do_write`) and sends no more interrupts on that connection. A request already read by the daemon cannot be left by its caller (`request_wait_answer`: "Either request is already in userspace, or it was forced. Wait it out."), so a caller hit by SIGKILL stays blocked until the filesystem answers, however long that takes (for example a network backend that is down). A filesystem has no way to notice the signal and answer early.

## Reproducer

Works as a normal user (`fusermount3`); same `Cargo.toml` as above, with name `intr-repro`. `src/main.rs`:

```rust
use fuser::{Config, Errno, FileHandle, Filesystem, INodeNo, ReplyAttr, Request, Session};
use std::{fs, process::Command, thread, time::{Duration, Instant}};

struct Slow;
impl Filesystem for Slow {
    // Answers after 5 s, standing in for a backend that is unreachable.
    fn getattr(&self, _r: &Request, _i: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        thread::spawn(move || { thread::sleep(Duration::from_secs(5)); reply.error(Errno::EIO); });
    }
    // fuser 0.18 has no `interrupt` callback to override here.
}

fn main() {
    let mp = "/var/tmp/fuser-intr-mnt";
    fs::create_dir_all(mp).unwrap();
    let bg = Session::new(Slow, mp, &Config::default()).unwrap().spawn().unwrap();
    let mut child = Command::new("stat").arg(mp).spawn().unwrap(); // blocks in the getattr
    thread::sleep(Duration::from_millis(500));
    let t = Instant::now();
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();
    let waited = t.elapsed();
    println!("SIGKILLed caller took {waited:?} to die");
    let _ = bg.umount_and_join();
    if waited > Duration::from_secs(2) {
        println!("BUG REPRODUCED: a killed caller is stuck until the filesystem answers");
        std::process::exit(1);
    }
}
```

## Expected vs actual

Expected: the filesystem learns that the request was interrupted and can answer `EINTR`; the SIGKILLed caller exits within milliseconds.

Actual (`cargo run`):

```
SIGKILLed caller took 4.501015868s to die
BUG REPRODUCED: a killed caller is stuck until the filesystem answers
```

## Suggested fix

Add a defaulted callback and call it instead of answering `ENOSYS`. Send no reply (the protocol needs one only to ask for a requeue with `EAGAIN`). Also allow `Interrupt` in the `allow_root` ACL match, since the kernel sends it for anyone's request:

```diff
 // src/lib.rs, trait Filesystem
+    /// The kernel interrupted request `unique` (the caller got a signal).
+    /// Answer it with `EINTR` soon, or ignore. The interrupt may arrive before
+    /// the request it names.
+    fn interrupt(&self, _req: &Request, _unique: RequestId) {}

 // src/request.rs, dispatch()
-            ll::Operation::Interrupt(_) => {
-                // TODO: handle FUSE_INTERRUPT
-                return Err(Errno::ENOSYS);
-            }
+            ll::Operation::Interrupt(x) => {
+                filesystem.interrupt(self.request_header(), x.unique());
+            }
```

With that (and a filesystem that implements `interrupt` to answer `EINTR`: a map from `req.unique()` to a cancel flag, set by `interrupt`) the same scenario prints:

```
SIGKILLed caller took 3.667474ms to die
```

## Notes

- The kernel sends one interrupt per request, for the first signal, which may be a handled one, not necessarily fatal.
- The interrupt can overtake the request it names on a multi-threaded session; a filesystem should remember it.

---

# 3. Feature: resume an already-initialised `/dev/fuse` fd, learn the negotiated `FUSE_INIT`, and stop serving without unmounting (session handover)

**Version**: fuser 0.18.0 from crates.io (upstream commit `9c957f74efe715112049298cdf1d601781829c8d`, tag `v0.18.0`). Checked against fuser 0.18.0 on Fedora Rawhide, kernel 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28).

## Problem

To replace a filesystem process without the kernel seeing an unmount (an in-place upgrade; a new pod taking over a mount), the new process must serve an existing, already-initialised connection. In 0.18 that is impossible:

- `Session::new` and `Session::from_fd` both run `handshake()` (`src/session.rs`), which requires the first request to be `FUSE_INIT` and fails with `EIO` / `InvalidData` on anything else. A handed-over fd's first request is an ordinary one.
- `handshake()` computes the negotiated flags, `max_write`, `max_background` etc., then keeps only `proto_version`. The next server cannot learn what was agreed.
- Every way `run()` ends destroys the filesystem, closes the fd (the kernel aborts the connection when the last descriptor closes) and drops the `Mount` (unmounts). A worker parked in `read(2)` on `/dev/fuse` can only be woken by a request.

## Sketch of what can't be done today, and the API we added

```rust
// old process: serve, then hand over without unmounting
let mut session = Session::new(fs, mountpoint, &config)?;
let init: NegotiatedInit = session.negotiated_init().unwrap(); // serde-able; send it with the fd
let detacher: SessionDetacher = session.detacher()?;           // Clone + Send
// ... from another thread, on SIGUSR2 or similar:
detacher.detach();                                              // workers stop before their next read
match session.run_detachable()? {
    SessionEnd::Detached(DetachedSession { filesystem, fd, init }) => {
        // filesystem not destroyed; mount not unmounted; fd is a dup of the same connection.
        // Pass `fd` and `init` to the new process (SCM_RIGHTS).
    }
    SessionEnd::Ended => {} // unmounted / aborted, as run() today
}

// new process: no FUSE_INIT is read
init.check_resumable()?; // same major, no unknown capability, max_write fits our buffer
let session = Session::from_fd_resumed(fs, fd, SessionACL::Owner, config, init)?;
session.run()?;
```

Details of the API: `Session::negotiated_init()`, `NegotiatedInit` (kernel and our versions, offered and agreed `InitFlags`, `max_readahead`, `max_write`, `max_background`, `congestion_threshold`, `time_gran_ns`, `max_pages`, `max_stack_depth`; `check_resumable()`), `Session::from_fd_resumed`, `Session::detacher` / `SessionDetacher::detach`, `Session::run_detachable` -> `SessionEnd`, `Mount::disarm` (leave the mount in place). Detaching sets the descriptors `O_NONBLOCK` and has workers `poll(2)` the fd plus a private pipe, so a request that was read is always answered and one that was not stays queued in the kernel for the next server. Refused for `auto_unmount`. Sessions that are not armed keep the blocking loop unchanged.

## Notes

- The reference implementation is the three `CONSTELLATION PATCH (negotiated-init | from-fd-resumed | detach)` hunks, about 400 lines in `src/session.rs`, `src/channel.rs`, `src/mnt/mod.rs`; we can turn them into a PR.
- The snippet is a sketch against that patch; only the bug reproducers above were run standalone.

---

# 4. Feature: FUSE-over-io_uring transport (kernel 6.14+, ABI 7.42)

**Version**: fuser 0.18.0 from crates.io (upstream commit `9c957f74efe715112049298cdf1d601781829c8d`, tag `v0.18.0`). Checked against fuser 0.18.0 on Fedora Rawhide, kernel 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28).

## Problem

fuser 0.18 only reads and writes requests through `/dev/fuse`. Kernels 6.14+ offer FUSE-over-io_uring (`fuse.enable_uring=Y`; per-CPU queues, registered `URING_CMD` entries, replies committed from any thread), which cuts per-request syscalls and wakeups on read-heavy filesystems. There is no way to use it from fuser.

## Sketch of the API we added (cargo feature `io-uring`, off by default)

```rust
let mut config = Config::default();
config.io_uring = true;                  // falls back to /dev/fuse if the kernel did not offer it
config.io_uring_queue_depth = 32;
let session = Session::new(fs, mountpoint, &config)?;
session.run()?;                          // one ring per worker thread; a /dev/fuse reader still serves INIT, FORGET, INTERRUPT
// in a read handler, write the payload straight into the ring entry's buffer:
reply.fill(len, |buf| file.read_at(buf, off).map_err(Errno::from));   // ReplyData::fill
```

## Notes

- Upstream work: Skory's fork, `github.com/Skory/fuser` (branches `io-uring/*`; seven commits on top of its `master`, last `0104570` "Add a transport benchmark comparing /dev/fuse and io_uring"; design gist `gist.github.com/Skory/4fb49ff602919596fa3c444c8abecd87`). We vendored that work onto 0.18.0 (`io-uring` crate 0.7.x as the only new dependency) and ran its tests, 157 passing with the feature, on kernel 7.x.
- Same licence (MIT). A session served over rings cannot be handed over (item 3): its entries are owned by the ring's task.
- This is a pointer to existing work, not a proposal to merge our copy; the question is whether upstream wants to take the fork's stack.
