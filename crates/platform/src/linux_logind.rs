//! systemd-logind as a [`LifecycleSource`] (plan 46 M1.3 step 1, §5.4).
//!
//! A node holding sticky delegation grants must hand them back before the
//! host suspends or shuts down. logind offers the hook: a *delay inhibitor*
//! (`sleep:shutdown`) postpones the transition until its file descriptor is
//! closed, but for at most `InhibitDelayMaxUSec` after logind emits
//! `PrepareForSleep(true)` / `PrepareForShutdown(true)`. This source
//!
//! 1. takes the inhibitor when it starts,
//! 2. on either signal with `true` computes `deadline = now +
//!    InhibitDelayMaxUSec`, delivers [`LifecycleEvent::Suspending`] to its
//!    subscribers and runs the handler it was given (on a blocking thread,
//!    so it may take as long as it needs, up to the deadline),
//! 3. closes the inhibitor once the handler returns, letting logind go on,
//! 4. on `PrepareForSleep(false)` delivers [`LifecycleEvent::Resumed`] and
//!    takes the inhibitor again (also after a cancelled shutdown).
//!
//! The handler is how the caller says "I am done": a subscriber cannot, a
//! queue has no completion. `packaging/systemd` ships the drop-in that
//! raises `InhibitDelayMaxSec`.
//!
//! Without a system bus or without logind (a container, a non-systemd
//! host) [`LogindLifecycle::system`] returns `None` and logs one line:
//! there is then simply no such source, which is not an error.
//!
//! [`FakeLogind`] is the same interface served from a unix socket, for the
//! tests here and the harness: it hands out inhibitor descriptors it can
//! tell the release of, and emits the signals on request.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use zbus::zvariant::OwnedFd as ZFd;

use crate::lifecycle::{LifecycleEvent, LifecycleSource, ManualLifecycle, Subscription};

const DEST: &str = "org.freedesktop.login1";
const PATH: &str = "/org/freedesktop/login1";
const IFACE: &str = "org.freedesktop.login1.Manager";

/// logind's own default `InhibitDelayMaxSec`, used when the property cannot
/// be read.
const DEFAULT_DELAY: Duration = Duration::from_secs(5);

/// How long [`LogindLifecycle::connect`] waits for the bus and the first
/// inhibitor before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Where logind is reached.
#[derive(Debug, Clone)]
pub enum Endpoint {
    /// The system bus (what a real host uses).
    System,
    /// A message bus at this D-Bus address (a private test bus).
    Bus(String),
    /// A direct peer connection, no bus: [`FakeLogind`]'s socket.
    Peer(String),
}

/// The logind source. Dropping it stops the worker and closes the inhibitor.
pub struct LogindLifecycle {
    events: Arc<ManualLifecycle>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

impl std::fmt::Debug for LogindLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogindLifecycle").finish_non_exhaustive()
    }
}

impl LogindLifecycle {
    /// The source on the system bus, or `None` (logged) when there is no
    /// logind to talk to. `on_suspending` runs with the deadline each time
    /// the host is about to suspend or shut down; the inhibitor is released
    /// when it returns.
    pub fn system(on_suspending: impl Fn(Instant) + Send + Sync + 'static) -> Option<Self> {
        Self::connect(Endpoint::System, on_suspending)
    }

    /// As [`LogindLifecycle::system`], at `endpoint`.
    pub fn connect(
        endpoint: Endpoint,
        on_suspending: impl Fn(Instant) + Send + Sync + 'static,
    ) -> Option<Self> {
        let events = Arc::new(ManualLifecycle::new());
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let worker_events = events.clone();
        let handler: Arc<dyn Fn(Instant) + Send + Sync> = Arc::new(on_suspending);
        let spawned = std::thread::Builder::new()
            .name("logind".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                rt.block_on(run(endpoint, worker_events, handler, ready_tx, stop_rx));
            });
        if let Err(e) = spawned {
            tracing::info!("no logind lifecycle source: cannot start its thread: {e}");
            return None;
        }
        match ready_rx.recv_timeout(CONNECT_TIMEOUT) {
            Ok(Ok(())) => Some(LogindLifecycle {
                events,
                _stop: stop_tx,
            }),
            Ok(Err(why)) => {
                tracing::info!("no logind lifecycle source: {why}");
                None
            }
            Err(_) => {
                tracing::info!("no logind lifecycle source: timed out connecting");
                None
            }
        }
    }
}

impl LifecycleSource for LogindLifecycle {
    fn subscribe(&self) -> Subscription {
        self.events.subscribe()
    }
}

async fn open(endpoint: Endpoint) -> zbus::Result<(zbus::Connection, zbus::Proxy<'static>)> {
    let conn = match endpoint {
        Endpoint::System => zbus::Connection::system().await?,
        Endpoint::Bus(addr) => {
            zbus::connection::Builder::address(addr.as_str())?
                .build()
                .await?
        }
        Endpoint::Peer(addr) => {
            zbus::connection::Builder::address(addr.as_str())?
                .p2p()
                .build()
                .await?
        }
    };
    let proxy = zbus::Proxy::new(&conn, DEST, PATH, IFACE).await?;
    Ok((conn, proxy))
}

async fn inhibit(proxy: &zbus::Proxy<'_>) -> zbus::Result<OwnedFd> {
    let fd: ZFd = proxy
        .call(
            "Inhibit",
            &(
                "sleep:shutdown",
                "constellation",
                "release sticky delegation grants",
                "delay",
            ),
        )
        .await?;
    Ok(fd.into())
}

async fn delay_max(proxy: &zbus::Proxy<'_>) -> Duration {
    match proxy.get_property::<u64>("InhibitDelayMaxUSec").await {
        Ok(us) => Duration::from_micros(us),
        Err(e) => {
            tracing::warn!("logind InhibitDelayMaxUSec unreadable ({e}); assuming 5 s");
            DEFAULT_DELAY
        }
    }
}

async fn run(
    endpoint: Endpoint,
    events: Arc<ManualLifecycle>,
    handler: Arc<dyn Fn(Instant) + Send + Sync>,
    ready: mpsc::Sender<Result<(), String>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    let started = async {
        let (conn, proxy) = open(endpoint).await?;
        let sleep = proxy.receive_signal("PrepareForSleep").await?;
        let shutdown = proxy.receive_signal("PrepareForShutdown").await?;
        let fd = inhibit(&proxy).await?;
        zbus::Result::Ok((conn, proxy, Box::pin(sleep), Box::pin(shutdown), fd))
    }
    .await;
    let (_conn, proxy, mut sleep, mut shutdown, fd) = match started {
        Ok(x) => x,
        Err(e) => {
            let _ = ready.send(Err(e.to_string()));
            return;
        }
    };
    let mut fd = Some(fd);
    let _ = ready.send(Ok(()));
    loop {
        let (is_sleep, msg) = tokio::select! {
            _ = &mut stop => break,
            m = sleep.next() => match m { Some(m) => (true, m), None => break },
            m = shutdown.next() => match m { Some(m) => (false, m), None => break },
        };
        let Ok(starting) = msg.body().deserialize::<bool>() else {
            continue;
        };
        if starting {
            let deadline = Instant::now() + delay_max(&proxy).await;
            events.push(LifecycleEvent::Suspending { deadline });
            let h = handler.clone();
            let _ = tokio::task::spawn_blocking(move || h(deadline)).await;
            // Closing the descriptor is what lets logind go on.
            fd = None;
        } else {
            if is_sleep {
                events.push(LifecycleEvent::Resumed);
            }
            if fd.is_none() {
                match inhibit(&proxy).await {
                    Ok(f) => fd = Some(f),
                    Err(e) => tracing::warn!("logind inhibitor not retaken: {e}"),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The fake logind.

/// One inhibitor the fake handed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeInhibitor {
    pub what: String,
    pub mode: String,
    /// The client still has its descriptor open.
    pub held: bool,
}

struct FakeState {
    delay: Duration,
    /// `(what, mode, read end of the pipe whose write end the client holds)`.
    inhibitors: Mutex<Vec<(String, String, OwnedFd)>>,
}

struct FakeManager(Arc<FakeState>);

#[zbus::interface(name = "org.freedesktop.login1.Manager")]
impl FakeManager {
    async fn inhibit(
        &self,
        what: &str,
        _who: &str,
        _why: &str,
        mode: &str,
    ) -> zbus::fdo::Result<ZFd> {
        let mut fds = [0i32; 2];
        // SAFETY: `fds` is a valid two-element array.
        let r = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if r != 0 {
            return Err(zbus::fdo::Error::Failed(
                io::Error::last_os_error().to_string(),
            ));
        }
        // SAFETY: both descriptors were just created and are owned here.
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        self.0
            .inhibitors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((what.to_string(), mode.to_string(), read));
        Ok(ZFd::from(write))
    }

    #[zbus(property, name = "InhibitDelayMaxUSec")]
    fn inhibit_delay_max_usec(&self) -> u64 {
        u64::try_from(self.0.delay.as_micros()).unwrap_or(u64::MAX)
    }
}

/// A logind stand-in serving the `Manager` interface on a unix socket, for
/// [`Endpoint::Peer`]. Runs on its own runtime thread until dropped.
pub struct FakeLogind {
    rt: tokio::runtime::Runtime,
    state: Arc<FakeState>,
    conns: Arc<tokio::sync::Mutex<Vec<zbus::Connection>>>,
    address: String,
}

impl FakeLogind {
    /// Listen on `socket` and report `delay` as `InhibitDelayMaxUSec`.
    pub fn listen(socket: &Path, delay: Duration) -> io::Result<FakeLogind> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let listener = {
            let _guard = rt.enter();
            tokio::net::UnixListener::bind(socket)?
        };
        let state = Arc::new(FakeState {
            delay,
            inhibitors: Mutex::new(Vec::new()),
        });
        let conns = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (st, cs) = (state.clone(), conns.clone());
        rt.spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (st, cs) = (st.clone(), cs.clone());
                tokio::spawn(async move {
                    let built = async {
                        zbus::connection::Builder::unix_stream(stream)
                            .server(zbus::Guid::generate())?
                            .p2p()
                            .serve_at(PATH, FakeManager(st))?
                            .build()
                            .await
                    }
                    .await;
                    if let Ok(conn) = built {
                        cs.lock().await.push(conn);
                    }
                });
            }
        });
        Ok(FakeLogind {
            rt,
            state,
            conns,
            address: format!("unix:path={}", socket.display()),
        })
    }

    /// The address to give [`Endpoint::Peer`].
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Emit `PrepareForSleep(start)` to every connected client.
    pub fn prepare_for_sleep(&self, start: bool) {
        self.emit("PrepareForSleep", start);
    }

    /// Emit `PrepareForShutdown(start)` to every connected client.
    pub fn prepare_for_shutdown(&self, start: bool) {
        self.emit("PrepareForShutdown", start);
    }

    fn emit(&self, name: &str, start: bool) {
        self.rt.block_on(async {
            for conn in self.conns.lock().await.iter() {
                let _ = conn
                    .emit_signal(None::<&str>, PATH, IFACE, name, &(start,))
                    .await;
            }
        });
    }

    /// Every inhibitor handed out so far, oldest first, and whether the
    /// client still holds it: a pipe's read end sees end-of-file once every
    /// write end is closed.
    pub fn inhibitors(&self) -> Vec<FakeInhibitor> {
        let list = self
            .state
            .inhibitors
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        list.iter()
            .map(|(what, mode, read)| {
                let mut b = [0u8; 1];
                // SAFETY: a one-byte buffer on a descriptor owned by `list`.
                let n = unsafe { libc::read(read.as_raw_fd(), b.as_mut_ptr().cast(), 1) };
                FakeInhibitor {
                    what: what.clone(),
                    mode: mode.clone(),
                    held: n != 0,
                }
            })
            .collect()
    }
}

impl std::fmt::Debug for FakeLogind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeLogind")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Duration = Duration::from_secs(10);

    fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
        let end = Instant::now() + T;
        while !ok() {
            assert!(Instant::now() < end, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn prepare_for_sleep_yields_suspending_with_the_delay_as_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let delay = Duration::from_secs(30);
        let fake = FakeLogind::listen(&dir.path().join("bus"), delay).unwrap();
        let src = LogindLifecycle::connect(Endpoint::Peer(fake.address().into()), |_| {}).unwrap();
        // The delay inhibitor is taken at start.
        assert_eq!(
            fake.inhibitors(),
            vec![FakeInhibitor {
                what: "sleep:shutdown".into(),
                mode: "delay".into(),
                held: true
            }]
        );
        let sub = src.subscribe();
        let before = Instant::now();
        fake.prepare_for_sleep(true);
        let Ok(LifecycleEvent::Suspending { deadline }) = sub.recv_timeout(T) else {
            panic!("expected Suspending");
        };
        assert!(deadline >= before + delay);
        assert!(deadline <= Instant::now() + delay);
        // A shutdown is the same event.
        fake.prepare_for_shutdown(true);
        assert!(matches!(
            sub.recv_timeout(T),
            Ok(LifecycleEvent::Suspending { .. })
        ));
    }

    #[test]
    fn the_inhibitor_is_released_after_the_callback_and_retaken_on_resume() {
        let dir = tempfile::tempdir().unwrap();
        let fake = FakeLogind::listen(&dir.path().join("bus"), Duration::from_secs(5)).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let go_rx = Mutex::new(go_rx);
        let src = LogindLifecycle::connect(Endpoint::Peer(fake.address().into()), move |d| {
            entered_tx.send(d).unwrap();
            let _ = go_rx.lock().unwrap().recv();
        })
        .unwrap();
        let sub = src.subscribe();
        fake.prepare_for_sleep(true);
        entered_rx.recv_timeout(T).expect("the callback ran");
        // While the callback runs the host is held back.
        std::thread::sleep(Duration::from_millis(200));
        assert!(fake.inhibitors()[0].held);
        go_tx.send(()).unwrap();
        wait_for("the inhibitor to be released", || {
            !fake.inhibitors()[0].held
        });
        assert!(matches!(
            sub.recv_timeout(T),
            Ok(LifecycleEvent::Suspending { .. })
        ));
        // Resume: the event, and a fresh inhibitor for the next suspend.
        fake.prepare_for_sleep(false);
        assert_eq!(sub.recv_timeout(T), Ok(LifecycleEvent::Resumed));
        wait_for("the inhibitor to be retaken", || {
            let i = fake.inhibitors();
            i.len() == 2 && i[1].held
        });
    }

    #[test]
    fn an_absent_logind_is_no_source_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        for ep in [
            Endpoint::Peer(format!("unix:path={}", dir.path().join("none").display())),
            Endpoint::Bus(format!("unix:path={}", dir.path().join("none").display())),
        ] {
            assert!(LogindLifecycle::connect(ep, |_| {}).is_none());
        }
    }

    #[test]
    fn dropping_the_source_closes_the_inhibitor() {
        let dir = tempfile::tempdir().unwrap();
        let fake = FakeLogind::listen(&dir.path().join("bus"), Duration::from_secs(5)).unwrap();
        let src = LogindLifecycle::connect(Endpoint::Peer(fake.address().into()), |_| {}).unwrap();
        assert!(fake.inhibitors()[0].held);
        drop(src);
        wait_for("the inhibitor to close", || !fake.inhibitors()[0].held);
    }
}
