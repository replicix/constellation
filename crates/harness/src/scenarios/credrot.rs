//! Plan 37 K6a (review round) `csi-credential-revocation`: an engine's S3
//! key pair rotated and the old pair **revoked** on an S3 server that checks
//! every signature, with I/O going on throughout.
//!
//! floci (the docker backend, and the kind lanes' S3) accepts any key pair,
//! so `harness k8s-scenario csi-secret-rotation` can only show that the
//! engines' S3 clients took the new pair. This scenario shows the rest on a
//! versitygw of its own with its internal IAM ([`Versitygw`]): two accounts
//! `A` and `B`, both with access to the bucket.
//!
//! 1. `constellation serve --await-unlock --create` (a Kubernetes engine
//!    pod's command, minus Kubernetes) is unlocked with `A` over its control
//!    socket, the way the CSI plugins do it; a writer then writes and reads
//!    back a file every 50 ms through `browse.write`/`browse.read`.
//! 2. The rotation order of the chart README: `fs.unlock` with `B`, wait
//!    until `fs.list` says the S3 clients sign with it
//!    (`credentials_in_use == credentials_generation`), then revoke `A` in
//!    versitygw. The writer carries on with no error.
//! 3. A rotation to the revoked `A`, and one to `B`'s id with a wrong
//!    secret, are refused (`Denied`, the engine's trial read of `meta.json`
//!    with them fails) and the generation stays: the engine keeps signing
//!    with `B`, and the writer still sees no error. Neither refusal's text
//!    carries S3's error body (versitygw echoes the key id and the string
//!    to sign there) or a key.
//! 4. The engine stops; a fresh one (a new state dir: it reads everything
//!    from S3) refuses `A` at its credential gate and starts with `B`, and
//!    every file the writer acknowledged reads back intact.
//! 5. The first engine's audit log holds the two accepted and the two
//!    refused `fs.unlock` (`{"err":"denied"}`), no engine log or audit line
//!    holds a key id, a secret or an S3 error body.

use crate::client::{constellation_bin, control_runtime};
use crate::s3env::{Versitygw, BUCKET};
use anyhow::{bail, ensure, Context, Result};
use constellation_control::methods::{BrowseMkdir, BrowseRead, BrowseWrite, FsList, FsUnlock};
use constellation_control::proto::types::{
    BrowseReadParams, BrowseWriteParams, FsUnlockParams, MkdirParams, UnlockCredentials,
};
use constellation_control::proto::{ControlError, ErrorKind, Secret};
use constellation_control::Client;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The two accounts (fixture values for a throwaway versitygw; never
/// printed all the same).
const KEY_A: &str = "credrotaccounta";
const SECRET_A: &str = "credrot-secret-of-account-a";
const KEY_B: &str = "credrotaccountb";
const SECRET_B: &str = "credrot-secret-of-account-b";

/// What must appear in no error text, log line or audit line.
const NEVER: [&str; 7] = [
    KEY_A,
    SECRET_A,
    KEY_B,
    SECRET_B,
    "StringToSign",
    "CanonicalRequest",
    "AWSAccessKeyId",
];

/// One `constellation serve --await-unlock` on the versitygw.
struct Engine {
    child: Child,
    socket: PathBuf,
    state: PathBuf,
    log: PathBuf,
}

impl Engine {
    fn start(dir: &Path, endpoint: &str, url: &str) -> Result<Engine> {
        std::fs::create_dir_all(dir)?;
        let socket = dir.join("sock/control.sock");
        let state = dir.join("state");
        let log = dir.join("serve.log");
        let out = std::fs::File::create(&log)?;
        let child = Command::new(constellation_bin())
            .arg("serve")
            .args(["--s3", url])
            .arg("--state-dir")
            .arg(&state)
            .arg("--control-socket")
            .arg(&socket)
            .args(["--create", "--await-unlock", "--chunk-size", "1048576"])
            // Endpoint and region only: the keys come by fs.unlock.
            .env("AWS_ENDPOINT", endpoint)
            .env("AWS_ENDPOINT_URL", endpoint)
            .env("AWS_ALLOW_HTTP", "true")
            .env("AWS_REGION", crate::s3auth::REGION)
            .env("AWS_DEFAULT_REGION", crate::s3auth::REGION)
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("AWS_SESSION_TOKEN")
            .env_remove("AWS_PROFILE")
            .env("HOME", dir)
            .env("XDG_CONFIG_HOME", dir.join("config"))
            .env("XDG_CACHE_HOME", dir.join("cache"))
            .env("XDG_DATA_HOME", dir.join("data"))
            .env("XDG_RUNTIME_DIR", dir.join("run"))
            .env("CONSTELLATION_PROFILE", "server")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(out.try_clone()?)
            .stderr(out)
            .spawn()
            .context("starting constellation serve")?;
        let mut engine = Engine {
            child,
            socket,
            state,
            log,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = engine.child.try_wait()? {
                bail!("serve exited before its gate answered ({status})");
            }
            let pinged = control_runtime().block_on(async {
                match Client::connect_unix(&engine.socket).await {
                    Ok(c) => c
                        .call::<constellation_control::methods::NodePing>(Default::default())
                        .await
                        .is_ok(),
                    Err(_) => false,
                }
            });
            if pinged {
                return Ok(engine);
            }
            ensure!(Instant::now() < deadline, "serve's gate never answered");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// `SIGTERM`, and its exit status within a minute.
    fn stop(&mut self) -> Result<()> {
        // SAFETY: signalling our own child.
        unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait()? {
                ensure!(status.success(), "serve exited {status}");
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "serve did not stop on SIGTERM");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn unlock(url: &str, key: &str, secret: &str) -> FsUnlockParams {
    FsUnlockParams {
        fs: url.to_string(),
        credentials: UnlockCredentials {
            access_key_id: Some(Secret::new(key)),
            secret_access_key: Some(Secret::new(secret)),
            session_token: None,
            e2e_passphrase: None,
        },
    }
}

/// `text` holds none of [`NEVER`] (which one, if it does: named by its
/// position, never printed).
fn clean(what: &str, text: &str) -> Result<()> {
    if let Some(i) = NEVER.iter().position(|n| text.contains(n)) {
        bail!("{what} holds forbidden text #{i} (a key, a secret or an S3 error body)");
    }
    Ok(())
}

/// The daemon's own `fs.list` entry: `(credentials_generation,
/// credentials_in_use)`.
async fn generations(c: &Client) -> Result<(u64, u64)> {
    let listing = c
        .call::<FsList>(Default::default())
        .await
        .map_err(|e| anyhow::anyhow!("fs.list: {}", e.message))?;
    let own = listing
        .filesystems
        .iter()
        .find(|f| f.name.is_none())
        .context("no filesystem of its own")?;
    Ok((own.credentials_generation, own.credentials_in_use))
}

/// A client of the daemon proper (the gate's connection closes after the
/// start).
async fn daemon(socket: &Path) -> Result<Client> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(c) = Client::connect_unix(socket).await {
            if c.call::<FsList>(Default::default()).await.is_ok() {
                return Ok(c);
            }
        }
        ensure!(Instant::now() < deadline, "the engine never served fs.list");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn content(i: u64) -> Vec<u8> {
    format!("credential rotation file {i}\n")
        .repeat(1 + (i % 7) as usize * 300)
        .into_bytes()
}

async fn read_file(c: &Client, path: &str) -> Result<Vec<u8>, ControlError> {
    c.call_chunks::<BrowseRead>(BrowseReadParams {
        path: path.to_string(),
        offset: 0,
        length: None,
    })
    .await?
    .collect_bytes()
    .await
}

/// The writer of steps 1-3: one file every 50 ms, read back at once.
struct Writer {
    stop: Arc<AtomicBool>,
    written: Arc<AtomicU64>,
    errors: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Writer {
    fn start(c: Client) -> Writer {
        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let task = control_runtime().spawn({
            let (stop, written, errors) = (stop.clone(), written.clone(), errors.clone());
            async move {
                let mut i = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    let path = format!("/data/f{i}");
                    let wrote = c
                        .call::<BrowseWrite>(BrowseWriteParams {
                            path: path.clone(),
                            offset: 0,
                            data: content(i).into(),
                            create: true,
                            create_mode: None,
                            truncate: true,
                        })
                        .await;
                    let outcome = match wrote {
                        Ok(_) => match read_file(&c, &path).await {
                            Ok(back) if back == content(i) => Ok(()),
                            Ok(back) => Err(format!("{path}: read back {} bytes", back.len())),
                            Err(e) => Err(format!("browse.read {path}: {}", e.message)),
                        },
                        Err(e) => Err(format!("browse.write {path}: {}", e.message)),
                    };
                    match outcome {
                        Ok(()) => {
                            i += 1;
                            written.store(i, Ordering::SeqCst);
                        }
                        Err(e) => {
                            errors.lock().unwrap().push(e);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        });
        Writer {
            stop,
            written,
            errors,
            task,
        }
    }

    fn written(&self) -> u64 {
        self.written.load(Ordering::SeqCst)
    }

    /// Wait until `n` more files than now are written.
    fn advance(&self, n: u64, within: Duration) -> Result<()> {
        let from = self.written();
        let deadline = Instant::now() + within;
        while self.written() < from + n {
            self.no_errors()?;
            ensure!(
                Instant::now() < deadline,
                "the writer stalled at {} files",
                self.written()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    fn no_errors(&self) -> Result<()> {
        let errors = self.errors.lock().unwrap();
        if errors.is_empty() {
            return Ok(());
        }
        for e in errors.iter() {
            clean("a writer error", e)?;
        }
        bail!("the writer saw {} error(s): {:?}", errors.len(), *errors)
    }

    fn finish(mut self) -> Result<u64> {
        self.stop.store(true, Ordering::SeqCst);
        control_runtime().block_on(&mut self.task).ok();
        self.no_errors()?;
        Ok(self.written())
    }
}

/// A rotation the engine must refuse: `Denied`, naming S3's error code
/// `code`, with nothing of S3's error body or the keys in it, and the
/// generations unchanged.
async fn refused_rotation(
    c: &Client,
    url: &str,
    key: &str,
    secret: &str,
    code: &str,
) -> Result<()> {
    let before = generations(c).await?;
    let e = match c.call::<FsUnlock>(unlock(url, key, secret)).await {
        Ok(_) => bail!("a rotation to an S3-refused pair ({code}) was accepted"),
        Err(e) => e,
    };
    clean("the refusal", &e.message)?;
    ensure!(
        e.kind == ErrorKind::Denied,
        "refused as {:?}: {}",
        e.kind,
        e.message
    );
    ensure!(
        e.message.contains(code) && e.message.contains("stay in use"),
        "the refusal does not say {code} / that the old pair stays: {}",
        e.message
    );
    let after = generations(c).await?;
    ensure!(
        after.0 == before.0,
        "the refused rotation moved the generation: {before:?} -> {after:?}"
    );
    eprintln!("   refused ({code}): {}", e.message);
    Ok(())
}

pub fn csi_credential_revocation(seed: u64) -> Result<()> {
    if let Some(why) = Versitygw::missing() {
        bail!("needs a native versitygw: {why}");
    }
    let vgw = Versitygw::start()?;
    vgw.create_user(KEY_A, SECRET_A)?;
    vgw.create_user(KEY_B, SECRET_B)?;
    let url = format!("s3://{BUCKET}/credrot-{seed}");
    let root = tempfile::Builder::new()
        .prefix("constellation-harness-credrot-")
        .tempdir()?;
    let rt = control_runtime();

    // 1. Unlocked with A; the writer under way.
    let mut first = Engine::start(&root.path().join("e1"), &vgw.endpoint, &url)?;
    let (client, writer) = rt.block_on(async {
        let gate = Client::connect_unix(&first.socket).await?;
        let started = gate
            .call::<FsUnlock>(unlock(&url, KEY_A, SECRET_A))
            .await
            .map_err(|e| anyhow::anyhow!("fs.unlock with A: {}", e.message))?;
        clean("the gate's answer", &started.detail)?;
        let c = daemon(&first.socket).await?;
        c.call::<BrowseMkdir>(MkdirParams {
            path: "/data".into(),
            mode: None,
            parents: true,
        })
        .await
        .map_err(|e| anyhow::anyhow!("browse.mkdir: {}", e.message))?;
        let writer = Writer::start(daemon(&first.socket).await?);
        anyhow::Ok((c, writer))
    })?;
    writer.advance(20, Duration::from_secs(60))?;
    let (g1, _) = rt.block_on(generations(&client))?;
    eprintln!(
        "   engine unlocked with account A (generation {g1}); writer at {}",
        writer.written()
    );

    // 2. Rotate to B, wait until the S3 clients sign with it, revoke A.
    rt.block_on(async {
        let rotated = client
            .call::<FsUnlock>(unlock(&url, KEY_B, SECRET_B))
            .await
            .map_err(|e| anyhow::anyhow!("fs.unlock with B: {}", e.message))?;
        ensure!(rotated.detail.contains("rotated"), "{}", rotated.detail);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (set, used) = generations(&client).await?;
            if set == g1 + 1 && used == set {
                return anyhow::Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "the S3 clients never signed with the rotation (generation {set}, in use {used})"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })?;
    // Requests signed just before (and their retries) keep A's signature:
    // a moment for them to finish, as the README's order has it.
    writer.advance(10, Duration::from_secs(60))?;
    vgw.delete_user(KEY_A)?;
    eprintln!(
        "   rotated to account B (generation {}), account A revoked",
        g1 + 1
    );
    writer.advance(40, Duration::from_secs(120))?;
    let (set, used) = rt.block_on(generations(&client))?;
    ensure!(
        set == g1 + 1 && used == set,
        "after the revocation: generation {set}, in use {used}"
    );
    eprintln!(
        "   writer at {} with A revoked, zero errors",
        writer.written()
    );

    // 3. Rotations S3 refuses are refused; B stays in use.
    rt.block_on(async {
        refused_rotation(&client, &url, KEY_A, SECRET_A, "InvalidAccessKeyId").await?;
        refused_rotation(
            &client,
            &url,
            KEY_B,
            "not-the-secret",
            "SignatureDoesNotMatch",
        )
        .await
    })?;
    writer.advance(20, Duration::from_secs(60))?;
    let (set, used) = rt.block_on(generations(&client))?;
    ensure!(
        set == g1 + 1 && used == set,
        "after the refusals: generation {set}, in use {used}"
    );
    let n = writer.finish()?;
    drop(client);
    eprintln!("   {n} files written and read back, zero errors");

    // 5 (first half). The first engine's audit log and log.
    let audit = std::fs::read_to_string(first.state.join("control-audit.jsonl"))
        .context("reading the first engine's audit log")?;
    clean("the audit log", &audit)?;
    let unlocks: Vec<serde_json::Value> = audit
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|r| r["method"] == "fs.unlock")
        .collect();
    let outcomes: Vec<String> = unlocks.iter().map(|r| r["outcome"].to_string()).collect();
    ensure!(
        outcomes
            == [
                "\"ok\"",
                "\"ok\"",
                r#"{"err":"denied"}"#,
                r#"{"err":"denied"}"#
            ],
        "fs.unlock audit outcomes: {outcomes:?}"
    );
    first.stop()?;
    clean("the first engine's log", &first.log_text())?;

    // 4. A fresh engine (state from S3 only): A refused at the gate, B
    //    starts it, every file reads back.
    let mut second = Engine::start(&root.path().join("e2"), &vgw.endpoint, &url)?;
    rt.block_on(async {
        let gate = Client::connect_unix(&second.socket).await?;
        match gate.call::<FsUnlock>(unlock(&url, KEY_A, SECRET_A)).await {
            Ok(_) => bail!("the gate accepted the revoked pair"),
            Err(e) => {
                clean("the gate's refusal", &e.message)?;
                ensure!(
                    e.kind == ErrorKind::Denied && e.message.contains("InvalidAccessKeyId"),
                    "the gate's refusal: {:?} {}",
                    e.kind,
                    e.message
                );
            }
        }
        gate.call::<FsUnlock>(unlock(&url, KEY_B, SECRET_B))
            .await
            .map_err(|e| anyhow::anyhow!("fs.unlock with B on a fresh engine: {}", e.message))?;
        let c = daemon(&second.socket).await?;
        for i in 0..n {
            let path = format!("/data/f{i}");
            let back = read_file(&c, &path)
                .await
                .map_err(|e| anyhow::anyhow!("browse.read {path}: {}", e.message))?;
            ensure!(back == content(i), "{path} read back {} bytes", back.len());
        }
        anyhow::Ok(())
    })?;
    eprintln!(
        "   a fresh engine refused A at its gate, started with B, and read all {n} files back"
    );
    second.stop()?;
    clean("the second engine's log", &second.log_text())?;
    Ok(())
}
