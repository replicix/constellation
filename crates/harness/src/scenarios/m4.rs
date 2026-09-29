//! Plan 30 §M4 scenarios: poison-record isolation, the holder-only
//! publish measurement, and the whole-cluster checks the chaos scenarios
//! run after their history checkers (convergence at quiescence including a
//! fresh replica, and exactly-once in the log).

use super::{eventually, journal_drained, raw_key, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{Context, Result};
use std::io::Read;
use std::time::Duration;

// ------------------------------------------------------------ bucket reads

/// Every key under `prefix` (ListObjectsV2, all pages), unproxied.
fn list_keys(endpoint: &str, prefix: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}");
        if let Some(t) = &token {
            url.push_str("&continuation-token=");
            url.push_str(&urlencode(t));
        }
        let mut body = String::new();
        crate::s3auth::get(&url)
            .call()
            .with_context(|| format!("listing {prefix}"))?
            .into_reader()
            .read_to_string(&mut body)?;
        let mut rest = body.as_str();
        while let Some(start) = rest.find("<Key>") {
            let after = &rest[start + 5..];
            let Some(end) = after.find("</Key>") else {
                break;
            };
            keys.push(after[..end].to_string());
            rest = &after[end..];
        }
        let truncated = body.contains("<IsTruncated>true</IsTruncated>");
        token = body.find("<NextContinuationToken>").and_then(|s| {
            let after = &body[s + "<NextContinuationToken>".len()..];
            after
                .find("</NextContinuationToken>")
                .map(|e| after[..e].to_string())
        });
        if !truncated || token.is_none() {
            return Ok(keys);
        }
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Every `Completed { rid }` in the filesystem's log, in log order: the
/// input of the log-level exactly-once check. A non-E2E segment is a zstd
/// postcard `(v, node, epoch, records)` (`cli::shipper::SegmentEnvelope`).
fn logged_completions(
    endpoint: &str,
    prefix: &str,
) -> Result<Vec<constellation_chaos::LoggedCompletion>> {
    let mut out = Vec::new();
    let mut keys = list_keys(endpoint, &format!("{prefix}/log/p0/"))?;
    keys.sort();
    for key in keys {
        let Some(seq) = key
            .rsplit('/')
            .next()
            .and_then(|n| n.strip_suffix(".zst"))
            .and_then(|n| u64::from_str_radix(n, 16).ok())
        else {
            continue;
        };
        let mut compressed = Vec::new();
        match crate::s3auth::get(&raw_key(endpoint, &key)).call() {
            Ok(resp) => {
                resp.into_reader().read_to_end(&mut compressed)?;
            }
            // Retention GC may delete a segment between the LIST and the
            // GET; its completions are older than anything in doubt.
            Err(ureq::Error::Status(404, _)) => continue,
            Err(e) => return Err(e).with_context(|| format!("fetching {key}")),
        }
        let payload =
            zstd::decode_all(&compressed[..]).with_context(|| format!("decompressing {key}"))?;
        let (v, node, epoch, records): (u32, u64, u64, Vec<constellation_meta::LogRecord>) =
            postcard::from_bytes(&payload).with_context(|| format!("decoding {key}"))?;
        // Plan 30 §M7 bumped the wire envelope to v3 (`through`) and §M9
        // added `rows`; both are appended fields this prefix decode never
        // reads, so only the version constant itself needed updating.
        anyhow::ensure!(v == 3, "{key}: unexpected segment envelope version {v}");
        for (index, rec) in records.iter().enumerate() {
            // Plan 30 §M13: a `Refused` (an inbox refusal the holder
            // shipped) is an outcome too.
            let (rid, refused) = match rec {
                constellation_meta::LogRecord::Completed { rid } => (rid, false),
                constellation_meta::LogRecord::Refused { rid, .. } => (rid, true),
                _ => continue,
            };
            out.push(constellation_chaos::LoggedCompletion {
                segment: seq,
                index,
                node,
                epoch,
                rid: (rid.node, rid.incarnation, rid.seq),
                refused,
            });
        }
    }
    Ok(out)
}

/// `s3://bucket/prefix` → `prefix`.
fn prefix_of(backend: &str) -> Result<&str> {
    backend
        .strip_prefix(&format!("s3://{BUCKET}/"))
        .context("backend is not under the harness bucket")
}

// --------------------------------------------------- chaos post-run checks

/// Plan 30 §M4 item 5's whole-cluster checks, run after a chaos run's
/// history checkers passed:
///
/// 1. every node drains (journal, pending uploads, speculation);
/// 2. a fresh node bootstraps from the bucket (head commit plus the log);
/// 3. **convergence at quiescence**: every node's tree under `work_root`,
///    the fresh node's included, is identical
///    (`constellation_chaos::check_convergence`);
/// 4. **exactly-once in the log**: no rid completes twice
///    (`constellation_chaos::check_log_completions`).
pub(super) fn after_chaos(
    env: &S3Env,
    root: &std::path::Path,
    backend: &str,
    clients: &[&Client],
    work_root: &str,
) -> Result<()> {
    for c in clients {
        eventually(
            &format!("{} drains after the chaos run", c.name),
            Duration::from_secs(120),
            || {
                journal_drained(c)?;
                let spec = c.control_status()?["speculation"].clone();
                anyhow::ensure!(
                    spec["outstanding"].as_u64() == Some(0)
                        && spec["pending_replay"].as_u64() == Some(0),
                    "speculation still outstanding: {spec}"
                );
                Ok(())
            },
        )?;
    }
    let mut fresh = Client::new(root, "fresh", &env.endpoint, backend)?.with_own_node_key();
    fresh
        .mount()
        .context("bootstrapping a fresh replica after the chaos run")?;
    let result = (|| -> Result<()> {
        let mut named: Vec<(&str, &Client)> =
            clients.iter().map(|c| (c.name.as_str(), *c)).collect();
        named.push(("fresh", &fresh));
        let mut last = None;
        let converged = eventually(
            "every replica, a fresh one included, shows the same tree",
            Duration::from_secs(120),
            || {
                let mut snaps = Vec::new();
                for (name, c) in &named {
                    let tree = constellation_chaos::snapshot_tree(&c.mnt.join(work_root))
                        .with_context(|| format!("walking {name}'s tree"))?;
                    snaps.push((name.to_string(), tree));
                }
                let verdict = constellation_chaos::check_convergence(&snaps);
                last = verdict.as_ref().err().cloned();
                verdict.map_err(|e| anyhow::anyhow!("{e}"))
            },
        );
        if let Err(e) = converged {
            if let Some(failure) = last {
                eprintln!("    convergence_at_quiescence: {failure}");
            }
            return Err(e);
        }
        let completions = logged_completions(&env.direct_endpoint, prefix_of(backend)?)?;
        eprintln!(
            "    exactly_once_log: {} outcomes (completions and refusals) across the log, each \
             rid once",
            completions.len()
        );
        constellation_chaos::check_log_completions(&completions)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    })();
    let _ = fresh.unmount();
    result
}

// ---------------------------------------------- holder-only publishing

/// Plan 30 §M4 item 3's measurement: S3 requests per node, by bucket area,
/// on an idle and on a busy 3-node cluster, with only the lease holder
/// publishing commits. Each node talks to S3 through its own counting
/// relay. The idle publish interval is shortened to 2 s so that both the
/// holder's idle publish and the followers' head checks
/// (`TreePublisher::follow_head`) fall inside each window.
///
/// Asserted: a node that did not hold the lease during a window PUTs no
/// commit (the publish-only write). A condemned-list read is *not*
/// holder-exclusive — `ChunkStore::put_chunk_mode` consults it after
/// every dedup hit (an upload that found its chunk already in S3), from
/// any writer, as the anti-resurrection guard (`gc::CondemnedView`,
/// unrelated to plan 30 §M4; a unique chunk no longer reads it) — so it
/// is reported per node for the measurement record but not asserted to
/// be zero for a non-holder. The per-area
/// breakdown is printed for the milestone's measurement record.
pub(super) fn publish_only_holder(seed: u64) -> Result<()> {
    const WINDOW_S: u64 = 20;
    let (env, root) = setup("publish-only-holder")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("pub-holder-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let counters = [
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    ];
    let mk = |name: &str, endpoint: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_PUBLISH_IDLE_S", "2")
            .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "2000")
            // Keep the lease where it starts: the assertion is per holder.
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000"))
    };
    let mut a = mk("pub-a", &counters[0].endpoint())?;
    let mut b = mk("pub-b", &counters[1].endpoint())?;
    let mut c = mk("pub-c", &counters[2].endpoint())?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;
    std::fs::write(a.mnt.join("marker"), b"holder")?;
    for f in [&b, &c] {
        eventually(
            &format!("marker reaches {}", f.name),
            Duration::from_secs(60),
            || {
                anyhow::ensure!(std::fs::read(f.mnt.join("marker"))? == b"holder");
                Ok(())
            },
        )?;
    }
    eventually("the writer drains", Duration::from_secs(60), || {
        journal_drained(&a)
    })?;
    let clients = [&a, &b, &c];
    let held = |c: &Client| -> bool {
        c.control_status()
            .map(|s| s["lease"]["held"] == true)
            .unwrap_or(false)
    };

    let mut windows = Vec::new();
    for busy in [false, true] {
        let holders_before: Vec<bool> = clients.iter().map(|c| held(c)).collect();
        for counter in &counters {
            counter.reset();
        }
        if busy {
            let deadline = std::time::Instant::now() + Duration::from_secs(WINDOW_S);
            let handles: Vec<_> = clients
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let dir = c.mnt.clone();
                    std::thread::spawn(move || -> Result<u64> {
                        let mut n = 0u64;
                        while std::time::Instant::now() < deadline {
                            let data = super::pattern(seed ^ (i as u64) << 32 ^ n, 512);
                            std::fs::write(dir.join(format!("busy-{i}-{n}")), &data)?;
                            n += 1;
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        Ok(n)
                    })
                })
                .collect();
            for h in handles {
                h.join()
                    .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
            }
            for c in clients {
                eventually("busy window drains", Duration::from_secs(60), || {
                    journal_drained(c)
                })?;
            }
            // One idle-publish interval after the burst.
            std::thread::sleep(Duration::from_secs(4));
        } else {
            std::thread::sleep(Duration::from_secs(WINDOW_S));
        }
        let holders_after: Vec<bool> = clients.iter().map(|c| held(c)).collect();
        let label = if busy { "busy" } else { "idle" };
        for ((i, client), counter) in clients.iter().enumerate().zip(&counters) {
            counter.ensure_sane()?;
            let requests = counter.requests();
            let tally = crate::reqlog::tally(&requests);
            let commit_puts = requests
                .iter()
                .filter(|r| r.method == "PUT" && r.touches("/commits/"))
                .count();
            let condemned_reads = requests
                .iter()
                .filter(|r| r.method == "GET" && r.touches("gc/condemned"))
                .count();
            let was_holder = holders_before[i] || holders_after[i];
            eprintln!(
                "    publish-only-holder: {label} {WINDOW_S}s {:>6} holder={was_holder} {tally} \
                 commit PUTs={commit_puts} condemned GETs={condemned_reads}\n        by area: {}",
                client.name,
                crate::reqlog::breakdown(&requests)
            );
            if !was_holder {
                // Condemned-list reads are not part of this assertion: see
                // the function doc — any writer's dedup hits read it,
                // holder or not.
                anyhow::ensure!(
                    commit_puts == 0,
                    "{} never held the lease during the {label} window but published \
                     {commit_puts} commit PUT(s)",
                    client.name
                );
            }
            windows.push((label, client.name.clone(), was_holder, commit_puts));
        }
    }
    anyhow::ensure!(
        windows
            .iter()
            .any(|(label, _, holder, puts)| *label == "busy" && *holder && *puts > 0),
        "no commit was published by the holder during the busy window: {windows:?}"
    );
    a.unmount()?;
    b.unmount()?;
    c.unmount()?;
    Ok(())
}

// -------------------------------------------------- poison isolation

/// Plan 30 §M4 item 2: one unrecoverable pending chunk holds back only the
/// records that need it.
///
/// Deterministic by construction (round 2; round 1 cut A's S3 path and
/// deleted the cache file by hand, racing an upload round that could
/// already hold the bytes). Two test-only fault points on A
/// (`cli::fault`): `CONSTELLATION_FAULT_LOSE_CHUNKS` makes the upload pass
/// drop `broken`'s one chunk from the cache right before reading it, and
/// `CONSTELLATION_FAULT_HOLD_SYNC_FILE` holds A's sync rounds while a file
/// exists (A acknowledges with `<file>.held`, so no round is in flight when
/// the writes happen). With the rounds held, A (the holder, write-back)
/// writes `broken`, `chmod`s it (a record that depends on the held
/// manifest) and writes an unrelated `after`; then the hold is lifted, and
/// the very first round's upload pass finds `broken`'s chunk gone.
///
/// Checked:
/// - **other inodes ship**: B sees `after` and `broken`'s create (empty,
///   mode unchanged) — everything but the held manifest and the chmod;
/// - **status**: A's `status.held` lists the inode with its lost chunk;
/// - **other inodes publish**: A publishes a new commit while the records
///   are held, and a fresh node D bootstrapped from the bucket (head
///   commit plus log) sees exactly what B sees;
/// - **`repair drop-held`** replays the chmod (B sees mode 0600),
///   materializes `/.constellation-conflict/broken@…` (full length, the lost
///   chunk a hole of zeros), and leaves A's held set and journal empty.
pub(super) fn poison_record_isolation(seed: u64) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let (env, root) = setup("poison-record-isolation")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("poison-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let lost = super::pattern(seed, 64 * 1024);
    let lost_hex = blake3::hash(&lost).to_hex().to_string();
    let hold = root.path().join("hold-sync");
    let held_marker = root.path().join("hold-sync.held");
    let mut a = Client::new(root.path(), "poison-a", &env.endpoint, &backend)?
        .with_own_node_key()
        .with_write_mode("back")
        .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
        .with_env("CONSTELLATION_PUBLISH_IDLE_S", "1")
        .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")
        .with_env("CONSTELLATION_FAULT_LOSE_CHUNKS", &lost_hex)
        .with_env(
            "CONSTELLATION_FAULT_HOLD_SYNC_FILE",
            &hold.display().to_string(),
        );
    let mut b = Client::new(root.path(), "poison-b", &env.endpoint, &backend)?.with_own_node_key();
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;
    std::fs::write(a.mnt.join("before"), b"shipped before the hold")?;
    eventually("A drains", Duration::from_secs(60), || journal_drained(&a))?;
    let a_node = a.control_status()?["node_id"]
        .as_u64()
        .context("A reports no node_id")?;

    // Hold A's rounds; wait until one has seen the hold.
    std::fs::write(&hold, b"hold")?;
    eventually("A's sync rounds are held", Duration::from_secs(30), || {
        anyhow::ensure!(held_marker.exists(), "no round has seen the hold yet");
        Ok(())
    })?;
    std::fs::write(a.mnt.join("broken"), &lost)?;
    std::fs::set_permissions(a.mnt.join("broken"), std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(a.mnt.join("after"), b"written after the broken file")?;
    let commits_before = super::commit_keys(&env.direct_endpoint, &prefix)?.len();
    std::fs::remove_file(&hold)?;

    // Everything that does not need the lost chunk ships.
    eventually("B sees the unrelated file", Duration::from_secs(60), || {
        anyhow::ensure!(std::fs::read(b.mnt.join("after"))? == b"written after the broken file");
        Ok(())
    })?;
    let ino = {
        let mut found = None;
        eventually("A reports the held set", Duration::from_secs(60), || {
            let held = a.control_status()?["held"].clone();
            let inodes = held["inodes"].as_array().cloned().unwrap_or_default();
            anyhow::ensure!(
                held["transactions"].as_u64().unwrap_or(0) >= 2 && inodes.len() == 1,
                "the manifest and the chmod are not both held: {held}"
            );
            anyhow::ensure!(
                inodes[0]["missing_chunks"]
                    .as_array()
                    .is_some_and(|m| m.len() == 1 && m[0] == lost_hex.as_str()),
                "the lost chunk is not listed: {held}"
            );
            found = inodes[0]["ino"].as_u64();
            Ok(())
        })?;
        found.context("held inode has no ino")?
    };
    let on_b = std::fs::metadata(b.mnt.join("broken"))
        .context("the broken file's create (it names no chunk) ships")?;
    anyhow::ensure!(
        on_b.len() == 0 && on_b.permissions().mode() & 0o777 != 0o600,
        "B must see neither the held manifest nor the chmod that depends on it: size {} mode {:o}",
        on_b.len(),
        on_b.permissions().mode() & 0o777
    );

    // The holder keeps publishing while the records are held, and what it
    // publishes is the log prefix: a fresh node sees what B sees.
    eventually(
        "A publishes a commit while records are held",
        Duration::from_secs(60),
        || {
            let keys = super::commit_keys(&env.direct_endpoint, &prefix)?;
            anyhow::ensure!(keys.len() > commits_before, "no new commit yet");
            let head = super::read_commit(&env.direct_endpoint, keys.last().unwrap())?;
            anyhow::ensure!(
                head["author"].as_u64() == Some(a_node),
                "head commit not A's: {head}"
            );
            anyhow::ensure!(
                a.control_status()?["held"]["transactions"]
                    .as_u64()
                    .unwrap_or(0)
                    >= 2,
                "the held records went away before the commit"
            );
            Ok(())
        },
    )?;
    let mut d = super::fresh_node(
        &env,
        root.path(),
        &backend,
        b"written after the broken file",
    )?;
    let on_d = (|| -> Result<()> {
        let m = std::fs::metadata(d.mnt.join("broken"))?;
        anyhow::ensure!(
            m.len() == 0 && m.permissions().mode() & 0o777 == on_b.permissions().mode() & 0o777,
            "a fresh node must see what B sees: size {} mode {:o}",
            m.len(),
            m.permissions().mode() & 0o777
        );
        Ok(())
    })();
    d.unmount()?;
    on_d?;

    // Drop it.
    let reply = a
        .control("locks.drop_held", serde_json::json!({ "ino": ino }))
        .context("drop-held failed")?;
    eprintln!("    poison-record-isolation: {}", reply["detail"]);
    eventually(
        "the chmod replays and reaches B",
        Duration::from_secs(60),
        || {
            let m = std::fs::metadata(b.mnt.join("broken"))?;
            anyhow::ensure!(
                m.permissions().mode() & 0o777 == 0o600,
                "mode not yet replayed"
            );
            Ok(())
        },
    )?;
    eventually(
        "the conflict copy reaches B",
        Duration::from_secs(60),
        || {
            let dir = b.mnt.join(".constellation-conflict");
            let names: Vec<String> = std::fs::read_dir(&dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            let copy = names
                .iter()
                .find(|n| n.starts_with("broken@"))
                .with_context(|| format!("no broken@ copy yet: {names:?}"))?;
            let data = std::fs::read(dir.join(copy))?;
            anyhow::ensure!(
                data.len() == lost.len() && data.iter().all(|b| *b == 0),
                "the conflict copy must keep the length with the lost chunk as a hole"
            );
            Ok(())
        },
    )?;
    eventually(
        "A's held set and journal drain",
        Duration::from_secs(60),
        || {
            let status = a.control_status()?;
            anyhow::ensure!(
                status["held"]["transactions"].as_u64() == Some(0)
                    && status["held"]["inodes"]
                        .as_array()
                        .is_some_and(|i| i.is_empty())
                    && status["speculation"]["copies_pending"].as_u64() == Some(0),
                "still held: {} / {}",
                status["held"],
                status["speculation"]
            );
            journal_drained(&a)
        },
    )?;
    a.unmount()?;
    b.unmount()?;
    Ok(())
}

/// A final flush that cannot finish must not wedge the daemon's exit.
///
/// Records held back behind a lost pending chunk (the same fault
/// `poison-record-isolation` injects) can never ship, so an unmount's
/// final flush fails. The daemon must still exit — promptly, non-zero,
/// saying why — and leave its journal and held rows on disk for the next
/// mount, where `repair drop-held` still recovers. With S3 unreachable at
/// unmount (the storm-hang evidence: the harness had already torn its
/// proxy down) and production-sized S3 retry budgets, the drain is
/// bounded by `CONSTELLATION_SHUTDOWN_STALL_S`, and what it could not
/// upload is still there after the remount.
pub(super) fn unmount_with_held_records(seed: u64) -> Result<()> {
    fn held_ino(c: &Client) -> Result<u64> {
        let held = c.control_status()?["held"].clone();
        let inodes = held["inodes"].as_array().cloned().unwrap_or_default();
        anyhow::ensure!(inodes.len() == 1, "no held inode yet: {held}");
        inodes[0]["ino"].as_u64().context("held inode has no ino")
    }
    let (env, root) = setup("unmount-with-held-records")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("unmount-held-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let lost = super::pattern(seed, 64 * 1024);
    let lost_hex = blake3::hash(&lost).to_hex().to_string();
    let node = |stall_s: &str| -> Result<Client> {
        Ok(Client::new(root.path(), "held-a", &env.endpoint, &backend)?
            .with_own_node_key()
            .with_write_mode("back")
            .with_env("CONSTELLATION_P2P", "off")
            .with_env("CONSTELLATION_FAULT_LOSE_CHUNKS", &lost_hex)
            .with_env("CONSTELLATION_SHUTDOWN_STALL_S", stall_s))
    };
    let mut a = node("120")?;
    a.fs_create()?;
    a.mount()?;
    std::fs::write(a.mnt.join("broken"), &lost)?;
    std::fs::write(a.mnt.join("after"), b"written after the broken file")?;
    eventually("A reports the held set", Duration::from_secs(60), || {
        held_ino(&a).map(|_| ())
    })?;

    // 1. S3 up: the final flush fails on the held records; exit anyway,
    //    at once, non-zero, saying why.
    let started = std::time::Instant::now();
    let status = a.unmount_exit(Duration::from_secs(60))?;
    let log = a.log_text();
    anyhow::ensure!(
        !status.success(),
        "a daemon whose final flush failed must exit non-zero, got {status}"
    );
    anyhow::ensure!(
        log.contains("final flush failed") && log.contains("held back"),
        "the exit must say why (final flush failed, held records): {}",
        a.tail_log_n(20)
    );
    eprintln!(
        "    unmount-with-held-records: S3 up: exited {status} after {:?}: {}",
        started.elapsed(),
        log.lines()
            .find(|l| l.contains("final flush failed"))
            .unwrap_or("")
    );

    // 2. The journal and held rows survived: the next mount holds them
    //    again. Cut S3 with the production S3 retry budget (object_store's
    //    default ten retries within three minutes, instead of the
    //    harness's fail-fast two) and leave an upload behind: the drain
    //    stalls, and the stall bound ends it.
    let mut a = node("15")?
        .with_env("CONSTELLATION_S3_MAX_RETRIES", "10")
        .with_env("CONSTELLATION_S3_RETRY_TIMEOUT_MS", "180000");
    a.mount()?;
    eventually(
        "the remount holds the records again",
        Duration::from_secs(60),
        || held_ino(&a).map(|_| ()),
    )?;
    proxy.cut()?;
    let offline = super::pattern(seed + 1, 200 * 1024);
    std::fs::write(a.mnt.join("offline"), &offline)?;
    let started = std::time::Instant::now();
    let status = a.unmount_exit(Duration::from_secs(120));
    proxy.heal()?;
    let status = status?;
    let log = a.log_text();
    anyhow::ensure!(
        !status.success(),
        "a daemon whose drain stalled (S3 down) must exit non-zero, got {status}"
    );
    anyhow::ensure!(
        log.contains("final flush failed"),
        "the exit must say why: {}",
        a.tail_log_n(20)
    );
    eprintln!(
        "    unmount-with-held-records: S3 down: exited {status} after {:?}: {}",
        started.elapsed(),
        log.lines()
            .find(|l| l.contains("final flush failed"))
            .unwrap_or("")
    );

    // 3. Recovery still works: the offline write is still there, drop
    //    the held inode, drain, clean exit.
    let mut a = node("120")?;
    a.mount()?;
    let ino = {
        let mut found = 0;
        eventually(
            "the second remount holds the records again",
            Duration::from_secs(60),
            || {
                found = held_ino(&a)?;
                Ok(())
            },
        )?;
        found
    };
    anyhow::ensure!(
        std::fs::read(a.mnt.join("offline"))? == offline,
        "the write made while S3 was down survived the stalled unmount"
    );
    a.control("locks.drop_held", serde_json::json!({ "ino": ino }))
        .context("drop-held failed")?;
    eventually("A's journal drains", Duration::from_secs(60), || {
        journal_drained(&a)
    })?;
    anyhow::ensure!(
        std::fs::read(a.mnt.join("after"))? == b"written after the broken file",
        "the unrelated file survived"
    );
    let status = a.unmount_exit(Duration::from_secs(60))?;
    anyhow::ensure!(
        status.success(),
        "a clean unmount must exit 0, got {status}: {}",
        a.tail_log_n(20)
    );
    // A fresh node sees the offline write: it really reached S3.
    let mut d = Client::new(root.path(), "held-d", &env.endpoint, &backend)?
        .with_own_node_key()
        .with_env("CONSTELLATION_P2P", "off");
    d.mount()?;
    let seen = eventually(
        "a fresh node reads the offline write",
        Duration::from_secs(60),
        || {
            anyhow::ensure!(std::fs::read(d.mnt.join("offline"))? == offline);
            Ok(())
        },
    );
    d.unmount()?;
    seen
}
