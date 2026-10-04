//! Plan 32 Step 8 (`budget=`) end to end, with the schedule leader **not**
//! the root lease holder (`snapsched-budget`; asked for by the M7 review
//! before plan 32 closes).
//!
//! Three nodes on the scenario's docker S3. `b` creates the filesystem,
//! takes the root lease at mount and keeps it: it is the holder and the only
//! writer, and it mounts with `CONSTELLATION_SNAPSCHED=0`, so it can never
//! lead the schedule. `a` and `c` run the scheduler; `a` binds the policy and
//! runs a tick at once to take the lead (`c` may win that race: the scenario
//! then runs with the roles swapped and says so). Every snapshot is created
//! and deleted through the holder-side batch, forwarded from the leader to
//! `b` over P2P; the budget's bytes come from the **leader's** accounting
//! index, built from its own replica.
//!
//! The policy is [`POLICY`]: two tiers that keep everything for an hour (so
//! the tier rule expires nothing and every deletion is the budget's), the
//! `last=2` floor, and a 3 MiB budget. Each of [`WRITES`] buckets replaces
//! `/proj/data` with 1 MiB of fresh seeded random bytes, so each auto
//! snapshot owns about 1 MiB nobody else has (the newest shares its chunk
//! with the live file and owns nothing). Early on, a manual snapshot is
//! taken and the first two auto snapshots are held (plain, and
//! `--by csi:test-uid`). The grace window of the policy's first sighting
//! ([`GRACE_S`]) outlasts all of that: the budget acts only once it closes,
//! so the whole over-budget history is there when it does.
//!
//! **The oracle** is computed while the window is still open and the tree
//! is frozen: `retention::evaluate` and `retention::budget_order` (the real
//! functions, from `constellation-meta`) over the listed rows give the
//! victim order; `snapshot.reclaim` on the leader gives `reclaim(kept)` and
//! `reclaim(order[..k])` for every prefix; the expected victims are the
//! shortest prefix with `reclaim(kept) − reclaim(prefix) ≤ budget` — the
//! rule M7 implements, answered by the same index through the control
//! surface rather than by the scheduler's own code.
//!
//! **Asserted:**
//! - inside the window nothing is deleted, on any node, and no node counts
//!   an expiry (the "in grace" protection);
//! - after it, **one** leader run deletes exactly the expected prefix: the
//!   survivors are everything else, and every journaled deletion carries the
//!   reason `budget`, names a victim of the prefix, and all of them sit in a
//!   single journal entry;
//! - what remains is within the budget (`snapshot.reclaim` of the kept
//!   survivors), and nothing protected went: the manual snapshot, both held
//!   ones (still held), the newest `last` two;
//! - `budget_used_bytes` comes from the leader: the leader's status reports
//!   it (within the budget, no note) when its run lands, while the follower
//!   reports none; after the takeover the new leader reports its own
//!   figure, within the budget, measured by its own index;
//! - a leader change right at the run deletes nothing twice: the leader is
//!   `kill -9`ed the moment its budget deletion shows in its counters
//!   (possibly before its audit object is written), the follower takes over,
//!   builds its own index and deletes nothing more over several expiry
//!   runs; no id is deleted twice in the journal; the killed node, mounted
//!   again, lists the same survivors.
//!
//! The root lease never moves (holder and epoch read before and after).

use super::m11::dump_logs_on_failure;
use super::m9::node_id;
use super::snapsched::{
    auto_snaps, facts, grace_window, journal_entries, now_ms, parse_policy, read_state, root_lease,
    sched_status, stat, tuned, Snap,
};
use super::{eventually, set_xattr, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{bail, ensure, Context, Result};
use constellation_meta::snapsched::{budget_order, evaluate};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::{Duration, Instant};

const POLICY: &str = "10s:1h 1m:1h; last=2; budget=3M";
const POLICY_XATTR: &str = "user.constellation.snapshots";
const BUDGET: u64 = 3 << 20;
/// Bytes each write puts in `/proj/data`.
const DATA: usize = 1 << 20;
/// Content changes, one per 10 s bucket.
const WRITES: usize = 9;
const BUCKET_MS: i64 = 10_000;
const TICK_MS: i64 = 1_000;
/// Root and `_snapsched` lease TTL: a takeover within the scenario.
const TTL_MS: i64 = 10_000;
/// The first sighting's grace window: the ~100 s of writes, the freeze
/// and the oracle's reclaim queries all fit inside it with room to spare
/// (the scenario fails if they do not, rather than racing the budget).
const GRACE_S: u64 = 180;
const EXPIRE_EVERY_S: u64 = 5;
/// `CONSTELLATION_SNAPACCT_REFRESH_S`: the budget acts only on an index at
/// most twice this old; 5 s keeps a pass close behind every snapshot.
const REFRESH_S: u64 = 5;
const CSI_OWNER: &str = "csi:test-uid";
const REASON_BUDGET: &str = "budget";

pub fn snapsched_budget(seed: u64) -> Result<()> {
    const NAME: &str = "snapsched-budget";
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("snapbudget-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let node = |name: &str| -> Result<Client> {
        Ok(tuned(
            Client::new(root.path(), name, &env.endpoint, &backend)?.with_own_node_key(),
            Some(TTL_MS),
            GRACE_S,
            EXPIRE_EVERY_S,
        )
        .with_env("CONSTELLATION_SNAPACCT_REFRESH_S", &REFRESH_S.to_string()))
    };
    let a = node("a")?;
    let mut b = node("b")?.with_env("CONSTELLATION_SNAPSCHED", "0");
    let c = node("c")?;
    b.fs_create()?;
    b.mount()?;
    let mut clients = [a, b, c];
    let result = (|| {
        clients[0].mount()?;
        clients[2].mount()?;
        body(seed, &mut clients, &env.direct_endpoint, &prefix)
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        if c.is_mounted() {
            let _ = c.unmount();
        }
    }
    result
}

/// `snapshot.reclaim` of `/proj@<name>` for each of `names`, once the
/// index answers (not building). Empty → 0 without asking.
fn reclaim(c: &Client, names: &[&str]) -> Result<u64> {
    if names.is_empty() {
        return Ok(0);
    }
    let selectors: Vec<String> = names.iter().map(|n| format!("/proj@{n}")).collect();
    let mut bytes = 0;
    eventually(
        &format!("{}'s reclaim estimate", c.name),
        Duration::from_secs(120),
        || {
            let est = c.control(
                "snapshot.reclaim",
                serde_json::json!({"selectors": selectors}),
            )?;
            ensure!(est["building"] == false, "building: {est}");
            bytes = est["bytes"].as_u64().context("no bytes")?;
            Ok(())
        },
    )?;
    Ok(bytes)
}

/// `/proj`'s policy root as `c`'s scheduler reports it.
fn root_status(c: &Client) -> Result<serde_json::Value> {
    let status = sched_status(c)?;
    status["roots"]
        .as_array()
        .and_then(|roots| roots.iter().find(|r| r["path"] == "/proj"))
        .cloned()
        .with_context(|| format!("{}: no /proj root in {status}", c.name))
}

/// A `/proj` snapshot's `(id, origin, held_by)`, `held_by` `Some("")` for
/// a plain hold.
type Row = (String, String, Option<String>);

/// Every snapshot of `/proj` by name.
fn proj_rows(c: &Client) -> Result<BTreeMap<String, Row>> {
    Ok(c.snapshot_rows()?
        .iter()
        .filter(|row| row["path"] == "/proj")
        .map(|row| {
            (
                row["name"].as_str().unwrap_or_default().to_string(),
                (
                    row["id"].as_str().unwrap_or_default().to_string(),
                    row["origin"].as_str().unwrap_or_default().to_string(),
                    (row["held"] == true)
                        .then(|| row["held_by"].as_str().unwrap_or("").to_string()),
                ),
            )
        })
        .collect())
}

fn write_data(rng: &mut StdRng, b: &Client) -> Result<()> {
    let mut buf = vec![0u8; DATA];
    rng.fill_bytes(&mut buf);
    let dir = b.mnt.join("proj");
    std::fs::write(dir.join(".data.tmp"), &buf)?;
    std::fs::rename(dir.join(".data.tmp"), dir.join("data"))?;
    Ok(())
}

fn body(seed: u64, clients: &mut [Client; 3], endpoint: &str, prefix: &str) -> Result<()> {
    let policy = parse_policy(POLICY)?;
    ensure!(
        policy.budget == Some(BUDGET),
        "{POLICY} parses to budget {:?}",
        policy.budget
    );
    let ids: Vec<u64> = clients.iter().map(node_id).collect::<Result<_>>()?;
    let b_id = ids[1];
    eventually("b holds the root lease", Duration::from_secs(30), || {
        let (holder, _) = root_lease(&clients[1])?;
        ensure!(holder == b_id, "holder {holder}, want b ({b_id})");
        Ok(())
    })?;
    wait_for_p2p(&[&clients[0], &clients[1], &clients[2]])?;
    ensure!(
        sched_status(&clients[1])?["enabled"] == false,
        "b's scheduler should be disabled"
    );
    let lease_before = root_lease(&clients[1])?;

    let mut rng = StdRng::seed_from_u64(seed);
    std::fs::create_dir(clients[1].mnt.join("proj"))?;
    write_data(&mut rng, &clients[1])?;

    // a binds the policy (forwarded to b) and takes the lead at once.
    eventually("a sees /proj", Duration::from_secs(60), || {
        ensure!(clients[0].mnt.join("proj/data").is_file(), "not yet");
        Ok(())
    })?;
    set_xattr(
        &clients[0].mnt.join("proj"),
        POLICY_XATTR,
        POLICY.as_bytes(),
    )
    .context("setting the policy")?;
    let (_, out, _) = clients[0].snapshot_cli(&["sched", "run"])?;
    eprintln!(
        "    snapsched-budget: `snapshot sched run` on a: {}",
        out.trim().replace('\n', " | ")
    );
    let mut leader = 0;
    eventually("a scheduler leads", Duration::from_secs(30), || {
        leader = match (
            sched_status(&clients[0])?["stats"]["leader"] == true,
            sched_status(&clients[2])?["stats"]["leader"] == true,
        ) {
            (true, false) => 0,
            (false, true) => 2,
            other => bail!("leaders (a, c): {other:?}"),
        };
        Ok(())
    })?;
    let follower = 2 - leader;
    eprintln!(
        "    snapsched-budget: leader {} (node {}), follower {}, root lease holder b (node {b_id})",
        clients[leader].name, ids[leader], clients[follower].name
    );
    let ino = root_status(&clients[leader])?["ino"]
        .as_u64()
        .context("no root ino")?;

    // The writes, one per bucket, 3 s into it (the bucket's snapshot is
    // taken at its start, so each write lands in the next one). After the
    // second auto snapshot: a manual snapshot and the two holds.
    let mut holds: BTreeMap<String, String> = BTreeMap::new();
    let mut held_names = Vec::new();
    let mut t_last_write = 0;
    for i in 0..WRITES {
        let next = (now_ms().div_euclid(BUCKET_MS) + 1) * BUCKET_MS + 3_000;
        std::thread::sleep(Duration::from_millis((next - now_ms()).max(0) as u64));
        write_data(&mut rng, &clients[1])?;
        t_last_write = now_ms();
        let snaps = auto_snaps(&clients[1].snapshot_rows()?)?;
        if holds.is_empty() && snaps.len() >= 2 {
            clients[0].snapshot_create("/proj@keep")?;
            for (s, by) in snaps.iter().zip(["", CSI_OWNER]) {
                let mut args = vec!["hold", s.id.as_str()];
                if !by.is_empty() {
                    args.extend(["--by", by]);
                }
                let (ok, stdout, stderr) = clients[0].snapshot_cli(&args)?;
                ensure!(ok, "holding {} ({args:?}): {stdout}{stderr}", s.name);
                holds.insert(s.id.clone(), by.to_string());
                held_names.push(s.name.clone());
            }
        }
        for c in clients.iter() {
            ensure!(
                stat(&sched_status(c)?, "expired") == 0,
                "{} expired something during the writes (write {i})",
                c.name
            );
        }
    }
    ensure!(holds.len() == 2, "the holds never happened");

    // Frozen: the last write is in a snapshot (skip-empty takes no more).
    let mut all: Vec<Snap> = Vec::new();
    eventually("the last write's snapshot", Duration::from_secs(60), || {
        all = auto_snaps(&clients[1].snapshot_rows()?)?;
        ensure!(
            all.last().is_some_and(|s| s.created > t_last_write),
            "newest {:?}",
            all.last().map(|s| &s.name)
        );
        Ok(())
    })?;
    let state = read_state(endpoint, prefix)?;
    let (_, until) = grace_window(&state, ino, None)?;

    // The oracle, on the frozen set, through the leader's index.
    let verdicts = evaluate(&policy, ino, &facts(ino, &all, &holds));
    ensure!(
        verdicts.iter().all(|v| v.keep),
        "the tier rule expires something: the scenario wants every deletion to be the budget's"
    );
    let fact_rows = facts(ino, &all, &holds);
    let order = budget_order(&policy, ino, &fact_rows, &verdicts);
    let name_of: BTreeMap<&str, &str> = all
        .iter()
        .map(|s| (s.id.as_str(), s.name.as_str()))
        .collect();
    let order_names: Vec<&str> = order.iter().map(|id| name_of[id.as_str()]).collect();
    let kept: Vec<&str> = all
        .iter()
        .filter(|s| !holds.contains_key(&s.id))
        .map(|s| s.name.as_str())
        .collect();
    let used = reclaim(&clients[leader], &kept)?;
    let mut prefix_bytes = Vec::new();
    let mut expect = None;
    for k in 0..=order_names.len() {
        let freed = reclaim(&clients[leader], &order_names[..k])?;
        prefix_bytes.push(freed);
        if expect.is_none() && used.saturating_sub(freed) <= BUDGET {
            expect = Some(k);
        }
    }
    let k = expect.with_context(|| {
        format!("no prefix of the order meets the budget: used {used}, freed {prefix_bytes:?}")
    })?;
    ensure!(
        k >= 2,
        "the budget would delete {k} snapshot(s) (used {used} of {BUDGET}): too few to show \
         anything"
    );
    let victims: BTreeSet<&str> = order_names[..k].iter().copied().collect();
    let mut table = format!(
        "    snapsched-budget: oracle over {} auto snapshots (used {used} bytes, budget {BUDGET}):\n",
        all.len()
    );
    for s in &all {
        let pos = order_names.iter().position(|n| *n == s.name);
        let _ = writeln!(
            table,
            "      {:<24} {:<20} order {:<3} {}",
            s.name,
            match holds.get(&s.id) {
                Some(by) if by.is_empty() => "held".to_string(),
                Some(by) => format!("held: {by}"),
                None => "candidate".to_string(),
            },
            pos.map_or("-".to_string(), |p| p.to_string()),
            if victims.contains(s.name.as_str()) {
                format!(
                    "victim (prefix frees {}, leaves {})",
                    prefix_bytes[pos.unwrap() + 1],
                    used - prefix_bytes[pos.unwrap() + 1]
                )
            } else {
                "survives".to_string()
            }
        );
    }
    eprint!("{table}");
    let follower_root = root_status(&clients[follower])?;
    ensure!(
        follower_root["budget_used_bytes"].is_null(),
        "the follower reports a budget figure: {follower_root}"
    );
    let left = until - now_ms();
    ensure!(
        left > 5_000,
        "the oracle finished {} ms before the grace window closed: too close to rule out a race",
        left
    );
    eprintln!(
        "    snapsched-budget: expected victims (shortest prefix of budget_order): {k} of {}; \
         the grace window closes in {left} ms",
        order.len()
    );

    // Inside the window: nothing goes. Then the leader's first budget run,
    // and the kill the moment it shows.
    let before: BTreeSet<String> = proj_rows(&clients[1])?.into_keys().collect();
    while now_ms() < until - 1_000 {
        std::thread::sleep(Duration::from_secs(1));
        let now: BTreeSet<String> = proj_rows(&clients[1])?.into_keys().collect();
        ensure!(
            now == before,
            "inside the grace window snapshots changed: gone {:?}",
            before.difference(&now).collect::<Vec<_>>()
        );
        for c in clients.iter() {
            ensure!(
                stat(&sched_status(c)?, "expired") == 0,
                "{} counted an expiry inside the grace window",
                c.name
            );
        }
    }
    let deadline = Instant::now() + Duration::from_secs(EXPIRE_EVERY_S + 60);
    let leader_root = loop {
        ensure!(
            Instant::now() < deadline,
            "no budget deletion within {} s of the window closing",
            EXPIRE_EVERY_S + 60
        );
        let status = sched_status(&clients[leader])?;
        if stat(&status, "budget_expired") > 0 || stat(&status, "expired") > 0 {
            break status["roots"]
                .as_array()
                .and_then(|r| r.iter().find(|r| r["path"] == "/proj"))
                .cloned()
                .unwrap_or_default();
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let t_kill = now_ms();
    clients[leader].kill9()?;
    eprintln!(
        "    snapsched-budget: {} killed {} ms after the window closed, right after its budget \
         run showed; its root status then: budget_used_bytes {}, budget_note {}",
        clients[leader].name,
        t_kill - until,
        leader_root["budget_used_bytes"],
        leader_root["budget_note"]
    );
    ensure!(
        leader_root["budget_used_bytes"]
            .as_u64()
            .is_some_and(|u| u <= BUDGET)
            && leader_root["budget_note"].is_null(),
        "the leader's status after its run: {leader_root}"
    );

    // The follower takes over, measures with its own index, and deletes
    // nothing more.
    eventually(
        "the follower leads",
        Duration::from_millis((2 * TTL_MS + 10 * TICK_MS) as u64),
        || {
            ensure!(
                sched_status(&clients[follower])?["stats"]["leader"] == true,
                "not yet"
            );
            Ok(())
        },
    )?;
    let mut measured = 0;
    eventually(
        "the new leader measures the budget",
        Duration::from_secs(120),
        || {
            let root = root_status(&clients[follower])?;
            measured = root["budget_used_bytes"]
                .as_u64()
                .with_context(|| format!("not yet: {root}"))?;
            Ok(())
        },
    )?;
    // Several expiry runs on the new leader.
    std::thread::sleep(Duration::from_secs(4 * EXPIRE_EVERY_S));
    let status = sched_status(&clients[follower])?;
    let root = root_status(&clients[follower])?;
    let follower_deleted = stat(&status, "budget_expired");
    ensure!(
        root["budget_used_bytes"]
            .as_u64()
            .is_some_and(|u| u <= BUDGET)
            && root["budget_note"].is_null(),
        "the new leader's root status: {root}"
    );

    // The survivors: exactly everything but the prefix, on b and c.
    let rows = proj_rows(&clients[1])?;
    let expect_left: BTreeSet<String> = before
        .iter()
        .filter(|n| !victims.contains(n.as_str()))
        .cloned()
        .collect();
    let left_names: BTreeSet<String> = rows.keys().cloned().collect();
    ensure!(
        left_names == expect_left,
        "survivors on b: gone too {:?}, kept too {:?}",
        expect_left.difference(&left_names).collect::<Vec<_>>(),
        left_names.difference(&expect_left).collect::<Vec<_>>()
    );
    ensure!(
        rows.get("keep")
            .is_some_and(|(_, origin, _)| origin == "manual"),
        "the manual snapshot is gone: {rows:?}"
    );
    for (name, (id, _, _)) in &rows {
        if let Some(by) = holds.get(id) {
            ensure!(
                rows[name].2.as_deref() == Some(by.as_str()),
                "{name} is no longer held by {by:?}: {:?}",
                rows[name]
            );
        }
    }
    for name in all
        .iter()
        .rev()
        .filter(|s| !holds.contains_key(&s.id))
        .take(2)
    {
        ensure!(
            rows.contains_key(&name.name),
            "`last` {} is gone",
            name.name
        );
    }
    let left_kept: Vec<&str> = kept
        .iter()
        .copied()
        .filter(|n| !victims.contains(n))
        .collect();
    let remaining = reclaim(&clients[follower], &left_kept)?;
    ensure!(
        remaining <= BUDGET,
        "{remaining} bytes remain, budget {BUDGET}"
    );

    // The journal: every deletion is the budget's, one of the prefix, at
    // most once, all in one entry (none when the leader died before its
    // audit write: then the listing above is the proof, and the new leader
    // deleted nothing).
    let entries = journal_entries(endpoint, prefix)?;
    let mut deleted: BTreeMap<String, (i64, u64)> = BTreeMap::new();
    let mut runs = BTreeSet::new();
    for e in &entries {
        for r in &e.roots {
            for s in &r.deleted {
                ensure!(
                    s.reason.as_deref() == Some(REASON_BUDGET),
                    "{} deleted for {:?}, not the budget",
                    s.name,
                    s.reason
                );
                ensure!(
                    victims.contains(s.name.as_str()),
                    "{} deleted by node {} is not one of the expected victims",
                    s.name,
                    e.node
                );
                ensure!(
                    deleted.insert(s.name.clone(), (e.ts, e.node)).is_none(),
                    "{} deleted twice (by node {} at {} and earlier at {:?})",
                    s.name,
                    e.node,
                    e.ts,
                    deleted.get(&s.name)
                );
                runs.insert((e.ts, e.node));
            }
        }
    }
    ensure!(
        runs.len() <= 1,
        "the budget took {} runs ({runs:?}), not one",
        runs.len()
    );
    let by_node = runs.first().map(|&(_, n)| n);
    ensure!(
        by_node.is_none() || deleted.len() == k,
        "the journaled run deleted {} of the {k} victims",
        deleted.len()
    );
    ensure!(
        follower_deleted == 0 || by_node == Some(ids[follower]),
        "the new leader deleted {follower_deleted} without journaling them"
    );

    // The killed node comes back and lists the same survivors.
    clients[leader].mount()?;
    eventually(
        "the killed node lists the survivors",
        Duration::from_secs(120),
        || {
            let names: BTreeSet<String> = proj_rows(&clients[leader])?.into_keys().collect();
            ensure!(names == expect_left, "{names:?}");
            Ok(())
        },
    )?;
    let lease_after = root_lease(&clients[1])?;
    ensure!(
        lease_after == lease_before,
        "the root lease moved: (holder, epoch) {lease_before:?} -> {lease_after:?}"
    );
    eprintln!(
        "    snapsched-budget: RESULT {k} victims deleted in one run by node {} ({}), journaled \
         {}; {} survive (manual `keep`, held {held_names:?}); remaining {remaining} <= {BUDGET} \
         bytes; new leader {} measured {measured}, deleted {follower_deleted}; root lease \
         (holder, epoch) {lease_before:?} before and after",
        by_node.map_or("?".to_string(), |n| n.to_string()),
        if by_node == Some(ids[leader]) {
            "the killed leader"
        } else if by_node == Some(ids[follower]) {
            "the new leader"
        } else {
            "unjournaled: the killed leader's batch landed before its audit write"
        },
        deleted.len(),
        expect_left.len(),
        clients[follower].name,
    );
    Ok(())
}
