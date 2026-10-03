//! Plan 32 M5c over the control protocol, against a real (offline)
//! engine with its accounting service: the sizes `snapshot.list` carries,
//! `snapshot.reclaim`, `snapshot.space`, `snapshot.space.verify` and the
//! dry-run estimate of `snapshot.delete_many` — each equal to what the
//! service itself answers (the control layer adds no numbers) — while the
//! index is ready, while it is building, and with accounting off.

use super::ops::tests::{fixture_with, Fixture};
use super::snapspace::LIST_CHUNKS_MAX;
use super::*;
use crate::snapacct::{SnapAcctConfig, SnapAcctMode, SnapAnswer};
use constellation_control::dispatch_in_process;
use constellation_control::proto::ErrorKind;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn call(f: &Fixture, method: &str, params: Value) -> Result<Value, ControlError> {
    let router = router(&f.svc);
    f.rt.block_on(dispatch_in_process(
        &router,
        &Principal::InProcess,
        method,
        params,
    ))
}

fn config(mode: SnapAcctMode, answer_wait: Duration) -> SnapAcctConfig {
    SnapAcctConfig {
        mode,
        answer_wait,
        ..SnapAcctConfig::from_env()
    }
}

/// `/vol` and `/other` with files written between snapshots, so the
/// numbers differ per snapshot: `/vol@a1`..`a3`, `/other@b1`. `f0` is
/// rewritten before `a3`, so its first content lives on only in `a1` and
/// `a2` (`a3`'s creation publishes the rewrite).
fn populate(f: &Fixture) {
    for dir in ["/vol", "/other"] {
        call(f, "browse.mkdir", json!({"path": dir})).unwrap();
    }
    let data = |n: usize, seed: u8| {
        let bytes: Vec<u8> = (0..n).map(|i| (i as u8).wrapping_mul(31) ^ seed).collect();
        serde_json::to_value(constellation_control::proto::ByteBuf::from(bytes)).unwrap()
    };
    let steps: [(&str, usize, Option<&str>); 5] = [
        ("/vol/f0", 300_000, Some("/vol@a1")),
        ("/vol/f1", 200_000, Some("/vol@a2")),
        ("/other/g0", 100_000, Some("/other@b1")),
        ("/vol/f0", 300_000, None),
        ("/vol/f2", 50_000, Some("/vol@a3")),
    ];
    for (i, (path, len, snapshot)) in steps.into_iter().enumerate() {
        call(
            f,
            "browse.write",
            json!({"path": path, "data": data(len, i as u8), "create": true, "truncate": true}),
        )
        .unwrap();
        if let Some(selector) = snapshot {
            call(f, "snapshot.create", json!({"selector": selector})).unwrap();
        }
    }
}

fn rows(listing: &Value) -> Vec<Value> {
    listing["snapshots"].as_array().unwrap().clone()
}

fn by_name<'a>(rows: &'a [Value], name: &str) -> &'a Value {
    rows.iter()
        .find(|r| r["name"] == name)
        .unwrap_or_else(|| panic!("no {name} in {rows:?}"))
}

/// Poll `snapshot.list {sizes: true}` until every row is `ok`.
fn ready_listing(f: &Fixture) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let listing = rows(&call(f, "snapshot.list", json!({"sizes": true})).unwrap());
        if listing.iter().all(|r| r["size_state"] == "ok") {
            return listing;
        }
        assert!(Instant::now() < deadline, "never ready: {listing:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn sizes_reclaim_and_space_are_the_indexs_numbers() {
    let f = fixture_with(
        &[&[]],
        Some(config(SnapAcctMode::Auto, Duration::from_secs(2))),
    );
    populate(&f);
    let acct = f.svc.engine.snapacct().clone();

    // `--verify` first: it also refreshes the live flags from the newest
    // commit (the rewrite of f0), which the periodic refresh would do
    // only after `CONSTELLATION_SNAPACCT_REFRESH_S`.
    let verified = call(&f, "snapshot.space.verify", json!({})).unwrap();
    assert_eq!(verified["mismatches"], 0, "{verified}");
    assert_eq!(verified["snapshots"], 4, "{verified}");
    assert!(verified["chunks"].as_u64().unwrap() > 0, "{verified}");

    let listing = ready_listing(&f);
    assert_eq!(listing.len(), 4);
    for row in &listing {
        let id = row["id"].as_str().unwrap();
        let n =
            f.rt.block_on(acct.snap_numbers(id))
                .unwrap()
                .ready()
                .unwrap();
        assert_eq!(row["used"], n.used, "{row}");
        assert_eq!(row["written"], n.written, "{row}");
        assert_eq!(row["refer"], n.refer, "{row}");
        assert_eq!(row["lsize"], n.lsize, "{row}");
        assert_eq!(row["as_of_seq"], n.as_of_seq, "{row}");
        assert!(row["building_pct"].is_null(), "{row}");
        // `refer_bytes` (plan 37 reads it) is untouched.
        assert!(row["refer_bytes"].is_u64(), "{row}");
    }
    // Real numbers, not zeros: each snapshot of `/vol` wrote its file.
    let a1 = by_name(&listing, "a1");
    assert!(a1["written"].as_u64().unwrap() >= 300_000, "{a1}");
    assert!(a1["lsize"].as_u64().unwrap() >= 300_000, "{a1}");
    let a3 = by_name(&listing, "a3");
    assert!(
        a3["refer"].as_u64().unwrap() > a1["refer"].as_u64().unwrap(),
        "{listing:?}"
    );

    // Once the index is current, a plain listing carries the sizes too
    // (a peek: it asks for nothing).
    // (`as_of_ms` is the last catch-up, which every pass restamps.)
    let undated = |rows: Vec<Value>| -> Vec<Value> {
        rows.into_iter()
            .map(|mut r| {
                assert!(r["as_of_ms"].as_u64().unwrap() > 0, "{r}");
                r.as_object_mut().unwrap().remove("as_of_ms");
                r
            })
            .collect()
    };
    let plain = rows(&call(&f, "snapshot.list", json!({})).unwrap());
    assert_eq!(undated(plain), undated(listing.clone()));

    // `snapshot.reclaim` = the service's `reclaim` of what the selectors
    // resolve to.
    let ids = |names: &[&str]| -> Vec<String> {
        names
            .iter()
            .map(|n| by_name(&listing, n)["id"].as_str().unwrap().to_string())
            .collect()
    };
    let est = call(&f, "snapshot.reclaim", json!({"selectors": ["/vol@a1%a2"]})).unwrap();
    let want =
        f.rt.block_on(acct.reclaim(&ids(&["a1", "a2"])))
            .unwrap()
            .ready()
            .unwrap();
    assert_eq!(est["bytes"], want.bytes, "{est}");
    assert_eq!(est["chunks"], want.chunks, "{est}");
    assert_eq!(est["building"], false);
    assert!(
        est["chunk_hashes"].is_null(),
        "listed only on request: {est}"
    );
    // The `list_chunks` test aid: the counted chunks, sorted, one per
    // chunk of the same estimate.
    let listed = call(
        &f,
        "snapshot.reclaim",
        json!({"selectors": ["/vol@a1%a2"], "list_chunks": true}),
    )
    .unwrap();
    assert_eq!(listed["chunks"], want.chunks, "{listed}");
    let hashes: Vec<&str> = listed["chunk_hashes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h.as_str().unwrap())
        .collect();
    assert_eq!(hashes.len() as u64, want.chunks, "{listed}");
    assert!(hashes.windows(2).all(|w| w[0] < w[1]), "{listed}");
    assert!(hashes.iter().all(|h| h.len() == 64), "{listed}");
    // Not for a viewer: the list costs one string per chunk.
    let viewer = Arc::new(router(&f.svc).with_policy(
        constellation_control::Policy::in_process_only().with_grant(
            constellation_control::authz::Grant {
                subject: constellation_control::authz::Subject::Device("viewer".into()),
                role: constellation_control::Role::Viewer,
            },
        ),
    ));
    let as_viewer = |params: Value| {
        f.rt.block_on(dispatch_in_process(
            &viewer,
            &Principal::Remote {
                device: "viewer".into(),
            },
            "snapshot.reclaim",
            params,
        ))
    };
    let denied = as_viewer(json!({"selectors": ["/vol@a1%a2"], "list_chunks": true})).unwrap_err();
    assert_eq!(denied.kind, ErrorKind::Denied, "{denied:?}");
    let estimate = as_viewer(json!({"selectors": ["/vol@a1%a2"]})).unwrap();
    assert_eq!(
        estimate["chunks"], want.chunks,
        "the estimate alone is a viewer's"
    );
    // Bounded: past the bound it refuses rather than list, and holds no
    // more than the bound meanwhile.
    let range: Vec<String> = f
        .svc
        .snapshot_resolve_rows(&["/vol@a1%a2".to_string()])
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect();
    let svc = f.svc.clone();
    let bound = want.chunks as usize - 1;
    let over =
        f.rt.block_on(async move {
            tokio::task::spawn_blocking(move || svc.reclaim_listed_of(&range, Some(bound))).await
        })
        .unwrap()
        .unwrap_err();
    assert_eq!(over.kind, ErrorKind::Invalid, "{over:?}");
    assert!(over.message.contains("at most"), "{over:?}");
    const { assert!(LIST_CHUNKS_MAX >= 100_000) };
    // f0's first content is only in a1 and a2 (rewritten before a3).
    assert!(want.bytes >= 300_000, "{want:?}");
    let a1_used = by_name(&listing, "a1")["used"].as_u64().unwrap();
    assert!(a1_used < 300_000, "f0's old content is shared by a1 and a2");

    // The dry run estimates exactly what it would delete: a held snapshot
    // is refused, and left out of the estimate.
    call(
        &f,
        "snapshot.hold",
        json!({"id": "/vol@a2", "held": true, "by": "user:ops"}),
    )
    .unwrap();
    let dry = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/vol@a1%a2"], "dry_run": true}),
    )
    .unwrap();
    assert_eq!(dry["refused"].as_array().unwrap().len(), 1, "{dry}");
    let want =
        f.rt.block_on(acct.reclaim(&ids(&["a1"])))
            .unwrap()
            .ready()
            .unwrap();
    assert_eq!(dry["reclaim"]["bytes"], want.bytes, "{dry}");
    assert_eq!(dry["reclaim"]["chunks"], want.chunks, "{dry}");
    // A real delete asks for no estimate.
    let gone = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/other@b1"]}),
    )
    .unwrap();
    assert!(gone["reclaim"].is_null(), "{gone}");
    assert_eq!(gone["deleted"].as_array().unwrap().len(), 1, "{gone}");

    // `snapshot.space`, filesystem-wide and for a path. The delete above changed the rows, so the
    // index answers `building` until a pass catches up; wait for it rather than for the 2 s answer
    // window (which a loaded host can outrun).
    f.rt.block_on(acct.catch_up()).unwrap();
    let space = call(&f, "snapshot.space", json!({})).unwrap();
    let want = f.rt.block_on(acct.space(None)).unwrap().ready().unwrap();
    assert_eq!(space["building"], false, "{space}");
    assert!(space["path"].is_null());
    assert_eq!(space["live_logical"], want.live_logical);
    assert_eq!(
        space["snapshots_total"]["bytes"],
        want.snapshots_total.bytes
    );
    assert_eq!(
        space["snapshots_total"]["chunks"],
        want.snapshots_total.chunks
    );
    assert_eq!(space["unique"]["bytes"], want.unique.bytes);
    assert_eq!(
        space["shared_snapshots_only"]["bytes"],
        want.shared_snapshots_only.bytes
    );
    assert_eq!(
        space["shared_with_live"]["bytes"],
        want.shared_with_live.bytes
    );
    assert_eq!(space["awaiting_gc"]["bytes"], want.awaiting_gc.bytes);
    assert_eq!(space["gc_horizon_ms"], acct.gc_horizon_ms());
    // No GC round has measured the bucket: no physical estimate.
    assert!(space["physical_ratio"].is_null() && space["physical_estimate"].is_null());
    // f0's first content is held by snapshots only.
    assert!(want.snapshots_total.bytes >= 300_000, "{want:?}");
    assert!(want.shared_snapshots_only.bytes >= 300_000, "{want:?}");

    let scoped = call(&f, "snapshot.space", json!({"path": "vol/"})).unwrap();
    let want =
        f.rt.block_on(acct.space(Some("/vol")))
            .unwrap()
            .ready()
            .unwrap();
    assert_eq!(scoped["path"], "/vol");
    assert_eq!(scoped["live_logical"], want.live_logical);
    assert_eq!(
        scoped["snapshots_total"]["bytes"],
        want.snapshots_total.bytes
    );
    let root = call(&f, "snapshot.space", json!({"path": "/"})).unwrap();
    assert!(
        root["path"].is_null(),
        "`/` is the whole filesystem: {root}"
    );
    let missing = call(&f, "snapshot.space", json!({"path": "/nope"})).unwrap_err();
    assert_eq!(
        missing.kind,
        constellation_control::proto::ErrorKind::NotFound
    );

    // The brute force still agrees after the hold and the delete.
    let verified = call(&f, "snapshot.space.verify", json!({})).unwrap();
    assert_eq!(verified["mismatches"], 0, "{verified}");
    assert_eq!(verified["snapshots"], 3, "{verified}");
}

#[test]
fn a_listing_says_building_until_the_index_is_built() {
    // Answer at once: the first request finds the index unbuilt.
    let f = fixture_with(&[&[]], Some(config(SnapAcctMode::Auto, Duration::ZERO)));
    populate(&f);
    let acct = f.svc.engine.snapacct().clone();

    // Under `auto` nothing is built before a size request, and a plain
    // listing is not one: no sizes, no state, no work.
    let plain = rows(&call(&f, "snapshot.list", json!({})).unwrap());
    for row in &plain {
        assert!(
            row["size_state"].is_null() && row["used"].is_null(),
            "{row}"
        );
    }
    assert_eq!(
        acct.stats()
            .passes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a plain listing caused accounting work"
    );

    // The request starts the build and answers `building`, never 0.
    let first = rows(&call(&f, "snapshot.list", json!({"sizes": true})).unwrap());
    for row in &first {
        assert_eq!(row["size_state"], "building", "{row}");
        assert!(row["building_pct"].as_u64().unwrap() < 100, "{row}");
        for key in ["used", "written", "refer", "lsize", "as_of_seq"] {
            assert!(row[key].is_null(), "{key} while building: {row}");
        }
    }
    let est = call(&f, "snapshot.reclaim", json!({"selectors": ["/vol@a1"]})).unwrap();
    // (The build may have finished meanwhile; while it has not, the
    // estimate says so and carries no number.)
    if est["building"] == true {
        assert_eq!(
            (est["bytes"].as_u64(), est["chunks"].as_u64()),
            (Some(0), Some(0))
        );
        assert!(est["building_pct"].as_u64().unwrap() < 100, "{est}");
    }

    // …and the build finishes on its own.
    let ready = ready_listing(&f);
    assert!(ready.iter().all(|r| r["used"].is_u64()));
    let est = call(&f, "snapshot.reclaim", json!({"selectors": ["/vol@a1"]})).unwrap();
    assert_eq!(est["building"], false, "{est}");
}

/// `snapshot.sched.status`'s per-root `used_bytes` (the per-root
/// `/metrics` gauge): Σ `USED` of the root directory's snapshots, from a
/// peek — absent, and no work caused, until the index is current.
#[test]
fn sched_status_carries_a_roots_used_only_once_the_index_is_current() {
    let f = fixture_with(&[&[]], Some(config(SnapAcctMode::Auto, Duration::ZERO)));
    populate(&f);
    call(
        &f,
        "snapshot.policy.set",
        json!({"path": "/vol", "expr": "1h:1d"}),
    )
    .unwrap();
    let acct = f.svc.engine.snapacct().clone();
    let vol_root = |status: &Value| -> Value {
        let roots = status["roots"].as_array().unwrap();
        assert_eq!(roots.len(), 1, "{status}");
        assert_eq!(roots[0]["path"], "/vol", "{status}");
        roots[0].clone()
    };

    let before = vol_root(&call(&f, "snapshot.sched.status", json!({})).unwrap());
    assert!(before["used_bytes"].is_null(), "{before}");
    assert_eq!(
        acct.stats()
            .passes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a status poll caused accounting work"
    );

    // Make `a3`'s copy of f2 unique to it (rewritten; `/other@b2`
    // publishes the rewrite), and refresh the live flags (`--verify`, as
    // above), so the sum is not 0.
    let data: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
    call(
        &f,
        "browse.write",
        json!({"path": "/vol/f2", "data": constellation_control::proto::ByteBuf::from(data),
               "create": true, "truncate": true}),
    )
    .unwrap();
    call(&f, "snapshot.create", json!({"selector": "/other@b2"})).unwrap();
    let verified = call(&f, "snapshot.space.verify", json!({})).unwrap();
    assert_eq!(verified["mismatches"], 0, "{verified}");

    let listing = ready_listing(&f);
    let want: u64 = listing
        .iter()
        .filter(|r| r["path"] == "/vol")
        .map(|r| r["used"].as_u64().unwrap())
        .sum();
    assert!(want > 0, "{listing:?}");
    let after = vol_root(&call(&f, "snapshot.sched.status", json!({})).unwrap());
    assert_eq!(after["used_bytes"], want, "{after}");
}

#[test]
fn accounting_off_says_off_and_refuses_the_space_methods() {
    let f = fixture_with(
        &[&[]],
        Some(config(SnapAcctMode::Off, Duration::from_secs(2))),
    );
    populate(&f);
    assert!(matches!(
        f.rt.block_on(f.svc.engine.snapacct().space(None)).unwrap(),
        SnapAnswer::Off
    ));

    let listing = rows(&call(&f, "snapshot.list", json!({"sizes": true})).unwrap());
    for row in &listing {
        assert_eq!(row["size_state"], "off", "{row}");
        assert!(row["used"].is_null() && row["refer"].is_null(), "{row}");
        assert!(row["refer_bytes"].is_u64(), "{row}");
    }
    let plain = rows(&call(&f, "snapshot.list", json!({})).unwrap());
    assert!(plain.iter().all(|r| r["size_state"].is_null()));

    for (method, params) in [
        ("snapshot.reclaim", json!({"selectors": ["/vol@a1"]})),
        ("snapshot.space", json!({})),
        ("snapshot.space.verify", json!({})),
    ] {
        let error = call(&f, method, params).unwrap_err();
        assert_eq!(
            error.kind,
            constellation_control::proto::ErrorKind::Unsupported,
            "{method}: {error:?}"
        );
        assert!(
            error.message.contains("CONSTELLATION_SNAPACCT=off"),
            "{method}"
        );
    }
    let dry = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/vol@a*"], "dry_run": true}),
    )
    .unwrap();
    assert!(dry["reclaim"].is_null(), "{dry}");
    assert_eq!(dry["resolved"].as_array().unwrap().len(), 3);
}

#[test]
fn space_of_a_regular_file_is_not_a_directory() {
    let f = fixture_with(
        &[&[]],
        Some(config(SnapAcctMode::Auto, Duration::from_secs(2))),
    );
    populate(&f);
    let err = call(&f, "snapshot.space", json!({"path": "/vol/f0"})).unwrap_err();
    assert!(err.message.contains("not a directory"), "{err}");
    let err = call(&f, "snapshot.space", json!({"path": "/nope"})).unwrap_err();
    assert!(err.message.contains("no such directory"), "{err}");
}
