//! Plan 32 Step 5 over the control protocol, against a real (offline)
//! engine: `snapshot.resolve`'s selectors and `snapshot.delete_many`'s
//! one batch, its per-item hold refusals, its dry run and its `force`.

use super::ops::tests::{fixture, fixture_with};
use super::*;
use crate::snapacct::{SnapAcctConfig, SnapAcctMode};
use constellation_control::dispatch_in_process;
use serde_json::{json, Value};

fn call(f: &super::ops::tests::Fixture, method: &str, params: Value) -> Result<Value, String> {
    let router = router(&f.svc);
    f.rt.block_on(dispatch_in_process(
        &router,
        &Principal::InProcess,
        method,
        params,
    ))
    .map_err(|e| e.message)
}

fn names(listing: &Value) -> Vec<String> {
    listing
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            format!(
                "{}@{}",
                s["path"].as_str().unwrap(),
                s["name"].as_str().unwrap()
            )
        })
        .collect()
}

fn ids_of(listing: &Value) -> Vec<String> {
    listing
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn selectors_resolve_and_delete_many_refuses_holds_per_item() {
    // Auto accounting, whatever the environment says: the dry run estimates.
    let f = fixture_with(
        &[&[]],
        Some(SnapAcctConfig {
            mode: SnapAcctMode::Auto,
            ..SnapAcctConfig::from_env()
        }),
    );
    for dir in ["/vol", "/other"] {
        call(&f, "browse.mkdir", json!({"path": dir})).unwrap();
    }
    // Interleaved, so a range of `/vol`'s chain spans `/other`'s snapshots.
    for (i, selector) in ["/vol@a1", "/other@b1", "/vol@a2", "/other@b2", "/vol@a3"]
        .iter()
        .enumerate()
    {
        call(
            &f,
            "browse.write",
            json!({"path": format!("/vol/f{i}"), "data": "eA==", "create": true}),
        )
        .unwrap();
        let created = call(&f, "snapshot.create", json!({"selector": selector})).unwrap();
        assert!(
            created["snapshot"]["seq"].as_u64().is_some(),
            "the status carries its commit seq: {created}"
        );
    }

    let resolved = call(&f, "snapshot.resolve", json!({"selectors": ["/vol@a1%a3"]})).unwrap();
    assert_eq!(
        names(&resolved["snapshots"]),
        ["/vol@a1", "/vol@a2", "/vol@a3"]
    );
    let resolved = call(
        &f,
        "snapshot.resolve",
        json!({"selectors": ["/other@*", "/vol@a3", "/vol@a3"]}),
    )
    .unwrap();
    assert_eq!(
        names(&resolved["snapshots"]),
        ["/other@b1", "/other@b2", "/vol@a3"]
    );
    let missing = call(&f, "snapshot.resolve", json!({"selectors": ["/vol@nope"]})).unwrap_err();
    assert!(missing.contains("/vol@nope"), "{missing}");

    call(
        &f,
        "snapshot.hold",
        json!({"id": "/vol@a2", "held": true, "by": "user:ops"}),
    )
    .unwrap();

    // A dry run reports what it would refuse and deletes nothing.
    let dry = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/vol@a*"], "dry_run": true}),
    )
    .unwrap();
    assert_eq!(names(&dry["resolved"]), ["/vol@a1", "/vol@a2", "/vol@a3"]);
    assert_eq!(dry["deleted"], json!([]));
    assert_eq!(dry["refused"].as_array().unwrap().len(), 1, "{dry}");
    assert_eq!(
        dry["refused"][0]["reason"],
        "snapshot /vol@a2 is held by user:ops; `snapshot release` first (or pass --force)"
    );
    // The dry run's reclaim estimate (its numbers: `snapspace_tests`).
    assert!(dry["reclaim"].is_object(), "a dry run estimates: {dry}");
    let all = call(&f, "snapshot.list", json!({})).unwrap();
    assert_eq!(all["snapshots"].as_array().unwrap().len(), 5);

    // The range deletes around the held one.
    let ids = ids_of(&dry["resolved"]);
    let out = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/vol@a1%a3"]}),
    )
    .unwrap();
    assert_eq!(out["deleted"], json!([ids[0], ids[2]]));
    assert_eq!(out["refused"][0]["id"], json!(ids[1]));
    let left = call(&f, "snapshot.list", json!({})).unwrap();
    assert_eq!(
        names(&left["snapshots"]),
        ["/other@b1", "/other@b2", "/vol@a2"]
    );

    // `force` (the method is admin already) deletes the held one too, and
    // a bare id selects as well as a name.
    let out = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": [ids[1], "/other@*"], "force": true}),
    )
    .unwrap();
    assert_eq!(out["deleted"].as_array().unwrap().len(), 3, "{out}");
    assert_eq!(out["refused"], json!([]));
    let left = call(&f, "snapshot.list", json!({})).unwrap();
    assert_eq!(left["snapshots"], json!([]));
}

/// A snapshot named `a%b` before names lost `%` and `*` stays addressable
/// by its name through every single-name method: `snapshot.hold`,
/// `clone.create` (which takes no id) and `snapshot.delete`. Only new
/// snapshots, and the multi-snapshot selectors, refuse the characters.
#[test]
fn a_name_taken_before_the_rule_stays_addressable() {
    let f = fixture(&[&[]]);
    call(&f, "browse.mkdir", json!({"path": "/vol"})).unwrap();
    call(
        &f,
        "browse.write",
        json!({"path": "/vol/f", "data": "eA==", "create": true}),
    )
    .unwrap();
    let refused = call(&f, "snapshot.create", json!({"selector": "/vol@c%d"})).unwrap_err();
    assert!(refused.contains("'%'"), "{refused}");
    for name in ["a%b", "x*"] {
        f.rt.block_on(f.svc.snapshots.create_with_legacy_name("/vol", name))
            .unwrap();
    }

    let held = call(
        &f,
        "snapshot.hold",
        json!({"id": "/vol@a%b", "held": true, "by": "user:ops"}),
    )
    .unwrap();
    assert_eq!(held["snapshot"]["held_by"], "user:ops", "{held}");
    call(
        &f,
        "snapshot.hold",
        json!({"id": "/vol@a%b", "held": false, "by": "user:ops"}),
    )
    .unwrap();
    call(
        &f,
        "clone.create",
        json!({"selector": "/vol@a%b", "destination": "/copy"}),
    )
    .unwrap();
    let copied = call(&f, "browse.stat", json!({"path": "/copy/f"})).unwrap();
    assert!(copied.to_string().contains("\"size\":1"), "{copied}");
    for selector in ["/vol@a%b", "/vol@x*"] {
        call(&f, "snapshot.delete", json!({"selector": selector})).unwrap();
    }
    let left = call(&f, "snapshot.list", json!({})).unwrap();
    assert_eq!(left["snapshots"], json!([]));
}

/// More deletes than one peer frame carries go out as several batches,
/// in order, and the per-item results line up across the split.
#[test]
fn delete_many_splits_into_frame_sized_batches() {
    let f = fixture(&[&[]]);
    call(&f, "browse.mkdir", json!({"path": "/vol"})).unwrap();
    let total = constellation_net::MAX_SNAPSHOT_DELETES_PER_BATCH + 3;
    for i in 0..total {
        call(
            &f,
            "snapshot.create",
            json!({"selector": format!("/vol@auto-{i:04}")}),
        )
        .unwrap();
    }
    // A held one in the second batch: the refusal must name it, not a
    // neighbour shifted by the split.
    let held = format!(
        "/vol@auto-{:04}",
        constellation_net::MAX_SNAPSHOT_DELETES_PER_BATCH + 1
    );
    call(
        &f,
        "snapshot.hold",
        json!({"id": held, "held": true, "by": "user:ops"}),
    )
    .unwrap();
    let out = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/vol@auto-*"]}),
    )
    .unwrap();
    assert_eq!(out["resolved"].as_array().unwrap().len(), total);
    assert_eq!(out["deleted"].as_array().unwrap().len(), total - 1);
    let refused = out["refused"].as_array().unwrap();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(
        refused[0]["id"],
        json!(crate::snapshot::snapshot_id_of(&held).unwrap())
    );
    let left = call(&f, "snapshot.list", json!({})).unwrap();
    assert_eq!(names(&left["snapshots"]), [held]);
}

/// The CLI deletes the ids it confirmed; one deleted elsewhere since is
/// refused on its own, and the rest still go. A missing *name* still
/// fails the whole call before anything is deleted.
#[test]
fn a_confirmed_id_deleted_meanwhile_is_refused_alone() {
    let f = fixture(&[&[]]);
    call(&f, "browse.mkdir", json!({"path": "/vol"})).unwrap();
    for name in ["a", "b", "c"] {
        call(
            &f,
            "snapshot.create",
            json!({"selector": format!("/vol@{name}")}),
        )
        .unwrap();
    }
    let ids = ids_of(
        &call(&f, "snapshot.resolve", json!({"selectors": ["/vol@*"]})).unwrap()["snapshots"],
    );
    let missing = call(
        &f,
        "snapshot.delete_many",
        json!({"selectors": ["/vol@a", "/vol@gone"]}),
    )
    .unwrap_err();
    assert!(missing.contains("/vol@gone"), "{missing}");
    call(&f, "snapshot.delete", json!({"selector": "/vol@b"})).unwrap();

    let out = call(&f, "snapshot.delete_many", json!({"selectors": ids})).unwrap();
    assert_eq!(out["deleted"], json!([ids[0], ids[2]]), "{out}");
    assert_eq!(out["refused"].as_array().unwrap().len(), 1, "{out}");
    assert_eq!(out["refused"][0]["id"], json!(ids[1]));
    assert!(
        out["refused"][0]["reason"]
            .as_str()
            .unwrap()
            .starts_with("no such snapshot"),
        "{out}"
    );
    let left = call(&f, "snapshot.list", json!({})).unwrap();
    assert_eq!(left["snapshots"], json!([]));
}
