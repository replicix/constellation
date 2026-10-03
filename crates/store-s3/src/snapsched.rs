//! The snapshot scheduler's audit trail in the bucket (plan 32 Step 4.1
//! item 4): one object per scheduler tick that did something,
//! `snapsched/journal/<ts>-<nonce>.json`, written create-if-absent the
//! way the GC journal is (`crate::gc::append_journal`), so a lost reply
//! retried lands on its own object and never overwrites another.
//!
//! An entry names, per policy root the tick touched, the root's inode,
//! its path then, the canonical policy, and what happened: the snapshots
//! it created, the creations it skipped (an unchanged subtree, or a name
//! another leader already took), the creations that failed, and the
//! snapshots it deleted. The scheduler's creation step (plan 32 M3)
//! writes `created`/`skipped`/`failed`; expiry (M4) adds `deleted` to the
//! same object. Nothing in the system reads these back to decide
//! anything: they are the record an operator (or a harness oracle)
//! replays retention against.
//!
//! It also holds the scheduler's one piece of *decision* state,
//! `snapsched/state.json` ([`SnapSchedState`], plan 32 Step 4.3): per
//! policy root, the canonical policy last seen and since when, and the
//! policies it replaced while their grace window is still open. Only the
//! `_snapsched` leader writes it, with a CAS on the ETag it read
//! ([`save_state`]); a lost CAS is [`StoreError::CasConflict`], and the
//! scheduler then deletes nothing in that run.

use crate::error::StoreError;
use crate::lease::LeaseMode;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// One tick's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalEntry {
    /// When the tick ran, by the leader's clock (Unix ms).
    pub ts: i64,
    /// The leading node.
    pub node: u64,
    pub roots: Vec<SnapSchedJournalRoot>,
}

/// What one tick did to one policy root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalRoot {
    pub root_ino: u64,
    /// The root's path at the tick (it may be renamed later; the inode is
    /// the identity).
    pub path: String,
    /// The policy in canonical form.
    pub policy: String,
    #[serde(default)]
    pub created: Vec<SnapSchedJournalSnap>,
    #[serde(default)]
    pub skipped: Vec<SnapSchedJournalSkip>,
    #[serde(default)]
    pub failed: Vec<SnapSchedJournalSkip>,
    #[serde(default)]
    pub deleted: Vec<SnapSchedJournalSnap>,
}

/// A snapshot created (or deleted) by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalSnap {
    pub id: String,
    pub name: String,
    pub created_unix_ms: i64,
    /// Why a deleted snapshot went (`no tier keeps it`, `policy removed
    /// with --expire`); `null` on a creation. No serde attribute: always
    /// written (`null` included). serde's derive itself reads an absent
    /// `Option` as `None`, so a journal object from before M4a still
    /// decodes, as a creation; nothing relies on that.
    pub reason: Option<String>,
}

/// A creation that did not produce a snapshot, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalSkip {
    pub name: String,
    pub reason: String,
}

/// `snapsched/journal/<ts as 16 hex digits>-<nonce>.json`: lists in time
/// order, like `gc/journal/`.
pub fn journal_key(ts: i64, nonce: &str) -> Path {
    Path::from(format!("snapsched/journal/{ts:016x}-{nonce}.json"))
}

pub fn journal_prefix() -> Path {
    Path::from("snapsched/journal")
}

/// Write `entry` under a fresh key, create-if-absent (a 409 retries; an
/// object already there can only be this write landed behind a lost
/// reply). Returns the key.
pub async fn append_journal(
    store: &Arc<dyn ObjectStore>,
    entry: &SnapSchedJournalEntry,
) -> Result<Path, StoreError> {
    let key = journal_key(entry.ts, &uuid::Uuid::new_v4().simple().to_string());
    crate::cas::create_content_addressed(store.as_ref(), &key, serde_json::to_vec(entry)?.into())
        .await?;
    Ok(key)
}

/// `snapsched/state.json`'s format. A reader refuses any other version
/// (fail closed: a scheduler that cannot read the grace state deletes
/// nothing). Bumped, never migrated.
pub const STATE_VERSION: u32 = 2;

/// The grace state (plan 32 Step 4.3), one object for the cluster.
///
/// ```json
/// {"version": 2, "updated_unix_ms": 1790603759000, "node": 2,
///  "roots": {"1099511627777": {
///     "canonical": "10s:1m 1m:4m; last=2",
///     "since_unix_ms": 1790600000000,
///     "prior": [{"canonical": "10s:1m 1m:1h",
///                "replaced_unix_ms": 1790600000000,
///                "until_unix_ms": 1790686400000}]}}}
/// ```
///
/// `canonical` is the policy's canonical form **without** `paused`:
/// pausing and resuming is not a policy change. `prior` lists the
/// policies `canonical` replaced whose grace window may still be open, in
/// the order they were replaced; a `canonical: null` prior is "nothing
/// was known before" (the root was first seen at `replaced_unix_ms`), and
/// keeps everything until its window closes. Times are the writing
/// leader's clock. `until_unix_ms` is `replaced_unix_ms` plus the
/// recording leader's `CONSTELLATION_SNAPSCHED_GRACE_S`: a reader keeps
/// the window open while `now < max(until_unix_ms, replaced_unix_ms +
/// its own grace)`, so a leader configured with a shorter grace never
/// closes a window recorded under a longer one.
///
/// Version 2 added `until_unix_ms`; a version 1 object is refused (no
/// migration), so a scheduler that meets one deletes nothing until it
/// is removed by hand. Every field is required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedState {
    pub version: u32,
    /// When, and by which node, it was last written. Informational.
    pub updated_unix_ms: i64,
    pub node: u64,
    /// By policy root inode.
    pub roots: BTreeMap<u64, SnapSchedRootGrace>,
}

impl Default for SnapSchedState {
    fn default() -> Self {
        SnapSchedState {
            version: STATE_VERSION,
            updated_unix_ms: 0,
            node: 0,
            roots: BTreeMap::new(),
        }
    }
}

/// One policy root's grace record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedRootGrace {
    /// The canonical policy last seen, without `paused`.
    pub canonical: String,
    /// When the scheduler first saw `canonical` on this root.
    pub since_unix_ms: i64,
    pub prior: Vec<SnapSchedPrior>,
}

/// A policy `canonical` replaced (or, `None`, the "unknown" before a
/// root's first sighting) and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedPrior {
    pub canonical: Option<String>,
    pub replaced_unix_ms: i64,
    /// When its grace window closes by the recording leader's grace
    /// length (`replaced_unix_ms + GRACE_S`).
    pub until_unix_ms: i64,
}

/// What [`save_state`] must find in the bucket: the version [`load_state`]
/// read, or nothing (`None`: there was no object).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateTag(Option<UpdateVersion>);

pub fn state_key() -> Path {
    Path::from("snapsched/state.json")
}

/// Read the grace state. No object yet is the empty state (and a tag
/// that makes [`save_state`] a create-if-absent). A body that does not
/// parse, or another [`STATE_VERSION`], is an error.
pub async fn load_state(
    store: &Arc<dyn ObjectStore>,
) -> Result<(SnapSchedState, StateTag), StoreError> {
    let key = state_key();
    match crate::control::get_json::<serde_json::Value>(store.as_ref(), &key).await? {
        None => Ok((SnapSchedState::default(), StateTag(None))),
        Some((value, meta)) => {
            // The version first, so an older format is refused by name
            // rather than as a missing field.
            let version = value.get("version").and_then(serde_json::Value::as_u64);
            if version != Some(u64::from(STATE_VERSION)) {
                return Err(StoreError::Meta(format!(
                    "{key}: format version {}, this binary reads {STATE_VERSION}",
                    version.map_or("none".to_string(), |v| v.to_string())
                )));
            }
            let state: SnapSchedState = serde_json::from_value(value)?;
            Ok((
                state,
                StateTag(Some(UpdateVersion {
                    e_tag: meta.e_tag,
                    version: meta.version,
                })),
            ))
        }
    }
}

/// Write the grace state, but only over the version `tag` names (or, for
/// a tag of no object, only if there still is none):
/// [`StoreError::CasConflict`] when another writer got there first. In
/// [`LeaseMode::SingleWriter`] (no `If-Match`) the update is
/// unconditional, as every swap is in that mode.
pub async fn save_state(
    store: &Arc<dyn ObjectStore>,
    mode: LeaseMode,
    state: &SnapSchedState,
    tag: &StateTag,
) -> Result<StateTag, StoreError> {
    let put = match (&tag.0, mode) {
        (None, _) => PutMode::Create,
        (Some(_), LeaseMode::SingleWriter) => PutMode::Overwrite,
        (Some(version), LeaseMode::Cas) => PutMode::Update(version.clone()),
    };
    let body = serde_json::to_vec(state)?;
    match crate::cas::put_conditional(
        store.as_ref(),
        &state_key(),
        body.into(),
        put,
        crate::cas::Verify::Body,
    )
    .await?
    {
        crate::cas::CasPut::Won(version) => Ok(StateTag(Some(version))),
        crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => Err(StoreError::CasConflict),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn entries_round_trip_and_list_in_time_order() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let entry = |ts| SnapSchedJournalEntry {
            ts,
            node: 3,
            roots: vec![SnapSchedJournalRoot {
                root_ino: 42,
                path: "/proj".into(),
                policy: "10s:1m".into(),
                created: vec![SnapSchedJournalSnap {
                    id: "i".into(),
                    name: "auto-20260928T140510Z".into(),
                    created_unix_ms: ts,
                    reason: None,
                }],
                skipped: vec![],
                failed: vec![],
                deleted: vec![],
            }],
        };
        let late = append_journal(&store, &entry(0x2000)).await.unwrap();
        let early = append_journal(&store, &entry(0x1000)).await.unwrap();
        let mut keys: Vec<Path> = store
            .list(Some(&journal_prefix()))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec![early.clone(), late]);
        let body = store.get(&early).await.unwrap().bytes().await.unwrap();
        let back: SnapSchedJournalEntry = serde_json::from_slice(&body).unwrap();
        assert_eq!(back, entry(0x1000));
    }

    #[tokio::test]
    async fn the_state_round_trips_and_a_stale_tag_conflicts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (empty, none) = load_state(&store).await.unwrap();
        assert_eq!(empty, SnapSchedState::default());
        let mut state = empty.clone();
        state.roots.insert(
            42,
            SnapSchedRootGrace {
                canonical: "10s:1m".into(),
                since_unix_ms: 7,
                prior: vec![SnapSchedPrior {
                    canonical: None,
                    replaced_unix_ms: 7,
                    until_unix_ms: 86_400_007,
                }],
            },
        );
        let first = save_state(&store, LeaseMode::Cas, &state, &none)
            .await
            .unwrap();
        // A second create-if-absent from the same empty read loses.
        let mut other = state.clone();
        other.node = 9;
        assert!(matches!(
            save_state(&store, LeaseMode::Cas, &other, &none).await,
            Err(StoreError::CasConflict)
        ));
        let (back, tag) = load_state(&store).await.unwrap();
        assert_eq!(back, state);
        assert_eq!(tag, first);
        state.roots.get_mut(&42).unwrap().prior.clear();
        let second = save_state(&store, LeaseMode::Cas, &state, &tag)
            .await
            .unwrap();
        // The tag read before that write is stale now.
        assert!(matches!(
            save_state(&store, LeaseMode::Cas, &other, &tag).await,
            Err(StoreError::CasConflict)
        ));
        assert_eq!(load_state(&store).await.unwrap().1, second);
        // The JSON shape: inode keys as strings, `null` for "unknown".
        let raw = store
            .get(&state_key())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(json["roots"]["42"]["canonical"], "10s:1m");
        let first = serde_json::to_value(&back).unwrap();
        assert_eq!(
            first["roots"]["42"]["prior"][0]["until_unix_ms"],
            86_400_007
        );
        // Every field is required: a body without `roots` is an error,
        // not an empty state.
        store
            .put(
                &state_key(),
                serde_json::to_vec(&serde_json::json!({
                    "version": STATE_VERSION, "updated_unix_ms": 0, "node": 0
                }))
                .unwrap()
                .into(),
            )
            .await
            .unwrap();
        assert!(load_state(&store).await.is_err());
        // Version 1 (no `until_unix_ms`) is refused by its version.
        let mut v1 = serde_json::to_value(&state).unwrap();
        v1["version"] = 1.into();
        store
            .put(&state_key(), serde_json::to_vec(&v1).unwrap().into())
            .await
            .unwrap();
        let error = load_state(&store).await.unwrap_err().to_string();
        assert!(error.contains("format version 1"), "{error}");
        // Another format version is refused, not guessed at.
        let mut future = state.clone();
        future.version = STATE_VERSION + 1;
        store
            .put(&state_key(), serde_json::to_vec(&future).unwrap().into())
            .await
            .unwrap();
        assert!(load_state(&store).await.is_err());
    }
}
