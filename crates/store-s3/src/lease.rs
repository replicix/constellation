//! Partition leases (DESIGN.md §4 "Leases (the authority mechanism)",
//! §5 "Write Authority: the One Rule").
//!
//! A lease is one small JSON object per partition, `leases/<part>.json`,
//! whose *only* commit primitive is a conditional write:
//!
//! - **create** (`If-None-Match: *`) claims a partition nobody has ever
//!   held,
//! - **swap** (`If-Match: <etag>`) renews it (same holder, same epoch),
//!   releases it, or takes it over from an expired/released holder.
//!
//! Safety comes entirely from the store rejecting a stale precondition:
//! two nodes racing for the same expired lease read the same etag, both
//! swap, and exactly one wins ([`StoreError::CasConflict`] for the
//! loser). No clock agreement is needed for *mutual exclusion* — only
//! for liveness (when may a lease be considered expired), and there the
//! TTL is deliberately coarse (~60 s) relative to any plausible skew.
//!
//! Every holder change bumps [`Lease::epoch`], which log segments carry
//! so a deposed holder's late flush is recognizable (fencing).
//!
//! ### Backends without `If-Match`
//!
//! Some backends (notably `object_store`'s `LocalFileSystem`, which the
//! host smoke lane uses) implement create-if-absent but not etag CAS.
//! Renew and takeover are impossible there, so [`LeaseStore`] can be
//! built in [`LeaseMode::SingleWriter`], where swaps degrade to
//! unconditional PUTs: the state machine above still runs and still
//! records holder/epoch, but mutual exclusion is *assumed*, not
//! enforced. Callers must refuse to steal a live foreign lease in that
//! mode and say so loudly — see `cli::lease`.

use crate::error::StoreError;
use crate::layout;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Lease TTL default; `CONSTELLATION_LEASE_TTL_MS` overrides it (tests
/// use short TTLs to exercise expiry and takeover).
pub const DEFAULT_LEASE_TTL_MS: u64 = 60_000;

/// Current lease encoding version.
pub const LEASE_VERSION: u32 = 1;

pub fn lease_ttl_ms() -> u64 {
    std::env::var("CONSTELLATION_LEASE_TTL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_LEASE_TTL_MS)
}

pub fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Write authority over one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub v: u32,
    pub partition: String,
    /// Node id of the holder (`nodes/<id>`); 0 means "never held".
    pub holder: u64,
    /// Monotonic per-partition counter, bumped on every holder change.
    /// Stamped into every log segment the holder ships (fencing).
    pub epoch: u64,
    pub expires_unix_ms: i64,
    /// Set by a clean unmount: the partition is immediately claimable,
    /// no TTL to wait out.
    pub released: bool,
    /// Node ids that have asked for this partition and are waiting, sorted
    /// and deduped. A would-be holder that finds the lease busy writes
    /// itself in here (the only field it may touch), which is what makes
    /// the holder's idle release *conditional*: leases are sticky, and a
    /// node that keeps writing never hands one back to nobody.
    ///
    /// The holder learns of a request the same way it learns of anything
    /// else — its next renew CAS fails against the edited object — so this
    /// costs no extra request on either side. Cleared by [`Lease::granted`]
    /// and [`Lease::released`]: the request has been answered. Preserved by
    /// [`Lease::renewed`], or a holder renewing would erase the very
    /// request it is supposed to act on.
    pub wanted_by: Vec<u64>,
    /// Plan 30 §M9: the holder's synchronous backups (node ids). Every
    /// acknowledgement the holder gives while `ack_policy` is `Backup`
    /// is held by each of them, so any one of them may seal the epoch
    /// and take the lease over without waiting for the TTL. Empty means
    /// no backup (today's behaviour). Changed only by the holder, by a
    /// CAS that bumps `config_version`.
    pub backups: Vec<u64>,
    /// Plan 30 §M9: bumped by every change of `backups`/`ack_policy`
    /// (and by `granted_delegations`), so two readers can tell which of
    /// two objects with the same holder and epoch is newer.
    pub config_version: u64,
    /// Plan 30 §M9: what an acknowledgement means under this tenure —
    /// see [`AckPolicy`]. A taker may claim an `S3` lease before it
    /// expires (the log-slot CAS fences the old holder), and a listed
    /// backup may claim a `Backup` one after sealing it.
    pub ack_policy: AckPolicy,
    /// Plan 30 §M9: set (by one CAS, before the tenure's first read
    /// delegation) once this tenure may have granted read delegations. A
    /// successor that takes the lease over *before* it expired must then
    /// wait out the previous tenure's grant horizon before it acknowledges
    /// any mutation (`crate::lease`'s M9 section in the design notes);
    /// with it clear there is nothing to wait for.
    pub granted_delegations: bool,
    /// Plan 30 §M10: node ids retired by an admin `leave --node-id`
    /// while this lease named them ([`fence_retired`]). Such a node never
    /// claims this lease again (`classify` refuses), whatever it believes
    /// it holds: the fence reaches a retired node the moment it reads the
    /// object, before any CAS, even if it has not yet seen its registry
    /// tombstone. Carried by every tenure that follows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired: Vec<u64>,
}

/// Plan 30 §M9: what an acknowledgement means under a tenure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckPolicy {
    /// Acked once journaled on the holder (today: Layer A only —
    /// requesters keep what they were acked and replay it by rid after a
    /// takeover). Takeover waits for the TTL.
    #[default]
    Local,
    /// Acked once every node in `backups` holds the batch (Layer B). A
    /// listed backup seals the epoch and takes over on heartbeat
    /// silence; the old holder cannot collect a write-all ack after the
    /// seal, so it cannot ack anything more.
    Backup,
    /// Acked once the record's segment is CAS-created in the log (Layer
    /// C, `ack=s3`). Any peer may take over on heartbeat silence: the
    /// next slot's CAS fences the old holder.
    S3,
}

impl Lease {
    /// A fresh grant to `holder` at `epoch`, valid for `ttl_ms`.
    pub fn granted(partition: &str, holder: u64, epoch: u64, ttl_ms: u64) -> Self {
        Self {
            v: LEASE_VERSION,
            partition: partition.to_string(),
            holder,
            epoch,
            expires_unix_ms: now_unix_ms() + ttl_ms as i64,
            released: false,
            wanted_by: Vec::new(),
            backups: Vec::new(),
            config_version: 0,
            ack_policy: AckPolicy::Local,
            granted_delegations: false,
            retired: Vec::new(),
        }
    }

    /// Plan 30 §M10: the fence an admin `leave --node-id` writes over a
    /// lease naming the retired `node`: the epoch bumps (every segment,
    /// renewal and flush re-claim of the old tenure is now stale), the
    /// lease expires at once and is not released (so the next taker ships
    /// an epoch marker, fencing the old tenure's late segments in the
    /// log), the backup set clears (no seal permit names the old
    /// tenure), and `node` joins [`Lease::retired`].
    pub fn fenced_for_retirement(&self, node: u64, now_ms: i64) -> Self {
        let mut retired = self.retired.clone();
        retired.push(node);
        retired.sort_unstable();
        retired.dedup();
        Self {
            // Cannot overflow: a lease read from the bucket is refused past
            // `MAX_EPOCH` (see `LeaseStore::get`), and every epoch in memory
            // descends from one that was.
            epoch: self.epoch.saturating_add(1),
            expires_unix_ms: now_ms.min(self.expires_unix_ms),
            released: false,
            wanted_by: Vec::new(),
            backups: Vec::new(),
            config_version: self.config_version + 1,
            ack_policy: AckPolicy::Local,
            retired,
            ..self.clone()
        }
    }

    /// Plan 30 §M9: this lease with a new backup set and acknowledgement
    /// policy, `config_version` bumped. Everything else is copied: a
    /// reconfiguration moves neither the holder nor the expiry.
    pub fn reconfigured(&self, backups: Vec<u64>, ack_policy: AckPolicy) -> Self {
        let mut backups = backups;
        backups.sort_unstable();
        backups.dedup();
        Self {
            backups,
            ack_policy,
            config_version: self.config_version + 1,
            ..self.clone()
        }
    }

    /// Plan 30 §M9: this lease marked as a tenure that grants read
    /// delegations (`config_version` bumped).
    pub fn with_granted_delegations(&self) -> Self {
        Self {
            granted_delegations: true,
            config_version: self.config_version + 1,
            ..self.clone()
        }
    }

    /// Same holder and epoch, pushed-out expiry. Any pending request in
    /// `wanted_by` survives: it is addressed to this holder and is only
    /// answered by releasing.
    pub fn renewed(&self, ttl_ms: u64) -> Self {
        Self {
            expires_unix_ms: now_unix_ms() + ttl_ms as i64,
            released: false,
            ..self.clone()
        }
    }

    /// Voluntary hand-back: holder and epoch are preserved as history,
    /// `released` makes the partition claimable without waiting. Requests
    /// are dropped — the partition is free, so there is nothing left to
    /// ask for, and a request carried into the next holder's tenure would
    /// make it release for a node that has long since moved on.
    pub fn released(&self) -> Self {
        Self {
            released: true,
            wanted_by: Vec::new(),
            ..self.clone()
        }
    }

    /// This lease with `node_id` recorded as waiting for it. Everything
    /// else — holder, epoch, expiry — is copied unchanged: a requester
    /// swaps the object, but it is not allowed to move the lease.
    pub fn wanting(&self, node_id: u64) -> Self {
        let mut wanted_by = self.wanted_by.clone();
        wanted_by.push(node_id);
        wanted_by.sort_unstable();
        wanted_by.dedup();
        Self {
            wanted_by,
            ..self.clone()
        }
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        now_ms >= self.expires_unix_ms
    }

    /// Claimable by anyone: never held, cleanly released, or expired.
    pub fn is_claimable(&self, now_ms: i64) -> bool {
        self.holder == 0 || self.released || self.is_expired(now_ms)
    }

    pub fn expires_in_ms(&self, now_ms: i64) -> i64 {
        // `expires_unix_ms` is whatever the bucket holds; status output
        // must not panic on an extreme one.
        self.expires_unix_ms.saturating_sub(now_ms)
    }
}

/// Largest lease epoch a lease read from the bucket may carry. Epochs
/// count holder changes, so a real one is nowhere near this; one past it
/// is corruption or hostile, and accepting it would let the next bump
/// wrap to a small value that collides with a genuinely old segment's
/// fencing epoch. Half the range keeps every `+ 1` in the crate exact.
pub const MAX_EPOCH: u64 = u64::MAX / 2;

/// The version token a swap must match. Thin wrapper so callers never
/// have to name `object_store` types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseTag(UpdateVersion);

impl LeaseTag {
    /// S3 object ETag, if the backend supplied one.
    pub fn etag(&self) -> Option<String> {
        self.0.e_tag.clone()
    }
}

/// How [`LeaseStore`] commits a swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMode {
    /// `If-Match` etag CAS: mutual exclusion enforced by the backend.
    Cas,
    /// Backend has no `If-Match`: swaps are unconditional PUTs and
    /// exclusion is assumed (single writer). Loud warning territory.
    SingleWriter,
}

/// Read/modify/write access to one partition's lease object.
///
/// `Clone` is cheap (an `Arc`, a `String`, a `Copy` enum) and is what lets
/// a renewal CAS run without holding the `cli` crate's keepers-map lock
/// (plan 30 M2b, see `cli::lease`'s module doc): the keeper clones its
/// store into an owned renewal job before the lock is dropped for the
/// CAS itself.
#[derive(Clone)]
pub struct LeaseStore {
    store: Arc<dyn ObjectStore>,
    partition: String,
    mode: LeaseMode,
}

impl LeaseStore {
    pub fn new(store: Arc<dyn ObjectStore>, partition: &str, mode: LeaseMode) -> Self {
        Self {
            store,
            partition: partition.to_string(),
            mode,
        }
    }

    pub fn mode(&self) -> LeaseMode {
        self.mode
    }

    pub fn partition(&self) -> &str {
        &self.partition
    }

    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Current lease plus the token needed to swap it; `None` when no
    /// node has ever claimed the partition.
    ///
    /// A body that does not parse is re-read promptly a few times before
    /// it is an error (`crate::control`: an emulator's GET can be torn by
    /// a concurrent CAS PUT; real S3's cannot).
    pub async fn get(&self) -> Result<Option<(Lease, LeaseTag)>, StoreError> {
        let path = layout::lease(&self.partition);
        let Some((lease, meta)) =
            crate::control::get_json::<Lease>(self.store.as_ref(), &path).await?
        else {
            return Ok(None);
        };
        if lease.epoch > MAX_EPOCH {
            return Err(StoreError::Meta(format!(
                "lease {path}: implausible epoch {}",
                lease.epoch
            )));
        }
        let tag = LeaseTag(UpdateVersion {
            e_tag: meta.e_tag,
            version: meta.version,
        });
        Ok(Some((lease, tag)))
    }

    /// Claim a partition that has no lease object at all.
    /// [`StoreError::CasConflict`] means somebody created it first.
    pub async fn try_create(&self, lease: &Lease) -> Result<LeaseTag, StoreError> {
        self.put(lease, PutMode::Create).await
    }

    /// Replace the lease, but only if it still has version `tag`.
    /// [`StoreError::CasConflict`] means another node changed it since
    /// the read — in [`LeaseMode::SingleWriter`] this cannot be
    /// detected and the write always lands.
    pub async fn try_swap(&self, lease: &Lease, tag: &LeaseTag) -> Result<LeaseTag, StoreError> {
        let mode = match self.mode {
            LeaseMode::Cas => PutMode::Update(tag.0.clone()),
            LeaseMode::SingleWriter => PutMode::Overwrite,
        };
        self.put(lease, mode).await
    }

    /// One conditional write, with plan 30 §M4's error-code rules
    /// (`crate::cas`): a 409 retries the same attempt, a 412 or a 404 on
    /// `If-Match` is [`StoreError::CasConflict`] (the caller re-reads) —
    /// unless the object turns out to be this very lease, byte for byte,
    /// in which case an earlier attempt of ours landed (a 5xx retried by
    /// `object_store`, or a timed-out reply) and the write is ours. A lease
    /// body carries its holder, epoch and a millisecond expiry, so no other
    /// writer can have produced the same bytes.
    async fn put(&self, lease: &Lease, mode: PutMode) -> Result<LeaseTag, StoreError> {
        let body = serde_json::to_vec(lease)?;
        match crate::cas::put_conditional(
            self.store.as_ref(),
            &layout::lease(&self.partition),
            body.into(),
            mode,
            crate::cas::Verify::Body,
        )
        .await?
        {
            crate::cas::CasPut::Won(version) => Ok(LeaseTag(version)),
            crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => Err(StoreError::CasConflict),
        }
    }
}

/// Partition ids whose lease is currently held by `node_id` (not
/// released, not expired). Used by admin `leave --node-id` to refuse
/// retiring a node that still appears to own write authority.
/// Plan 30 §M10: fence every lease that names `node` (admin `leave
/// --node-id`): CAS it to [`Lease::fenced_for_retirement`]. Returns the
/// partitions fenced. A lost race re-reads and retries; a released lease
/// is fenced too (its `retired` list is what stops the node re-claiming
/// it).
pub async fn fence_retired(
    store: Arc<dyn ObjectStore>,
    node: u64,
) -> Result<Vec<String>, StoreError> {
    use futures::TryStreamExt;
    let prefix = object_store::path::Path::from("leases");
    let metas = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    let mut out = Vec::new();
    for m in metas {
        let Some(partition) = m
            .location
            .filename()
            .and_then(|f| f.strip_suffix(".json"))
            .map(str::to_string)
        else {
            continue;
        };
        let leases = LeaseStore::new(store.clone(), &partition, LeaseMode::Cas);
        for _ in 0..5 {
            let Some((lease, tag)) = leases.get().await? else {
                break;
            };
            if lease.holder != node || lease.retired.contains(&node) {
                break;
            }
            let fenced = lease.fenced_for_retirement(node, now_unix_ms());
            match leases.try_swap(&fenced, &tag).await {
                Ok(_) => {
                    out.push(partition.clone());
                    break;
                }
                Err(StoreError::CasConflict) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    out.sort();
    Ok(out)
}

pub async fn live_leases_held_by(
    store: Arc<dyn ObjectStore>,
    node_id: u64,
) -> Result<Vec<String>, StoreError> {
    use futures::TryStreamExt;
    let prefix = object_store::path::Path::from("leases");
    let metas = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    let now = now_unix_ms();
    let mut out = Vec::new();
    for m in metas {
        let Ok(res) = store.get(&m.location).await else {
            continue;
        };
        let Ok(bytes) = res.bytes().await else {
            continue;
        };
        let Ok(lease) = serde_json::from_slice::<Lease>(&bytes) else {
            continue;
        };
        if lease.epoch > MAX_EPOCH {
            continue;
        }
        if lease.holder == node_id && !lease.is_claimable(now) {
            out.push(lease.partition);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    const P: &str = "p0";
    const TTL: u64 = 60_000;

    fn ls(mode: LeaseMode) -> LeaseStore {
        LeaseStore::new(Arc::new(InMemory::new()), P, mode)
    }

    /// The torn lease GET seen on floci while another node's CAS PUT
    /// was in flight (`json: EOF while parsing an object`): re-read at
    /// once, not reported.
    #[tokio::test]
    async fn a_torn_lease_read_is_retried_promptly() {
        let faulty = FaultyStore::new();
        let s = LeaseStore::new(faulty.clone(), P, LeaseMode::Cas);
        let tag = s.try_create(&Lease::granted(P, 7, 1, TTL)).await.unwrap();
        faulty.script(OpKind::Get, "leases", Calls::Nth(1), Fault::Truncated(40));
        let started = std::time::Instant::now();
        let (lease, read_tag) = s.get().await.unwrap().unwrap();
        assert_eq!((lease.holder, lease.epoch), (7, 1));
        assert_eq!(read_tag, tag);
        assert_eq!(faulty.calls(OpKind::Get, "leases"), 2);
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
    }

    /// A lease object carrying an epoch no holder change could have
    /// reached is refused, not bumped past `u64::MAX` by the next takeover.
    #[tokio::test]
    async fn an_implausible_lease_epoch_is_refused() {
        let s = ls(LeaseMode::Cas);
        let bad = Lease::granted(P, 1, u64::MAX, 1_000);
        s.store
            .put(
                &crate::layout::lease(P),
                object_store::PutPayload::from(serde_json::to_vec(&bad).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(s.get().await, Err(StoreError::Meta(_))));
        let fine = Lease::granted(P, 1, MAX_EPOCH, 1_000);
        s.store
            .put(
                &crate::layout::lease(P),
                object_store::PutPayload::from(serde_json::to_vec(&fine).unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.epoch, MAX_EPOCH);
        assert_eq!(fine.fenced_for_retirement(1, 0).epoch, MAX_EPOCH + 1);
        assert_eq!(
            Lease::granted(P, 1, 1, 1).expires_in_ms(i64::MIN),
            i64::MAX,
            "status arithmetic saturates on an extreme expiry"
        );
    }

    #[tokio::test]
    async fn create_then_renew_keeps_holder_and_epoch() {
        let s = ls(LeaseMode::Cas);
        assert!(s.get().await.unwrap().is_none());
        let tag = s.try_create(&Lease::granted(P, 7, 1, TTL)).await.unwrap();
        let (lease, read_tag) = s.get().await.unwrap().unwrap();
        assert_eq!((lease.holder, lease.epoch, lease.released), (7, 1, false));
        assert_eq!(read_tag, tag);
        assert!(!lease.is_claimable(now_unix_ms()));

        s.try_swap(&lease.renewed(TTL), &read_tag).await.unwrap();
        let (renewed, _) = s.get().await.unwrap().unwrap();
        assert_eq!((renewed.holder, renewed.epoch), (7, 1));
        assert!(renewed.expires_unix_ms >= lease.expires_unix_ms);
    }

    #[tokio::test]
    async fn second_create_conflicts() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        assert!(matches!(
            s.try_create(&Lease::granted(P, 2, 1, TTL)).await,
            Err(StoreError::CasConflict)
        ));
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 1);
    }

    #[tokio::test]
    async fn expired_lease_is_taken_over_with_epoch_bump() {
        let s = ls(LeaseMode::Cas);
        // TTL of 0 ms: expired the instant it is written.
        s.try_create(&Lease::granted(P, 1, 4, 0)).await.unwrap();
        let (old, tag) = s.get().await.unwrap().unwrap();
        assert!(old.is_expired(now_unix_ms()));
        assert!(old.is_claimable(now_unix_ms()));
        s.try_swap(&Lease::granted(P, 2, old.epoch + 1, TTL), &tag)
            .await
            .unwrap();
        let (new, _) = s.get().await.unwrap().unwrap();
        assert_eq!((new.holder, new.epoch), (2, 5));
        assert!(!new.is_claimable(now_unix_ms()));
    }

    #[tokio::test]
    async fn release_makes_it_claimable_before_expiry() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (held, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&held.released(), &tag).await.unwrap();
        let (rel, _) = s.get().await.unwrap().unwrap();
        assert!(rel.released && !rel.is_expired(now_unix_ms()));
        assert!(rel.is_claimable(now_unix_ms()));
        // History is preserved so the next holder can bump the epoch.
        assert_eq!((rel.holder, rel.epoch), (1, 1));
    }

    /// Two nodes read the same expired lease and both try to take it:
    /// exactly one swap lands, the loser sees `CasConflict`.
    #[tokio::test]
    async fn concurrent_takeover_has_exactly_one_winner() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, 0)).await.unwrap();
        let (_, tag_a) = s.get().await.unwrap().unwrap();
        let (_, tag_b) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 2, 2, TTL), &tag_a)
            .await
            .unwrap();
        assert!(matches!(
            s.try_swap(&Lease::granted(P, 3, 2, TTL), &tag_b).await,
            Err(StoreError::CasConflict)
        ));
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 2);
    }

    /// A deposed holder's renew must fail: its tag is stale.
    #[tokio::test]
    async fn stale_tag_renew_is_refused() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, 0)).await.unwrap();
        let (mine, my_tag) = s.get().await.unwrap().unwrap();
        let (_, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 2, 2, TTL), &tag)
            .await
            .unwrap();
        assert!(matches!(
            s.try_swap(&mine.renewed(TTL), &my_tag).await,
            Err(StoreError::CasConflict)
        ));
    }

    /// Without `If-Match` a swap cannot be refused; the fallback mode
    /// documents that by overwriting unconditionally.
    #[tokio::test]
    async fn single_writer_mode_swaps_unconditionally() {
        let s = ls(LeaseMode::SingleWriter);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (_, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 1, 2, TTL), &tag)
            .await
            .unwrap();
        // Same (now stale) tag still succeeds: no enforcement.
        s.try_swap(&Lease::granted(P, 1, 3, TTL), &tag)
            .await
            .unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.epoch, 3);
        // Create is still conditional even in this mode.
        assert!(matches!(
            s.try_create(&Lease::granted(P, 2, 1, TTL)).await,
            Err(StoreError::CasConflict)
        ));
    }

    /// The sticky-lease handshake in one object: a requester may add
    /// itself, a renewal must carry that request forward (erasing it would
    /// make the holder deaf to the only signal a peer has when P2P is
    /// down), and both ways of parting with the lease answer it.
    #[tokio::test]
    async fn renew_preserves_wanted_by_while_grant_and_release_clear_it() {
        let granted = Lease::granted(P, 7, 1, TTL);
        assert!(granted.wanted_by.is_empty());

        let wanted = granted.wanting(9).wanting(3).wanting(9);
        assert_eq!(wanted.wanted_by, vec![3, 9], "sorted and deduped");
        assert_eq!(
            (wanted.holder, wanted.epoch, wanted.expires_unix_ms),
            (granted.holder, granted.epoch, granted.expires_unix_ms),
            "a requester may not move the lease, only sign the waiting list"
        );

        assert_eq!(wanted.renewed(TTL).wanted_by, vec![3, 9]);
        assert!(wanted.released().wanted_by.is_empty());
        assert!(Lease::granted(P, 9, wanted.epoch + 1, TTL)
            .wanted_by
            .is_empty());

        // And through the store, since that is where it has to survive.
        let s = ls(LeaseMode::Cas);
        s.try_create(&granted).await.unwrap();
        let (cur, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&cur.wanting(9), &tag).await.unwrap();
        let (edited, tag) = s.get().await.unwrap().unwrap();
        assert_eq!((edited.holder, edited.epoch), (7, 1));
        assert_eq!(edited.wanted_by, vec![9]);
        s.try_swap(&edited.renewed(TTL), &tag).await.unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.wanted_by, vec![9]);
    }

    #[tokio::test]
    async fn forward_compatible_decode() {
        let s = ls(LeaseMode::Cas);
        s.store
            .put(
                &layout::lease(P),
                PutPayload::from(
                    br#"{"v":9,"partition":"p0","holder":5,"epoch":3,
                         "expires_unix_ms":1,"released":false,"future":42}"#
                        .to_vec(),
                ),
            )
            .await
            .unwrap();
        let (lease, _) = s.get().await.unwrap().unwrap();
        assert_eq!((lease.holder, lease.epoch), (5, 3));
        // Missing fields default rather than failing the mount.
        let s2 = ls(LeaseMode::Cas);
        s2.store
            .put(&layout::lease(P), PutPayload::from(br#"{}"#.to_vec()))
            .await
            .unwrap();
        let (empty, _) = s2.get().await.unwrap().unwrap();
        assert_eq!(empty.holder, 0);
        assert!(empty.is_claimable(now_unix_ms()));
    }

    // ---- plan 30 §M4 item 1: each error code, at both lease CAS sites ----

    fn faulty(store: &Arc<FaultyStore>) -> LeaseStore {
        LeaseStore::new(store.clone(), P, LeaseMode::Cas)
    }

    /// 409 on the create: the same attempt is retried and lands.
    #[tokio::test]
    async fn create_retries_a_409() {
        let store = FaultyStore::new();
        store.script(OpKind::Put, "leases/", Calls::Nth(1), Fault::Status(409));
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 7, 1, TTL)).await.unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 7);
        assert_eq!(store.calls(OpKind::Put, "leases/"), 2);
    }

    /// 409 on a swap: retried as the same attempt, with the same etag.
    #[tokio::test]
    async fn swap_retries_a_409() {
        let store = FaultyStore::new();
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 7, 1, TTL)).await.unwrap();
        let (lease, tag) = s.get().await.unwrap().unwrap();
        store.script(OpKind::Put, "leases/", Calls::First(2), Fault::Status(409));
        s.try_swap(&lease.renewed(TTL), &tag).await.unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 7);
    }

    /// Our takeover landed but the reply was a 412 (a 5xx retried by
    /// `object_store` after the write was applied): it is our lease, and
    /// the returned tag is the live one, so the next renewal works.
    #[tokio::test]
    async fn a_takeover_that_landed_behind_a_412_is_won() {
        let store = FaultyStore::new();
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 1, 1, 0)).await.unwrap();
        let (old, tag) = s.get().await.unwrap().unwrap();
        store.script(
            OpKind::Put,
            "leases/",
            Calls::Nth(1),
            Fault::AppliedThen(412),
        );
        let mine = Lease::granted(P, 2, old.epoch + 1, TTL);
        let new_tag = s.try_swap(&mine, &tag).await.unwrap();
        s.try_swap(&mine.renewed(TTL), &new_tag).await.unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 2);
    }

    /// A create that landed behind a 412 is ours as well.
    #[tokio::test]
    async fn a_create_that_landed_behind_a_412_is_won() {
        let store = FaultyStore::new();
        store.script(
            OpKind::Put,
            "leases/",
            Calls::Nth(1),
            Fault::AppliedThen(412),
        );
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 7, 1, TTL)).await.unwrap();
    }

    /// A genuine 412 (another holder's object is there) is a lost race.
    #[tokio::test]
    async fn a_genuine_412_is_a_conflict() {
        let store = FaultyStore::new();
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 1, 1, 0)).await.unwrap();
        let (_, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 2, 2, TTL), &tag)
            .await
            .unwrap();
        assert!(matches!(
            s.try_swap(&Lease::granted(P, 3, 2, TTL), &tag).await,
            Err(StoreError::CasConflict)
        ));
    }

    /// 404 on `If-Match` (the lease object was deleted under us): a
    /// conflict, and the caller's re-read finds no lease.
    #[tokio::test]
    async fn a_404_on_if_match_is_a_conflict_and_the_reread_sees_nothing() {
        let store = FaultyStore::new();
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (lease, tag) = s.get().await.unwrap().unwrap();
        store.inner().delete(&layout::lease(P)).await.unwrap();
        assert!(matches!(
            s.try_swap(&lease.renewed(TTL), &tag).await,
            Err(StoreError::CasConflict)
        ));
        assert!(s.get().await.unwrap().is_none());
    }

    /// A scripted AWS-style 404 on `If-Match` (rewritten to `Precondition`
    /// by the S3 client) is classified the same way.
    #[tokio::test]
    async fn an_aws_style_404_is_a_conflict() {
        let store = FaultyStore::new();
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (lease, tag) = s.get().await.unwrap().unwrap();
        store.script(OpKind::Put, "leases/", Calls::Nth(1), Fault::Status(404));
        // Renew with a TTL far enough from the original that the two
        // millisecond expiries can never coincide (the own-write
        // recognition this exercises is byte equality, so a renewal
        // landing in the same millisecond as the grant would otherwise
        // make this flaky rather than a real conflict).
        assert!(matches!(
            s.try_swap(&lease.renewed(TTL + 10_000), &tag).await,
            Err(StoreError::CasConflict)
        ));
    }

    /// 500 and timeouts are transient errors, never a conflict: a renewal
    /// must not conclude it was deposed from them.
    #[tokio::test]
    async fn a_500_or_timeout_is_an_error_not_a_conflict() {
        let store = FaultyStore::new();
        let s = faulty(&store);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (lease, tag) = s.get().await.unwrap().unwrap();
        for fault in [Fault::Status(500), Fault::Timeout] {
            store.clear();
            store.script(OpKind::Put, "leases/", Calls::Nth(1), fault);
            match s.try_swap(&lease.renewed(TTL), &tag).await {
                Err(StoreError::ObjectStore(_)) => {}
                other => panic!("{fault:?}: {other:?}"),
            }
        }
    }
}
