//! A cluster-wide singleton lease: at most one node runs the guarded
//! background job at a time. Extracted from the GC lease dance (plan 22,
//! Step 4) so bucket GC and the pruner share one CAS-over-S3 primitive
//! instead of copy-pasting it.
//!
//! # Renewal and fencing
//!
//! A GC round outlives one lease TTL by construction: it publishes its
//! condemned list, waits a full TTL, and only then deletes (DESIGN.md
//! §14). Without renewal the lease had lapsed by the time the round
//! deleted, so a second round — every daemon runs its own daily tick —
//! could take the lease and publish a *new* condemned pointer while the
//! first was still deleting. Every destructive step of a holder is
//! therefore fenced on the lease object itself: [`SingletonLease::renew`]
//! is a CAS against the ETag of the object this holder last wrote, and a
//! conflict means another holder took the lease in between. The caller
//! stops at once. This never depends on the clock: a round that paused
//! for minutes finds its next renewal refused, whatever its own notion of
//! time says.
//!
//! In [`LeaseMode::SingleWriter`] (a backend without `If-Match`) the swap
//! is unconditional and exclusion is assumed, as everywhere else in that
//! mode.

use anyhow::{bail, Context, Result};
use constellation_store_s3::{Lease, LeaseMode, LeaseStore, LeaseTag, StoreError};
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// Another holder took the singleton lease: the caller must not take
/// another destructive step.
#[derive(Debug, thiserror::Error)]
#[error("{name} singleton lease was taken by another holder; this round is fenced")]
pub struct Fenced {
    pub name: &'static str,
}

/// A held singleton lease. Drop-safe only via [`Self::release`]; a leak
/// is corrected by the lease TTL expiring, so a crashed holder never
/// wedges the job permanently.
pub struct SingletonLease {
    store: LeaseStore,
    lease: Lease,
    tag: LeaseTag,
    name: &'static str,
    ttl_ms: u64,
}

impl SingletonLease {
    /// Acquire the named singleton (`_gc`, `_prune`, …) or fail loudly if
    /// a live holder still owns it. `name` is the lease partition key.
    pub async fn acquire(
        store: Arc<dyn ObjectStore>,
        name: &'static str,
        mode: LeaseMode,
    ) -> Result<Self> {
        let ttl = constellation_store_s3::lease::lease_ttl_ms();
        Self::acquire_with_ttl(store, name, mode, ttl).await
    }

    /// [`Self::acquire`] with an explicit TTL (tests expire leases fast).
    pub async fn acquire_with_ttl(
        store: Arc<dyn ObjectStore>,
        name: &'static str,
        mode: LeaseMode,
        ttl_ms: u64,
    ) -> Result<Self> {
        let leases = LeaseStore::new(store, name, mode);
        let now = constellation_store_s3::lease::now_unix_ms();
        // Holder id: pid in the high half, a low-entropy nonce in the low
        // half, matching the GC lease's construction.
        let holder = (std::process::id() as u64) << 32 | now as u64 & 0xffff_ffff;
        let ttl = ttl_ms.max(1);
        let (lease, tag) = match leases.get().await? {
            None => {
                let lease = Lease::granted(name, holder, 1, ttl);
                let tag = leases.try_create(&lease).await?;
                (lease, tag)
            }
            Some((previous, tag)) if previous.is_claimable(now) => {
                let lease = Lease::granted(name, holder, previous.epoch + 1, ttl);
                let tag = leases.try_swap(&lease, &tag).await?;
                (lease, tag)
            }
            Some((previous, _)) => {
                bail!(
                    "{name} singleton lease is held by {} for another {} ms",
                    previous.holder,
                    previous.expires_in_ms(now)
                )
            }
        };
        Ok(Self {
            store: leases,
            lease,
            tag,
            name,
            ttl_ms: ttl,
        })
    }

    /// The lease's epoch (bumped on every holder change).
    pub fn epoch(&self) -> u64 {
        self.lease.epoch
    }

    /// Push the expiry out by one TTL, by a CAS on the object this holder
    /// last wrote. [`Fenced`] when another holder replaced it: the caller
    /// must stop. Any other error is the store's.
    ///
    /// This is the fence every destructive step runs behind: a successful
    /// renewal proves that, at the moment the store applied the CAS, no
    /// other holder had taken the lease since this one acquired or last
    /// renewed it.
    pub async fn renew(&mut self) -> Result<()> {
        let renewed = self.lease.renewed(self.ttl_ms);
        match self.store.try_swap(&renewed, &self.tag).await {
            Ok(tag) => {
                self.lease = renewed;
                self.tag = tag;
                Ok(())
            }
            Err(StoreError::CasConflict) => Err(Fenced { name: self.name }.into()),
            Err(error) => Err(error).with_context(|| format!("renewing the {} lease", self.name)),
        }
    }

    /// Wait at least `wait`, renewing the lease along the way so it never
    /// lapses during a long grace period. Renewals happen every third of
    /// the TTL; the wait's own length is unaffected. [`Fenced`] the moment
    /// a renewal is refused.
    pub async fn hold_for(&mut self, wait: Duration) -> Result<()> {
        let slice = Duration::from_millis((self.ttl_ms / 3).max(1));
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            tokio::time::sleep(slice.min(deadline - now)).await;
            self.renew().await?;
        }
        Ok(())
    }

    /// Best-effort release. A failure is harmless: the TTL reclaims it.
    pub async fn release(self) {
        let _ = self.store.try_swap(&self.lease.released(), &self.tag).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// A holder whose lease lapsed (it paused, or its wait outlived the
    /// TTL) and was taken by another holder is fenced at its next
    /// renewal; the new holder renews freely. Nothing here depends on the
    /// old holder noticing the time: the CAS on the lease object decides.
    #[tokio::test]
    async fn a_lapsed_lease_taken_by_another_holder_fences_the_first() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut first = SingletonLease::acquire_with_ttl(store.clone(), "_gc", LeaseMode::Cas, 20)
            .await
            .unwrap();
        first.renew().await.unwrap();
        // The first holder keeps the lease while it renews on time.
        assert!(
            SingletonLease::acquire_with_ttl(store.clone(), "_gc", LeaseMode::Cas, 20)
                .await
                .is_err(),
            "a live lease is not claimable"
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        let mut second = SingletonLease::acquire_with_ttl(store.clone(), "_gc", LeaseMode::Cas, 20)
            .await
            .expect("an expired lease is claimable");
        assert_eq!(second.epoch(), first.epoch() + 1);
        let error = first.renew().await.unwrap_err();
        assert!(error.downcast_ref::<Fenced>().is_some(), "{error:#}");
        assert!(first.hold_for(Duration::from_millis(5)).await.is_err());
        second.renew().await.unwrap();
        second.hold_for(Duration::from_millis(50)).await.unwrap();
        // The lease never lapsed during the hold: still not claimable.
        assert!(
            SingletonLease::acquire_with_ttl(store.clone(), "_gc", LeaseMode::Cas, 20)
                .await
                .is_err()
        );
        second.release().await;
        SingletonLease::acquire_with_ttl(store, "_gc", LeaseMode::Cas, 20)
            .await
            .expect("a released lease is claimable");
    }
}
