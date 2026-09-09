//! A cluster-wide singleton lease: at most one node runs the guarded
//! background job at a time. Extracted from the GC lease dance (plan 22,
//! Step 4) so bucket GC and the pruner share one CAS-over-S3 primitive
//! instead of copy-pasting it.

use anyhow::{bail, Result};
use constellation_store_s3::{Lease, LeaseMode, LeaseStore, LeaseTag};
use object_store::ObjectStore;
use std::sync::Arc;

/// A held singleton lease. Drop-safe only via [`Self::release`]; a leak
/// is corrected by the lease TTL expiring, so a crashed holder never
/// wedges the job permanently.
pub struct SingletonLease {
    store: LeaseStore,
    lease: Lease,
    tag: LeaseTag,
}

impl SingletonLease {
    /// Acquire the named singleton (`_gc`, `_prune`, …) or fail loudly if
    /// a live holder still owns it. `name` is the lease partition key.
    pub async fn acquire(
        store: Arc<dyn ObjectStore>,
        name: &'static str,
        mode: LeaseMode,
    ) -> Result<Self> {
        let leases = LeaseStore::new(store, name, mode);
        let now = constellation_store_s3::lease::now_unix_ms();
        // Holder id: pid in the high half, a low-entropy nonce in the low
        // half, matching the GC lease's construction.
        let holder = (std::process::id() as u64) << 32 | now as u64 & 0xffff_ffff;
        let ttl = constellation_store_s3::lease::lease_ttl_ms();
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
        })
    }

    /// Best-effort release. A failure is harmless: the TTL reclaims it.
    pub async fn release(self) {
        let _ = self.store.try_swap(&self.lease.released(), &self.tag).await;
    }
}
