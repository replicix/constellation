//! Plan 32 Step 5 / Step 7.6's space surfaces over the accounting index:
//! the sizes `snapshot.list` adds to each row, `snapshot.reclaim`,
//! `snapshot.space`, `snapshot.space.verify`, and the reclaim estimate of a
//! `snapshot.delete_many` dry run.
//!
//! Every number comes from [`SnapAcctService`]; this module changes types
//! and nothing else. The one figure computed here is the physical estimate,
//! a unit change of the service's own ratio (`physical_ratio` = stored per
//! logical byte = 1 / the service's logical-over-stored ratio).
//!
//! **Which calls cause accounting work.** Under `CONSTELLATION_SNAPACCT=auto`
//! the index is built on the first size *request*: `snapshot.reclaim`,
//! `snapshot.space`, `snapshot.space.verify`, a `snapshot.list` with
//! `sizes: true`, and a `delete_many` dry run. A plain `snapshot.list` (the
//! CSI driver's, a script's) only *peeks*: it fills the sizes when the index
//! is current and causes no work otherwise. A real `delete_many` asks for
//! nothing: its preview is the dry run before it.
//!
//! While the index does not match the snapshot rows, every answer says
//! `building` (with the share applied) instead of a number; with
//! accounting off, rows say `off` and the two space methods refuse with
//! `unsupported`.

use super::{unary, EngineControl};
use crate::snapacct::{SnapAcctService, SnapAnswer};
use constellation_control::methods::{SnapshotReclaim, SnapshotSpace, SnapshotSpaceVerify};
use constellation_control::proto::types as api;
use constellation_control::proto::ControlError;
use constellation_control::Router;
use constellation_meta::MetaStore;
use std::sync::Arc;

pub(super) fn register(r: &mut Router, svc: &Arc<EngineControl>) {
    unary::<SnapshotReclaim>(r, svc, |s, c, p| {
        // The hash list is a test aid with a cost (one string per chunk):
        // not for every viewer, and bounded.
        if p.list_chunks && c.role < constellation_control::authz::Role::Operator {
            return Err(ControlError::denied(
                "listing the reclaimable chunks (`list_chunks`) needs the operator role",
            )
            .with_remediation("ask without list_chunks for the estimate alone"));
        }
        let ids: Vec<String> = s
            .snapshot_resolve_rows(&p.selectors)
            .map_err(ControlError::failed)?
            .into_iter()
            .map(|row| row.id)
            .collect();
        let list_max = p.list_chunks.then_some(LIST_CHUNKS_MAX);
        s.reclaim_listed_of(&ids, list_max)?.ok_or_else(off)
    });
    unary::<SnapshotSpace>(r, svc, |s, _, p| s.snapshot_space(p.path.as_deref()));
    unary::<SnapshotSpaceVerify>(r, svc, |s, _, _| s.snapshot_space_verify());
}

/// The most hashes `snapshot.reclaim {list_chunks}` returns: about 6.7 MB
/// of JSON, inside one control frame (`MAX_FRAME_LEN`, 8 MiB). A larger
/// estimate is refused rather than listed.
pub(crate) const LIST_CHUNKS_MAX: usize = 100_000;

fn off() -> ControlError {
    ControlError::unsupported("snapshot accounting is off (CONSTELLATION_SNAPACCT=off)")
        .with_remediation("set CONSTELLATION_SNAPACCT=auto (the default) or on and restart")
}

fn internal(error: anyhow::Error) -> ControlError {
    ControlError::failed(format!("snapshot accounting: {error:#}"))
}

impl EngineControl {
    fn snapacct(&self) -> &Arc<SnapAcctService> {
        self.engine.snapacct()
    }

    /// Run one accounting query on the engine's runtime (the bodies run on
    /// blocking threads, as the rest of the service does).
    fn acct<T>(
        &self,
        f: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        tokio::task::block_in_place(|| self.rt.block_on(f))
    }

    /// Fill each row's sizes from the index (module docs: `demand` is a
    /// size request, otherwise a peek). Without `demand`, a missing or
    /// stale index leaves the rows as they are.
    pub(crate) fn fill_sizes(
        &self,
        rows: &mut [api::SnapshotStatus],
        demand: bool,
    ) -> Result<(), ControlError> {
        if rows.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
        let answer = self
            .acct(self.snapacct().snap_numbers_many(&ids, demand))
            .map_err(internal)?;
        match answer {
            SnapAnswer::Ready(mut numbers) => {
                for row in rows.iter_mut() {
                    // A row deleted between the listing and the query has
                    // no numbers: it is left without sizes, not zeroed.
                    let Some(n) = numbers.remove(&row.id) else {
                        continue;
                    };
                    row.used = Some(n.used);
                    row.written = Some(n.written);
                    row.refer = Some(n.refer);
                    row.lsize = Some(n.lsize);
                    row.as_of_seq = Some(n.as_of_seq);
                    row.as_of_ms = Some(n.as_of_ms);
                    row.size_state = Some(api::SizeState::Ok);
                }
            }
            SnapAnswer::Building { pct } if demand => {
                for row in rows.iter_mut() {
                    row.size_state = Some(api::SizeState::Building);
                    row.building_pct = Some(pct);
                }
            }
            SnapAnswer::Off if demand => {
                for row in rows.iter_mut() {
                    row.size_state = Some(api::SizeState::Off);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// `reclaim(D)` for these ids; `None` when accounting is off.
    pub(crate) fn reclaim_of(
        &self,
        ids: &[String],
    ) -> Result<Option<api::ReclaimEstimate>, ControlError> {
        self.reclaim_listed_of(ids, None)
    }

    /// [`Self::reclaim_of`], with the counted chunks' hashes when
    /// `list_max` is given (`snapshot.reclaim`'s `list_chunks` test aid);
    /// refused, without building the list past the bound, when more than
    /// `list_max` chunks count.
    pub(crate) fn reclaim_listed_of(
        &self,
        ids: &[String],
        list_max: Option<usize>,
    ) -> Result<Option<api::ReclaimEstimate>, ControlError> {
        let answer = self
            .acct(self.snapacct().reclaim_listed(ids, list_max))
            .map_err(internal)?;
        Ok(match answer {
            SnapAnswer::Ready((r, None)) if list_max.is_some() => {
                return Err(ControlError::invalid(format!(
                    "the estimate counts {} chunks; list_chunks lists at most {}",
                    r.chunks,
                    list_max.unwrap_or_default()
                ))
                .with_remediation("name fewer snapshots, or ask without list_chunks"));
            }
            SnapAnswer::Ready((r, hashes)) => Some(api::ReclaimEstimate {
                bytes: r.bytes,
                chunks: r.chunks,
                as_of_seq: r.as_of_seq,
                as_of_ms: r.as_of_ms,
                building: false,
                building_pct: 0,
                chunk_hashes: hashes
                    .map(|hashes| hashes.iter().map(|hash| hash.to_hex()).collect()),
            }),
            SnapAnswer::Building { pct } => Some(api::ReclaimEstimate {
                building: true,
                building_pct: pct,
                ..Default::default()
            }),
            SnapAnswer::Off => None,
        })
    }

    /// `snapshot.space`. `/` (or nothing) is the whole filesystem.
    pub(crate) fn snapshot_space(
        &self,
        path: Option<&str>,
    ) -> Result<api::SpaceBreakdown, ControlError> {
        let path = path.map(super::normalize_control_path).filter(|p| p != "/");
        if let Some(p) = &path {
            let failed = |e: constellation_meta::MetaError| ControlError::failed(format!("{e:#}"));
            let not_found = || ControlError::not_found(format!("no such directory: {p}"));
            let ino = self
                .meta
                .resolve_path(p)
                .map_err(failed)?
                .ok_or_else(not_found)?;
            let attr = self
                .meta
                .getattr(ino)
                .map_err(failed)?
                .ok_or_else(not_found)?;
            if attr.kind.as_u8() != constellation_fs_core::InodeKind::Dir.as_u8() {
                return Err(ControlError::invalid(format!("{p}: not a directory")));
            }
        }
        let acct = self.snapacct();
        let answer = self.acct(acct.space(path.as_deref())).map_err(internal)?;
        let amount = |a: crate::snapacct::Amount| api::SpaceAmount {
            bytes: a.bytes,
            chunks: a.chunks,
        };
        let mut out = api::SpaceBreakdown {
            path,
            gc_horizon_ms: acct.gc_horizon_ms(),
            estimate_pending: acct.recheck_pending(),
            ..Default::default()
        };
        match answer {
            SnapAnswer::Off => return Err(off()),
            SnapAnswer::Building { pct } => {
                out.building = true;
                out.building_pct = pct;
            }
            SnapAnswer::Ready(b) => {
                let physical_ratio = b
                    .compression_ratio
                    .filter(|r| r.is_finite() && *r > 0.0)
                    .map(|r| 1.0 / r);
                out.live_logical = b.live_logical;
                out.snapshots_total = amount(b.snapshots_total);
                out.unique = amount(b.unique);
                out.shared_snapshots_only = amount(b.shared_snapshots_only);
                out.shared_with_live = amount(b.shared_with_live);
                out.awaiting_gc = amount(b.awaiting_gc);
                out.physical_ratio = physical_ratio;
                out.physical_estimate =
                    physical_ratio.map(|r| (b.snapshots_total.bytes as f64 * r).round() as u64);
                out.as_of_seq = b.as_of_seq;
                out.as_of_ms = b.as_of_ms;
            }
        }
        Ok(out)
    }

    /// `snapshot.space.verify`.
    pub(crate) fn snapshot_space_verify(&self) -> Result<api::SpaceVerified, ControlError> {
        if self.snapacct().mode() == crate::snapacct::SnapAcctMode::Off {
            return Err(off());
        }
        let report = self.acct(self.snapacct().verify()).map_err(internal)?;
        Ok(api::SpaceVerified {
            mismatches: report.mismatches,
            details: report.details,
            snapshots: report.snapshots,
            chunks: report.chunks,
            as_of_seq: report.as_of_seq,
        })
    }
}
