//! The engine's side of the old `StatusSource` (plan 31 C5): the status
//! report and the 37 original operations, moved here from the daemon's
//! `DaemonStatus` with their semantics unchanged. Each is synchronous —
//! it parks on the engine's runtime (`block_in_place` + `block_on`)
//! exactly as it did behind the old line-JSON socket — and the router in
//! [`super`] runs it on a blocking thread.

use super::{fuse_requests_status, normalize_control_path, peer_addr_strings, EngineControl};
use crate::{
    backend, doctor, fsck, gc, held, leave, paths, snapshot, snapshot_batch, sync, writeback,
};
use constellation_control::proto::types as api;
use constellation_control::proto::ControlError;
use constellation_store_s3::ChunkStore;

/// A replicated snapshot row as the control protocol reports it. Plan 32
/// §0.4's `origin` is a small integer in the row and a word on the wire,
/// so a reader never has to know that 1 means "a policy's own".
pub(crate) fn snapshot_status(row: constellation_meta::SnapshotRow) -> api::SnapshotStatus {
    let seq = snapshot::SnapshotRoot::parse(&row.root_hash)
        .ok()
        .map(|root| root.seq);
    api::SnapshotStatus {
        seq,
        id: row.id,
        path: row.path,
        name: row.name,
        root_hash: row.root_hash,
        created_unix_ms: row.created_unix_ms,
        origin: match row.origin {
            0 => "manual".to_string(),
            1 => "auto".to_string(),
            other => format!("unknown({other})"),
        },
        policy_ino: row.policy_ino,
        held: row.held,
        held_by: row.held_by.filter(|by| !by.is_empty()),
        creator: row.creator,
        refer_bytes: row.refer_bytes,
        ..Default::default()
    }
}

impl EngineControl {
    /// `clone.create` is a namespace mutation: take the root write lease
    /// (the clone's inodes are written here, under it) and force the
    /// subtree's pending data + journal through before reading the source.
    /// (`quota.set` takes only the lease, [`Self::acquire_write_lease`].)
    /// Snapshot rows do *not* come through here — they execute at
    /// whichever node holds the lease ([`Self::snapshot_batch`], plan 32
    /// Step 0.1), so taking, deleting or holding a snapshot never moves
    /// it.
    pub(crate) fn acquire_namespace_barrier(&self, path: &str) -> std::result::Result<(), String> {
        let ino = self
            .meta
            .resolve_path(path)
            .map_err(|error| error.to_string())?
            .unwrap_or(constellation_fs_core::types::ROOT_INO);
        self.acquire_write_lease()?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(sync::SyncRequest::Barrier { ino, reply })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "snapshot barrier stopped".to_string())?
            .map_err(String::from)
    }

    /// Take the write lease for a control-plane metadata mutation that is
    /// journaled like any other (`quota.set`), without
    /// [`Self::acquire_namespace_barrier`]'s drain: holding the lease is
    /// what makes the local journal record shippable, and nothing is
    /// observed or published that pending writes would need to be part of.
    pub(crate) fn acquire_write_lease(&self) -> std::result::Result<(), String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(sync::SyncRequest::Acquire { reply })
            .map_err(|_| "sync task is not running".to_string())?;
        let progress = tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "lease acquisition stopped".to_string())??;
        if !progress.acquired {
            return Err("subtree write lease is held by another node".into());
        }
        Ok(())
    }

    /// Plan 32 Step 0.1: run snapshot row operations as one batch at the
    /// root-lease holder (here, forwarded to it, or here after taking a
    /// free lease) and return the per-item results in item order. The
    /// same API the scheduler reaches through
    /// [`crate::Engine::snapshot_batches`] without a control round trip.
    pub fn snapshot_batch(
        &self,
        items: Vec<snapshot_batch::SnapshotItem>,
    ) -> std::result::Result<Vec<snapshot_batch::ItemResult>, String> {
        let batches = self.engine.snapshot_batches().clone();
        // One control request is one batch and one attempt: the caller
        // (a person, or the CLI) retries with a new request.
        let rid = batches.next_rid();
        tokio::task::block_in_place(|| self.rt.block_on(batches.submit(rid, items)))
            .map_err(|error| format!("{error:#}"))
    }

    /// A batch of one, whose single result the caller interprets.
    fn snapshot_item(
        &self,
        item: snapshot_batch::SnapshotItem,
    ) -> std::result::Result<snapshot_batch::ItemResult, String> {
        self.snapshot_batch(vec![item])?
            .pop()
            .ok_or_else(|| "the snapshot batch returned no result".to_string())
    }

    /// Assemble the pruner's dependency bundle from the daemon's shared
    /// handles (plan 22). `replica_lag` is derived from the sync task's
    /// heartbeat.
    pub(crate) fn prune_deps(&self) -> crate::prune::PruneDeps {
        use std::sync::atomic::Ordering;
        let lag = std::time::Duration::from_millis(
            crate::prune::now_unix_ms().saturating_sub(self.last_sync_ms.load(Ordering::Relaxed)),
        );
        crate::prune::PruneDeps {
            store: self.store.clone(),
            meta: self.meta.clone(),
            sync_tx: self.sync_tx.clone(),
            lease: self.lease.clone(),
            forward: self.forward.clone(),
            node_id: self.node_id,
            lease_mode: self.lease_mode,
            read_only_member: self.read_only_member,
            departed: self.departed.clone(),
            epoch_frozen: Some(self.epochs.writes_refused.clone()),
            stats: self.prune_stats.clone(),
            replica_lag: lag,
        }
    }

    pub(crate) fn status(&self) -> api::StatusReport {
        let (mounts, fuse) = self.mounts_and_fuse();
        let core = self.core.lock().unwrap().clone();
        let stats = core.stats;
        let speculation = (
            stats.speculation_rolled_back,
            stats.stranded_replayed,
            stats.replay_conflicts,
        );
        let gate_pending = self.lease.gate_pending();
        let usage = self.cache.usage();
        let memory = self.cache.memory_stats().unwrap_or_default();
        let p0_lease = self.lease.status();
        let designations = self.list_designations();
        let mut epoch = self.epochs.status();
        epoch.promise_puts = stats.promise_puts;
        epoch.promise_requests_answered = stats.promise_requests_answered;
        epoch.promise_requests_refused = stats.promise_requests_refused;
        epoch.promise_checks = stats.promise_checks;
        epoch.takeovers_refused_promises = stats.takeovers_refused_promises;
        epoch.promise_flush_exempt = stats.promise_flush_exempt;
        epoch.stale_claims = stats.epoch_stale_claims;
        epoch.streamed_ahead = stats.epoch_streamed_ahead;
        epoch.streamed_installed = stats.epoch_streamed_installed;
        epoch.forwards_streamed = stats.epoch_forwards_streamed;
        epoch.handoffs_behind = stats.epoch_handoffs_behind;
        let coop = self.coop.report();
        let s3_coop = coop.per_source.iter().find(|s| s.id == "s3").cloned();
        let peer_snap = self.peers.snapshot();
        let mut peers: Vec<api::PeerStatus> = Vec::with_capacity(1 + peer_snap.len());
        // S3 is always first so operators can compare the durable path
        // against peer lat/BW/hit% in the same table.
        peers.push(api::PeerStatus {
            node_id: 0,
            connected: true,
            hostname: Some("S3".into()),
            coop: s3_coop,
            s3: true,
            path: String::new(),
            ..Default::default()
        });
        peers.extend(peer_snap.into_iter().map(|p| {
            let addrs = peer_addr_strings(&p.addr);
            let designations: Vec<String> = designations
                .iter()
                .filter(|d| d.designee == p.node_id)
                .map(|d| d.path.clone())
                .collect();
            let coop = coop
                .per_source
                .iter()
                .find(|s| s.id == format!("peer-{}", p.node_id))
                .cloned();
            let path = match p.path {
                constellation_net::PathKind::Unknown => coop
                    .as_ref()
                    .map(|c| c.path.clone())
                    .filter(|s| !s.is_empty() && s != "unknown")
                    .unwrap_or_else(|| "unknown".into()),
                other => other.as_str().into(),
            };
            api::PeerStatus {
                node_id: p.node_id,
                connected: p.connected,
                rtt_ms: p.rtt_ms,
                last_seen_ms: p.last_seen.map(|t| t.elapsed().as_millis() as u64),
                hostname: (!p.hostname.is_empty()).then_some(p.hostname.clone()),
                version: (!p.version.is_empty()).then_some(p.version.clone()),
                pubkey: Some(p.pubkey_hex.clone()),
                endpoint_id: Some(p.addr.id.to_string()),
                addrs,
                created_unix: (p.created_unix > 0).then_some(p.created_unix),
                p2p_updated_unix: p.p2p_updated_unix,
                ro: p.ro,
                epoch_member: epoch.members.contains(&p.node_id),
                designations,
                coop,
                s3: false,
                path,
                paths: paths::status(self.peers.path_summary(p.node_id)),
            }
        }));
        let p2p = api::P2pStatus {
            enabled: self.peers.is_enabled(),
            node_addr: self
                .peers
                .node_addr()
                .and_then(|a| serde_json::to_string(&a).ok()),
            relay: self.peers.relay_label(),
            peers,
        };
        let enrolled = !self.departed.load(std::sync::atomic::Ordering::Relaxed)
            && !matches!(
                self.meta.kv_get("left").ok().flatten().as_deref(),
                Some("1")
            );
        api::StatusReport {
            handover: self.host.handover_status(),
            fs_uuid: self.fs_uuid.clone(),
            backend: self.backend.clone(),
            mounts,
            node_id: self.node_id,
            version: self.version.clone(),
            enrolled,
            uptime_s: self.started.elapsed().as_secs(),
            spool: api::SpoolStatus {
                journal_backlog: constellation_meta::MetaStore::journal_len(&*self.meta)
                    .unwrap_or(0),
                head_seq: core.ship.as_ref().map(|s| s.head_seq).unwrap_or(0),
                conflicts: stats.conflicts,
                last_ship_error: core.last_error.clone(),
                ship_rounds_completed: stats.rounds_completed,
                // Plan 30 M5: a round is never cancelled by a request any
                // more (requests are events the round interleaves with).
                ship_rounds_cancelled: 0,
            },
            cache: api::CacheStatus {
                used_bytes: usage.used,
                budget_bytes: usage.budget,
                chunks: usage.entries as u64,
                pinned_bytes: usage.pinned,
                staging_bytes: self.staging_budget.used(),
                staging_budget_bytes: self.staging_budget.budget(),
                memory_budget_bytes: memory.budget_bytes,
                memory_used_bytes: memory.used_bytes,
                memory_chunks: memory.entries,
                memory_protected_bytes: memory.protected_bytes,
                memory_hits: memory.hits,
                memory_misses: memory.misses,
                memory_coalesced: memory.coalesced,
                memory_evictions: memory.evictions,
                cache_verify: self.cache.verify_mode().as_str().to_string(),
                open_pins: self.cache.open_pin_total(),
            },
            lease: p0_lease,
            p2p,
            pins: self.list_pins(),
            designations,
            epoch,
            reintegration: self.reintegration.snapshot(
                if matches!(
                    self.meta.kv_get("lease_lost").ok().flatten().as_deref(),
                    Some("1")
                ) {
                    self.meta.unmarked_journal_len().unwrap_or(0)
                } else {
                    0
                },
                speculation.2,
            ),
            speculation: {
                let counts = self.meta.speculation_counts().unwrap_or_default();
                api::SpeculationStatus {
                    outstanding: counts.outstanding,
                    pending_replay: counts.pending_replay,
                    rolled_back: speculation.0,
                    stranded_replayed: speculation.1,
                    replay_conflicts: speculation.2,
                    local: counts.local,
                    local_rolled_back: stats.local_rolled_back,
                    depositions: stats.depositions,
                    epoch_markers: stats.epoch_markers,
                    gate_pending,
                    copies_pending: core.copies.0,
                    copies_stalled: core.copies.1,
                }
            },
            held: held::status(&self.meta),
            session: {
                let s = self.meta.session().stats();
                api::SessionStatus {
                    budget_ms: self.meta.session().budget().as_millis() as u64,
                    reads: s.reads,
                    fast: s.fast,
                    covered: s.covered,
                    waited: s.waited,
                    timeouts: s.timeouts,
                    degraded_held: s.degraded_held,
                    replay_blocked: s.replay_blocked,
                    waits_ms: s.waits_ms.to_vec(),
                    wait_ms_total: s.wait_ms_total,
                    raised: s.raised,
                    watermark_ttl_ms: self.meta.session().watermark_ttl().as_millis() as u64,
                    abandoned: s.abandoned,
                    voided_ended: s.voided_ended,
                }
            },
            ack: {
                let a = &core.ack;
                let s = self.meta.session().stats();
                api::AckStatus {
                    ack_s3: core.ack_s3,
                    policy: a.policy.to_string(),
                    backups: a.backups.clone(),
                    candidate: a.candidate,
                    config_version: a.config_version,
                    durable: a.durable,
                    parked_acks: a.parked_acks as u64,
                    gated: self.lease.ack_gated(),
                    backing_holder: a.backing_holder,
                    backing_epoch: a.backing_epoch,
                    backing_acked: a.backing_acked,
                    sealed_epoch: a.sealed_epoch,
                    backups_added: stats.backups_added,
                    backups_removed: stats.backups_removed,
                    reconfig_cas: stats.reconfig_cas,
                    backup_appends: stats.backup_appends,
                    backup_acks: stats.backup_acks,
                    backup_ack_timeouts: stats.backup_ack_timeouts,
                    acks_waited: stats.acks_waited,
                    ack_wait_ms_total: stats.ack_wait_ms_total,
                    acks_aborted: stats.acks_aborted,
                    streamed_ahead: stats.streamed_ahead,
                    streamed_installed: stats.streamed_installed,
                    streamed_dropped: stats.streamed_dropped,
                    awaited_log: stats.awaited_log,
                    awaited_log_streamed: stats.awaited_log_streamed,
                    awaited_log_streamed_deleg: stats.awaited_log_streamed_deleg,
                    backup_persisted: stats.backup_persisted,
                    seals: stats.seals,
                    backup_takeovers: stats.backup_takeovers,
                    backup_tail_applied: stats.backup_tail_applied,
                    s3_fast_takeovers: stats.s3_fast_takeovers,
                    ack_floor_waits: stats.ack_floor_waits,
                    stale_liveness_refusals: stats.stale_liveness_refusals,
                    epoch_carry_refused: stats.epoch_carry_refused,
                    refusals_journaled: stats.refusals_journaled,
                    unacked_replays_refused: stats.unacked_replays_refused,
                    reads_durability_blocked: s.durability_blocked,
                }
            },
            delegation: {
                let v = &core.delegation;
                api::DelegationReport {
                    enabled: core.delegation_enabled,
                    table: self
                        .meta
                        .delegation_table()
                        .iter()
                        .map(|d| api::DelegationStatus {
                            dir: d.dir,
                            path: self.meta.path_of(d.dir).unwrap_or_default(),
                            node: d.node,
                            gen: d.gen,
                            designated: d.designated,
                            range: d.range.label(),
                        })
                        .collect(),
                    mine: v.mine.clone(),
                    gens: v.gens.clone(),
                    executed: stats.deleg_executed,
                    fast_path_executed: core.delegation_fast_path_executed,
                    fast_path_routed: core.delegation_fast_path_routed,
                    forwarded_to_delegate: stats.deleg_forwarded,
                    deps_waits: stats.deleg_deps_waits,
                    parked_expired: stats.deleg_parked_expired,
                    not_owner: stats.deleg_not_owner,
                    installed: stats.deleg_installed,
                    streamed_txs: stats.deleg_streamed_txs,
                    stream_refused: stats.deleg_stream_refused,
                    renewals: stats.deleg_renewals,
                    renewals_refused: stats.deleg_renewals_refused,
                    recalls_received: stats.deleg_recalls_received,
                    delegated: stats.deleg_delegated,
                    appended_txs: stats.deleg_appended_txs,
                    stream_refusals: stats.deleg_stream_refusals,
                    deps_unsatisfied_at_append: stats.deleg_deps_unsatisfied_at_append,
                    cross_subtree: stats.deleg_cross_subtree,
                    recalls_sent: stats.deleg_recalls_sent,
                    recalls_drained: stats.deleg_recalls_drained,
                    recalls_expired: stats.deleg_recalls_expired,
                    reclaimed: stats.deleg_reclaimed,
                    ended: stats.deleg_ended,
                    deps_overflow_to_root: stats.deps_overflow_to_root,
                    exec_parked: stats.deleg_exec_parked,
                    stranded: stats.local_rolled_back,
                    kinds: v.kinds.clone(),
                    backups: v.backups.clone(),
                    placement: core.placement_top.clone(),
                    inherited: stats.deleg_inherited,
                    refused_designated: stats.deleg_refused_designated,
                    designated: stats.deleg_designated,
                    redelegated: stats.deleg_redelegated,
                    seals_sent: stats.deleg_seals_sent,
                    sealed_drained: stats.deleg_sealed_drained,
                    restreams: stats.deleg_restreams,
                    backup_appends: stats.deleg_backup_appends,
                    backup_acks: stats.deleg_backup_acks,
                    acks_parked: stats.deleg_acks_parked,
                    backup_persisted: stats.deleg_backup_persisted,
                    backup_seals: stats.deleg_backup_seals,
                    place_evaluations: stats.place_evaluations,
                    place_delegated: stats.place_delegated,
                    place_recalled: stats.place_recalled,
                    place_skipped_cooldown: stats.place_skipped_cooldown,
                    place_skipped_unreachable: stats.place_skipped_unreachable,
                    place_splits: stats.place_splits,
                    place_range_recalls: stats.place_range_recalls,
                    read_index_served: stats.deleg_read_index_served,
                    read_grants: stats.deleg_read_grants,
                }
            },
            cto: {
                let d = self.meta.read_delegations();
                let c = d.stats();
                api::CtoStatus {
                    strict: self.engine.cto_strict(),
                    grants_enabled: core.read_delegations,
                    strict_reads: c.strict_reads,
                    holder_local: c.holder_local,
                    delegation_local: c.delegation_local,
                    read_index: c.read_index,
                    s3_tail: c.s3_tail,
                    degraded: c.degraded,
                    read_index_ms_total: c.read_index_ms_total,
                    read_index_ms: c.read_index_ms.to_vec(),
                    renewals: c.renewals,
                    delegations_installed: c.delegations_installed,
                    delegations_raced: c.delegations_raced,
                    delegations_held: d.held_count() as u64,
                    recalled: c.recalled,
                    read_index_served: stats.read_index_served,
                    read_index_refused: stats.read_index_refused,
                    grants: c.grants,
                    live_grants: d.live_grants(),
                    recalls_sent: stats.recalls_sent,
                    recalls_acked: stats.recalls_acked,
                    recalls_expired: stats.recalls_expired,
                    recall_waits: stats.recall_waits,
                    recall_wait_ms_total: stats.recall_wait_ms_total,
                    held_replies: stats.held_replies,
                    held_retries: stats.held_retries,
                    fuse_writes_recalled: c.fuse_writes_recalled,
                    fuse_recall_wait_ms_total: c.fuse_recall_wait_ms_total,
                    parked_acks: core.read.parked_acks as u64,
                    recalls_in_flight: core.read.recalls_in_flight as u64,
                }
            },
            locks: {
                let t = self.meta.locks();
                let l = t.stats();
                api::LockStatus {
                    mode: if core.locks_cluster {
                        "cluster"
                    } else {
                        "local"
                    }
                    .to_string(),
                    grants_held: t.held_count() as u64,
                    requests: stats.lock_requests,
                    local_hits: l.local_hits,
                    local_conflicts: l.local_conflicts,
                    granted: l.granted,
                    would_block: stats.lock_would_block,
                    unavailable: stats.lock_unavailable,
                    grant_ms_total: stats.lock_grant_ms_total,
                    grant_ms: stats.lock_grant_ms.to_vec(),
                    renewals: stats.lock_renewals,
                    lost: stats.lock_lost,
                    recalled: l.recalled,
                    recalled_busy: l.recalled_busy,
                    released: stats.lock_released,
                    fenced_io: l.fenced_io,
                    grants_waited: l.grants_waited,
                    grant_wait_ms_total: l.grant_wait_ms_total,
                    grants_degraded: l.grants_degraded,
                    grants_table: t.grants_len() as u64,
                    grants_made: stats.lock_grants,
                    recalls_sent: stats.lock_recalls_sent,
                    recalls_released: stats.lock_recalls_released,
                    recalls_expired: stats.lock_recalls_expired,
                    reclaimed: stats.lock_reclaimed,
                    waiters_parked: stats.lock_waiters_parked,
                    grace_refusals: stats.lock_grace_refusals,
                    requeued_in_place: stats.lock_requeued_in_place,
                    released_superseded: stats.lock_released_superseded,
                    requests_in_flight: core.lock_requests_in_flight as u64,
                    waiters: core.lock_waiters as u64,
                    recalls_in_flight: core.lock_recalls_in_flight as u64,
                }
            },
            coop,
            prefetch: self.prefetch_stats.snapshot(),
            fsync: {
                let f = self.engine.fsync_waits().status();
                api::FsyncStatus {
                    mode: f.mode.into(),
                    timeout_ms: f.timeout_ms,
                    kernel_cap_ms: f.kernel_cap_ms,
                    waiting: f.waiting,
                    longest_wait_ms: f.longest_wait_ms,
                    max_wait_ms: f.max_wait_ms,
                    waited: f.waited,
                    retries: f.retries,
                    timeouts: f.timeouts,
                    permanent_errors: f.permanent_errors,
                    interrupted: f.interrupted,
                }
            },
            writeback: {
                let remote = self.meta.remote_chunks().unwrap_or_default();
                let probe = self.upload.probe.lock().unwrap();
                let existence = self.upload.existence.report();
                api::WritebackStatus {
                    mode: self.write_mode.get().as_str().into(),
                    dirty_bytes: self
                        .cache
                        .dirty_bytes()
                        .saturating_add(self.staging_budget.used()),
                    pending_uploads: self.meta.pending_upload_count().unwrap_or(0),
                    upload_concurrency: self.upload.gate.target() as u32,
                    remote_probe_enabled: probe.enabled(),
                    remote_probe_hit_rate: probe.hit_rate(),
                    existence_bloom_hits: existence.bloom_hits,
                    existence_chunk_ref_hits: existence.chunk_ref_hits,
                    existence_misses: existence.misses,
                    existence_peer_hints: existence.peer_hints,
                    remote_chunks_awaited: remote.len() as u64,
                    remote_chunks_oldest_s: remote
                        .iter()
                        .map(|r| r.enrolled_ms)
                        .min()
                        .map(|oldest| {
                            (constellation_store_s3::lease::now_unix_ms() - oldest).max(0) as u64
                                / 1000
                        })
                        .unwrap_or(0),
                    handoffs_sent: self
                        .upload
                        .handoff
                        .sent
                        .load(std::sync::atomic::Ordering::Relaxed),
                    handoffs_ok: self
                        .upload
                        .handoff
                        .ok
                        .load(std::sync::atomic::Ordering::Relaxed),
                    handoff_chunks: self
                        .upload
                        .handoff
                        .chunks
                        .load(std::sync::atomic::Ordering::Relaxed),
                    handoff_chunks_accepted: self
                        .upload
                        .handoff
                        .accepted
                        .load(std::sync::atomic::Ordering::Relaxed),
                }
            },
            forwarded_ok: self.forward.ok.load(std::sync::atomic::Ordering::Relaxed),
            forwarded_err: self.forward.err.load(std::sync::atomic::Ordering::Relaxed),
            forward_p50_ms: self.forward.p50_ms(),
            log_stream: {
                let v = core.stream;
                api::LogStreamStatus {
                    enabled: core.stream_enabled,
                    upstream: v.upstream,
                    live: v.live,
                    buffered: v.buffered,
                    applied: stats.stream_applied,
                    tail_skips: stats.stream_tail_skips,
                    subscribes: stats.stream_subscribes,
                    refused: stats.stream_refused,
                    ended: stats.stream_ended,
                    gaps: stats.stream_gaps,
                    lost: stats.stream_lost,
                    timeouts: stats.stream_timeouts,
                    overflows: stats.stream_overflows,
                    duplicates: stats.stream_duplicates,
                    serving: v.serving,
                    served: stats.stream_served,
                    declined: stats.stream_declined,
                    frames_sent: stats.stream_frames_sent,
                    subscribers_dropped: stats.stream_subscribers_dropped,
                }
            },
            forward_dedup_hits: stats.forward_dedup_hits,
            forward_retries: stats.forward_retries,
            forward_indoubt_resolved: stats.forward_indoubt_resolved,
            own_s3: api::OwnS3Status {
                stalled: core.own_s3.stalled,
                peers_reach_s3: core.own_s3.peers_reach_s3,
                stalled_for_ms: core.own_s3.since.map(|since| {
                    (constellation_store_s3::lease::now_unix_ms() - since.0).max(0) as u64
                }),
                retries: stats.s3_less_retries,
                forwards: stats.s3_less_forwards,
                deadlines: stats.s3_less_deadlines,
                readopted_for_forward: stats.readopt_for_forward,
            },
            inbox: crate::inbox::status(
                crate::inbox::inbox_enabled(),
                &stats,
                &core.inbox,
                self.lease.touches(),
            ),
            placement_reason: self.placement.last_reason.lock().unwrap().clone(),
            atime: {
                use std::sync::atomic::Ordering::Relaxed;
                let s = &self.atime.stats;
                api::AtimeStatus {
                    mode: self.atime.mode().as_str().to_string(),
                    queued: s.queued.load(Relaxed),
                    coalesced: s.coalesced.load(Relaxed),
                    applied: s.applied.load(Relaxed),
                    dropped_cap: s.dropped_cap.load(Relaxed),
                    forward_ok: s.forward_ok.load(Relaxed),
                    forward_err: s.forward_err.load(Relaxed),
                    local_only: s.local_only.load(Relaxed),
                    skew_clamped: s.skew_clamped.load(Relaxed),
                }
            },
            quota: {
                use constellation_meta::MetaStore;
                api::QuotaStatus {
                    max_bytes: self.meta.quota().ok().flatten(),
                    used_bytes: self.meta.usage().0,
                }
            },
            prune: {
                use std::sync::atomic::Ordering::Relaxed;
                let s = &self.prune_stats;
                api::PruneStatus {
                    runs: s.runs.load(Relaxed),
                    roots: s.roots.load(Relaxed),
                    armed_roots: s.armed_roots.load(Relaxed),
                    unparseable_roots: s.unparseable_roots.load(Relaxed),
                    inert_roots: s.inert_roots.load(Relaxed),
                    entries_examined: s.entries_examined.load(Relaxed),
                    selected: s.selected.load(Relaxed),
                    deleted: s.deleted.load(Relaxed),
                    bytes_deleted: s.bytes_deleted.load(Relaxed),
                    bytes_freed: s.bytes_freed.load(Relaxed),
                    skipped_reverify: s.skipped_reverify.load(Relaxed),
                    skipped_forward_err: s.skipped_forward_err.load(Relaxed),
                    skipped_hardlink: s.skipped_hardlink.load(Relaxed),
                    skipped_repartition: s.skipped_repartition.load(Relaxed),
                    leases_acquired: s.leases_acquired.load(Relaxed),
                    refused_lag: s.refused_lag.load(Relaxed),
                    last_run_unix_ms: s.last_run_unix_ms.load(Relaxed),
                    last_parse_error: s.last_parse_error.lock().ok().and_then(|g| g.clone()),
                }
            },
            snapsched: self.snapsched_stats.status(),
            snapacct: {
                use std::sync::atomic::Ordering::Relaxed;
                let snapacct = self.engine.snapacct();
                let s = snapacct.stats();
                api::SnapAcctStatus {
                    mode: snapacct.mode().as_str().to_string(),
                    maintaining: snapacct.maintaining(),
                    building: s.building.load(Relaxed),
                    build_progress_pct: s.build_progress_pct.load(Relaxed),
                    indexed_chunks: s.indexed_chunks.load(Relaxed),
                    index_bytes: s.index_bytes.load(Relaxed),
                    as_of_seq: s.as_of_seq.load(Relaxed),
                    refresh_ms_last: s.refresh_ms_last.load(Relaxed),
                    verify_mismatches: s.verify_mismatches.load(Relaxed),
                    stalled_chains: s.stalled_chains.load(Relaxed),
                    refreshes_deferred: s.refreshes_deferred.load(Relaxed),
                    passes: s.passes.load(Relaxed),
                    errors: s.errors.load(Relaxed),
                    last_error: s.last_error.lock().ok().and_then(|g| g.clone()),
                }
            },
            fuse_requests: fuse_requests_status(&self.engine.op_watch().snapshot()),
            lifecycle: self.engine.lifecycle().status(),
            s3: backend::s3_request_counts(),
            vfs_ops: super::ops::vfs_ops_status(),
            fuse,
        }
    }

    pub(crate) fn pin(&self, path: &str) -> std::result::Result<String, String> {
        let pins = self.pins.clone();
        let path = path.to_string();
        // The API handler runs on the runtime already, so block_in_place
        // keeps the fetch off the async executor without a nested runtime.
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { pins.pin(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    pub(crate) fn unpin(&self, path: &str) -> std::result::Result<String, String> {
        let pins = self.pins.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { pins.unpin(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    pub(crate) fn list_pins(&self) -> Vec<api::PinStatus> {
        let pins = self.pins.clone();
        tokio::task::block_in_place(|| self.rt.block_on(async move { pins.status().await }))
    }

    pub(crate) fn offline(
        &self,
        path: &str,
        read_only: bool,
    ) -> std::result::Result<String, String> {
        let designations = self.designations.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { designations.offline(&path, read_only).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    pub(crate) fn online(&self, path: &str) -> std::result::Result<String, String> {
        let designations = self.designations.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { designations.online(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    pub(crate) fn list_designations(&self) -> Vec<api::DesignationStatus> {
        self.designations
            .snapshot()
            .into_iter()
            .map(|d| api::DesignationStatus {
                path: d.path,
                designee: d.designee,
                read_only: d.read_only,
            })
            .collect()
    }

    pub(crate) fn delegate(
        &self,
        path: &str,
        node: u64,
        range: Option<&str>,
    ) -> std::result::Result<String, String> {
        let dir = self
            .meta
            .resolve_path(path)
            .map_err(|e| format!("{e:#}"))?
            .ok_or_else(|| format!("no such directory: {path}"))?;
        // Plan 30 §M12: `"<idx>/<count>"`, `count` a power of two.
        let range = match range.map(str::trim).filter(|r| !r.is_empty()) {
            None => (0u8, 0u32),
            Some(r) => {
                let (i, k) = r
                    .split_once('/')
                    .ok_or_else(|| format!("range {r}: expected <idx>/<count>"))?;
                let idx: u32 = i.parse().map_err(|_| format!("range {r}: bad index"))?;
                let count: u32 = k.parse().map_err(|_| format!("range {r}: bad count"))?;
                if !count.is_power_of_two() || count > 16 || idx >= count {
                    return Err(format!(
                        "range {r}: count must be 2, 4, 8 or 16 and idx below it"
                    ));
                }
                (count.trailing_zeros() as u8, idx)
            }
        };
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(sync::SyncRequest::Delegate {
                dir,
                node,
                range,
                reply,
            })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "sync task stopped".to_string())?
        })
    }

    pub(crate) fn undelegate(&self, path: &str) -> std::result::Result<String, String> {
        let dir = self
            .meta
            .resolve_path(path)
            .map_err(|e| format!("{e:#}"))?
            .ok_or_else(|| format!("no such directory: {path}"))?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(sync::SyncRequest::Undelegate { dir, reply })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "sync task stopped".to_string())?
        })
    }

    pub(crate) fn list_delegations(&self) -> Vec<api::DelegationStatus> {
        self.meta
            .delegation_table()
            .iter()
            .map(|d| api::DelegationStatus {
                dir: d.dir,
                path: self.meta.path_of(d.dir).unwrap_or_default(),
                node: d.node,
                gen: d.gen,
                designated: d.designated,
                range: d.range.label(),
            })
            .collect()
    }

    pub(crate) fn reintegrate(&self) -> std::result::Result<String, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(sync::SyncRequest::Reintegrate(reply))
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "reintegration task stopped".to_string())?
        })
    }

    pub(crate) fn leave(
        &self,
        node_id: Option<u64>,
        force: bool,
    ) -> std::result::Result<String, String> {
        match node_id {
            Some(target) => {
                let store = self.store.clone();
                let designations = self.designations.clone();
                let self_id = self.node_id;
                tokio::task::block_in_place(|| {
                    self.rt.block_on(async move {
                        leave::admin_leave(store, &designations, self_id, target, force)
                            .await
                            .map(|_| format!("retired node {target} in the registry"))
                            .map_err(|e| e.to_string())
                    })
                })
            }
            None => {
                leave::refuse_open_epoch(&self.epochs).map_err(|e| e.to_string())?;
                let (reply, receive) = tokio::sync::oneshot::channel();
                self.sync_tx
                    .send(sync::SyncRequest::Leave { force, reply })
                    .map_err(|_| "sync task is not running".to_string())?;
                let detail = tokio::task::block_in_place(|| {
                    self.rt
                        .block_on(receive)
                        .map_err(|_| "leave task stopped".to_string())?
                })?;
                self.departed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                // Unmount after the response is on the wire: the control
                // handler answers, then every view is detached (plan 21,
                // step 1 — `leave` is node-level, not scoped to whichever
                // view happened to answer the call).
                let host = self.host.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    host.detach_all();
                });
                Ok(detail)
            }
        }
    }

    pub(crate) fn set_write_mode(&self, mode: &str) -> std::result::Result<String, String> {
        let requested: writeback::WriteMode = mode.parse().map_err(str::to_string)?;
        if self.write_mode.get() == requested {
            return Ok(format!("write mode already {}", requested.as_str()));
        }
        if requested == writeback::WriteMode::Through {
            let (reply, receive) = tokio::sync::oneshot::channel();
            self.sync_tx
                .send(sync::SyncRequest::DrainInode {
                    ino: 0,
                    fsync: false,
                    reply,
                })
                .map_err(|_| "sync task is not running".to_string())?;
            tokio::task::block_in_place(|| {
                self.rt
                    .block_on(receive)
                    .map_err(|_| "upload drain stopped".to_string())?
                    .map_err(String::from)
            })?;
        }
        self.write_mode.set(requested);
        Ok(format!("write mode set to {}", requested.as_str()))
    }

    /// `snapshot.create`: a batch of one, `origin` as given (manual from
    /// the control protocol). A name already taken is today's error,
    /// whether the object came from an earlier create or from this one's
    /// own retry on another holder (the scheduler, not this, counts the
    /// latter as success).
    pub(crate) fn snapshot_create(
        &self,
        selector: &str,
        options: &snapshot::SnapshotOptions,
    ) -> std::result::Result<(String, api::SnapshotStatus), String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        if let Some(by) = options.owner() {
            snapshot::validate_owner(by).map_err(|error| format!("{error:#}"))?;
        }
        let item = snapshot_batch::SnapshotItem::Create {
            path: path.clone(),
            name: name.clone(),
            origin: options.origin,
            policy_ino: options.policy_ino,
            creator: self.node_id,
            held: options.held,
            held_by: options.owner().map(str::to_string),
            skip_if_unchanged_since: None,
        };
        match self.snapshot_item(item)? {
            snapshot_batch::ItemResult::Created { seq, row, .. } => {
                let row = snapshot_batch::row_from_wire(row);
                Ok((snapshot::created_detail(&row, seq), snapshot_status(row)))
            }
            snapshot_batch::ItemResult::AlreadyExists { .. } => {
                Err(format!("snapshot {path}@{name} already exists"))
            }
            snapshot_batch::ItemResult::Refused { reason } => Err(reason),
            other => Err(format!("unexpected snapshot create result {other:?}")),
        }
    }

    /// Plan 32 §0.4: set or release a retention hold. `target` is a
    /// snapshot id or a `path@name` selector.
    ///
    /// A hold is a journaled metadata mutation like create and delete, and
    /// it is only worth anything if it reaches the whole cluster: a hold
    /// written on a node that may not write would be invisible to the
    /// holder, whose `snapshot.delete` (and plan 32 Step 4's expiry)
    /// reads `held` from its own replica. So it is a batch item: the
    /// holder writes it (plan 32 Step 0.1), under the owner rule its
    /// replica's transaction checks.
    pub(crate) fn snapshot_hold(
        &self,
        target: &str,
        held: bool,
        by: Option<&str>,
        force: bool,
    ) -> std::result::Result<(String, api::SnapshotStatus), String> {
        let by = by.filter(|by| !by.is_empty());
        if let Some(by) = by {
            snapshot::validate_owner(by).map_err(|error| format!("{error:#}"))?;
        }
        let id = snapshot::snapshot_id_of(target).map_err(|error| format!("{error:#}"))?;
        let item = snapshot_batch::SnapshotItem::Hold {
            id,
            held,
            by: by.map(str::to_string),
            force,
        };
        match self.snapshot_item(item)? {
            snapshot_batch::ItemResult::HoldSet { row } => {
                let row = snapshot_batch::row_from_wire(row);
                Ok((snapshot::hold_detail(&row), snapshot_status(row)))
            }
            snapshot_batch::ItemResult::NotFound => Err(format!("no such snapshot: {target}")),
            snapshot_batch::ItemResult::Refused { reason } => Err(reason),
            other => Err(format!("unexpected snapshot hold result {other:?}")),
        }
    }

    /// `snapshot.list`, with each row's sizes when `sizes` asks for them
    /// or the accounting index has them ready ([`Self::fill_sizes`]).
    pub(crate) fn snapshot_list(
        &self,
        path: Option<&str>,
        sizes: bool,
    ) -> std::result::Result<Vec<api::SnapshotStatus>, ControlError> {
        let mut rows: Vec<api::SnapshotStatus> = self
            .snapshots
            .list(path)
            .map_err(|error| ControlError::failed(format!("{error:#}")))?
            .into_iter()
            .map(snapshot_status)
            .collect();
        self.fill_sizes(&mut rows, sizes)?;
        Ok(rows)
    }

    /// `snapshot.resolve`: plan 32 Step 5's selectors over this replica's
    /// rows ([`snapshot::resolve_selectors`]).
    ///
    /// A snapshot taken, held or deleted through another node is in this
    /// replica only once it has tailed that node's records. So a selector
    /// that fails to resolve is retried once after a best-effort tail to
    /// the log head (bounded like the batch's own catch-up): taking a
    /// snapshot on one node and naming it on another a moment later works
    /// as it did when every selector was hashed to an id, and the common
    /// case costs no tail.
    pub(crate) fn snapshot_resolve_rows(
        &self,
        selectors: &[String],
    ) -> std::result::Result<Vec<constellation_meta::SnapshotRow>, String> {
        if let Ok(rows) = self.snapshots.resolve(selectors) {
            return Ok(rows);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        if self
            .sync_tx
            .send(sync::SyncRequest::TailToHead { reply })
            .is_ok()
        {
            let _ = tokio::task::block_in_place(|| {
                self.rt.block_on(tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    receive,
                ))
            });
        }
        self.snapshots
            .resolve(selectors)
            .map_err(|error| format!("{error:#}"))
    }

    pub(crate) fn snapshot_resolve(
        &self,
        selectors: &[String],
    ) -> std::result::Result<Vec<api::SnapshotStatus>, String> {
        self.snapshot_resolve_rows(selectors)
            .map(|rows| rows.into_iter().map(snapshot_status).collect())
    }

    /// `snapshot.delete_many`: resolve here, then delete the snapshots
    /// at the lease holder in batches (plan 32 Step 0.1), so a range of a
    /// hundred snapshots costs one round trip and never moves the lease.
    /// A batch carries at most [`constellation_net::MAX_SNAPSHOT_DELETES_PER_BATCH`]
    /// deletes, each under its own rid, run one after another: thousands
    /// of `auto-*` snapshots would not fit one peer frame.
    ///
    /// Whether a snapshot is held is the holder's call, made inside its
    /// delete (its replica is the one every hold was written to), and its
    /// refusal names the owner. A dry run has no holder to ask: it reports
    /// the holds this replica knows of, with the same message, and
    /// deletes nothing. A delete whose row went but whose `snaps/` object
    /// did not counts as deleted: the snapshot is gone from every listing,
    /// and GC's reconciliation (plan 32 §0.3) removes the orphan object.
    ///
    /// Bare ids that no longer resolve (the CLI deletes the ids it had
    /// confirmed, and one may have been deleted elsewhere since) are
    /// refused one by one instead of failing the call; a missing
    /// `path@name`, range end or glob still fails it, before anything is
    /// deleted.
    ///
    /// A batch that fails (or answers with the wrong number of results)
    /// does not fail the call: the batches before it did their work, so
    /// the result is partial. That batch's snapshots are refused as
    /// "not confirmed" (a forwarded batch may have run at the holder
    /// before its reply was lost), the ones after it as "not attempted".
    pub(crate) fn snapshot_delete_many(
        &self,
        selectors: &[String],
        dry_run: bool,
        force: bool,
    ) -> std::result::Result<api::SnapshotsDeleted, String> {
        let mut refused = Vec::new();
        let rows = match self.snapshot_resolve_rows(selectors) {
            Ok(rows) => rows,
            Err(error) if selectors.iter().any(|s| s.contains('@')) => return Err(error),
            Err(_) => {
                let all = self
                    .snapshots
                    .list(None)
                    .map_err(|error| format!("{error:#}"))?;
                let (found, missing): (Vec<String>, Vec<String>) = selectors
                    .iter()
                    .cloned()
                    .partition(|id| all.iter().any(|row| row.id == *id));
                refused.extend(missing.into_iter().map(|id| api::SnapshotRefusal {
                    reason: format!("no such snapshot: {id} (deleted meanwhile?)"),
                    id,
                }));
                snapshot::resolve_selectors(&all, &found).map_err(|error| format!("{error:#}"))?
            }
        };
        let mut deleted = Vec::new();
        if dry_run {
            refused.extend(rows.iter().filter(|row| row.held && !force).map(|row| {
                api::SnapshotRefusal {
                    id: row.id.clone(),
                    reason: snapshot::held_refusal(row),
                }
            }));
        } else {
            let mut failed: Option<String> = None;
            for batch in rows.chunks(constellation_net::MAX_SNAPSHOT_DELETES_PER_BATCH) {
                if let Some(error) = &failed {
                    refused.extend(batch.iter().map(|row| api::SnapshotRefusal {
                        id: row.id.clone(),
                        reason: format!("not attempted: an earlier delete batch failed: {error}"),
                    }));
                    continue;
                }
                let items = batch
                    .iter()
                    .map(|row| snapshot_batch::SnapshotItem::Delete {
                        id: row.id.clone(),
                        force,
                    })
                    .collect();
                let results = match self.snapshot_batch(items) {
                    Ok(results) if results.len() == batch.len() => Ok(results),
                    Ok(results) => Err(format!(
                        "the snapshot batch returned {} results for {} deletes",
                        results.len(),
                        batch.len()
                    )),
                    Err(error) => Err(error),
                };
                let results = match results {
                    Ok(results) => results,
                    Err(error) => {
                        refused.extend(batch.iter().map(|row| api::SnapshotRefusal {
                            id: row.id.clone(),
                            reason: format!(
                                "not confirmed: its delete batch failed ({error}); it may \
                                 have been deleted — check `snapshot ls`"
                            ),
                        }));
                        failed = Some(error);
                        continue;
                    }
                };
                for (row, result) in batch.iter().zip(results) {
                    let id = row.id.clone();
                    match result {
                        snapshot_batch::ItemResult::Deleted => deleted.push(id),
                        snapshot_batch::ItemResult::DeletedObjectRemains { reason } => {
                            tracing::warn!(
                                id,
                                reason,
                                "snapshot deleted; its snaps/ object is left for GC's reconciliation"
                            );
                            deleted.push(id);
                        }
                        snapshot_batch::ItemResult::NotFound => {
                            refused.push(api::SnapshotRefusal {
                                id,
                                reason: format!(
                                    "snapshot {}@{} does not exist (deleted meanwhile)",
                                    row.path, row.name
                                ),
                            })
                        }
                        snapshot_batch::ItemResult::Refused { reason } => {
                            refused.push(api::SnapshotRefusal { id, reason })
                        }
                        other => refused.push(api::SnapshotRefusal {
                            id,
                            reason: format!("unexpected snapshot delete result {other:?}"),
                        }),
                    }
                }
            }
        }
        // A dry run is the preview of a delete (the CLI's confirmation
        // prompt shows it), so it says what the delete would give back:
        // `reclaim` of exactly the snapshots it would delete.
        let reclaim = if dry_run {
            let ids: Vec<String> = rows
                .iter()
                .filter(|row| !refused.iter().any(|r| r.id == row.id))
                .map(|row| row.id.clone())
                .collect();
            // An estimate that fails is left out, not the dry run.
            self.reclaim_of(&ids).unwrap_or_else(|error| {
                tracing::warn!(%error, "snapshot delete dry run: no reclaim estimate");
                None
            })
        } else {
            None
        };
        Ok(api::SnapshotsDeleted {
            resolved: rows.into_iter().map(snapshot_status).collect(),
            deleted,
            refused,
            reclaim,
        })
    }

    pub(crate) fn snapshot_delete(
        &self,
        selector: &str,
        force: bool,
    ) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        let item = snapshot_batch::SnapshotItem::Delete {
            id: constellation_store_s3::snapshot_id(&path, &name),
            force,
        };
        match self.snapshot_item(item)? {
            snapshot_batch::ItemResult::Deleted => Ok(format!("deleted snapshot {path}@{name}")),
            snapshot_batch::ItemResult::DeletedObjectRemains { reason } => Err(format!(
                "snapshot {path}@{name}: its row was deleted, but deleting its snaps/ object \
                 failed (left as an orphan): {reason}"
            )),
            snapshot_batch::ItemResult::NotFound => {
                Err(format!("snapshot {path}@{name} does not exist"))
            }
            snapshot_batch::ItemResult::Refused { reason } => Err(reason),
            other => Err(format!("unexpected snapshot delete result {other:?}")),
        }
    }

    pub(crate) fn clone_snapshot(
        &self,
        selector: &str,
        destination: &str,
    ) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.acquire_namespace_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let destination = destination.to_string();
        let result = tokio::task::block_in_place(|| {
            self.rt
                .block_on(snapshots.clone_to(&path, &name, &destination))
        })
        .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(sync::SyncRequest::Nudge);
        Ok(result)
    }

    pub(crate) fn snap_refs(&self, id: &str) -> std::result::Result<Vec<String>, String> {
        let snapshots = self.snapshots.clone();
        let id = id.to_string();
        tokio::task::block_in_place(|| self.rt.block_on(snapshots.refs(&id)))
            .map_err(|error| format!("{error:#}"))
    }

    pub(crate) fn read_dir(
        &self,
        path: &str,
    ) -> std::result::Result<Vec<api::DirectoryEntry>, String> {
        use constellation_meta::MetaStore;
        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let entries = self.meta.readdir(ino).map_err(|error| error.to_string())?;
        Ok(entries
            .into_iter()
            .map(|entry| api::DirectoryEntry {
                path: if normalized == "/" {
                    format!("/{}", entry.name)
                } else {
                    format!("{normalized}/{}", entry.name)
                },
                name: entry.name,
                ino: entry.ino,
                kind: format!("{:?}", entry.kind).to_lowercase(),
            })
            .collect())
    }

    pub(crate) fn inspect(&self, path: &str) -> std::result::Result<api::InspectStatus, String> {
        use constellation_meta::MetaStore;
        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let attr = self
            .meta
            .getattr(ino)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: stale inode"))?;
        let manifest = self
            .meta
            .manifest(ino)
            .map_err(|error| error.to_string())?
            .map(|bytes| {
                constellation_fs_core::manifest::Manifest::decode(&bytes)
                    .map(|manifest| {
                        use constellation_fs_core::manifest::ChunkInfo;
                        let chunks = match &manifest.chunks {
                            ChunkInfo::Inline(map) => map
                                .iter()
                                .take(8)
                                .map(|(index, hash)| format!("{index}:{}", hash.to_hex()))
                                .collect(),
                            ChunkInfo::Spilled(hash) => vec![format!("spilled:{}", hash.to_hex())],
                        };
                        api::ManifestStatus {
                            chunk_size: manifest.layout.chunk_size,
                            chunk_count: manifest.layout.chunk_count(manifest.file_len),
                            spilled: manifest.is_spilled(),
                            file_len: manifest.file_len,
                            digest: blake3::hash(&bytes).to_hex().to_string(),
                            chunks,
                        }
                    })
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        Ok(api::InspectStatus {
            path: normalized,
            ino: attr.ino,
            kind: format!("{:?}", attr.kind).to_lowercase(),
            size: attr.size,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            nlink: attr.nlink,
            atime_ns: attr.atime_ns,
            mtime_ns: attr.mtime_ns,
            ctime_ns: attr.ctime_ns,
            rdev: attr.rdev,
            manifest,
        })
    }

    pub(crate) fn force_release(&self, part: &str) -> std::result::Result<String, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        // A cooperative release asked for by the operator: the core's
        // handoff job, addressed as if this node itself asked.
        self.sync_tx
            .send(sync::SyncRequest::HandOff {
                requester: self.node_id,
                // The S3 handoff job (declined inside an active epoch).
                epoch_applied: None,
                reply,
            })
            .map_err(|_| "sync task is not running".to_string())?;
        let handed = tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "lease release task stopped".to_string())?;
        match handed {
            Some(handed) => Ok(format!(
                "voluntarily released {part} at epoch {}; this was cooperative, not fencing",
                handed.epoch
            )),
            None => Err(format!(
                "{part} was not held locally or could not be flushed; no fencing was attempted"
            )),
        }
    }

    pub(crate) fn drop_held(&self, ino: u64, remote: bool) -> std::result::Result<String, String> {
        held::drop_held(&self.meta, ino, remote)
    }

    pub(crate) fn doctor(&self) -> std::result::Result<api::DoctorStatus, String> {
        let store = ChunkStore::new(self.store.clone());
        tokio::task::block_in_place(|| {
            self.rt.block_on(async {
                let caps = store.probe_conditional_writes().await?;
                let report = constellation_store_s3::probe_cas_semantics(store.inner()).await?;
                Ok::<_, constellation_store_s3::StoreError>((caps, report))
            })
        })
        .map(|(caps, report)| api::DoctorStatus {
            create_if_absent: caps.create_if_absent,
            etag_cas: caps.etag_cas,
            cas_probes: doctor::api_probes(&report),
            versioning: report.versioning.as_str().to_string(),
        })
        .map_err(|error| error.to_string())
    }

    pub(crate) fn cache_list(&self) -> Vec<api::CacheEntryStatus> {
        self.cache
            .entries()
            .into_iter()
            .map(|(hash, size, state)| api::CacheEntryStatus {
                hash: hash.to_hex(),
                size,
                state: format!("{state:?}").to_lowercase(),
            })
            .collect()
    }

    pub(crate) fn set_quota(&self, max_bytes: Option<u64>) -> std::result::Result<String, String> {
        use constellation_meta::MetaStore;
        // The lease, not a barrier. This used to run `snapshot_barrier("/")`
        // (since renamed `acquire_namespace_barrier`) first, which waits
        // for the node's *whole* journal backlog to reach zero within one
        // sync round and fails the call ("journal not
        // shipped: no lease") whenever concurrent writes keep it nonzero —
        // plan 37 K0 Track B measured that as most `quota.set`s failing at
        // 64 concurrent CSI `CreateVolume`s. A snapshot needs that drain
        // because it publishes an immutable root that must contain every
        // pending write (plan 32); a quota observes and publishes nothing:
        // it writes one journaled value, enforced locally and best-effort
        // (`View::quota_check`), and replicates when the record ships —
        // the drain *before* the write never made the quota itself durable
        // any sooner. Scoping the barrier to a subtree would not have
        // helped either: the round-waiter checks the whole journal
        // whatever `ino` it is given.
        self.acquire_write_lease()?;
        self.meta
            .set_quota(max_bytes)
            .map_err(|e| format!("{e:#}"))?;
        // Node-level cap, but each mounted view caches its own read of it
        // (`View`'s `QUOTA_CACHE_TTL`) — invalidate every view, not just
        // whichever one happened to build this DaemonStatus.
        self.engine.invalidate_quota_caches();
        let _ = self.sync_tx.send(sync::SyncRequest::Nudge);
        Ok(match max_bytes {
            Some(cap) => format!("quota set to {cap} bytes"),
            None => "quota cleared (unlimited)".into(),
        })
    }

    pub(crate) fn get_quota(&self) -> std::result::Result<(Option<u64>, u64), String> {
        use constellation_meta::MetaStore;
        let max = self.meta.quota().map_err(|e| format!("{e:#}"))?;
        let (used, _) = self.meta.usage();
        Ok((max, used))
    }

    pub(crate) fn prune_run(
        &self,
        path: Option<&str>,
        dry_run: bool,
    ) -> std::result::Result<String, String> {
        // Restrict to the marked root governing `path`, if one was given.
        let only = match path {
            Some(p) => {
                let ino = self
                    .meta
                    .resolve_path(p)
                    .map_err(|e| format!("{e:#}"))?
                    .ok_or_else(|| format!("no such path: {p}"))?;
                match self
                    .meta
                    .effective_prune_policy(ino)
                    .map_err(|e| format!("{e:#}"))?
                {
                    Some((root, _)) => Some(vec![root]),
                    None => return Err(format!("no prune policy governs {p}")),
                }
            }
            None => None,
        };
        let deps = self.prune_deps();
        let report = tokio::task::block_in_place(|| {
            self.rt
                .block_on(async { crate::prune::run(&deps, only, dry_run).await })
        })
        .map_err(|e| format!("{e:#}"))?;
        if let Some(why) = report.refused {
            return Err(format!("prune refused: {why}"));
        }
        let (mut sel, mut del) = (0u64, 0u64);
        for r in &report.roots {
            sel += r.selected;
            del += r.deleted;
        }
        Ok(format!(
            "prune {}: {} roots, {} selected, {} deleted",
            if report.dry_run { "dry-run" } else { "run" },
            report.roots.len(),
            sel,
            del
        ))
    }

    pub(crate) fn prune_ls(&self) -> std::result::Result<Vec<api::PruneRootStatus>, String> {
        let roots = self.meta.prune_roots().map_err(|e| format!("{e:#}"))?;
        let mut out = Vec::new();
        for (ino, expr) in roots {
            let path = self.meta.path_of(ino).unwrap_or_else(|_| "?".into());
            match constellation_meta::prune::Policy::parse(&expr) {
                Ok(policy) => {
                    let note = if let Some((
                        constellation_meta::prune::Watermark::Percent(_),
                        _,
                        constellation_meta::prune::Of::Fs,
                    )) = policy.lru_watermarks()
                    {
                        use constellation_meta::MetaStore;
                        if self.meta.quota().ok().flatten().unwrap_or(0) == 0 {
                            Some("inert: lru percentage needs a quota".to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    out.push(api::PruneRootStatus {
                        path,
                        policy: policy.to_string(),
                        armed: policy.armed,
                        valid: !policy.off,
                        note,
                    });
                }
                Err(e) => out.push(api::PruneRootStatus {
                    path,
                    policy: expr,
                    armed: false,
                    valid: false,
                    note: Some(format!("unparseable: {}", e.msg)),
                }),
            }
        }
        Ok(out)
    }

    pub(crate) fn gc_run(
        &self,
        verify_only: bool,
    ) -> std::result::Result<serde_json::Value, String> {
        let store = self.store.clone();
        let chunks = self.pins.chunks();
        let meta = self.meta.clone();
        let lease_mode = self.lease_mode;
        let peers = self.peers.clone();
        let tail = gc::GcTail::Daemon(self.sync_tx.clone());
        let report = tokio::task::block_in_place(|| {
            self.rt.block_on(gc::run(
                store,
                chunks,
                meta,
                lease_mode,
                verify_only,
                Some(&peers),
                &tail,
            ))
        })
        .map_err(|e| format!("{e:#}"))?;
        serde_json::to_value(&report).map_err(|e| format!("{e:#}"))
    }

    pub(crate) fn fsck_run(
        &self,
        repair: bool,
        force_release: Option<&str>,
    ) -> std::result::Result<serde_json::Value, String> {
        let store = self.store.clone();
        let chunks = self.pins.chunks();
        let meta = self.meta.clone();
        let lease_mode = self.lease_mode;
        let state_dir = self.state_dir.clone();
        let compression = self.compression;
        let force_release = force_release.map(str::to_string);
        let report = tokio::task::block_in_place(|| {
            self.rt.block_on(async move {
                let logs = match chunks.e2e_keys() {
                    Some(keys) => {
                        constellation_store_s3::LogStore::new_e2e(store.clone(), keys.clone())
                    }
                    None => constellation_store_s3::LogStore::new(store.clone()),
                };
                fsck::run(
                    store,
                    chunks,
                    &logs,
                    meta,
                    Some(&state_dir),
                    compression,
                    lease_mode,
                    repair,
                    force_release.as_deref(),
                )
                .await
            })
        })
        .map_err(|e| format!("{e:#}"))?;
        serde_json::to_value(&report).map_err(|e| format!("{e:#}"))
    }
}

#[cfg(test)]
mod tests {
    use super::snapshot_status;
    use constellation_meta::SnapshotRow;

    /// Plan 32 §0.4 on the wire: `snapshot.list` reports the hold, its
    /// owner, the origin as a word, and REFER — and an empty owner is no
    /// owner, never an empty string a UI would print.
    #[test]
    fn a_row_becomes_the_status_the_protocol_documents() {
        let mut row = SnapshotRow::new("id", "/vol", "pvc-1", "mtree:1:ab:9", 42);
        row.held = true;
        row.held_by = Some("csi:content-uid".into());
        row.creator = 7;
        row.refer_bytes = Some(4096);
        let status = snapshot_status(row.clone());
        assert_eq!(status.origin, "manual");
        assert!(status.held);
        assert_eq!(status.held_by.as_deref(), Some("csi:content-uid"));
        assert_eq!(status.creator, 7);
        assert_eq!(status.refer_bytes, Some(4096));

        row.origin = 1;
        row.policy_ino = 4096;
        row.held_by = Some(String::new());
        let status = snapshot_status(row.clone());
        assert_eq!(status.origin, "auto");
        assert_eq!(status.policy_ino, 4096);
        assert_eq!(status.held_by, None);

        // An origin from a future plan 32 step still reports as itself
        // rather than being silently read as "manual".
        row.origin = 9;
        assert_eq!(snapshot_status(row).origin, "unknown(9)");
    }
}
