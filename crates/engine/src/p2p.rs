//! The node's side of the P2P fast path (DESIGN.md §8): [`P2pBridge`],
//! the `constellation_net::PeerService` that hands every peer request to
//! the authority driver's sync task, and the endpoint's bring-up
//! ([`start_p2p`]) and registry refresh ([`refresh_peers`]). Moved from
//! the `constellation` binary into the engine in plan 31 C4c: it is how
//! an [`crate::Engine`] talks to its peers, whatever process hosts it.

use crate::{authority_driver, epoch, placement, sync};

/// Bridges the P2P layer to the daemon's sync task.
///
/// Both directions are latency-only. A `SegmentPublished` hint just
/// nudges the syncer, which would have polled anyway; a `LeaseRequest`
/// asks the sync task to flush and release, and S3's CAS remains the
/// authority for who actually holds the lease.
/// How long an epoch proposal's member-side S3 probe may take: a member
/// whose probe has not succeeded by then is treated as cut from S3 (and
/// may join, today's behaviour). Well inside the proposer's wait for the
/// answer (`epoch::PROPOSE_REQUEST_TIMEOUT`, 2 s), so a probe that hangs
/// in a real outage never turns the ack into a timeout — which would stop
/// the epoch from forming in exactly the outage it is for.
const EPOCH_MEMBER_S3_PROBE: std::time::Duration = std::time::Duration::from_millis(300);

pub(crate) struct P2pBridge {
    pub(crate) node_id: u64,
    /// Plan 32 Step 0.1: executes a peer's snapshot batch when this node
    /// holds the root lease.
    pub(crate) snapshot_batches: std::sync::Arc<crate::snapshot_batch::SnapshotBatcher>,
    pub(crate) nudge: tokio::sync::mpsc::UnboundedSender<sync::SyncRequest>,
    pub(crate) epochs: std::sync::Arc<epoch::EpochManager>,
    /// The bucket, for an epoch proposal's member-side S3 probe.
    pub(crate) store: std::sync::Arc<dyn object_store::ObjectStore>,
    pub(crate) coop: std::sync::Arc<crate::coop::Coop>,
    pub(crate) placement: std::sync::Arc<placement::Placement>,
}

impl P2pBridge {
    /// Plan 30 §M10's member-side S3 probe: one lease GET, bounded by
    /// [`EPOCH_MEMBER_S3_PROBE`]. A member whose own rounds are failing at
    /// S3 is in the outage and answers at once (a probe would only delay
    /// an epoch's formation, eating into the carried lease's window).
    async fn probe_s3(&self) -> bool {
        !self.epochs.s3_failing() && {
            let leases = constellation_store_s3::LeaseStore::new(
                self.store.clone(),
                constellation_store_s3::log::PARTITION,
                constellation_store_s3::LeaseMode::Cas,
            );
            matches!(
                tokio::time::timeout(EPOCH_MEMBER_S3_PROBE, leases.get()).await,
                Ok(Ok(_))
            )
        }
    }
}

impl constellation_net::PeerService for P2pBridge {
    /// EC2 follow-up 3c, as fixed for `epoch-member-lost`: a would-be
    /// proposer asks (`PingS3`), and this member probes S3 now — the same
    /// bounded lease GET a proposal's member rule uses — rather than
    /// trusting a last-answer time: a follower on the holder's log stream
    /// may not have made an S3 request for seconds, and in a real bucket
    /// outage its stale "yes" delayed the epoch past the holder's usable
    /// lease (it formed carrying nothing, and every write failed).
    fn s3_probe(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(self.probe_s3())
    }

    fn segment_published(&self, part: &str, seq: u64, epoch: u64) {
        tracing::debug!(part, seq, epoch, "peer published a segment");
        // The core decides: a node following the holder's log stream has
        // it (or will) and ignores the hint; any other node tails now.
        let _ = self
            .nudge
            .send(sync::SyncRequest::SegmentHint { seq, epoch });
    }

    fn log_subscribe(
        &self,
        requester: u64,
        req_id: u64,
        from: u64,
    ) -> Option<tokio::sync::mpsc::Receiver<constellation_net::LogEvent>> {
        // The driver queues into `sink` (bounded in frames and in bytes)
        // and never waits; this relay hands frames to the stream writer
        // one at a time, so `queued_bytes` counts exactly what is waiting
        // for the subscriber.
        let (sink, mut queue) = tokio::sync::mpsc::channel(authority_driver::log_stream_queue());
        let (writer_tx, writer_rx) = tokio::sync::mpsc::channel(1);
        let queued_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        self.nudge
            .send(sync::SyncRequest::LogSubscribe {
                requester,
                req: req_id,
                from,
                sink,
                queued_bytes: queued_bytes.clone(),
            })
            .ok()?;
        tokio::spawn(async move {
            while let Some(event) = queue.recv().await {
                if let constellation_net::LogEvent::Frame {
                    segment: Some((_, bytes)),
                    ..
                } = &event
                {
                    queued_bytes
                        .fetch_sub(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                if writer_tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        Some(writer_rx)
    }

    fn lease_requested(
        &self,
        part: String,
        requester: u64,
        epoch_applied: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let declined = constellation_net::Payload::LeaseHandoff {
                part: part.clone(),
                epoch: 0,
                released: false,
                etag: None,
                head_seq: None,
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::HandOff {
                    requester,
                    epoch_applied,
                    reply: tx,
                })
                .is_err()
            {
                return declined;
            }
            match rx.await {
                Ok(Some(handed)) => {
                    tracing::info!(
                        part,
                        requester,
                        epoch = handed.epoch,
                        "handed the lease to a peer"
                    );
                    constellation_net::Payload::LeaseHandoff {
                        part,
                        epoch: handed.epoch,
                        released: true,
                        etag: handed.etag,
                        head_seq: handed.head_seq,
                    }
                }
                // Not ours, flush failed, or the task went away: the
                // requester falls back to the S3 path, which is always
                // correct — it just costs the TTL wait.
                _ => declined,
            }
        })
    }

    fn deleg_backup_append_requested(
        &self,
        from: u64,
        req_id: u64,
        gen: u64,
        txs: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let sealed = constellation_net::Payload::DelegBackupAck {
                req_id,
                gen,
                acked: 0,
                sealed: true,
            };
            let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::DelegateTx>>(&txs) else {
                return sealed;
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::PeerDelegBackupAppend {
                    from,
                    gen,
                    txs,
                    reply,
                })
                .is_err()
            {
                return sealed;
            }
            match receive.await {
                Ok((acked, is_sealed)) => constellation_net::Payload::DelegBackupAck {
                    req_id,
                    gen,
                    acked,
                    sealed: is_sealed,
                },
                Err(_) => sealed,
            }
        })
    }

    fn deleg_seal_requested(
        &self,
        root: u64,
        req_id: u64,
        gen: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let none = constellation_net::Payload::DelegSealed {
                req_id,
                gen,
                sealed: false,
                txs: Vec::new(),
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::PeerDelegSeal { root, gen, reply })
                .is_err()
            {
                return none;
            }
            match receive.await {
                Ok((sealed, txs)) => constellation_net::Payload::DelegSealed {
                    req_id,
                    gen,
                    sealed,
                    txs: postcard::to_allocvec(&txs).unwrap_or_default(),
                },
                Err(_) => none,
            }
        })
    }

    fn epoch_proposed(
        &self,
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        proposer: u64,
        epoch_slack: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            // Plan 30 §M10 (the member rule, see `handle_propose_checked`):
            // probe S3 now — one lease GET, bounded — rather than trust a
            // last-success time that a follower on the holder's log stream
            // may not have refreshed for seconds.
            // A member whose own rounds are failing at S3 is in the
            // outage: it joins at once (a probe would only delay the
            // formation, eating into the carried lease's usable window).
            let reaches_s3 = self.probe_s3().await;
            self.epochs.handle_propose_checked(
                epoch_id,
                members,
                base,
                proposer,
                epoch_slack,
                reaches_s3,
            )
        })
    }

    fn epoch_aborted(&self, epoch_id: String, proposer: u64) {
        if self.epochs.handle_abort(&epoch_id, proposer) {
            let _ = self.nudge.send(sync::SyncRequest::EpochChanged);
        }
    }

    fn epoch_activated(&self, activation: constellation_net::EpochActivation) {
        self.epochs.handle_activate(activation);
        let _ = self.nudge.send(sync::SyncRequest::EpochChanged);
        let _ = self.nudge.send(sync::SyncRequest::Nudge);
    }

    fn promise_requested(
        &self,
        requester: u64,
        req_id: u64,
        expires_unix_ms: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let refuse = constellation_net::Payload::PromiseReply {
                req_id,
                until: None,
                epoch_slack: 0,
            };
            if crate::fault::p2p_denied(requester) {
                // Fault injection: the link is cut; no answer.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                return refuse;
            }
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::PeerPromiseRequest {
                    requester,
                    expires_unix_ms,
                    reply,
                })
                .is_err()
            {
                return refuse;
            }
            match receive.await {
                Ok((until, epoch_slack)) => constellation_net::Payload::PromiseReply {
                    req_id,
                    until,
                    epoch_slack,
                },
                Err(_) => refuse,
            }
        })
    }

    fn cache_digest(&self, digest: constellation_net::DigestSnapshot) {
        self.coop.apply_digest(digest);
    }

    fn cache_digest_delta(&self, delta: constellation_net::DigestDelta) {
        self.coop.apply_delta(delta);
    }

    fn cache_summary(&self, node_id: u64, summary: constellation_net::reconcile::Summary) {
        self.coop.apply_summary(node_id, summary);
    }

    fn cache_set_delta(&self, node_id: u64, delta: constellation_net::reconcile::Delta) {
        self.coop.apply_set_delta(node_id, delta);
    }

    fn reconcile_requested(
        &self,
        queries: Vec<constellation_net::reconcile::Query>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move { self.coop.reconcile_reply(queries).await })
    }

    fn serve_chunk(
        &self,
        hash: [u8; 32],
        from_hex: String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<u8>, constellation_net::ChunkDecline>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move { self.coop.serve_chunk(hash, &from_hex).await })
    }

    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn chunks_durable(
        &self,
        from: u64,
        hashes: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let (reply, done) = tokio::sync::oneshot::channel();
            let hashes = hashes
                .into_iter()
                .map(constellation_fs_core::ChunkHash)
                .collect();
            if self
                .nudge
                .send(sync::SyncRequest::ChunksDurable {
                    from,
                    hashes,
                    reply,
                })
                .is_ok()
            {
                let _ = done.await;
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn mutate_requested(
        &self,
        part: String,
        requester: u64,
        req_id: u64,
        _epoch_seen: u64,
        op: Vec<u8>,
        rid: (u64, u32, u64),
        acked_through: u64,
        deps: Vec<u8>,
        pending: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let _ = part;
            let pending = pending
                .into_iter()
                .map(constellation_fs_core::ChunkHash)
                .collect();
            let rid = constellation_meta::Rid {
                node: rid.0,
                incarnation: rid.1,
                seq: rid.2,
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            let started = std::time::Instant::now();
            tracing::trace!(target: "constellation::fwd", rid = rid.seq, rnode = rid.node, "mutate request queued");
            let (outcome, base, position, gen, own_chunks) = if self
                .nudge
                .send(sync::SyncRequest::Mutate {
                    requester,
                    op,
                    rid,
                    acked_through,
                    deps,
                    pending,
                    reply,
                })
                .is_ok()
            {
                receive.await.unwrap_or((
                    constellation_meta::MutateOutcome::Busy,
                    None,
                    constellation_meta::Position::ZERO,
                    0,
                    constellation_meta::OwnChunks::None,
                ))
            } else {
                (
                    constellation_meta::MutateOutcome::Busy,
                    None,
                    constellation_meta::Position::ZERO,
                    0,
                    constellation_meta::OwnChunks::None,
                )
            };
            tracing::trace!(target: "constellation::fwd", rid = rid.seq, rnode = rid.node, "mutate reply taken");
            tracing::trace!(
                requester,
                service_us = started.elapsed().as_micros() as u64,
                "forwarded mutate served"
            );
            let own_wire = own_chunks.to_wire();
            constellation_net::Payload::MutateReply {
                req_id,
                outcome: outcome.to_postcard().unwrap_or_default(),
                base,
                position_seq: position.seq,
                position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
                position_streams: position.streams_wire(),
                gen,
                own_chunks: own_wire.0,
                own_inos: own_wire.1,
            }
        })
    }

    fn snapshot_batch_requested(
        &self,
        requester: u64,
        req_id: u64,
        rid: (u64, u32, u64),
        items: Vec<constellation_net::SnapshotItem>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            tracing::debug!(
                requester,
                items = items.len(),
                "snapshot batch forwarded to this node"
            );
            // The rid keys this node's dedup: a peer may only name its own.
            if rid.0 != requester {
                let outcome = constellation_net::SnapshotBatchOutcome::Failed(format!(
                    "batch rid belongs to node {}, not the requester {requester}",
                    rid.0
                ));
                return constellation_net::Payload::SnapshotBatchReply { req_id, outcome };
            }
            let mut outcome = self
                .snapshot_batches
                .execute(crate::snapshot_batch::rid_from_wire(rid), &items)
                .await;
            // Reasons name paths: bound them, or a large batch's reply
            // outgrows the frame after the batch has run.
            outcome.clip_reasons();
            constellation_net::Payload::SnapshotBatchReply { req_id, outcome }
        })
    }

    fn delegate_stream_requested(
        &self,
        from: u64,
        req_id: u64,
        gen: u64,
        txs: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let refuse = constellation_net::Payload::DelegateStreamAck {
                req_id,
                gen,
                through: 0,
                refused: true,
            };
            let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::DelegateTx>>(&txs) else {
                return refuse;
            };
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::PeerDelegateStream {
                    from,
                    gen,
                    txs,
                    reply,
                })
                .is_err()
            {
                return refuse;
            }
            match receive.await {
                Ok((through, refused)) => constellation_net::Payload::DelegateStreamAck {
                    req_id,
                    gen,
                    through,
                    refused,
                },
                Err(_) => refuse,
            }
        })
    }

    fn deleg_renew_requested(
        &self,
        from: u64,
        req_id: u64,
        gen: u64,
        backup: u64,
        stream_head: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let (ttl_ms, locks, lock_grace_ms, lock_floor) = if self
                .nudge
                .send(sync::SyncRequest::PeerDelegRenew {
                    from,
                    gen,
                    backup,
                    stream_head,
                    reply,
                })
                .is_ok()
            {
                receive.await.unwrap_or_default()
            } else {
                Default::default()
            };
            constellation_net::Payload::DelegRenewed {
                req_id,
                gen,
                ttl_ms,
                locks: crate::locks::grants_wire(&locks),
                lock_grace_ms,
                lock_floor: crate::locks::floor_wire(&lock_floor),
            }
        })
    }

    fn deleg_recall_requested(
        &self,
        root: u64,
        req_id: u64,
        dir: u64,
        gen: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let (through, locks) = if self
                .nudge
                .send(sync::SyncRequest::PeerDelegRecall {
                    root,
                    dir,
                    gen,
                    reply,
                })
                .is_ok()
            {
                receive.await.unwrap_or_default()
            } else {
                Default::default()
            };
            constellation_net::Payload::DelegRecalled {
                req_id,
                gen,
                through,
                locks: crate::locks::grants_wire(&locks.grants),
                lock_floor: crate::locks::floor_wire(&locks.floor),
            }
        })
    }

    fn chunk_handoff_requested(
        &self,
        requester: u64,
        req_id: u64,
        hashes: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(sync::SyncRequest::AcceptHandoff {
                    requester,
                    hashes: hashes
                        .into_iter()
                        .map(constellation_fs_core::ChunkHash)
                        .collect(),
                    reply,
                })
                .is_ok();
            let uploaded = sent && receive.await.unwrap_or(false);
            constellation_net::Payload::ChunkHandoffReply { req_id, uploaded }
        })
    }

    fn read_index_requested(
        &self,
        requester: u64,
        req_id: u64,
        ino: u64,
        dir: bool,
        name: Option<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(sync::SyncRequest::PeerReadIndex {
                    requester,
                    ino,
                    dir,
                    name,
                    reply,
                })
                .is_ok();
            let outcome = if sent {
                receive
                    .await
                    .unwrap_or(constellation_authority::ReadIndexOutcome::Busy)
            } else {
                constellation_authority::ReadIndexOutcome::Busy
            };
            use constellation_authority::ReadIndexOutcome as O;
            let (status, holder, position, grant) = match outcome {
                O::Ok { position, grant } => {
                    (0, 0, position, grant.map(|g| (g.id, g.ttl_ms, g.epoch)))
                }
                O::NotHolder { holder } => (1, holder, constellation_meta::Position::ZERO, None),
                O::Busy => (2, 0, constellation_meta::Position::ZERO, None),
            };
            constellation_net::Payload::ReadIndexReply {
                req_id,
                status,
                holder,
                position_seq: position.seq,
                position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
                grant,
                position_streams: position.streams_wire(),
            }
        })
    }

    fn backup_append_requested(
        &self,
        holder: u64,
        req_id: u64,
        epoch: u64,
        config_version: u64,
        from: u64,
        txs: Vec<u8>,
        through: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            // `sealed` is the safe refusal: the holder never counts this
            // node again until a reconfiguration (a node that cannot
            // persist the append must not be credited).
            let refuse = constellation_net::Payload::BackupAck {
                req_id,
                epoch,
                acked: 0,
                sealed: true,
            };
            if crate::fault::p2p_denied(holder) {
                // Fault injection: the link is cut; no answer.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                return refuse;
            }
            let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::BackupTx>>(&txs) else {
                return refuse;
            };
            tracing::trace!(target: "constellation::fwd", from, "append queued");
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::PeerBackupAppend {
                    holder,
                    epoch,
                    config_version,
                    from,
                    txs,
                    through,
                    reply,
                })
                .is_err()
            {
                return refuse;
            }
            let out = match receive.await {
                Ok((acked, sealed)) => constellation_net::Payload::BackupAck {
                    req_id,
                    epoch,
                    acked,
                    sealed,
                },
                Err(_) => refuse,
            };
            tracing::trace!(target: "constellation::fwd", from, "append reply taken");
            out
        })
    }

    fn stream_ahead(&self, from: u64, epoch: u64, base: u64, txs: Vec<u8>) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let Ok(txs) = postcard::from_bytes::<Vec<constellation_meta::BackupTx>>(&txs) else {
            return;
        };
        let _ = self.nudge.send(sync::SyncRequest::PeerStreamAhead {
            from,
            epoch,
            base,
            txs,
        });
    }

    fn lock_requested(
        &self,
        requester: u64,
        req_id: u64,
        ino: u64,
        exclusive: bool,
        blocking: bool,
        sent: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let outcome = if self
                .nudge
                .send(sync::SyncRequest::PeerLockRequest {
                    requester,
                    ino,
                    mode: crate::locks::mode_of(exclusive),
                    blocking,
                    sent,
                    reply,
                })
                .is_ok()
            {
                receive
                    .await
                    .unwrap_or(constellation_authority::LockOutcome::Busy)
            } else {
                constellation_authority::LockOutcome::Busy
            };
            constellation_net::Payload::LockReply {
                req_id,
                outcome: crate::locks::outcome_wire(&outcome),
            }
        })
    }

    fn lock_recall_requested(
        &self,
        owner: u64,
        req_id: u64,
        ino: u64,
        grant: (u64, u64),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(sync::SyncRequest::PeerLockRecall {
                    owner,
                    ino,
                    grant: crate::locks::grant_of(grant),
                    reply,
                })
                .is_ok();
            // A dropped reply (daemon shutting down) acks nothing: the
            // owner outwaits the grant.
            if sent && receive.await.is_ok() {
                return constellation_net::Payload::LockRecalled { req_id };
            }
            constellation_net::Payload::LockRecalled { req_id: 0 }
        })
    }

    fn lock_renew_requested(
        &self,
        from: u64,
        req_id: u64,
        entries: Vec<constellation_net::LockRenewWire>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let sent = self
                .nudge
                .send(sync::SyncRequest::PeerLockRenew {
                    from,
                    entries: crate::locks::renew_entries_of(entries),
                    reply,
                })
                .is_ok();
            let results = if sent { receive.await.ok() } else { None };
            match results {
                Some(results) => constellation_net::Payload::LockRenewed {
                    req_id,
                    results: crate::locks::renew_results_wire(&results),
                },
                // Unanswered: a reply for no request, which the renewer
                // treats as a failed renewal and retries.
                None => constellation_net::Payload::LockRenewed {
                    req_id: 0,
                    results: Vec::new(),
                },
            }
        })
    }

    fn lock_test_requested(
        &self,
        requester: u64,
        req_id: u64,
        ino: u64,
        exclusive: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let outcome = if self
                .nudge
                .send(sync::SyncRequest::PeerLockTest {
                    requester,
                    ino,
                    mode: crate::locks::mode_of(exclusive),
                    reply,
                })
                .is_ok()
            {
                receive
                    .await
                    .unwrap_or(constellation_authority::LockTestOutcome::NotOwner { owner: 0 })
            } else {
                constellation_authority::LockTestOutcome::NotOwner { owner: 0 }
            };
            constellation_net::Payload::LockTestReply {
                req_id,
                outcome: crate::locks::test_outcome_wire(outcome),
            }
        })
    }

    fn lock_granted(
        &self,
        from: u64,
        ino: u64,
        sent: i64,
        outcome: constellation_net::LockOutcomeWire,
    ) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let _ = self.nudge.send(sync::SyncRequest::PeerLockGranted {
            from,
            ino,
            sent,
            outcome: crate::locks::outcome_of(outcome),
        });
    }

    fn lock_released(&self, from: u64, ino: u64, grant: (u64, u64), position: &[u8]) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let _ = self.nudge.send(sync::SyncRequest::PeerLockReleased {
            from,
            ino,
            grant: crate::locks::grant_of(grant),
            position: constellation_meta::Position::from_postcard(position),
        });
    }

    fn lock_mirror(&self, from: u64, ver: u64, grants: Vec<u8>, floor: Vec<u8>) {
        if crate::fault::p2p_denied(from) {
            return;
        }
        let _ = self.nudge.send(sync::SyncRequest::PeerLockMirror {
            from,
            ver,
            grants: crate::locks::grants_of(&grants),
            floor: crate::locks::floor_of(&floor),
        });
    }

    fn read_recall_requested(
        &self,
        holder: u64,
        req_id: u64,
        ino: u64,
        grant: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(sync::SyncRequest::PeerRecall {
                    holder,
                    ino,
                    grant,
                    reply,
                })
                .is_ok()
            {
                // Acked only once the delegation is no longer honoured;
                // a dropped reply (daemon shutting down) acks nothing.
                if receive.await.is_err() {
                    return constellation_net::Payload::ReadRecalled { req_id: 0 };
                }
                return constellation_net::Payload::ReadRecalled { req_id };
            }
            constellation_net::Payload::ReadRecalled { req_id: 0 }
        })
    }

    fn lease_offered(&self, _part: String, epoch: u64) {
        // Only while this node writes: see `Placement::writing_now`.
        if !self.placement.writing_now() {
            tracing::debug!(epoch, "declining a lease offer: not writing now");
            return;
        }
        let _ = self.nudge.send(sync::SyncRequest::ClaimOffer { epoch });
    }

    fn peer_rtts(&self, node_id: u64, rtts: Vec<(u64, u16)>) {
        self.placement.note_peer_rtts(node_id, rtts);
    }
}

/// Start the P2P fast path, or return a disabled handle.
///
/// Everything here is best-effort by design (plan 02 / DESIGN.md §8): a
/// missing node key, an unbindable endpoint, or an unreachable gossip
/// topic all degrade to the S3 polling path rather than failing the
/// mount. `CONSTELLATION_P2P=off` skips it entirely.
pub(crate) async fn start_p2p(
    host: &constellation_platform::HostServices,
    fsmeta: &constellation_store_s3::FsMeta,
    e2e_keys: Option<&constellation_store_s3::SharedE2eKeys>,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    node_id: u64,
    version: &str,
) -> constellation_net::Peers {
    if !constellation_net::enabled() {
        tracing::info!("P2P disabled by CONSTELLATION_P2P; using the S3 path only");
        return constellation_net::Peers::disabled();
    }
    let (keys, key_name) = constellation_net::identity::default_key_store(host);
    let key_path = keys.describe(&key_name);
    let (key, generated) = match constellation_net::identity::load_or_create_in(&*keys, &key_name) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, path = %key_path,
                "no usable node key; running without the P2P fast path");
            return constellation_net::Peers::disabled();
        }
    };
    if generated {
        tracing::info!(path = %key_path, "generated a host node key");
    }
    // E2E filesystems seed the topic from the keyring (never on S3 in the
    // clear); non-E2E uses `meta.json`. A pre-secret filesystem with no
    // seed at all falls back to the UUID.
    let e2e_seed = e2e_keys.map(|keys| *keys.gossip_secret());
    let seed = e2e_seed.or_else(|| fsmeta.gossip_seed());
    let topic = constellation_net::topic_for(seed.as_ref(), &fsmeta.uuid.to_string());
    if seed.is_none() {
        tracing::info!(
            "filesystem predates gossip_secret; deriving the topic from its UUID \
             (weaker: the UUID is not a secret)"
        );
    }
    let p2p = match constellation_net::P2p::spawn(key, topic).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "could not bind the P2P endpoint; using the S3 path only");
            return constellation_net::Peers::disabled();
        }
    };
    let relay = p2p.relay_label().to_string();
    let addr = p2p.advertised_addr().await;
    let pubkey = p2p.pubkey_hex();
    let peers = constellation_net::Peers::new(p2p, node_id);
    // Publish how peers reach us, then learn about them.
    match serde_json::to_value(&addr) {
        Ok(addr_json) => {
            if let Err(e) = constellation_store_s3::publish_p2p(
                store.clone(),
                node_id,
                &pubkey,
                addr_json,
                version,
            )
            .await
            {
                tracing::warn!(error = %e, "could not publish our P2P address; peers cannot dial us");
            }
        }
        Err(e) => tracing::warn!(error = %e, "could not serialize our P2P address"),
    }
    let _ = refresh_peers(&peers, store, None).await;
    tracing::info!(
        node_id,
        peers = peers.snapshot().len(),
        %relay,
        "P2P fast path ready"
    );
    peers
}

/// Re-publish this node's P2P address when the registry's copy (in
/// `scan`) is not what [`constellation_net::Peers::advertised_addr_now`]
/// gives now: an admitted interface came, went or changed its address
/// since the last publish (or the home relay changed). Peers dial what
/// the registry says, so a stale record leaves them dialing addresses
/// that are gone. The published address carries no observed (NAT)
/// addresses ([`constellation_net::AddrPolicy::filter`]), so a relay
/// reconnect that changes those re-publishes nothing.
pub async fn republish_addr_if_changed(
    peers: &constellation_net::Peers,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    node_id: u64,
    version: &str,
    scan: &constellation_store_s3::RegistryScan,
) {
    let (Some(addr), Some(pubkey)) = (peers.advertised_addr_now().await, peers.pubkey_hex()) else {
        return;
    };
    // No direct address at all (the network is down, or not up yet): the
    // record we have is the best guess at what comes back.
    if constellation_net::addrs::direct_addrs(&addr).is_empty() {
        return;
    }
    let Some(own) = scan.live().into_iter().find(|n| n.node_id == node_id) else {
        return;
    };
    let published = own
        .p2p_addr
        .and_then(|v| serde_json::from_value::<constellation_net::EndpointAddr>(v).ok());
    if published.as_ref() == Some(&addr) {
        return;
    }
    let Ok(addr_json) = serde_json::to_value(&addr) else {
        return;
    };
    tracing::info!(
        before = ?published.map(|a| a.addrs),
        now = ?addr.addrs,
        "local P2P addresses changed; re-publishing the registry record"
    );
    if let Err(e) =
        constellation_store_s3::publish_p2p(store, node_id, &pubkey, addr_json, version).await
    {
        tracing::warn!(error = %e, "could not re-publish our P2P address");
    }
}

/// Re-read the registry into the peer directory and allowlist.
pub async fn refresh_peers(
    peers: &constellation_net::Peers,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    epochs: Option<&epoch::EpochManager>,
) -> Option<constellation_store_s3::RegistryScan> {
    if !peers.is_enabled() {
        return None;
    }
    // One LIST of the registry serves both reads (EC2 finding R2-2: they
    // used to list and read every record separately, every 5 s). The
    // peer directory is tolerant of unreadable records (a peer we cannot
    // dial only loses its fast path); the epoch roster is not, so it
    // takes the scan's fail-closed view (`crate::epoch::apply_roster`).
    let scan = constellation_store_s3::registry_scan(store.as_ref()).await;
    if let Some(epochs) = epochs {
        crate::epoch::apply_roster(
            epochs,
            match &scan {
                Ok(scan) => scan.roster(),
                Err(e) => Err(constellation_store_s3::StoreError::Registry(format!(
                    "registry unreachable: {e}"
                ))),
            },
            scan.is_err(),
        );
    }
    let scan = scan.ok();
    match scan.as_ref().map(|scan| scan.live()).ok_or(()) {
        Ok(nodes) => {
            let records: Vec<constellation_net::PeerEnrollment> = nodes
                .into_iter()
                .filter_map(|n| {
                    Some(constellation_net::PeerEnrollment {
                        node_id: n.node_id,
                        pubkey_hex: n.pubkey?,
                        addr_json: n.p2p_addr?,
                        hostname: n.hostname,
                        version: n.version.unwrap_or_default(),
                        created_unix: n.created_unix,
                        p2p_updated_unix: n.p2p_updated_unix,
                        ro: n.ro,
                    })
                })
                .collect();
            peers.refresh_registry(records);
        }
        Err(()) => {
            tracing::debug!("registry refresh failed; keeping the cached peer set")
        }
    }
    scan
}
