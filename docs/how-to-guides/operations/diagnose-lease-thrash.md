# Diagnose lease thrash

Use forwarding counters, placement status, and handoff logs to distinguish a
healthy shared workload from repeated lease movement.

## 1. Confirm the symptom

Inspect daemon logs while the workload runs:

```bash
journalctl --user -u constellation -f | grep 'handed the lease to a peer'
```

`handed the lease to a peer` means a requester used the handoff fallback.
Occasional lines during startup or a holder failure are expected. Repeated
lines during steady writes indicate forwarding is timing out, being declined,
or targeting stale holder information.

## 2. Read forwarding status

```bash
constellation status --state-dir "$STATE_DIR"   # or a registered name: `constellation status myfs`
```

Check:

- `forwarded_ok`: should rise for successful non-holder writes.
- `forwarded_err`: rising quickly points to unreachable peers, stale holder
  redirects, `Busy`, decode errors, or an undersized forward timeout.
- `forward_p50_ms`: compare with peer RTT and
  `CONSTELLATION_FORWARD_TIMEOUT_MS` (500 ms by default).
- `pushed_segments_applied`: confirms small committed segments are reaching
  this node over P2P.
- `p2p.enabled` and peer paths: forwarding cannot work when P2P is disabled or
  the holder is disconnected.

## 3. Check placement

Read `placement_reason` in status. The holder considers approximately the last
60 seconds of writes and recommends a writer only when its weighted RTT cost
is less than 70% of the current holder's cost. It then enforces a 60-second
dwell after a successful migration.

No placement reason can be normal: candidates must be recent writers with
direct paths and complete `PeerRtts` data. Relay-only writers are not placement
candidates.

## 4. Isolate automatic placement

To test whether placement is contributing to movement, remount every relevant
node with:

```bash
export CONSTELLATION_LEASE_PLACEMENT=off
```

This disables holder-driven `LeaseOffer` decisions, but leaves forwarded
mutations and correctness fallback enabled. If handoff logs continue, diagnose
P2P reachability and forward timeout rather than placement.

## 5. Check idle release

Bursts separated by more than the default 30 seconds can legitimately release
and reacquire the lease. Raise `CONSTELLATION_LEASE_IDLE_RELEASE_MS` above the
normal inter-burst gap, then remount. Do not use a long idle interval to hide
forwarding failures during an active workload.

See [Lease placement](../../reference/features/lease-placement.md),
[Forwarded mutations](../../reference/features/forwarded-mutations.md), and
[Configuration](../../reference/configuration.md).
