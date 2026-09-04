# Run a chaos soak

Drive same-path conflict races across multiple already-mounted
Constellation nodes for minutes or hours. The tool does not provision
VMs or mount the filesystem; bring nodes up separately, then start
workers and a coordinator.

## Prerequisites

- Constellation mounted on each node (same bucket/prefix)
- `chaos` binary on the coordinator and each worker
  (`cargo build -p constellation-chaos --release`)
- Worker listen addresses reachable from the coordinator (open TCP port)

Fleet bring-up (EC2, binary sync, mount) is out of band — for example
[`.scratch/concurrent_access/aws/manage.sh`](../../../.scratch/concurrent_access/aws/manage.sh)
and [`mount.sh`](../../../mount.sh).

## 1. Start a worker on each node

```bash
chaos worker --listen 0.0.0.0:7400 --mount /tmp/constellation
```

Use the node's reachable IP in the coordinator's `--workers` list
(private IP in a VPC, etc.).

## 2. Run the soak from the coordinator

```bash
chaos run \
  --workers 10.108.0.70:7400,10.108.0.11:7400 \
  --profile soak \
  --duration 4h \
  --seed 42 \
  --store ./chaos-store
```

Useful flags:

- `--scenario namespace|data|cto|all` — restrict workload families
- `--seed` — reproduce a failing schedule
- `--duration` — `60s`, `30m`, or `4h`

## 3. Interpret the result

Exit 0 writes `success` under `--store/<run_id>/`. Non-zero leaves:

- `config.json` — profile and seed
- `history.jsonl` — full invoke/complete stream
- `failure.md` — checker name, message, op ids

After each conflict storm the coordinator **polls** verify reads until all
workers agree (close-to-open lag), up to 60s on soak / 30s on ci. A timeout
with lasting disagreement is a real failure — see
[lasting divergence](../../reference/tools/chaos.md#lasting-divergence-after-a-quiesce-barrier).

Workers are left running so you can inspect the mount.

## 4. Reproduce and re-check

```bash
# Same seed and worker set
chaos run --workers … --profile soak --seed 42 --store ./chaos-store

# Offline checker only
chaos check --history ./chaos-store/<run_id>/history.jsonl
```

## Local mounts without TCP

If several mounts already exist on one host:

```bash
chaos run --mounts /mnt/c0,/mnt/c1,/mnt/c2 --profile ci --seed 42 --store ./out
```

**Give each mount its own `CONSTELLATION_NODE_KEY`.** The default key
path is per user (`~/.config/constellation/node.key`), so several
mounts on one host silently share one P2P identity: every dial between
them fails ("Connecting to ourself is not supported") and the whole
fast path — forwarded mutations, lease handoffs, cooperative cache —
degrades to S3 polling with full idle-release/TTL waits. The daemon
logs a warning when a registry peer advertises its own key, but the
mounts still work (slowly), which makes the misconfiguration easy to
miss. The harness scenarios that model a real fleet
(`disjoint-write-4`, `chaos-soak-4`) set a per-client key via
`Client::with_own_node_key()`.

For the one-click CI path (floci + auto-mount), prefer
`harness run chaos-ci` — see [Testing](TESTING.md).
