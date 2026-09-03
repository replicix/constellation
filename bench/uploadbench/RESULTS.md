# Upload concurrency: live paths and controller choice

All live runs used 4 MiB objects, HTTP/1.1 `object_store` PUTs, 40 s
fixed sweeps (plus in-flight drain) and 90 s adaptive runs. Production
`constellation` still caps the search at 128
(`UPLOAD_CONCURRENCY_HARD_MAX`). Bench `--max-concurrency` can go
higher; that is how N=256 was measured.

## What the three paths look like

### EU home → `us-west-2`, VPN bandwidth cap (earlier)

Fixed N, 0–100% errors once the tunnel and the 30 s client timeout
disagree.

| N | mean MiB/s | err% | p50 |
|---|------------|------|-----|
| 4 | 2.17 | 0 | — |
| 8 | 3.86 | 0 | — |
| 16 | **7.08** | 0 | 6.5 s |
| 32 | 5.57 | 1.9 | 11.6 s |
| 64 | 2.23 | 57 | 11.8 s |
| 128 | 0 | 100 | — |
| 256 | 0.76 | 94 | 26 s |

Knee around **N=16 (~7 MiB/s)**. More concurrency saturates the tunnel
and trips timeouts, which the controller must treat as congestion.

### EU home → `us-west-2`, no VPN (this session)

Same machine, VPN down. 0 errors through N=128; N=256 hits the 30 s
request timeout.

| N | mean MiB/s | peak | p50 | p95 | err% |
|---|------------|------|-----|-----|------|
| 4 | 6.86 | 11.6 | 1.9 s | 4.5 s | 0 |
| 8 | 13.61 | 22.5 | 1.9 s | 4.0 s | 0 |
| 16 | 15.81 | 37.8 | 3.0 s | 10.3 s | 0 |
| 32 | 22.04 | 42.5 | 4.2 s | 12.0 s | 0 |
| 64 | 23.97 | 51.0 | 8.4 s | 16.5 s | 0 |
| **128** | **26.26** | 72.4 | 16.7 s | 25.5 s | 0 |
| 256 | 11.14 | 94.7 | 32.9 s | 58.1 s | 37.6 |

Uplink is ~25–30 MiB/s once BDP is filled. N=4–8 is still RTT-bound
(almost linear). N=32–64 is the latency-efficient plateau; N=128 still
buys a little mean goodput; N=256 is past the timeout cliff. Production
cap **128 is the right ceiling** for this path, not 256.

p95 at N=128 (25.5 s) sits just under the default 30 s client timeout.
That is why 256 collapses and why 128 is the last safe step, not a
reason to raise the cap.

### `us-west-2` EC2 → `us-west-2` (same region)

2 vCPU instance, `--keep-objects`, 0 errors.

| N | mean MiB/s | p50 | p95 |
|---|------------|-----|-----|
| 16 | 770 | 76 ms | 115 ms |
| 32 | 1500 | 76 ms | 128 ms |
| **64** | **2367** | 100 ms | 150 ms |
| 128 | 2251 | 208 ms | 333 ms |
| 256 | 2638 | 380 ms | 575 ms |

Same-region is a fat, short pipe. Throughput is still climbing at N=64.
N=128 is slightly *slower* with ~2× latency (queueing, not errors).
N=256 buys a bit more goodput at ~5× p50. A bulk write-back drain cares
about bytes/s, so 64 is the latency-efficient knee and 128 (production
cap) sits on the plateau, not on a cliff.

## Controllers on the live EU path (no VPN)

90 s each, start at 4, cap 128, **untuned** AIMD vs Vegas PID:

| ctrl | mean MiB/s | mean N | max N | final N | err% |
|------|------------|--------|-------|---------|------|
| aimd | 12.80 | 10.5 | 15 | 14 | 0 |
| pid | 3.18 | 1.6 | 6 | 1 | 0 |

Fixed-N oracle on the same path: 13.6 MiB/s at 8, 22 at 32, 26 at 128.

**PID** regulates latency inflation (setpoint 1.3× baseline). Filling
BDP *is* latency inflation, so it climbed to 6 then collapsed to 1 and
stayed there. Unusable for bulk S3 write-back.

**AIMD** is the right *objective* (total goodput, not RTT) but the
untuned search **stalled around 14**. Time series: 4→6→8→10, then a
noisy 2 s window failed the 5% gain test, step collapsed to +1, and the
rest of the run inched 8…15. That is why mean throughput matched fixed
N=8, not the 20+ MiB/s the path actually has.

## Tuning (now in `constellation-upload-concurrency`)

Production already called `AdaptiveConcurrency`; this is a policy
change in that crate, not a second implementation behind a flag.

1. **Slow-start doubling** (`N → 2N`) until a probe is rejected, then
   binary-search the failed step. High-BDP paths reach the knee in a
   handful of windows instead of +50% then +1 after one noisy reject.
2. **Longer windows** (3 s min, 8 s max, up to 12 samples). 2 s windows
   at 2 s service times were almost all phase noise.
3. **Two-window confirmation** before treating “no 5% gain” as a knee.
   A 5% *regression* still reverts immediately (errors still halve,
   coalesced).

Synthetic 1 Gbps / 150 ms / SlowDown storm after the change: AIMD
**84.5 MiB/s** mean (was 82.2), peak 108, recovered to 80% of pre-fault
in 10.0 s, final N=16 after the fault. Fixed N=4: 51.0 MiB/s. PID: 29.2
MiB/s and did not recover.

Retuned AIMD confirmed live (EU → us-west-2, no VPN, 90 s, start 4,
cap 128): **23.0 MiB/s** mean, peak 75, **0 errors**, mean N 76.9,
max 128, final 96. Climb was `4→8→16→32→64→128` by t≈32 s, then
binary-search settled in the 96–128 band after one overshoot reject.
Versus untuned AIMD (12.8 MiB/s / N≈14) and fixed N=128 (26.3 MiB/s):
the search now reaches the plateau; the remaining gap to the fixed
oracle is startup + settle time inside a 90 s window.

## Decision

- **Ship the retuned AIMD.** It is already what `crates/cli` uses
  (`UploadRuntime` + `CONSTELLATION_UPLOAD_MAX_CONCURRENCY`, default
  cap 128). Do not switch production to the PID controller.
- **Keep the 128 cap.** It is the last 0-error point on the EU WAN path
  and the plateau on same-region. 256 is a timeout cliff on WAN.
- **Do not pin a global fixed N.** The three paths want ~16, ~64, and
  ~128 respectively. A fixed 8 (old write-back default) leaves ~2–3×
  goodput on the table at home and ~15× in-region.
- Leave PID in `bench/uploadbench` as a negative result: latency-ratio
  control is the wrong signal when the bottleneck is BDP, not a server
  queue you should back away from.
