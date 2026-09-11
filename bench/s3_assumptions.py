#!/usr/bin/env python3
"""Measure the S3 assumptions behind the Constellation metadata-plane redesign.

Run:  AWS_PROFILE=<profile> python3 bench/s3_assumptions.py [sections...]   (needs boto3)
      sections: prims tail ship boot all (default: all); add --cleanup to delete.

Target selection (env, all optional):
  S3_BUCKET     bucket name          (required)
  S3_PREFIX     key prefix           (default: bootstrap-test)
  S3_REGION     region               (default: us-west-2)
  S3_ENDPOINT   custom endpoint URL  (e.g. https://s3.eu-west-mil.io.cloud.ovh.net for OVH)
  S3_ADDRESSING path|virtual         (default: virtual; OVH/MinIO usually want path)
  Credentials: AWS_PROFILE, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (OVH keys work as-is).

Sections
  prims  per-request latency: LIST / GET-hit / GET-404 / HEAD / PUT /
         conditional PUT (If-None-Match) / CAS PUT (If-Match).
         -> is a GET-404 really cheaper/faster than a LIST for "is seq N there"?
  tail   reader catch-up over 64 small segments: LIST-per-round (today,
         shipper.rs tail_part_listed) vs speculative GET-next pipelined k=8.
  ship   writer throughput over 32 segments: LIST+PUT per segment (today,
         sync_all_inner), PUT-only sequential, PUT pipelined depth 4.
  boot   bootstrap download of ~64 MiB: one object (today's DB image) vs
         4096 x 16 KiB blobs at 64/256 concurrency (Merkle tree blobs) vs
         64 x 1 MiB (packed tree blobs).
"""
import argparse
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = os.environ["S3_BUCKET"]
PREFIX = os.environ.get("S3_PREFIX", "bootstrap-test")
REGION = os.environ.get("S3_REGION", "us-west-2")
ENDPOINT = os.environ.get("S3_ENDPOINT") or None

s3 = boto3.session.Session(profile_name=os.environ.get("AWS_PROFILE")).client(
    "s3",
    region_name=REGION,
    endpoint_url=ENDPOINT,
    config=Config(
        max_pool_connections=300,
        retries={"max_attempts": 10, "mode": "adaptive"},
        tcp_keepalive=True,
        s3={"addressing_style": os.environ.get("S3_ADDRESSING", "virtual")},
    ),
)


def key(*parts):
    return "/".join([PREFIX, *[str(p) for p in parts]])


def timed(fn, *a, **kw):
    t = time.perf_counter()
    r = fn(*a, **kw)
    return (time.perf_counter() - t) * 1000.0, r


def stats(ms, label, n=None):
    ms = sorted(ms)
    p = lambda q: ms[min(len(ms) - 1, int(q * len(ms)))]
    print(
        f"  {label:<44} n={len(ms):>4}  p50={p(0.5):7.1f}ms  p90={p(0.9):7.1f}ms  "
        f"max={ms[-1]:7.1f}ms  mean={statistics.fmean(ms):7.1f}ms"
    )


def put(k, body, **extra):
    return s3.put_object(Bucket=BUCKET, Key=k, Body=body, **extra)


def get(k):
    return s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()


def get_404(k):
    try:
        s3.get_object(Bucket=BUCKET, Key=k)
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return True
        raise


def head(k):
    return s3.head_object(Bucket=BUCKET, Key=k)


def list_from(prefix, start_after=None, max_keys=1000):
    kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=max_keys)
    if start_after:
        kw["StartAfter"] = start_after
    r = s3.list_objects_v2(**kw)
    return [o["Key"] for o in r.get("Contents", [])]


def exists(k):
    try:
        head(k)
        return True
    except ClientError:
        return False


def pmap(fn, items, concurrency):
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(fn, it) for it in items]
        return [f.result() for f in as_completed(futs)]


def seg_key(part, seq):
    return key("log", part, f"{seq:016x}.zst")


def ensure_segments(part, n, size):
    """Idempotently create n segments of `size` bytes under log/<part>/."""
    have = set(list_from(key("log", part) + "/"))
    todo = [s for s in range(1, n + 1) if seg_key(part, s) not in have]
    if todo:
        body = os.urandom(size)
        pmap(lambda s: put(seg_key(part, s), body), todo, 32)
    return n


# ---------------------------------------------------------------- sections
SECTIONS = {}


def section(name):
    def deco(fn):
        SECTIONS[name] = fn
        return fn

    return deco


@section("prims")
def prims(n=20):
    """Latency of each S3 primitive the design relies on."""
    print(f"\n== prims: per-request latency (client -> {REGION}"
          f"{' @ ' + ENDPOINT if ENDPOINT else ''}) ==")
    base = key("prims")
    small = b"x" * 200
    put(f"{base}/hit", small)
    ensure_segments("prims-full", 1000, 64)  # a 1000-key prefix for LIST

    stats([timed(list_from, f"{base}/nothing/")[0] for _ in range(n)], "LIST (empty prefix)")
    stats([timed(list_from, key("log", "prims-full") + "/")[0] for _ in range(n)],
          "LIST (1000 keys, one page)")
    stats([timed(list_from, key("log", "prims-full") + "/",
                 seg_key("prims-full", 990))[0] for _ in range(n)],
          "LIST StartAfter -> 10 keys (tail today)")
    stats([timed(get, f"{base}/hit")[0] for _ in range(n)], "GET hit (200 B)")
    stats([timed(get_404, f"{base}/missing")[0] for _ in range(n)], "GET 404 (GET-next miss)")
    stats([timed(head, f"{base}/hit")[0] for _ in range(n)], "HEAD hit")
    stats([timed(put, f"{base}/put", small)[0] for _ in range(n)], "PUT 200 B (plain)")
    stats([timed(put, f"{base}/put10k", os.urandom(10_000))[0] for _ in range(n)],
          "PUT 10 KB (a log segment)")

    def cas_create():
        k = f"{base}/create-{time.time_ns()}"
        return put(k, small, IfNoneMatch="*")

    stats([timed(cas_create)[0] for _ in range(n)], "PUT If-None-Match:* (segment CAS)")

    def cas_swap_pair():
        # lease renew: GET etag then PUT If-Match (2 RTT today; the PUT alone is the CAS)
        etag = s3.head_object(Bucket=BUCKET, Key=f"{base}/lease")["ETag"]
        return put(f"{base}/lease", small, IfMatch=etag)

    put(f"{base}/lease", small)
    stats([timed(cas_swap_pair)[0] for _ in range(n)], "HEAD + PUT If-Match (lease CAS)")

    def cas_conflict(client=s3):
        try:
            client.put_object(Bucket=BUCKET, Key=f"{base}/lease", Body=small, IfMatch='"deadbeef"')
            return False
        except ClientError as e:
            return e.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    ok = [timed(cas_conflict) for _ in range(5)]
    assert all(r for _, r in ok), "If-Match with a stale etag must 412"
    stats([t for t, _ in ok], "PUT If-Match stale -> 412 (fencing works)")
    # Same, with client retries off: tells whether the slow 412 is the
    # server or botocore retrying PreconditionFailed under the hood.
    noretry = boto3.session.Session(profile_name=os.environ.get("AWS_PROFILE")).client(
        "s3", region_name=REGION, endpoint_url=ENDPOINT,
        config=Config(retries={"max_attempts": 1},
                      s3={"addressing_style": os.environ.get("S3_ADDRESSING", "virtual")}),
    )
    ok = [timed(cas_conflict, noretry) for _ in range(5)]
    assert all(r for _, r in ok)
    stats([t for t, _ in ok], "PUT If-Match stale -> 412 (no client retry)")


@section("tail")
def tail(n_segs=64, size=9_500):
    """Reader catch-up: LIST every round (today) vs GET-next pipelined."""
    print(f"\n== tail: reader catches up {n_segs} segments of {size} B ==")
    part = "tail"
    ensure_segments(part, n_segs, size)
    prefix = key("log", part) + "/"

    # (a) today: LIST from next, GET the contiguous run 8-wide, re-LIST until empty.
    t0 = time.perf_counter()
    applied, lists, gets = 0, 0, 0
    while True:
        keys = list_from(prefix, seg_key(part, applied))
        lists += 1
        run = [s for s in range(applied + 1, applied + 1 + len(keys))]
        if not run:
            break
        for i in range(0, len(run), 8):
            pmap(lambda s: get(seg_key(part, s)), run[i:i + 8], 8)
            gets += len(run[i:i + 8])
        applied = run[-1]
    a = time.perf_counter() - t0
    print(f"  LIST-per-round (today):        {a*1000:7.0f} ms  LIST={lists} GET={gets}")

    # (b) proposed: speculative GET next..next+k; a 404 ends the run. No LIST.
    def get_or_none(s):
        try:
            return s, get(seg_key(part, s))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return s, None
            raise

    for k in (8, 16):
        t0 = time.perf_counter()
        applied, gets, misses = 0, 0, 0
        while True:
            res = dict(pmap(get_or_none, range(applied + 1, applied + 1 + k), k))
            gets += k
            got = 0
            for s in range(applied + 1, applied + 1 + k):
                if res[s] is None:
                    break
                got += 1
            misses += k - got
            applied += got
            if got < k:
                break
        b = time.perf_counter() - t0
        print(f"  GET-next pipelined k={k:<2}:        {b*1000:7.0f} ms  GET={gets} (404s={misses}) LIST=0")

    # (c) idle poll cost: "is there anything new?" when nothing is new
    stats([timed(list_from, prefix, seg_key(part, n_segs))[0] for _ in range(10)],
          "idle poll via LIST (today, every 500ms)")
    stats([timed(get_404, seg_key(part, n_segs + 1))[0] for _ in range(10)],
          "idle poll via GET-next 404")


@section("ship")
def ship(n_segs=32, size=9_500):
    """Writer throughput for a burst of segments under different loops."""
    print(f"\n== ship: writer ships {n_segs} segments of {size} B ==")
    body = os.urandom(size)
    run_id = time.time_ns()

    def fresh(name):
        return f"ship-{name}-{run_id}"

    def cas_put(part, s):
        return put(seg_key(part, s), body, IfNoneMatch="*")

    # (a) today: every round = LIST own partition (returns nothing new) + 1 CAS PUT
    part = fresh("today")
    t0 = time.perf_counter()
    for s in range(1, n_segs + 1):
        list_from(key("log", part) + "/", seg_key(part, s - 1))
        cas_put(part, s)
    a = time.perf_counter() - t0
    print(f"  LIST+PUT per segment (today):  {a*1000:7.0f} ms  -> {n_segs/a:5.1f} seg/s")

    # (b) holder does not tail its own partition: PUT only, sequential
    part = fresh("putonly")
    t0 = time.perf_counter()
    for s in range(1, n_segs + 1):
        cas_put(part, s)
    b = time.perf_counter() - t0
    print(f"  PUT only, sequential:          {b*1000:7.0f} ms  -> {n_segs/b:5.1f} seg/s")

    # (c) pipelined CAS PUTs (holder is the only appender; readers wait for contiguity)
    for depth in (4, 8):
        part = fresh(f"pipe{depth}")
        t0 = time.perf_counter()
        pmap(lambda s: cas_put(part, s), range(1, n_segs + 1), depth)
        c = time.perf_counter() - t0
        print(f"  PUT pipelined depth={depth}:        {c*1000:7.0f} ms  -> {n_segs/c:5.1f} seg/s")

    # sanity: CAS still rejects a duplicate seq after pipelining
    try:
        cas_put(part, 1)
        raise AssertionError("duplicate seq must fail")
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        print("  CAS on duplicate seq -> 412: OK")


@section("boot")
def boot(total_mib=64):
    """Bootstrap: one big object vs many small blobs vs packed blobs."""
    print(f"\n== boot: download ~{total_mib} MiB three ways ==")
    total = total_mib << 20
    shapes = [
        ("image", 1, total),                # today's whole-DB checkpoint
        ("tree16k", total // 16384, 16384),  # Merkle dir blobs
        ("pack1m", total_mib, 1 << 20),      # packed tree blobs
    ]
    for name, count, size in shapes:
        pfx = key("boot", name)
        have = len(list_from(pfx + "/")) if count > 1 else int(exists(f"{pfx}/0"))
        if have < count:
            print(f"  seeding {name}: {count} x {size} B ...", end="", flush=True)
            t0 = time.perf_counter()
            if count == 1:
                # multipart upload via the transfer manager (what a real checkpoint uses)
                import io
                s3.upload_fileobj(io.BytesIO(os.urandom(size)), BUCKET, f"{pfx}/0")
            else:
                body = os.urandom(size)
                pmap(lambda i: put(f"{pfx}/{i}", body), range(count), 64)
            print(f" {time.perf_counter()-t0:5.1f}s")

    def fetch_all(pfx, count, conc):
        got = pmap(lambda i: len(get(f"{pfx}/{i}")), range(count), conc)
        assert sum(got) == total
        return got

    import io
    t0 = time.perf_counter()
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET, key("boot", "image") + "/0", buf)
    a = time.perf_counter() - t0
    print(f"  1 x {total_mib} MiB image (transfer mgr):     {a:6.2f}s  {total/a/2**20:6.1f} MiB/s")

    t0 = time.perf_counter()
    get(key("boot", "image") + "/0")
    a = time.perf_counter() - t0
    print(f"  1 x {total_mib} MiB image (single GET):       {a:6.2f}s  {total/a/2**20:6.1f} MiB/s")

    for conc in (64, 256):
        t0 = time.perf_counter()
        fetch_all(key("boot", "tree16k"), total // 16384, conc)
        b = time.perf_counter() - t0
        print(f"  {total//16384} x 16 KiB blobs, conc={conc:<3}:        {b:6.2f}s  "
              f"{total/b/2**20:6.1f} MiB/s  ({total//16384/b:5.0f} GET/s)")

    for conc in (16, 64):
        t0 = time.perf_counter()
        fetch_all(key("boot", "pack1m"), total_mib, conc)
        c = time.perf_counter() - t0
        print(f"  {total_mib} x 1 MiB packed, conc={conc:<3}:          {c:6.2f}s  {total/c/2**20:6.1f} MiB/s")


def cleanup():
    print("\n== cleanup ==")
    n = 0
    token = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=PREFIX + "/")
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        objs = [{"Key": o["Key"]} for o in r.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs, "Quiet": True})
            n += len(objs)
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    print(f"  deleted {n} objects under {PREFIX}/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sections", nargs="*", default=["all"])
    ap.add_argument("--cleanup", action="store_true")
    args = ap.parse_args()
    # No explicit credential check: boto3's default chain (profile, env,
    # instance role, SSO cache) decides; a missing identity fails loudly below.
    print(f"bucket=s3://{BUCKET}/{PREFIX}  region={REGION}  endpoint={ENDPOINT or 'aws default'}")
    ms, _ = timed(list_from, key("rtt-probe") + "/")
    print(f"warm-up LIST: {ms:.0f} ms")
    wanted = SECTIONS if "all" in args.sections else {s: SECTIONS[s] for s in args.sections}
    for name, fn in wanted.items():
        fn()
    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    main()

