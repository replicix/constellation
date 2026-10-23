#!/usr/bin/env python3
"""One fio JSON result (group_reporting) -> key=value pairs: bandwidth, iops, completion latency p50/p99 (us)."""
import json, sys
d = json.load(open(sys.argv[1]))
j = d["jobs"][0]
out = []
for rw in ("read", "write"):
    x = j[rw]
    if x["io_bytes"] == 0:
        continue
    cl = x.get("clat_ns", {}).get("percentile", {})
    out.append(f"{rw}_bw_kib={x['bw']} {rw}_iops={x['iops']:.0f} {rw}_clat_p50_us={cl.get('50.000000', 0)/1000:.0f} "
               f"{rw}_clat_p99_us={cl.get('99.000000', 0)/1000:.0f}")
out.append(f"runtime_ms={j.get('job_runtime', 0)}")
print(" ".join(out))
