# Adaptive prefetch

Constellation pipelines cold reads into the local clean-chunk cache without
letting each open file create an independent, unbounded download pool.

Sequential reads start with an 8 MiB byte window. The window doubles when a
demand read catches the prefetcher, shrinks after sustained consumption without
a stall, and never exceeds 256 MiB or one quarter of the cache budget.
Out-of-order reads within the active byte window are treated as reordering;
larger seeks start a new stream.

All streams share one fair round-robin scheduler and one global S3 concurrency
gate. The gate starts at eight requests and uses the upload controller's
slow-start/AIMD search to maximize aggregate goodput, capped at 128. Cache hits
and peer wins do not train the S3 controller. Fetches use 500 ms observation
windows (uploads retain their longer three-second windows), allowing finite
high-latency reads to adapt before they finish. The cooperative selector's
goodput remains per-source and per-transfer, so its per-stream value can fall
while aggregate throughput rises; that is expected.

Ordered directory walks trigger after three forward file reads (skips of up to
eight entries are tolerated). Scan-ahead submits each following file's first
two chunks, up to a 16 MiB window, through the same scheduler and gate. Random
access does not trigger it. Disable this behavior with
`CONSTELLATION_SCAN_AHEAD=off`.

S3 response bodies and decoded plaintext stream through cache-owned spill
files. With end-to-end encryption, ciphertext first streams to a temporary
file; authenticated decryption is still a one-shot operation, serialized to
bound concurrent memory spikes.

The byte-window design follows mountpoint-s3's readahead model; global adaptive
request scheduling follows zfetch-style controlled concurrency rather than a
fixed per-file depth.
