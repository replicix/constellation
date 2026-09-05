# Adaptive prefetch

Constellation pipelines cold reads into the local clean-chunk cache without
letting each open file create an independent, unbounded download pool.

Sequential reads start with an 8 MiB byte window. The scheduler raises that
window enough to keep two fair shares of the current global gate queued, and
doubles it when a demand read catches the prefetcher. It never exceeds 2 GiB or
one quarter of the cache budget. Live streams survive 60 seconds without a read
so a multi-second S3 GET cannot repeatedly reset the learned window. Only
sequential streams grow on a stall; a miss on a random-access handle is not
evidence that readahead is behind.
Out-of-order reads within the active byte window are treated as reordering;
larger seeks start a new stream.

Each stream remembers the first chunk index it has not yet offered, so a read
enqueues only the newly exposed leading edge rather than rescanning the whole
window. That keeps per-read scheduling cost constant as the window grows into
hundreds of chunks, instead of holding the dispatcher's lock while walking them.

All streams share one fair round-robin scheduler and one global S3 concurrency
gate. The gate opens at 32 requests and uses the upload controller's
slow-start/AIMD search to maximize aggregate goodput, capped at 128 by default
and configurable up to 512. A cold high-latency path needs many requests in
flight before it delivers useful throughput, so opening at the old value of 8
spent tens of seconds below the knee. Cache hits and peer wins do not train the
S3 controller. Fetches use 500 ms observation
windows (uploads retain their longer three-second windows), allowing finite
high-latency reads to adapt before they finish. The cooperative selector's
goodput remains per-source and per-transfer, so its per-stream value can fall
while aggregate throughput rises; that is expected.

The congestion limit is global, but queue windows are per sequential stream.
Ready streams rotate round-robin and divide both queued bytes and active
requests. When a new stream appears, completed slots go to it until active
shares rebalance, instead of letting an older bulk read retain every permit.
Demand reads never acquire the background gate; if they reach a chunk that is
still queued, they claim it for immediate foreground fetch rather than
downloading it twice.

Ordered directory walks trigger after three forward file reads (skips of up to
eight entries are tolerated). Scan-ahead submits each following file's first
two chunks, up to a 16 MiB window, through the same scheduler and gate. A scan
is reaped after 60 idle seconds, matching the stream reaper: one slow file read
on a high-latency path must not look like the end of the walk. Random
access does not trigger it. Disable this behavior with
`CONSTELLATION_SCAN_AHEAD=off`.

S3 response bodies and decoded plaintext stream through cache-owned spill
files. With end-to-end encryption, ciphertext first streams to a temporary
file; authenticated decryption is still a one-shot operation, serialized to
bound concurrent memory spikes.

The byte-window design follows mountpoint-s3's readahead model; global adaptive
request scheduling follows zfetch-style controlled concurrency rather than a
fixed per-file depth.
