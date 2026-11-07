| candidate | n | peers | churn/min | loss / reorder | staleness p50 / p99 / max ms | unresolved | sessions per peer-min | repair B/peer-min | digest wire B/min (deltas+summaries) | stale-positive lookups (removed <=60 s) | missed (held) | false-positive (absent) | final FN | peers not converged | notes |
# sim --spec c1 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Exact | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Exact | 100000 | 8 | 1% | 0% / 5% | 132 / 489 / 803 | 0 | 7.90 | 61438 | 42824 | 94/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Exact | 100000 | 8 | 1% | 1% / 0% | 128 / 321 / 804 | 0 | 2.73 | 21230 | 42824 | 56/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Exact | 100000 | 8 | 1% | 1% / 5% | 137 / 499 / 1011 | 0 | 9.82 | 76827 | 42824 | 144/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Exact | 100000 | 8 | 1% | 10% / 0% | 144 / 560 / 1296 | 0 | 22.04 | 175776 | 42824 | 269/384000 | 2/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Exact | 100000 | 8 | 1% | 10% / 5% | 149 / 674 / 1309 | 0 | 26.52 | 212401 | 42824 | 343/384000 | 3/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Exact | 100000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 12660 | 15120 | 233/383680 | 1/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c1 --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Exact | 100000 | 8 | 10% | 1% / 5% | 135 / 511 / 809 | 28 | 9.94 | 253481 | 123297 | 142/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c1 --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Exact | 100000 | 64 | 1% | 1% / 5% | 135 / 498 / 1053 | 0 | 9.73 | 75953 | 42824 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c1 --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Exact | 100000 | 8 | 10% | 1% / 5% | 135 / 512 / 809 | 42 | 9.94 | 278718 | 122916 | 152/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Ef | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Ef | 100000 | 8 | 1% | 0% / 5% | 132 / 489 / 803 | 0 | 7.90 | 61438 | 42824 | 94/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Ef | 100000 | 8 | 1% | 1% / 0% | 128 / 321 / 804 | 0 | 2.73 | 21230 | 42824 | 56/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Ef | 100000 | 8 | 1% | 1% / 5% | 137 / 499 / 1011 | 0 | 9.82 | 76827 | 42824 | 144/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Ef | 100000 | 8 | 1% | 10% / 0% | 144 / 560 / 1296 | 0 | 22.04 | 175776 | 42824 | 269/384000 | 2/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Ef | 100000 | 8 | 1% | 10% / 5% | 149 / 674 / 1309 | 0 | 26.52 | 212401 | 42824 | 343/384000 | 3/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Ef | 100000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 12660 | 15120 | 233/383680 | 1/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6a --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Ef | 100000 | 8 | 10% | 1% / 5% | 135 / 511 / 809 | 28 | 9.94 | 253481 | 123297 | 142/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 3s wall
# sim --spec c6a --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Ef | 100000 | 64 | 1% | 1% / 5% | 135 / 498 / 1053 | 0 | 9.73 | 75953 | 42824 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 7s wall
# sim --spec c6a --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Ef | 100000 | 8 | 10% | 1% / 5% | 135 / 512 / 809 | 42 | 9.94 | 278718 | 122916 | 152/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 4s wall
# sim --spec c6b --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c6b --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 1% | 0% / 5% | 133 / 518 / 828 | 0 | 7.90 | 341574 | 42824 | 94/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c6b --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 1% | 1% / 0% | 128 / 354 / 824 | 0 | 2.73 | 117791 | 42824 | 56/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c6b --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 1% | 1% / 5% | 138 / 534 / 1051 | 0 | 9.82 | 425001 | 42824 | 144/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c6b --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 1% | 10% / 0% | 147 / 589 / 1316 | 0 | 22.04 | 955810 | 42824 | 269/384000 | 2/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6b --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 1% | 10% / 5% | 154 / 705 / 1329 | 0 | 26.52 | 1151250 | 42824 | 343/384000 | 3/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6b --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 0.1% | 1% / 5% | 137 / 834 / 1408 | 0 | 2.04 | 85784 | 15120 | 233/383680 | 1/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c6b --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 10% | 1% / 5% | 136 / 532 / 829 | 28 | 9.94 | 562861 | 123297 | 142/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c6b --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 64 | 1% | 1% / 5% | 137 / 532 / 1078 | 0 | 9.73 | 420688 | 42824 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 4s wall
# sim --spec c6b --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Suffix(4, 64) | 100000 | 8 | 10% | 1% / 5% | 136 / 532 / 829 | 42 | 9.94 | 597201 | 122916 | 152/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c3 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 35/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 1% | 0% / 5% | 131 / 473 / 768 | 0 | 7.90 | 131880 | 42824 | 94/384000 | 0/384000 | 52/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 1% | 1% / 0% | 128 / 294 / 764 | 0 | 2.73 | 45270 | 42824 | 56/384000 | 0/384000 | 35/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 164677 | 42824 | 144/384000 | 0/384000 | 52/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 1% | 10% / 0% | 141 / 529 / 1256 | 0 | 22.04 | 379364 | 42824 | 269/384000 | 2/384000 | 37/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 459216 | 42824 | 343/384000 | 3/384000 | 43/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 0.1% | 1% / 5% | 136 / 815 / 1348 | 0 | 2.04 | 24937 | 15120 | 233/383680 | 1/384000 | 42/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c3 --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 10% | 1% / 5% | 134 / 495 / 214833 | 62 | 9.96 | 687293 | 123297 | 175/384000 | 4/384000 | 40/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 2, 1s wall
# sim --spec c3 --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 64 | 1% | 1% / 5% | 134 / 481 / 1018 | 0 | 9.73 | 162643 | 42824 | 1253/3072000 | 4/3072000 | 349/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c3 --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256) | 100000 | 8 | 10% | 1% / 5% | 137 / 497 / 137438 | 48 | 27.40 | 1071900 | 122916 | 200/384000 | 4/384000 | 38/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 1493, 1s wall
# sim --spec c4 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 6/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 1% | 0% / 5% | 131 / 473 / 768 | 0 | 7.90 | 131880 | 42824 | 94/384000 | 0/384000 | 7/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 1% | 1% / 0% | 128 / 294 / 764 | 0 | 2.73 | 45270 | 42824 | 56/384000 | 0/384000 | 10/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 164677 | 42824 | 144/384000 | 0/384000 | 15/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 1% | 10% / 0% | 141 / 529 / 1256 | 0 | 22.04 | 379364 | 42824 | 269/384000 | 2/384000 | 10/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 459216 | 42824 | 343/384000 | 3/384000 | 6/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 0.1% | 1% / 5% | 136 / 815 / 1348 | 0 | 2.04 | 24937 | 15120 | 233/383680 | 1/384000 | 10/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c4 --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 10% | 1% / 5% | 134 / 495 / 809 | 36 | 9.94 | 686980 | 123297 | 150/384000 | 4/384000 | 14/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c4 --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 64 | 1% | 1% / 5% | 134 / 481 / 1018 | 0 | 9.73 | 162643 | 42824 | 1253/3072000 | 4/3072000 | 77/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 2s wall
# sim --spec c4 --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Quotient(14, 256) | 100000 | 8 | 10% | 1% / 5% | 134 / 495 / 829 | 42 | 9.94 | 835301 | 122916 | 152/384000 | 4/384000 | 14/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c5 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Fuse(4096) | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 5/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c5 --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 8 | 1% | 0% / 5% | 132 / 504 / 863 | 0 | 7.90 | 707955 | 42824 | 94/384000 | 0/384000 | 4/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c5 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Fuse(4096) | 100000 | 8 | 1% | 1% / 0% | 128 / 339 / 824 | 0 | 2.73 | 239309 | 42824 | 57/384000 | 0/384000 | 8/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c5 --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 8 | 1% | 1% / 5% | 137 / 517 / 1111 | 0 | 9.82 | 890753 | 42824 | 144/384000 | 0/384000 | 6/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c5 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Fuse(4096) | 100000 | 8 | 1% | 10% / 0% | 146 / 609 / 1396 | 0 | 22.04 | 2140240 | 42824 | 276/384000 | 2/384000 | 10/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c5 --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 8 | 1% | 10% / 5% | 152 / 718 / 1409 | 0 | 26.52 | 2607752 | 42824 | 353/384000 | 3/384000 | 10/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c5 --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 46619 | 15120 | 233/383680 | 1/384000 | 6/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c5 --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 8 | 10% | 1% / 5% | 138 / 711 / 1088 | 28 | 9.94 | 5217913 | 123297 | 145/384000 | 4/384000 | 6/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 2s wall
# sim --spec c5 --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 64 | 1% | 1% / 5% | 136 / 516 / 1113 | 0 | 9.73 | 881057 | 42824 | 1275/3072000 | 4/3072000 | 34/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 3s wall
# sim --spec c5 --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Fuse(4096) | 100000 | 8 | 10% | 1% / 5% | 138 / 920 / 1404 | 42 | 9.94 | 6478735 | 122916 | 153/384000 | 4/384000 | 3/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 3s wall
# sim --spec bloom --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Bloom | 100000 | 8 | 1% | 0% / 0% | 244 / 29694 / 81875 | 8 | 0.00 | 0 | 42824 | 112207/384000 | 0/384000 | 3133/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 8 | 1% | 0% / 5% | 610 / 29690 / 81797 | 9 | 0.00 | 0 | 42824 | 111815/384000 | 11/384000 | 3211/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Bloom | 100000 | 8 | 1% | 1% / 0% | 248 / 29852 / 82513 | 9 | 0.00 | 0 | 42824 | 112936/384000 | 9/384000 | 3168/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 8 | 1% | 1% / 5% | 899 / 40709 / 111813 | 10 | 0.00 | 0 | 42824 | 114481/384000 | 20/384000 | 3160/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Bloom | 100000 | 8 | 1% | 10% / 0% | 2757 / 57323 / 238149 | 18 | 0.00 | 0 | 42824 | 135757/384000 | 61/384000 | 3125/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 8 | 1% | 10% / 5% | 3261 / 58067 / 119144 | 26 | 0.00 | 0 | 42824 | 128674/384000 | 63/384000 | 3241/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 8 | 0.1% | 1% / 5% | 768 / 33803 / 66476 | 8 | 0.00 | 0 | 15120 | 113853/383680 | 2/384000 | 3121/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec bloom --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 8 | 10% | 1% / 5% | 1142 / 42160 / 287574 | 374 | 0.00 | 0 | 123297 | 116137/384000 | 215/384000 | 3424/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec bloom --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 64 | 1% | 1% / 5% | 816 / 29964 / 109775 | 95 | 0.00 | 0 | 42824 | 911212/3072000 | 161/3072000 | 25329/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 4s wall
# sim --spec bloom --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Bloom | 100000 | 8 | 10% | 1% / 5% | 176 / 29891 / 299333 | 242 | 0.00 | 0 | 122916 | 116082/384000 | 254/384000 | 3404/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c8a --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 0 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 42824 | 0/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c8a --n 100000 --peers 8 --churn 1 --loss 0 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 1% | 0% / 5% | 132 / 489 / 803 | 0 | 7.90 | 61438 | 42824 | 94/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c8a --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 0 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 1% | 1% / 0% | 128 / 321 / 804 | 0 | 2.73 | 21230 | 42824 | 56/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c8a --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 1% | 1% / 5% | 137 / 499 / 1011 | 0 | 9.82 | 76827 | 42824 | 144/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c8a --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 0 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 1% | 10% / 0% | 144 / 560 / 1296 | 0 | 22.04 | 175776 | 42824 | 269/384000 | 2/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c8a --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 1% | 10% / 5% | 149 / 674 / 1309 | 0 | 26.52 | 212401 | 42824 | 343/384000 | 3/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 1s wall
# sim --spec c8a --n 100000 --peers 8 --churn 0.1 --loss 0.01 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 12660 | 15120 | 233/383680 | 1/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 0s wall
# sim --spec c8a --n 100000 --peers 8 --churn 10 --loss 0.01 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 10% | 1% / 5% | 135 / 511 / 809 | 28 | 9.94 | 253481 | 123297 | 142/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 6s wall
# sim --spec c8a --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 64 | 1% | 1% / 5% | 135 / 498 / 1053 | 0 | 9.73 | 75953 | 42824 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 7s wall
# sim --spec c8a --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
| Hybrid(16, 256) | 100000 | 8 | 10% | 1% / 5% | 135 / 512 / 809 | 42 | 9.94 | 278718 | 122916 | 152/384000 | 4/384000 | 0/384000 | 0 | 0 | replay 0/0 ok, grow-sessions 0, 10s wall
# sim --spec c1 --replay --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Exact+replay | 100000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4548 | 42824 | 144/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 786/786 ok, grow-sessions 0, 0s wall
# sim --spec c1 --replay --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Exact+replay | 100000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12767 | 42824 | 343/384000 | 3/384000 | 0/384000 | 0 | 0 | replay 2122/2122 ok, grow-sessions 0, 0s wall
# sim --spec c6b --replay --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Suffix(4, 64)+replay | 100000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4548 | 42824 | 144/384000 | 0/384000 | 0/384000 | 0 | 0 | replay 786/786 ok, grow-sessions 0, 0s wall
# sim --spec c6b --replay --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Suffix(4, 64)+replay | 100000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12767 | 42824 | 343/384000 | 3/384000 | 0/384000 | 0 | 0 | replay 2122/2122 ok, grow-sessions 0, 0s wall
# sim --spec c3 --replay --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256)+replay | 100000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4548 | 42824 | 144/384000 | 0/384000 | 48/384000 | 0 | 0 | replay 786/786 ok, grow-sessions 0, 0s wall
# sim --spec c3 --replay --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Cuckoo(16, 256)+replay | 100000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12767 | 42824 | 343/384000 | 3/384000 | 44/384000 | 0 | 0 | replay 2122/2122 ok, grow-sessions 0, 0s wall
# sim --spec c4 --replay --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
| Quotient(14, 256)+replay | 100000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4548 | 42824 | 144/384000 | 0/384000 | 15/384000 | 0 | 0 | replay 786/786 ok, grow-sessions 0, 0s wall
# sim --spec c4 --replay --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
| Quotient(14, 256)+replay | 100000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12767 | 42824 | 343/384000 | 3/384000 | 6/384000 | 0 | 0 | replay 2122/2122 ok, grow-sessions 0, 0s wall
