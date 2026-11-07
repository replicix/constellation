# cuckoo-load
first failure (fill from empty; fraction of slots used at the first failed eviction chain), f=16, 300 trials
| slots | min | p5 | median | trials failing before 90% | before 95% |
| 256 | 0.926 | 0.957 | 0.980 | 0 | 10 |
| 1024 | 0.950 | 0.962 | 0.975 | 0 | 0 |
| 4096 | 0.955 | 0.964 | 0.971 | 0 | 0 |
| 16384 | 0.952 | 0.963 | 0.968 | 0 | 0 |

steady churn at a fixed load: remove a random present key, insert a fresh one (2000000 cycles per cell; a failure leaves the shard to be rebuilt from the owner)
| slots | target load | failures | failures per 1M cycles | rebuilds (refetches) |
| 256 | 50% | 0 | 0.0 | 0 |
| 256 | 70% | 0 | 0.0 | 0 |
| 256 | 80% | 0 | 0.0 | 0 |
| 256 | 85% | 0 | 0.0 | 0 |
| 256 | 90% | 1 | 0.5 | 1 |
| 256 | 93% | 74 | 37.0 | 74 |
| 256 | 95% | 110 | 55.0 | 110 |
| 4096 | 50% | 0 | 0.0 | 0 |
| 4096 | 70% | 0 | 0.0 | 0 |
| 4096 | 80% | 0 | 0.0 | 0 |
| 4096 | 85% | 0 | 0.0 | 0 |
| 4096 | 90% | 0 | 0.0 | 0 |
| 4096 | 93% | 0 | 0.0 | 0 |
| 4096 | 95% | 0 | 0.0 | 0 |
