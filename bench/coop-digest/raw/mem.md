| candidate | n per peer | peers | mode | live B/key (allocator) | RSS B/key | self-reported B/key | live | RSS delta |
# mem --spec c1 --n 10000 --peers 1
| C1 exact | 10000 | 1 | bulk | 13.41 | 36.04 | 13.39 | 130.9 KiB | 352.0 KiB |
# mem --spec c6a --n 10000 --peers 1
| C6a EF-coded exact | 10000 | 1 | bulk | 7.83 | 16.38 | 7.81 | 76.5 KiB | 160.0 KiB |
# mem --spec c6b:4:64 --n 10000 --peers 1
| C6b suffix w=32 | 10000 | 1 | bulk | 5.24 | 13.93 | 5.23 | 51.2 KiB | 136.0 KiB |
# mem --spec c6b:3:64 --n 10000 --peers 1
| C6b suffix w=24 | 10000 | 1 | bulk | 4.24 | 13.11 | 4.23 | 41.4 KiB | 128.0 KiB |
# mem --spec c6b:2:64 --n 10000 --peers 1
| C6b suffix w=16 | 10000 | 1 | bulk | 3.24 | 11.88 | 3.23 | 31.7 KiB | 116.0 KiB |
# mem --spec c3:12:256 --n 10000 --peers 1
| C3 cuckoo f=12 | 10000 | 1 | bulk | 2.26 | 10.65 | 2.24 | 22.1 KiB | 104.0 KiB |
# mem --spec c3:16:256 --n 10000 --peers 1
| C3 cuckoo f=16 | 10000 | 1 | bulk | 2.86 | 10.65 | 2.84 | 27.9 KiB | 104.0 KiB |
# mem --spec c3:24:256 --n 10000 --peers 1
| C3 cuckoo f=24 | 10000 | 1 | bulk | 4.04 | 11.88 | 4.02 | 39.5 KiB | 116.0 KiB |
# mem --spec c3:32:256 --n 10000 --peers 1
| C3 cuckoo f=32 | 10000 | 1 | bulk | 5.23 | 13.11 | 5.21 | 51.1 KiB | 128.0 KiB |
# mem --spec c4:14:256 --n 10000 --peers 1
| C4 quotient eps=2^-14 | 10000 | 1 | bulk | 3.99 | 37.27 | 3.97 | 38.9 KiB | 364.0 KiB |
# mem --spec c4:24:256 --n 10000 --peers 1
| C4 quotient eps=2^-24 | 10000 | 1 | bulk | 6.04 | 33.59 | 6.02 | 58.9 KiB | 328.0 KiB |
# mem --spec c5 --n 10000 --peers 1
| C5 fuse16+overlay | 10000 | 1 | bulk | 2.78 | 17.61 | 2.76 | 27.1 KiB | 172.0 KiB |
# mem --spec bloom --n 10000 --peers 1
| Bloom (10 b/key, k=7) | 10000 | 1 | bulk | 1.27 | 9.01 | 1.25 | 12.4 KiB | 88.0 KiB |
# mem --spec c8a --n 10000 --peers 1
| C8a cuckoo over disk exact | 10000 | 1 | bulk | 2.95 | 18.02 | 2.90 | 28.8 KiB | 176.0 KiB |
# mem --spec c1 --n 100000 --peers 1
| C1 exact | 100000 | 1 | bulk | 11.60 | 12.62 | 11.60 | 1.11 MiB | 1.20 MiB |
# mem --spec c6a --n 100000 --peers 1
| C6a EF-coded exact | 100000 | 1 | bulk | 7.15 | 7.50 | 7.15 | 698.1 KiB | 732.0 KiB |
# mem --spec c6b:4:64 --n 100000 --peers 1
| C6b suffix w=32 | 100000 | 1 | bulk | 4.98 | 5.32 | 4.98 | 486.8 KiB | 520.0 KiB |
# mem --spec c6b:3:64 --n 100000 --peers 1
| C6b suffix w=24 | 100000 | 1 | bulk | 3.98 | 4.42 | 3.98 | 389.1 KiB | 432.0 KiB |
# mem --spec c6b:2:64 --n 100000 --peers 1
| C6b suffix w=16 | 100000 | 1 | bulk | 2.98 | 3.36 | 2.98 | 291.5 KiB | 328.0 KiB |
# mem --spec c3:12:256 --n 100000 --peers 1
| C3 cuckoo f=12 | 100000 | 1 | bulk | 2.15 | 2.25 | 2.15 | 210.0 KiB | 220.0 KiB |
# mem --spec c3:16:256 --n 100000 --peers 1
| C3 cuckoo f=16 | 100000 | 1 | bulk | 2.74 | 2.79 | 2.74 | 267.9 KiB | 272.0 KiB |
# mem --spec c3:24:256 --n 100000 --peers 1
| C3 cuckoo f=24 | 100000 | 1 | bulk | 3.93 | 3.97 | 3.93 | 383.8 KiB | 388.0 KiB |
# mem --spec c3:32:256 --n 100000 --peers 1
| C3 cuckoo f=32 | 100000 | 1 | bulk | 5.12 | 5.16 | 5.11 | 499.6 KiB | 504.0 KiB |
# mem --spec c4:14:256 --n 100000 --peers 1
| C4 quotient eps=2^-14 | 100000 | 1 | bulk | 3.18 | 5.98 | 3.17 | 310.2 KiB | 584.0 KiB |
# mem --spec c4:24:256 --n 100000 --peers 1
| C4 quotient eps=2^-24 | 100000 | 1 | bulk | 4.81 | 6.88 | 4.81 | 470.2 KiB | 672.0 KiB |
# mem --spec c5 --n 100000 --peers 1
| C5 fuse16+overlay | 100000 | 1 | bulk | 2.71 | 3.52 | 2.71 | 264.9 KiB | 344.0 KiB |
# mem --spec bloom --n 100000 --peers 1
| Bloom (10 b/key, k=7) | 100000 | 1 | bulk | 1.26 | 1.23 | 1.25 | 122.6 KiB | 120.0 KiB |
# mem --spec c8a --n 100000 --peers 1
| C8a cuckoo over disk exact | 100000 | 1 | bulk | 2.81 | 4.18 | 2.80 | 274.3 KiB | 408.0 KiB |
# mem --spec c1 --n 1000000 --peers 1
| C1 exact | 1000000 | 1 | bulk | 11.88 | 12.15 | 11.88 | 11.33 MiB | 11.59 MiB |
# mem --spec c6a --n 1000000 --peers 1
| C6a EF-coded exact | 1000000 | 1 | bulk | 6.54 | 6.81 | 6.54 | 6.24 MiB | 6.50 MiB |
# mem --spec c6b:4:64 --n 1000000 --peers 1
| C6b suffix w=32 | 1000000 | 1 | bulk | 4.79 | 5.03 | 4.79 | 4.56 MiB | 4.80 MiB |
# mem --spec c6b:3:64 --n 1000000 --peers 1
| C6b suffix w=24 | 1000000 | 1 | bulk | 3.79 | 4.06 | 3.79 | 3.61 MiB | 3.88 MiB |
# mem --spec c6b:2:64 --n 1000000 --peers 1
| C6b suffix w=16 | 1000000 | 1 | bulk | 2.79 | 3.05 | 2.79 | 2.66 MiB | 2.91 MiB |
# mem --spec c3:12:256 --n 1000000 --peers 1
| C3 cuckoo f=12 | 1000000 | 1 | bulk | 2.07 | 2.14 | 2.07 | 1.98 MiB | 2.04 MiB |
# mem --spec c3:16:256 --n 1000000 --peers 1
| C3 cuckoo f=16 | 1000000 | 1 | bulk | 2.66 | 2.72 | 2.66 | 2.54 MiB | 2.59 MiB |
# mem --spec c3:24:256 --n 1000000 --peers 1
| C3 cuckoo f=24 | 1000000 | 1 | bulk | 3.85 | 3.91 | 3.85 | 3.67 MiB | 3.73 MiB |
# mem --spec c3:32:256 --n 1000000 --peers 1
| C3 cuckoo f=32 | 1000000 | 1 | bulk | 5.03 | 5.07 | 5.03 | 4.80 MiB | 4.84 MiB |
# mem --spec c4:14:256 --n 1000000 --peers 1
| C4 quotient eps=2^-14 | 1000000 | 1 | bulk | 3.64 | 3.90 | 3.64 | 3.48 MiB | 3.72 MiB |
# mem --spec c4:24:256 --n 1000000 --peers 1
| C4 quotient eps=2^-24 | 1000000 | 1 | bulk | 5.60 | 5.92 | 5.60 | 5.34 MiB | 5.65 MiB |
# mem --spec c5 --n 1000000 --peers 1
| C5 fuse16+overlay | 1000000 | 1 | bulk | 2.70 | 2.81 | 2.70 | 2.58 MiB | 2.68 MiB |
# mem --spec bloom --n 1000000 --peers 1
| Bloom (10 b/key, k=7) | 1000000 | 1 | bulk | 1.26 | 1.26 | 1.26 | 1.20 MiB | 1.20 MiB |
# mem --spec c8a --n 1000000 --peers 1
| C8a cuckoo over disk exact | 1000000 | 1 | bulk | 2.73 | 2.85 | 2.73 | 2.60 MiB | 2.71 MiB |
# mem --spec c1 --n 4000000 --peers 1
| C1 exact | 4000000 | 1 | bulk | 11.88 | 12.16 | 11.88 | 45.32 MiB | 46.39 MiB |
# mem --spec c6a --n 4000000 --peers 1
| C6a EF-coded exact | 4000000 | 1 | bulk | 6.29 | 6.56 | 6.29 | 24.01 MiB | 25.04 MiB |
# mem --spec c6b:4:64 --n 4000000 --peers 1
| C6b suffix w=32 | 4000000 | 1 | bulk | 4.79 | 5.02 | 4.79 | 18.26 MiB | 19.16 MiB |
# mem --spec c6b:3:64 --n 4000000 --peers 1
| C6b suffix w=24 | 4000000 | 1 | bulk | 3.79 | 4.05 | 3.79 | 14.44 MiB | 15.44 MiB |
# mem --spec c6b:2:64 --n 4000000 --peers 1
| C6b suffix w=16 | 4000000 | 1 | bulk | 2.79 | 3.04 | 2.79 | 10.63 MiB | 11.59 MiB |
# mem --spec c3:12:256 --n 4000000 --peers 1
| C3 cuckoo f=12 | 4000000 | 1 | bulk | 2.07 | 2.14 | 2.07 | 7.90 MiB | 8.15 MiB |
# mem --spec c3:16:256 --n 4000000 --peers 1
| C3 cuckoo f=16 | 4000000 | 1 | bulk | 2.66 | 2.71 | 2.66 | 10.16 MiB | 10.36 MiB |
# mem --spec c3:24:256 --n 4000000 --peers 1
| C3 cuckoo f=24 | 4000000 | 1 | bulk | 3.85 | 3.91 | 3.85 | 14.68 MiB | 14.90 MiB |
# mem --spec c3:32:256 --n 4000000 --peers 1
| C3 cuckoo f=32 | 4000000 | 1 | bulk | 5.03 | 5.07 | 5.03 | 19.19 MiB | 19.32 MiB |
# mem --spec c4:14:256 --n 4000000 --peers 1
| C4 quotient eps=2^-14 | 4000000 | 1 | bulk | 3.64 | 3.76 | 3.64 | 13.89 MiB | 14.35 MiB |
# mem --spec c4:24:256 --n 4000000 --peers 1
| C4 quotient eps=2^-24 | 4000000 | 1 | bulk | 5.59 | 5.70 | 5.59 | 21.34 MiB | 21.75 MiB |
# mem --spec c5 --n 4000000 --peers 1
| C5 fuse16+overlay | 4000000 | 1 | bulk | 2.70 | 2.73 | 2.70 | 10.31 MiB | 10.43 MiB |
# mem --spec bloom --n 4000000 --peers 1
| Bloom (10 b/key, k=7) | 4000000 | 1 | bulk | 1.05 | 1.05 | 1.05 | 4.01 MiB | 4.02 MiB |
# mem --spec c8a --n 4000000 --peers 1
| C8a cuckoo over disk exact | 4000000 | 1 | bulk | 2.73 | 2.80 | 2.73 | 10.40 MiB | 10.70 MiB |
# mem --spec c1 --n 8000000 --peers 1
| C1 exact | 8000000 | 1 | bulk | 11.11 | 11.25 | 11.11 | 84.78 MiB | 85.84 MiB |
# mem --spec c6a --n 8000000 --peers 1
| C6a EF-coded exact | 8000000 | 1 | bulk | 5.77 | 6.45 | 5.77 | 44.06 MiB | 49.20 MiB |
# mem --spec c6b:4:64 --n 8000000 --peers 1
| C6b suffix w=32 | 8000000 | 1 | bulk | 4.39 | 4.66 | 4.39 | 33.52 MiB | 35.55 MiB |
# mem --spec c6b:3:64 --n 8000000 --peers 1
| C6b suffix w=24 | 8000000 | 1 | bulk | 3.39 | 3.63 | 3.39 | 25.89 MiB | 27.66 MiB |
# mem --spec c6b:2:64 --n 8000000 --peers 1
| C6b suffix w=16 | 8000000 | 1 | bulk | 2.39 | 2.58 | 2.39 | 18.26 MiB | 19.71 MiB |
# mem --spec c3:12:256 --n 8000000 --peers 1
| C3 cuckoo f=12 | 8000000 | 1 | bulk | 2.07 | 2.14 | 2.07 | 15.80 MiB | 16.29 MiB |
# mem --spec c3:16:256 --n 8000000 --peers 1
| C3 cuckoo f=16 | 8000000 | 1 | bulk | 2.66 | 2.71 | 2.66 | 20.32 MiB | 20.70 MiB |
# mem --spec c3:24:256 --n 8000000 --peers 1
| C3 cuckoo f=24 | 8000000 | 1 | bulk | 3.85 | 3.91 | 3.85 | 29.35 MiB | 29.80 MiB |
# mem --spec c3:32:256 --n 8000000 --peers 1
| C3 cuckoo f=32 | 8000000 | 1 | bulk | 5.03 | 5.06 | 5.03 | 38.39 MiB | 38.64 MiB |
# mem --spec c4:14:256 --n 8000000 --peers 1
| C4 quotient eps=2^-14 | 8000000 | 1 | bulk | 3.63 | 3.73 | 3.63 | 27.72 MiB | 28.46 MiB |
# mem --spec c4:24:256 --n 8000000 --peers 1
| C4 quotient eps=2^-24 | 8000000 | 1 | bulk | 5.58 | 5.69 | 5.58 | 42.57 MiB | 43.40 MiB |
# mem --spec c5 --n 8000000 --peers 1
| C5 fuse16+overlay | 8000000 | 1 | bulk | 2.70 | 2.72 | 2.70 | 20.62 MiB | 20.76 MiB |
# mem --spec bloom --n 8000000 --peers 1
| Bloom (10 b/key, k=7) | 8000000 | 1 | bulk | 0.53 | 0.53 | 0.53 | 4.01 MiB | 4.02 MiB |
# mem --spec c8a --n 8000000 --peers 1
| C8a cuckoo over disk exact | 8000000 | 1 | bulk | 2.73 | 2.79 | 2.73 | 20.80 MiB | 21.25 MiB |
# mem --spec c1 --n 1000000 --peers 64
| C1 exact | 1000000 | 64 | bulk | 11.88 | 12.39 | 11.88 | 725.12 MiB | 756.39 MiB |
# mem --spec c6a --n 1000000 --peers 64
| C6a EF-coded exact | 1000000 | 64 | bulk | 6.54 | 6.92 | 6.54 | 399.39 MiB | 422.61 MiB |
# mem --spec c6b --n 1000000 --peers 64
| C6b suffix w=32 | 1000000 | 64 | bulk | 4.79 | 5.14 | 4.79 | 292.15 MiB | 313.80 MiB |
# mem --spec c3 --n 1000000 --peers 64
| C3 cuckoo f=16 | 1000000 | 64 | bulk | 2.66 | 2.84 | 2.66 | 162.56 MiB | 173.20 MiB |
# mem --spec c4 --n 1000000 --peers 64
| C4 quotient eps=2^-14 | 1000000 | 64 | bulk | 3.64 | 3.84 | 3.64 | 221.88 MiB | 234.21 MiB |
# mem --spec c5 --n 1000000 --peers 64
| C5 fuse16+overlay | 1000000 | 64 | bulk | 2.70 | 2.83 | 2.70 | 164.72 MiB | 172.70 MiB |
# mem --spec bloom --n 1000000 --peers 64
| Bloom (10 b/key, k=7) | 1000000 | 64 | bulk | 1.26 | 1.38 | 1.26 | 76.68 MiB | 84.43 MiB |
# mem --spec c8a --n 1000000 --peers 64
| C8a cuckoo over disk exact | 1000000 | 64 | bulk | 2.73 | 2.90 | 2.73 | 166.39 MiB | 177.14 MiB |
# mem --spec c1 --n 8000000 --peers 64
| C1 exact | 8000000 | 64 | bulk | 11.11 | 11.24 | 11.11 | 5.30 GiB | 5.36 GiB |
# mem --spec c6a --n 8000000 --peers 64
| C6a EF-coded exact | 8000000 | 64 | bulk | 5.77 | 6.45 | 5.77 | 2.75 GiB | 3.08 GiB |
# mem --spec c6b --n 8000000 --peers 64
| C6b suffix w=32 | 8000000 | 64 | bulk | 4.39 | 4.66 | 4.39 | 2.09 GiB | 2.22 GiB |
# mem --spec c3 --n 8000000 --peers 64
| C3 cuckoo f=16 | 8000000 | 64 | bulk | 2.66 | 2.71 | 2.66 | 1.27 GiB | 1.29 GiB |
# mem --spec c4 --n 8000000 --peers 64
| C4 quotient eps=2^-14 | 8000000 | 64 | bulk | 3.64 | 3.71 | 3.64 | 1.73 GiB | 1.77 GiB |
# mem --spec c5 --n 8000000 --peers 64
| C5 fuse16+overlay | 8000000 | 64 | bulk | 2.70 | 2.70 | 2.70 | 1.29 GiB | 1.29 GiB |
# mem --spec bloom --n 8000000 --peers 64
| Bloom (10 b/key, k=7) | 8000000 | 64 | bulk | 0.53 | 0.53 | 0.53 | 256.75 MiB | 257.02 MiB |
# mem --spec c8a --n 8000000 --peers 64
| C8a cuckoo over disk exact | 8000000 | 64 | bulk | 2.73 | 2.78 | 2.73 | 1.30 GiB | 1.32 GiB |
# mem --spec c1 --n 1000000 --peers 1 --mode sync
| C1 exact | 1000000 | 1 | sync | 11.88 | 28.18 | 11.88 | 11.33 MiB | 26.88 MiB | initial sync: 2 sessions, 258 rounds, 7.16 MiB
# mem --spec c6a --n 1000000 --peers 1 --mode sync
| C6a EF-coded exact | 1000000 | 1 | sync | 6.54 | 23.30 | 6.54 | 6.24 MiB | 22.22 MiB | initial sync: 2 sessions, 258 rounds, 7.16 MiB
# mem --spec c6b --n 1000000 --peers 1 --mode sync
| C6b suffix w=32 | 1000000 | 1 | sync | 4.79 | 16.67 | 4.79 | 4.56 MiB | 15.90 MiB | initial sync: 2 sessions, 258 rounds, 7.16 MiB
# mem --spec c3 --n 1000000 --peers 1 --mode sync
| C3 cuckoo f=16 | 1000000 | 1 | sync | 2.66 | 14.44 | 2.66 | 2.54 MiB | 13.77 MiB | initial sync: 2 sessions, 258 rounds, 7.16 MiB
# mem --spec c4 --n 1000000 --peers 1 --mode sync
| C4 quotient eps=2^-14 | 1000000 | 1 | sync | 3.64 | 15.59 | 3.64 | 3.48 MiB | 14.86 MiB | initial sync: 2 sessions, 258 rounds, 7.16 MiB
# mem --spec c5 --n 1000000 --peers 1 --mode sync
| C5 fuse16+overlay | 1000000 | 1 | sync | 2.70 | 14.50 | 2.70 | 2.58 MiB | 13.83 MiB | initial sync: 2 sessions, 258 rounds, 7.16 MiB
# mem --spec c1 --n 16000000 --peers 1
| Exact | 16000000 | 1 | not mirrored: n exceeds MAX_MIRROR_KEYS (8388608), peer treated as unknown | | | |
# mem --spec c1 --n 16000000 --peers 1 --force
| C1 exact | 16000000 | 1 | bulk | 10.37 | 10.44 | 10.37 | 158.18 MiB | 159.25 MiB |
# mem --spec c6b --n 16000000 --peers 1
| C6b suffix w=32 | 16000000 | 1 | bulk | 4.20 | 4.27 | 4.20 | 64.04 MiB | 65.19 MiB |
# mem --spec c3 --n 16000000 --peers 1
| C3 cuckoo f=16 | 16000000 | 1 | bulk | 2.66 | 2.71 | 2.66 | 40.64 MiB | 41.39 MiB |
# mem --spec c4 --n 16000000 --peers 1
| C4 quotient eps=2^-14 | 16000000 | 1 | bulk | 3.63 | 3.72 | 3.63 | 55.46 MiB | 56.77 MiB |
# mem --spec c5 --n 16000000 --peers 1
| C5 fuse16+overlay | 16000000 | 1 | bulk | 2.70 | 2.71 | 2.70 | 41.19 MiB | 41.36 MiB |
# mem --spec c8a --n 16000000 --peers 1
| C8a cuckoo over disk exact | 16000000 | 1 | bulk | 2.73 | 2.78 | 2.73 | 41.59 MiB | 42.48 MiB |
# mem --spec c6b --n 32000000 --peers 1
| C6b suffix w=32 | 32000000 | 1 | bulk | 4.10 | 4.14 | 4.10 | 125.07 MiB | 126.36 MiB |
# mem --spec c3 --n 32000000 --peers 1
| C3 cuckoo f=16 | 32000000 | 1 | bulk | 2.51 | 2.53 | 2.51 | 76.54 MiB | 77.30 MiB |
# mem --spec c4 --n 32000000 --peers 1
| C4 quotient eps=2^-14 | 32000000 | 1 | bulk | 3.54 | 3.57 | 3.54 | 108.14 MiB | 109.08 MiB |
# mem --spec c5 --n 32000000 --peers 1
| C5 fuse16+overlay | 32000000 | 1 | bulk | 2.70 | 2.71 | 2.70 | 82.39 MiB | 82.62 MiB |
# mem --spec c8a --n 32000000 --peers 1
| C8a cuckoo over disk exact | 32000000 | 1 | bulk | 2.57 | 2.60 | 2.57 | 78.45 MiB | 79.31 MiB |
