| candidate | n | churn/min | changes per tick | publisher absorb p50 / p99 / mean ns | receiver apply p50 / p99 / mean ns | delta wire | summary wire | snapshot wire | B/key | pub bytes | notes |
# steady --spec c1 --n 1000000 --churn 1 --minutes 2
| C1 exact | 1000000 | 1% | 41.7 | 7100 / 16160 / 7340 | 6480 / 9060 / 6597 | 122932 | 248 | 0 | 11.88 / 12.09 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6a --n 1000000 --churn 1 --minutes 2
| C6a EF-coded exact | 1000000 | 1% | 41.7 | 6190 / 12480 / 6368 | 59480 / 69520 / 59835 | 122932 | 248 | 0 | 6.54 / 6.55 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6b --n 1000000 --churn 1 --minutes 2
| C6b suffix w=32 | 1000000 | 1% | 41.7 | 5940 / 9670 / 6091 | 6250 / 11390 / 6654 | 122932 | 248 | 0 | 4.79 / 6.26 | 11.33 MiB | delta log 108371 B/min
# steady --spec c3 --n 1000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 1000000 | 1% | 41.7 | 5970 / 7500 / 6083 | 3750 / 6150 / 3943 | 122932 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 108371 B/min
# steady --spec c4 --n 1000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 1% | 41.7 | 5760 / 8110 / 5874 | 5600 / 12650 / 6324 | 122932 | 248 | 0 | 3.64 / 3.72 | 11.33 MiB | delta log 108371 B/min
# steady --spec c5 --n 1000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 5660 / 7620 / 5742 | 2500 / 3050 / 2538 | 122932 | 248 | 1294288 | 2.70 / 2.90 | 11.33 MiB | resnap=64: 245 snapshots, build 0.07 ms each, overlay 4291 entries at end
# steady --spec bloom --n 1000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 1% | 41.7 | 5160 / 7380 / 5384 | 1540 / 2510 / 1588 | 185280 | 248 | 2529318 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 20.2 ms per interval
# steady --spec c8a --n 1000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 1% | 41.7 | 5980 / 14331 / 7020 | 9170 / 17511 / 322777 | 122932 | 248 | 0 | 2.73 / 2.79 | 11.33 MiB | delta log 108371 B/min
# steady --spec c8a --n 1000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 1% | 41.7 | 6990 / 13170 / 7788 | 8820 / 16340 / 241744 | 122932 | 248 | 0 | 2.73 / 2.79 | 11.33 MiB | delta log 108371 B/min
# steady --spec bloom --n 1000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 1% | 41.7 | 5400 / 8210 / 5676 | 1540 / 2540 / 1630 | 185280 | 248 | 2529318 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 20.4 ms per interval
# steady --spec c5 --n 1000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 6060 / 8370 / 6147 | 2530 / 3070 / 2558 | 122932 | 248 | 1294288 | 2.70 / 2.90 | 11.33 MiB | resnap=64: 245 snapshots, build 0.07 ms each, overlay 4291 entries at end
# steady --spec c4 --n 1000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 1% | 41.7 | 5900 / 8130 / 6064 | 5570 / 14090 / 6330 | 122932 | 248 | 0 | 3.64 / 3.72 | 11.33 MiB | delta log 108371 B/min
# steady --spec c3 --n 1000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 1000000 | 1% | 41.7 | 5800 / 8160 / 5989 | 3800 / 6120 / 3956 | 122932 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6b --n 1000000 --churn 1 --minutes 2
| C6b suffix w=32 | 1000000 | 1% | 41.7 | 6110 / 7870 / 6195 | 6010 / 9910 / 6344 | 122932 | 248 | 0 | 4.79 / 6.26 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6a --n 1000000 --churn 1 --minutes 2
| C6a EF-coded exact | 1000000 | 1% | 41.7 | 6300 / 8780 / 6498 | 59201 / 68251 / 59639 | 122932 | 248 | 0 | 6.54 / 6.55 | 11.33 MiB | delta log 108371 B/min
# steady --spec c1 --n 1000000 --churn 1 --minutes 2
| C1 exact | 1000000 | 1% | 41.7 | 6290 / 7881 / 6416 | 5870 / 9820 / 5998 | 122932 | 248 | 0 | 11.88 / 12.09 | 11.33 MiB | delta log 108371 B/min
# steady --spec c1 --n 1000000 --churn 1 --minutes 2
| C1 exact | 1000000 | 1% | 41.7 | 6301 / 8540 / 6460 | 6270 / 9140 / 6399 | 122932 | 248 | 0 | 11.88 / 12.09 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6a --n 1000000 --churn 1 --minutes 2
| C6a EF-coded exact | 1000000 | 1% | 41.7 | 5880 / 7740 / 5997 | 58381 / 65681 / 58683 | 122932 | 248 | 0 | 6.54 / 6.55 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6b --n 1000000 --churn 1 --minutes 2
| C6b suffix w=32 | 1000000 | 1% | 41.7 | 5750 / 7480 / 5856 | 5970 / 10990 / 6387 | 122932 | 248 | 0 | 4.79 / 6.26 | 11.33 MiB | delta log 108371 B/min
# steady --spec c3 --n 1000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 1000000 | 1% | 41.7 | 5710 / 7680 / 5844 | 3670 / 5870 / 3827 | 122932 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 108371 B/min
# steady --spec c4 --n 1000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 1% | 41.7 | 5630 / 8280 / 5795 | 5590 / 11430 / 6212 | 122932 | 248 | 0 | 3.64 / 3.72 | 11.33 MiB | delta log 108371 B/min
# steady --spec c5 --n 1000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 5550 / 10400 / 5705 | 2500 / 2940 / 2540 | 122932 | 248 | 1294288 | 2.70 / 2.90 | 11.33 MiB | resnap=64: 245 snapshots, build 0.07 ms each, overlay 4291 entries at end
# steady --spec bloom --n 1000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 1% | 41.7 | 5270 / 8490 / 5487 | 1530 / 2740 / 1625 | 185280 | 248 | 2529318 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 20.3 ms per interval
# steady --spec c8a --n 1000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 1% | 41.7 | 5960 / 11820 / 6766 | 9070 / 16370 / 271717 | 122932 | 248 | 0 | 2.73 / 2.79 | 11.33 MiB | delta log 108371 B/min
# steady --spec c8a --n 1000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 1% | 41.7 | 8930 / 18010 / 9372 | 8680 / 15610 / 355787 | 122932 | 248 | 0 | 2.73 / 2.79 | 11.33 MiB | delta log 108371 B/min
# steady --spec bloom --n 1000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 1% | 41.7 | 5300 / 8410 / 5550 | 1560 / 2720 / 1643 | 185280 | 248 | 2529318 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 20.3 ms per interval
# steady --spec c5 --n 1000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 5790 / 8460 / 5937 | 2510 / 3100 / 2533 | 122932 | 248 | 1294288 | 2.70 / 2.90 | 11.33 MiB | resnap=64: 245 snapshots, build 0.07 ms each, overlay 4291 entries at end
# steady --spec c4 --n 1000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 1% | 41.7 | 5990 / 8390 / 6132 | 5490 / 12640 / 6132 | 122932 | 248 | 0 | 3.64 / 3.72 | 11.33 MiB | delta log 108371 B/min
# steady --spec c3 --n 1000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 1000000 | 1% | 41.7 | 5870 / 7740 / 5962 | 3810 / 7420 / 3961 | 122932 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6b --n 1000000 --churn 1 --minutes 2
| C6b suffix w=32 | 1000000 | 1% | 41.7 | 6050 / 13050 / 6213 | 6340 / 11190 / 6694 | 122932 | 248 | 0 | 4.79 / 6.26 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6a --n 1000000 --churn 1 --minutes 2
| C6a EF-coded exact | 1000000 | 1% | 41.7 | 7060 / 9940 / 7217 | 58331 / 65081 / 58606 | 122932 | 248 | 0 | 6.54 / 6.55 | 11.33 MiB | delta log 108371 B/min
# steady --spec c1 --n 1000000 --churn 1 --minutes 2
| C1 exact | 1000000 | 1% | 41.7 | 7340 / 10280 / 7465 | 6070 / 8951 / 6180 | 122932 | 248 | 0 | 11.88 / 12.09 | 11.33 MiB | delta log 108371 B/min
# steady --spec c1 --n 1000000 --churn 10 --minutes 2
| C1 exact | 1000000 | 10% | 416.7 | 43070 / 66390 / 46158 | 26790 / 64811 / 31483 | 877311 | 248 | 0 | 11.88 / 12.82 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6a --n 1000000 --churn 10 --minutes 2
| C6a EF-coded exact | 1000000 | 10% | 416.7 | 39920 / 59581 / 42265 | 566014 / 593134 / 566612 | 877311 | 248 | 0 | 6.54 / 6.54 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6b --n 1000000 --churn 10 --minutes 2
| C6b suffix w=32 | 1000000 | 10% | 416.7 | 39670 / 53491 / 41131 | 35960 / 67770 / 38118 | 877311 | 248 | 0 | 4.79 / 7.84 | 11.33 MiB | delta log 862734 B/min
# steady --spec c3 --n 1000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 1000000 | 10% | 416.7 | 36950 / 53370 / 38932 | 23420 / 36610 / 24509 | 877311 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 862734 B/min
# steady --spec c4 --n 1000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 10% | 416.7 | 37280 / 54850 / 39400 | 39500 / 62841 / 41138 | 877311 | 248 | 0 | 3.64 / 3.90 | 11.33 MiB | delta log 862734 B/min
# steady --spec c5 --n 1000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 1000000 | 10% | 416.7 | 37330 / 53761 / 38724 | 18401 / 24711 / 18534 | 877311 | 248 | 15593578 | 2.70 / 2.96 | 11.33 MiB | resnap=64: 2959 snapshots, build 0.07 ms each, overlay 8195 entries at end
# steady --spec bloom --n 1000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 10% | 416.7 | 42181 / 55980 / 43282 | 10980 / 15170 / 11150 | 1613696 | 248 | 2526797 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 21.1 ms per interval
# steady --spec c8a --n 1000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 10% | 416.7 | 82701 / 103391 / 82026 | 114181 / 641521309 / 14142178 | 877311 | 248 | 0 | 2.73 / 2.77 | 11.33 MiB | delta log 862734 B/min
# steady --spec c8a --n 1000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 10% | 416.7 | 82030 / 102831 / 81708 | 113241 / 642864498 / 16064294 | 877311 | 248 | 0 | 2.73 / 2.77 | 11.33 MiB | delta log 862734 B/min
# steady --spec bloom --n 1000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 10% | 416.7 | 42821 / 59311 / 44009 | 11020 / 15280 / 11232 | 1613696 | 248 | 2526797 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 21.3 ms per interval
# steady --spec c5 --n 1000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 1000000 | 10% | 416.7 | 37320 / 59971 / 39146 | 18520 / 24610 / 18565 | 877311 | 248 | 15593578 | 2.70 / 2.96 | 11.33 MiB | resnap=64: 2959 snapshots, build 0.07 ms each, overlay 8195 entries at end
# steady --spec c4 --n 1000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 10% | 416.7 | 37900 / 57861 / 40239 | 40031 / 66091 / 41947 | 877311 | 248 | 0 | 3.64 / 3.90 | 11.33 MiB | delta log 862734 B/min
# steady --spec c3 --n 1000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 1000000 | 10% | 416.7 | 37490 / 57851 / 39893 | 23951 / 39690 / 25322 | 877311 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6b --n 1000000 --churn 10 --minutes 2
| C6b suffix w=32 | 1000000 | 10% | 416.7 | 40961 / 60931 / 43224 | 37030 / 81140 / 39992 | 877311 | 248 | 0 | 4.79 / 7.84 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6a --n 1000000 --churn 10 --minutes 2
| C6a EF-coded exact | 1000000 | 10% | 416.7 | 41590 / 62530 / 44016 | 566434 / 612684 / 567814 | 877311 | 248 | 0 | 6.54 / 6.54 | 11.33 MiB | delta log 862734 B/min
# steady --spec c1 --n 1000000 --churn 10 --minutes 2
| C1 exact | 1000000 | 10% | 416.7 | 44210 / 61680 / 46574 | 27800 / 66071 / 32121 | 877311 | 248 | 0 | 11.88 / 12.82 | 11.33 MiB | delta log 862734 B/min
# steady --spec c1 --n 1000000 --churn 10 --minutes 2
| C1 exact | 1000000 | 10% | 416.7 | 44761 / 65910 / 47589 | 28280 / 65541 / 32726 | 877311 | 248 | 0 | 11.88 / 12.82 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6a --n 1000000 --churn 10 --minutes 2
| C6a EF-coded exact | 1000000 | 10% | 416.7 | 40911 / 60180 / 43172 | 564294 / 613794 / 566371 | 877311 | 248 | 0 | 6.54 / 6.54 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6b --n 1000000 --churn 10 --minutes 2
| C6b suffix w=32 | 1000000 | 10% | 416.7 | 40730 / 63130 / 43127 | 36651 / 67611 / 38787 | 877311 | 248 | 0 | 4.79 / 7.84 | 11.33 MiB | delta log 862734 B/min
# steady --spec c3 --n 1000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 1000000 | 10% | 416.7 | 37170 / 53020 / 39252 | 23900 / 44680 / 25558 | 877311 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 862734 B/min
# steady --spec c4 --n 1000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 10% | 416.7 | 38201 / 60661 / 41324 | 40060 / 67470 / 41970 | 877311 | 248 | 0 | 3.64 / 3.90 | 11.33 MiB | delta log 862734 B/min
# steady --spec c5 --n 1000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 1000000 | 10% | 416.7 | 37231 / 55491 / 38899 | 18540 / 24770 / 18678 | 877311 | 248 | 15593578 | 2.70 / 2.96 | 11.33 MiB | resnap=64: 2959 snapshots, build 0.07 ms each, overlay 8195 entries at end
# steady --spec bloom --n 1000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 10% | 416.7 | 43160 / 58801 / 44347 | 11070 / 16470 / 11302 | 1613696 | 248 | 2526797 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 21.2 ms per interval
# steady --spec c8a --n 1000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 10% | 416.7 | 82741 / 104751 / 81934 | 118380 / 641493548 / 14142334 | 877311 | 248 | 0 | 2.73 / 2.77 | 11.33 MiB | delta log 862734 B/min
# steady --spec c8a --n 1000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 10% | 416.7 | 82830 / 103341 / 82631 | 116061 / 642454338 / 16175216 | 877311 | 248 | 0 | 2.73 / 2.77 | 11.33 MiB | delta log 862734 B/min
# steady --spec bloom --n 1000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 10% | 416.7 | 42751 / 58210 / 44001 | 11030 / 17860 / 11266 | 1613696 | 248 | 2526797 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 21.1 ms per interval
# steady --spec c5 --n 1000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 1000000 | 10% | 416.7 | 37610 / 57430 / 39193 | 18440 / 24870 / 18556 | 877311 | 248 | 15593578 | 2.70 / 2.96 | 11.33 MiB | resnap=64: 2959 snapshots, build 0.07 ms each, overlay 8195 entries at end
# steady --spec c4 --n 1000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 10% | 416.7 | 37850 / 58441 / 40928 | 40070 / 68450 / 42096 | 877311 | 248 | 0 | 3.64 / 3.90 | 11.33 MiB | delta log 862734 B/min
# steady --spec c3 --n 1000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 1000000 | 10% | 416.7 | 37590 / 57010 / 40246 | 24021 / 38750 / 25375 | 877311 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6b --n 1000000 --churn 10 --minutes 2
| C6b suffix w=32 | 1000000 | 10% | 416.7 | 40920 / 62671 / 43470 | 36650 / 60041 / 38404 | 877311 | 248 | 0 | 4.79 / 7.84 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6a --n 1000000 --churn 10 --minutes 2
| C6a EF-coded exact | 1000000 | 10% | 416.7 | 40720 / 61100 / 43238 | 565544 / 589734 / 566073 | 877311 | 248 | 0 | 6.54 / 6.54 | 11.33 MiB | delta log 862734 B/min
# steady --spec c1 --n 1000000 --churn 10 --minutes 2
| C1 exact | 1000000 | 10% | 416.7 | 42990 / 67100 / 46580 | 28020 / 68611 / 33422 | 877311 | 248 | 0 | 11.88 / 12.82 | 11.33 MiB | delta log 862734 B/min
# steady --spec c1 --n 8000000 --churn 1 --minutes 2
| C1 exact | 8000000 | 1% | 333.3 | 105481 / 123211 / 106276 | 97191 / 119631 / 98358 | 715014 | 250 | 0 | 11.11 / 11.32 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6a --n 8000000 --churn 1 --minutes 2
| C6a EF-coded exact | 8000000 | 1% | 333.3 | 108541 / 121391 / 109204 | 896267 / 946387 / 899211 | 715014 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6b --n 8000000 --churn 1 --minutes 2
| C6b suffix w=32 | 8000000 | 1% | 333.3 | 108281 / 126501 / 109896 | 145401 / 243471 / 155111 | 715014 | 250 | 0 | 4.39 / 6.44 | 84.78 MiB | delta log 700438 B/min
# steady --spec c3 --n 8000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 8000000 | 1% | 333.3 | 97491 / 117581 / 99236 | 69351 / 98790 / 72892 | 715014 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 700438 B/min
# steady --spec c4 --n 8000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 1% | 333.3 | 98160 / 119911 / 99195 | 90381 / 134661 / 94650 | 715014 | 250 | 0 | 3.63 / 3.70 | 84.78 MiB | delta log 700438 B/min
# steady --spec c5 --n 8000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 94351 / 114531 / 95514 | 19690 / 28620 / 19988 | 715014 | 250 | 10312910 | 2.70 / 2.89 | 84.78 MiB | resnap=64: 1953 snapshots, build 0.08 ms each, overlay 34796 entries at end
# steady --spec bloom --n 8000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 1% | 333.3 | 105610 / 127250 / 106886 | 9670 / 21851 / 11365 | 1294624 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 181.9 ms per interval
# steady --spec c8a --n 8000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 1% | 333.3 | 110621 / 150991 / 111939 | 152951 / 171171 / 30923966 | 715014 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 700438 B/min
# steady --spec c8a --n 8000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 1% | 333.3 | 111541 / 142391 / 112546 | 156741 / 173601 / 31729624 | 715014 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 700438 B/min
# steady --spec bloom --n 8000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 1% | 333.3 | 104611 / 121771 / 105366 | 9690 / 20640 / 11105 | 1294624 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 180.7 ms per interval
# steady --spec c5 --n 8000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 96200 / 117741 / 97979 | 19530 / 27281 / 19837 | 715014 | 250 | 10312910 | 2.70 / 2.89 | 84.78 MiB | resnap=64: 1953 snapshots, build 0.08 ms each, overlay 34796 entries at end
# steady --spec c4 --n 8000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 1% | 333.3 | 97020 / 117611 / 98219 | 86830 / 131371 / 91669 | 715014 | 250 | 0 | 3.63 / 3.70 | 84.78 MiB | delta log 700438 B/min
# steady --spec c3 --n 8000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 8000000 | 1% | 333.3 | 97561 / 118161 / 99721 | 70440 / 98581 / 74531 | 715014 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6b --n 8000000 --churn 1 --minutes 2
| C6b suffix w=32 | 8000000 | 1% | 333.3 | 104831 / 118781 / 105914 | 141921 / 230002 / 150446 | 715014 | 250 | 0 | 4.39 / 6.44 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6a --n 8000000 --churn 1 --minutes 2
| C6a EF-coded exact | 8000000 | 1% | 333.3 | 108841 / 124270 / 109277 | 895257 / 940686 / 897637 | 715014 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 700438 B/min
# steady --spec c1 --n 8000000 --churn 1 --minutes 2
| C1 exact | 8000000 | 1% | 333.3 | 101071 / 117101 / 102284 | 89731 / 112471 / 91743 | 715014 | 250 | 0 | 11.11 / 11.32 | 84.78 MiB | delta log 700438 B/min
# steady --spec c1 --n 8000000 --churn 1 --minutes 2
| C1 exact | 8000000 | 1% | 333.3 | 101251 / 117031 / 102098 | 91231 / 114611 / 92617 | 715014 | 250 | 0 | 11.11 / 11.32 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6a --n 8000000 --churn 1 --minutes 2
| C6a EF-coded exact | 8000000 | 1% | 333.3 | 109241 / 125931 / 110176 | 894766 / 927346 / 895684 | 715014 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6b --n 8000000 --churn 1 --minutes 2
| C6b suffix w=32 | 8000000 | 1% | 333.3 | 105471 / 120791 / 106864 | 141461 / 248692 / 151284 | 715014 | 250 | 0 | 4.39 / 6.44 | 84.78 MiB | delta log 700438 B/min
# steady --spec c3 --n 8000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 8000000 | 1% | 333.3 | 97291 / 117271 / 99373 | 70841 / 101191 / 74837 | 715014 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 700438 B/min
# steady --spec c4 --n 8000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 1% | 333.3 | 95681 / 114921 / 96437 | 91511 / 134411 / 94879 | 715014 | 250 | 0 | 3.63 / 3.70 | 84.78 MiB | delta log 700438 B/min
# steady --spec c5 --n 8000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 94491 / 113560 / 95771 | 19430 / 29760 / 19850 | 715014 | 250 | 10312910 | 2.70 / 2.89 | 84.78 MiB | resnap=64: 1953 snapshots, build 0.08 ms each, overlay 34796 entries at end
# steady --spec bloom --n 8000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 1% | 333.3 | 110391 / 131141 / 111346 | 10060 / 22570 / 11858 | 1294624 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 181.2 ms per interval
# steady --spec c8a --n 8000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 1% | 333.3 | 108270 / 138731 / 109509 | 150381 / 177021 / 31033138 | 715014 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 700438 B/min
# steady --spec c8a --n 8000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 1% | 333.3 | 110121 / 139451 / 110362 | 153342 / 178021 / 31695315 | 715014 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 700438 B/min
# steady --spec bloom --n 8000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 1% | 333.3 | 105111 / 127051 / 106207 | 9870 / 24041 / 11972 | 1294624 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 180.7 ms per interval
# steady --spec c5 --n 8000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 93581 / 117420 / 95271 | 19440 / 27061 / 19694 | 715014 | 250 | 10312910 | 2.70 / 2.89 | 84.78 MiB | resnap=64: 1953 snapshots, build 0.08 ms each, overlay 34796 entries at end
# steady --spec c4 --n 8000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 1% | 333.3 | 96580 / 110911 / 97614 | 90050 / 129611 / 93784 | 715014 | 250 | 0 | 3.63 / 3.70 | 84.78 MiB | delta log 700438 B/min
# steady --spec c3 --n 8000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 8000000 | 1% | 333.3 | 96511 / 112481 / 97270 | 70980 / 99411 / 74616 | 715014 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6b --n 8000000 --churn 1 --minutes 2
| C6b suffix w=32 | 8000000 | 1% | 333.3 | 105801 / 125511 / 106757 | 139821 / 229541 / 150256 | 715014 | 250 | 0 | 4.39 / 6.44 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6a --n 8000000 --churn 1 --minutes 2
| C6a EF-coded exact | 8000000 | 1% | 333.3 | 107991 / 121651 / 108723 | 893276 / 937606 / 895410 | 715014 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 700438 B/min
# steady --spec c1 --n 8000000 --churn 1 --minutes 2
| C1 exact | 8000000 | 1% | 333.3 | 101831 / 119841 / 102827 | 91291 / 112321 / 93232 | 715014 | 250 | 0 | 11.11 / 11.32 | 84.78 MiB | delta log 700438 B/min
# steady --spec c1 --n 8000000 --churn 10 --minutes 2
| C1 exact | 8000000 | 10% | 3333.3 | 951217 / 1182118 / 962693 | 905556 / 987347 / 908500 | 6427911 | 250 | 0 | 11.11 / 12.06 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6a --n 8000000 --churn 10 --minutes 2
| C6a EF-coded exact | 8000000 | 10% | 3333.3 | 989137 / 1240379 / 994936 | 9048684 / 9199566 / 9038001 | 6427911 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6b --n 8000000 --churn 10 --minutes 2
| C6b suffix w=32 | 8000000 | 10% | 3333.3 | 897986 / 1174059 / 909585 | 1146188 / 1717802 / 1171297 | 6427911 | 250 | 0 | 4.39 / 7.74 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c3 --n 8000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 8000000 | 10% | 3333.3 | 890826 / 1152248 / 900194 | 687245 / 856586 / 694462 | 6427911 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c4 --n 8000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 10% | 3333.3 | 906077 / 1116368 / 913176 | 742976 / 1067458 / 756521 | 6427911 | 250 | 0 | 3.63 / 3.89 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c5 --n 8000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 8000000 | 10% | 3333.3 | 857066 / 1104788 / 868435 | 145501 / 160401 / 145967 | 6427911 | 250 | 125283177 | 2.70 / 2.96 | 84.78 MiB | resnap=64: 23738 snapshots, build 0.08 ms each, overlay 61579 entries at end
# steady --spec bloom --n 8000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 10% | 3333.3 | 876856 / 1318459 / 906917 | 148171 / 177951 / 144172 | 12835456 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 182.6 ms per interval
# steady --spec c8a --n 8000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 10% | 3333.3 | 1076367 / 1622491 / 1181933 | 1415760 / 5197515971 / 420881398 | 6427911 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c8a --n 8000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 10% | 3333.3 | 1049517 / 1610871 / 1149974 | 1374450 / 5204307891 / 421781511 | 6427911 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 6398934 B/min
# steady --spec bloom --n 8000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 10% | 3333.3 | 874036 / 1307879 / 902938 | 149381 / 174632 / 145426 | 12835456 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 182.4 ms per interval
# steady --spec c5 --n 8000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 8000000 | 10% | 3333.3 | 860246 / 1114888 / 869135 | 146862 / 162421 / 147567 | 6427911 | 250 | 125283177 | 2.70 / 2.96 | 84.78 MiB | resnap=64: 23738 snapshots, build 0.08 ms each, overlay 61579 entries at end
# steady --spec c4 --n 8000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 10% | 3333.3 | 892726 / 1081008 / 897584 | 738645 / 1111927 / 750619 | 6427911 | 250 | 0 | 3.63 / 3.89 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c3 --n 8000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 8000000 | 10% | 3333.3 | 895996 / 1100988 / 901753 | 697745 / 883556 / 699856 | 6427911 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6b --n 8000000 --churn 10 --minutes 2
| C6b suffix w=32 | 8000000 | 10% | 3333.3 | 893337 / 1196188 / 905753 | 1133138 / 1764202 / 1166675 | 6427911 | 250 | 0 | 4.39 / 7.74 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6a --n 8000000 --churn 10 --minutes 2
| C6a EF-coded exact | 8000000 | 10% | 3333.3 | 984716 / 1171198 / 989512 | 9031963 / 9166613 / 9017226 | 6427911 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c1 --n 8000000 --churn 10 --minutes 2
| C1 exact | 8000000 | 10% | 3333.3 | 931017 / 1138898 / 936785 | 883317 / 971407 / 886468 | 6427911 | 250 | 0 | 11.11 / 12.06 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c1 --n 8000000 --churn 10 --minutes 2
| C1 exact | 8000000 | 10% | 3333.3 | 979237 / 1126128 / 984520 | 905716 / 993827 / 908247 | 6427911 | 250 | 0 | 11.11 / 12.06 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6a --n 8000000 --churn 10 --minutes 2
| C6a EF-coded exact | 8000000 | 10% | 3333.3 | 997777 / 1201278 / 1002572 | 9055632 / 9194304 / 9040320 | 6427911 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6b --n 8000000 --churn 10 --minutes 2
| C6b suffix w=32 | 8000000 | 10% | 3333.3 | 907137 / 1203438 / 917042 | 1166558 / 1779732 / 1191529 | 6427911 | 250 | 0 | 4.39 / 7.74 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c3 --n 8000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 8000000 | 10% | 3333.3 | 888776 / 1153118 / 895062 | 685605 / 865176 / 689549 | 6427911 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c4 --n 8000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 10% | 3333.3 | 896076 / 976057 / 899660 | 736115 / 1071868 / 748006 | 6427911 | 250 | 0 | 3.63 / 3.89 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c5 --n 8000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 8000000 | 10% | 3333.3 | 859876 / 1073097 / 872340 | 145861 / 158971 / 146321 | 6427911 | 250 | 125283177 | 2.70 / 2.96 | 84.78 MiB | resnap=64: 23738 snapshots, build 0.08 ms each, overlay 61579 entries at end
# steady --spec bloom --n 8000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 10% | 3333.3 | 883906 / 1338109 / 920358 | 146901 / 187751 / 146854 | 12835456 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 180.1 ms per interval
# steady --spec c8a --n 8000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 10% | 3333.3 | 1142878 / 1604941 / 1156851 | 1369580 / 5195760312 / 420648707 | 6427911 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c8a --n 8000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 10% | 3333.3 | 1054887 / 1595801 / 1150754 | 1379999 / 5195966926 / 421440807 | 6427911 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 6398934 B/min
# steady --spec bloom --n 8000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 10% | 3333.3 | 885376 / 1298949 / 926295 | 151591 / 179232 / 146465 | 12835456 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 183.4 ms per interval
# steady --spec c5 --n 8000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 8000000 | 10% | 3333.3 | 874856 / 1097358 / 882008 | 148731 / 195612 / 150922 | 6427911 | 250 | 125283177 | 2.70 / 2.96 | 84.78 MiB | resnap=64: 23738 snapshots, build 0.08 ms each, overlay 61579 entries at end
# steady --spec c4 --n 8000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 10% | 3333.3 | 903947 / 1148718 / 912542 | 735456 / 1100497 / 749414 | 6427911 | 250 | 0 | 3.63 / 3.89 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c3 --n 8000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 8000000 | 10% | 3333.3 | 891766 / 1137298 / 898015 | 686294 / 810255 / 690244 | 6427911 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6b --n 8000000 --churn 10 --minutes 2
| C6b suffix w=32 | 8000000 | 10% | 3333.3 | 913626 / 1165318 / 919398 | 1215999 / 1691201 / 1232181 | 6427911 | 250 | 0 | 4.39 / 7.74 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6a --n 8000000 --churn 10 --minutes 2
| C6a EF-coded exact | 8000000 | 10% | 3333.3 | 1025327 / 1234139 / 1031864 | 9078734 / 9223685 / 9073825 | 6427911 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c1 --n 8000000 --churn 10 --minutes 2
| C1 exact | 8000000 | 10% | 3333.3 | 940467 / 1198019 / 950627 | 876746 / 978497 / 883231 | 6427911 | 250 | 0 | 11.11 / 12.06 | 84.78 MiB | delta log 6398934 B/min
