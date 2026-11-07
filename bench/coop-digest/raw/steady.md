| candidate | n | churn/min | changes per tick | publisher absorb p50 / p99 / mean ns | receiver apply p50 / p99 / mean ns | delta wire B/peer-min | summary wire B/peer-min | snapshot wire B/peer-min | receiver B/key at start / after | publisher set bytes | notes |
# steady --spec c1 --n 1000000 --churn 0.1 --minutes 2
| C1 exact | 1000000 | 0.1% | 4.2 | 940 / 1600 / 1036 | 1190 / 2030 / 1237 | 42491 | 248 | 0 | 11.88 / 11.92 | 11.33 MiB | delta log 28394 B/min
# steady --spec c6a --n 1000000 --churn 0.1 --minutes 2
| C6a EF-coded exact | 1000000 | 0.1% | 4.2 | 920 / 1490 / 1012 | 6670 / 9670 / 6911 | 42491 | 248 | 0 | 6.54 / 6.55 | 11.33 MiB | delta log 28394 B/min
# steady --spec c6b --n 1000000 --churn 0.1 --minutes 2
| C6b suffix w=32 | 1000000 | 0.1% | 4.2 | 880 / 1530 / 969 | 1110 / 1860 / 1156 | 42491 | 248 | 0 | 4.79 / 5.02 | 11.33 MiB | delta log 28394 B/min
# steady --spec c3 --n 1000000 --churn 0.1 --minutes 2
| C3 cuckoo f=16 | 1000000 | 0.1% | 4.2 | 920 / 1550 / 1033 | 720 / 1210 / 761 | 42491 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 28394 B/min
# steady --spec c4 --n 1000000 --churn 0.1 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 0.1% | 4.2 | 890 / 1480 / 988 | 980 / 4050 / 1134 | 42491 | 248 | 0 | 3.64 / 3.66 | 11.33 MiB | delta log 28394 B/min
# steady --spec c5 --n 1000000 --churn 0.1 --minutes 2
| C5 fuse16+overlay | 1000000 | 0.1% | 4.2 | 840 / 1490 / 943 | 590 / 900 / 617 | 42491 | 248 | 0 | 2.70 / 2.74 | 11.33 MiB | resnap=64: 0 snapshots, build 0.00 ms each, overlay 1998 entries at end
# steady --spec bloom --n 1000000 --churn 0.1 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 0.1% | 4.2 | 790 / 1440 / 880 | 340 / 1010 / 363 | 40944 | 248 | 2529234 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 20.4 ms per interval
# steady --spec c8a --n 1000000 --churn 0.1 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 0.1% | 4.2 | 850 / 1340 / 917 | 1250 / 2010 / 1311 | 42491 | 248 | 0 | 2.73 / 2.77 | 11.33 MiB | delta log 28394 B/min
# steady --spec c1 --n 1000000 --churn 1 --minutes 2
| C1 exact | 1000000 | 1% | 41.7 | 6021 / 7421 / 6131 | 6681 / 9180 / 6742 | 122932 | 248 | 0 | 11.88 / 12.09 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6a --n 1000000 --churn 1 --minutes 2
| C6a EF-coded exact | 1000000 | 1% | 41.7 | 6050 / 8741 / 6159 | 59571 / 68051 / 59875 | 122932 | 248 | 0 | 6.54 / 6.55 | 11.33 MiB | delta log 108371 B/min
# steady --spec c6b --n 1000000 --churn 1 --minutes 2
| C6b suffix w=32 | 1000000 | 1% | 41.7 | 5750 / 8030 / 5925 | 6370 / 10060 / 6623 | 122932 | 248 | 0 | 4.79 / 6.26 | 11.33 MiB | delta log 108371 B/min
# steady --spec c3 --n 1000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 1000000 | 1% | 41.7 | 5780 / 7990 / 5950 | 3930 / 6371 / 4061 | 122932 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 108371 B/min
# steady --spec c4 --n 1000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 1% | 41.7 | 5780 / 7930 / 5874 | 5370 / 12700 / 6049 | 122932 | 248 | 0 | 3.64 / 3.72 | 11.33 MiB | delta log 108371 B/min
# steady --spec c5 --n 1000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 5630 / 7380 / 5753 | 2520 / 6900 / 2604 | 122932 | 248 | 1294288 | 2.70 / 2.90 | 11.33 MiB | resnap=64: 245 snapshots, build 0.07 ms each, overlay 4291 entries at end
# steady --spec bloom --n 1000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 1% | 41.7 | 5380 / 7790 / 5543 | 1540 / 2660 / 1636 | 185280 | 248 | 2529318 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 20.3 ms per interval
# steady --spec c8a --n 1000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 1% | 41.7 | 6050 / 12090 / 6885 | 9480 / 16460 / 253959 | 122932 | 248 | 0 | 2.73 / 2.79 | 11.33 MiB | delta log 108371 B/min
# steady --spec c1 --n 1000000 --churn 10 --minutes 2
| C1 exact | 1000000 | 10% | 416.7 | 44520 / 71171 / 48883 | 28890 / 73111 / 35412 | 877311 | 248 | 0 | 11.88 / 12.82 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6a --n 1000000 --churn 10 --minutes 2
| C6a EF-coded exact | 1000000 | 10% | 416.7 | 40100 / 58430 / 42484 | 564426 / 594127 / 565317 | 877311 | 248 | 0 | 6.54 / 6.54 | 11.33 MiB | delta log 862734 B/min
# steady --spec c6b --n 1000000 --churn 10 --minutes 2
| C6b suffix w=32 | 1000000 | 10% | 416.7 | 40290 / 55741 / 41930 | 36070 / 71741 / 38463 | 877311 | 248 | 0 | 4.79 / 7.84 | 11.33 MiB | delta log 862734 B/min
# steady --spec c3 --n 1000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 1000000 | 10% | 416.7 | 37130 / 52921 / 39306 | 23540 / 36831 / 24610 | 877311 | 248 | 0 | 2.66 / 2.67 | 11.33 MiB | delta log 862734 B/min
# steady --spec c4 --n 1000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 1000000 | 10% | 416.7 | 37801 / 56611 / 40270 | 39481 / 64060 / 41256 | 877311 | 248 | 0 | 3.64 / 3.90 | 11.33 MiB | delta log 862734 B/min
# steady --spec c5 --n 1000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 1000000 | 10% | 416.7 | 36981 / 56921 / 38781 | 18360 / 24230 / 18426 | 877311 | 248 | 15593578 | 2.70 / 2.96 | 11.33 MiB | resnap=64: 2959 snapshots, build 0.07 ms each, overlay 8195 entries at end
# steady --spec bloom --n 1000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 1000000 | 10% | 416.7 | 42711 / 56820 / 43875 | 10980 / 18390 / 11235 | 1613696 | 248 | 2526797 | 1.26 / 1.26 | 11.33 MiB | bloom snapshot rebuild 21.1 ms per interval
# steady --spec c8a --n 1000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 1000000 | 10% | 416.7 | 83021 / 102131 / 81912 | 115011 / 315988864 / 6017597 | 877311 | 248 | 0 | 2.73 / 2.77 | 11.33 MiB | delta log 862734 B/min
# steady --spec c1 --n 8000000 --churn 0.1 --minutes 2
| C1 exact | 8000000 | 0.1% | 33.3 | 13030 / 18530 / 13276 | 11570 / 18740 / 11820 | 105168 | 250 | 0 | 11.11 / 11.14 | 84.78 MiB | delta log 90702 B/min
# steady --spec c6a --n 8000000 --churn 0.1 --minutes 2
| C6a EF-coded exact | 8000000 | 0.1% | 33.3 | 13510 / 20141 / 13832 | 92471 / 106561 / 93295 | 105168 | 250 | 0 | 5.77 / 5.78 | 84.78 MiB | delta log 90702 B/min
# steady --spec c6b --n 8000000 --churn 0.1 --minutes 2
| C6b suffix w=32 | 8000000 | 0.1% | 33.3 | 13790 / 20900 / 14138 | 22150 / 32541 / 22598 | 105168 | 250 | 0 | 4.39 / 4.83 | 84.78 MiB | delta log 90702 B/min
# steady --spec c3 --n 8000000 --churn 0.1 --minutes 2
| C3 cuckoo f=16 | 8000000 | 0.1% | 33.3 | 13210 / 19740 / 13505 | 9450 / 14930 / 9609 | 105168 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 90702 B/min
# steady --spec c4 --n 8000000 --churn 0.1 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 0.1% | 33.3 | 12440 / 18560 / 12684 | 12370 / 21150 / 12987 | 105168 | 250 | 0 | 3.63 / 3.65 | 84.78 MiB | delta log 90702 B/min
# steady --spec c5 --n 8000000 --churn 0.1 --minutes 2
| C5 fuse16+overlay | 8000000 | 0.1% | 33.3 | 13700 / 19920 / 13966 | 2700 / 6150 / 2957 | 105168 | 250 | 0 | 2.70 / 2.73 | 84.78 MiB | resnap=64: 0 snapshots, build 0.00 ms each, overlay 15990 entries at end
# steady --spec bloom --n 8000000 --churn 0.1 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 0.1% | 33.3 | 14051 / 21760 / 14365 | 1440 / 4140 / 1686 | 153440 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 181.8 ms per interval
# steady --spec c8a --n 8000000 --churn 0.1 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 0.1% | 33.3 | 13831 / 20550 / 14146 | 16770 / 23651 / 16967 | 105168 | 250 | 0 | 2.73 / 2.76 | 84.78 MiB | delta log 90702 B/min
# steady --spec c1 --n 8000000 --churn 1 --minutes 2
| C1 exact | 8000000 | 1% | 333.3 | 101701 / 123251 / 103831 | 91081 / 118661 / 93060 | 715014 | 250 | 0 | 11.11 / 11.32 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6a --n 8000000 --churn 1 --minutes 2
| C6a EF-coded exact | 8000000 | 1% | 333.3 | 110791 / 131421 / 112124 | 893179 / 920560 / 894907 | 715014 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 700438 B/min
# steady --spec c6b --n 8000000 --churn 1 --minutes 2
| C6b suffix w=32 | 8000000 | 1% | 333.3 | 101421 / 115261 / 102077 | 135372 / 235953 / 148477 | 715014 | 250 | 0 | 4.39 / 6.44 | 84.78 MiB | delta log 700438 B/min
# steady --spec c3 --n 8000000 --churn 1 --minutes 2
| C3 cuckoo f=16 | 8000000 | 1% | 333.3 | 96631 / 120441 / 99481 | 72361 / 99631 / 76342 | 715014 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 700438 B/min
# steady --spec c4 --n 8000000 --churn 1 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 1% | 333.3 | 96251 / 120591 / 98286 | 84641 / 134472 / 90628 | 715014 | 250 | 0 | 3.63 / 3.70 | 84.78 MiB | delta log 700438 B/min
# steady --spec c5 --n 8000000 --churn 1 --minutes 2
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 95081 / 118672 / 97430 | 19130 / 26780 / 19499 | 715014 | 250 | 10312910 | 2.70 / 2.89 | 84.78 MiB | resnap=64: 1953 snapshots, build 0.08 ms each, overlay 34796 entries at end
# steady --spec bloom --n 8000000 --churn 1 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 1% | 333.3 | 104131 / 129432 / 105399 | 9800 / 29780 / 12692 | 1294624 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 182.8 ms per interval
# steady --spec c8a --n 8000000 --churn 1 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 1% | 333.3 | 113671 / 149021 / 115439 | 153411 / 177101 / 14668491 | 715014 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 700438 B/min
# steady --spec c1 --n 8000000 --churn 10 --minutes 2
| C1 exact | 8000000 | 10% | 3333.3 | 955490 / 1188232 / 964727 | 911649 / 977180 / 913917 | 6427911 | 250 | 0 | 11.11 / 12.06 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6a --n 8000000 --churn 10 --minutes 2
| C6a EF-coded exact | 8000000 | 10% | 3333.3 | 1001911 / 1299853 / 1015135 | 9066894 / 9172545 / 9048207 | 6427911 | 250 | 0 | 5.77 / 5.77 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c6b --n 8000000 --churn 10 --minutes 2
| C6b suffix w=32 | 8000000 | 10% | 3333.3 | 920859 / 1238893 / 936469 | 1179862 / 1822069 / 1215427 | 6427911 | 250 | 0 | 4.39 / 7.74 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c3 --n 8000000 --churn 10 --minutes 2
| C3 cuckoo f=16 | 8000000 | 10% | 3333.3 | 894099 / 1154132 / 901363 | 685077 / 830818 / 690512 | 6427911 | 250 | 0 | 2.66 / 2.66 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c4 --n 8000000 --churn 10 --minutes 2
| C4 quotient eps=2^-14 | 8000000 | 10% | 3333.3 | 899150 / 1143402 / 904638 | 729938 / 1112202 / 746763 | 6427911 | 250 | 0 | 3.63 / 3.89 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c5 --n 8000000 --churn 10 --minutes 2
| C5 fuse16+overlay | 8000000 | 10% | 3333.3 | 857099 / 1086631 / 873803 | 147411 / 161442 / 147947 | 6427911 | 250 | 125283177 | 2.70 / 2.96 | 84.78 MiB | resnap=64: 23738 snapshots, build 0.08 ms each, overlay 61579 entries at end
# steady --spec bloom --n 8000000 --churn 10 --minutes 2
| Bloom (10 b/key, k=7) | 8000000 | 10% | 3333.3 | 894740 / 1339844 / 925868 | 147161 / 175092 / 143276 | 12835456 | 250 | 8448256 | 0.53 / 0.53 | 84.78 MiB | bloom snapshot rebuild 184.1 ms per interval
# steady --spec c8a --n 8000000 --churn 10 --minutes 2
| C8a cuckoo over disk exact | 8000000 | 10% | 3333.3 | 1170772 / 1647298 / 1173042 | 1383385 / 2589365605 / 209157720 | 6427911 | 250 | 0 | 2.73 / 2.80 | 84.78 MiB | delta log 6398934 B/min
# steady --spec c5 --n 1000000 --churn 1 --minutes 2 --resnap 16
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 4920 / 9000 / 5471 | 2480 / 3090 / 2551 | 122932 | 248 | 5941354 | 2.70 / 2.78 | 11.33 MiB | resnap=16: 1124 snapshots, build 0.07 ms each, overlay 1909 entries at end
# steady --spec c5 --n 1000000 --churn 1 --minutes 2 --resnap 256
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 5860 / 8550 / 5979 | 2470 / 2870 / 2481 | 122932 | 248 | 0 | 2.70 / 2.96 | 11.33 MiB | resnap=256: 0 snapshots, build 0.00 ms each, overlay 19893 entries at end
# steady --spec c5 --n 1000000 --churn 1 --minutes 2 --resnap 1024
| C5 fuse16+overlay | 1000000 | 1% | 41.7 | 5860 / 7920 / 6005 | 2460 / 3060 / 2505 | 122932 | 248 | 0 | 2.70 / 2.96 | 11.33 MiB | resnap=1024: 0 snapshots, build 0.00 ms each, overlay 19893 entries at end
# steady --spec c5 --n 8000000 --churn 1 --minutes 2 --resnap 16
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 95971 / 125901 / 98161 | 17200 / 25600 / 17813 | 715014 | 250 | 47475411 | 2.70 / 2.77 | 84.78 MiB | resnap=16: 8990 snapshots, build 0.08 ms each, overlay 15439 entries at end
# steady --spec c5 --n 8000000 --churn 1 --minutes 2 --resnap 256
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 100011 / 130521 / 102313 | 19750 / 29231 / 20328 | 715014 | 250 | 0 | 2.70 / 2.95 | 84.78 MiB | resnap=256: 0 snapshots, build 0.00 ms each, overlay 159211 entries at end
# steady --spec c5 --n 8000000 --churn 1 --minutes 2 --resnap 1024
| C5 fuse16+overlay | 8000000 | 1% | 333.3 | 93731 / 118221 / 96560 | 19491 / 28131 / 20120 | 715014 | 250 | 0 | 2.70 / 2.95 | 84.78 MiB | resnap=1024: 0 snapshots, build 0.00 ms each, overlay 159211 entries at end
