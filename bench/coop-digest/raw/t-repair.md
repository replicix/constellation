| scenario | candidate | method | result | sessions | rounds | wire | owner / receiver CPU ms | derived wall | FN |
## rep=1 n=1000000
# repair --n 1000000 --specs c1,c6a,c6b,c3,c4,c5,c8a --scenarios gap1,gap1k,gap100k,restart,initial
| gap1 | C1 exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.0 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | real | ok | 1 | 3 | 5.9 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 649 B | 95.0 / 95.3 | 191.3 / 240.3 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 649 B | 95.1 / 106.1 | 202.2 / 251.2 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 2 | 6.3 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 1 | 31.2 KiB | 0.1 / 0.2 | 1.2 / 50.2 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 649 B | 95.4 / 97.3 | 193.7 / 242.7 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.5 / 3.3 | 18.8 / 655.8 | 0 |
| gap1k | C1 exact | real | ok | 1 | 13 | 544.8 KiB | 0.0 / 6.1 | 19.1 / 656.1 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 27.2 KiB | 221.3 / 221.7 | 449.0 / 743.0 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.6 / 16.7 | 32.3 / 669.3 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 27.2 KiB | 221.6 / 235.4 | 463.0 / 757.0 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 1.4 | 2.4 / 51.4 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 11 | 578.5 KiB | 1.2 / 1.7 | 13.9 / 552.9 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 33 | 1.73 MiB | 2.1 / 6.6 | 41.7 / 1658.7 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 33 | 1.73 MiB | 1.8 / 7.9 | 42.8 / 1659.8 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 1 | 250 | 7.01 MiB | 10.3 / 27.1 | 287.3 / 12537.3 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.4 / 18.3 | 33.6 / 670.6 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 27.2 KiB | 222.0 / 224.6 | 452.5 / 746.5 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap100k | C1 exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.2 / 43.6 | 226.9 / 8262.9 | 0 |
| gap100k | C1 exact | real | ok | 1 | 164 | 8.16 MiB | 0.0 / 64.7 | 228.7 / 8264.7 | 0 |
| gap100k | C1 exact | riblt | ok | 1 | 49 | 1.66 MiB | 355.7 / 403.8 | 808.5 / 3209.5 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 4.2 | 20.2 / 804.2 | 0 |
| gap100k | C6a EF-coded exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.5 / 201.9 | 385.3 / 8421.3 | 0 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 49 | 1.66 MiB | 351.0 / 539.4 | 939.4 / 3340.4 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 130.9 | 147.0 / 931.0 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 1 | 147 | 8.05 MiB | 7.3 / 19.6 | 174.0 / 7377.0 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 6.3 | 22.4 / 806.4 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.98 MiB | 7.3 / 30.0 | 181.3 / 7237.3 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.9 | 19.9 / 803.9 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.98 MiB | 7.3 / 36.8 | 188.0 / 7244.0 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 7.8 | 23.8 / 807.8 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.0 / 27.8 | 293.8 / 12837.8 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.1 | 19.2 / 803.2 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.8 / 2718.3 | 2902.1 / 10938.1 | 0 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 49 | 1.66 MiB | 350.1 / 2616.5 | 3015.6 / 5416.6 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 3904.3 | 3920.4 / 4704.4 | 0 |
| restart | C1 exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.5 / 22.2 | 115.6 / 4084.6 | 0 |
| restart | C1 exact | real | ok | 1 | 81 | 3.80 MiB | 0.0 / 35.5 | 116.5 / 4085.5 | 0 |
| restart | C1 exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.6 / 308.5 | 622.1 / 1357.1 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.8 / 109.5 | 203.3 / 4172.3 | 0 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.1 / 347.4 | 660.5 / 1395.5 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 1 | 104 | 5.69 MiB | 6.5 / 14.1 | 124.6 / 5220.6 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.92 MiB | 7.1 / 29.5 | 180.6 / 7236.6 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.92 MiB | 6.7 / 36.0 | 186.7 / 7242.7 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 9.9 / 27.6 | 293.5 / 12837.5 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.2 / 614.2 | 707.4 / 4676.4 | 0 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.9 / 429.4 | 743.3 / 1478.3 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| initial | C1 exact | rbsr | ok | 2 | 258 | 7.16 MiB | 10.9 / 28.8 | 297.7 / 12939.7 | 0 |
| initial | C1 exact | real | n/a | | | | | | |
| initial | C1 exact | riblt | ok | 1 | 446 | 16.86 MiB | 643.7 / 586.6 | 1676.3 / 23530.3 | 0 |
| initial | C1 exact | replay | n/a | | | | | | |
| initial | C6a EF-coded exact | rbsr | ok | 2 | 258 | 7.16 MiB | 10.9 / 45.9 | 314.8 / 12956.8 | 0 |
| initial | C6a EF-coded exact | real | n/a | | | | | | |
| initial | C6a EF-coded exact | riblt | ok | 1 | 446 | 16.86 MiB | 641.2 / 1975.7 | 3062.9 / 24916.9 | 0 |
| initial | C6a EF-coded exact | replay | n/a | | | | | | |
| initial | C6b suffix w=32 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.9 / 14.2 | 283.1 / 12925.1 | 0 |
| initial | C6b suffix w=32 | real | n/a | | | | | | |
| initial | C6b suffix w=32 | riblt | n/a | | | | | | |
| initial | C6b suffix w=32 | replay | n/a | | | | | | |
| initial | C3 cuckoo f=16 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.4 / 26.0 | 294.4 / 12936.4 | 0 |
| initial | C3 cuckoo f=16 | real | n/a | | | | | | |
| initial | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| initial | C3 cuckoo f=16 | replay | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.1 / 32.5 | 300.6 / 12942.6 | 0 |
| initial | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| initial | C5 fuse16+overlay | rbsr | ok | 2 | 258 | 7.16 MiB | 9.9 / 28.2 | 296.1 / 12938.1 | 0 |
| initial | C5 fuse16+overlay | real | n/a | | | | | | |
| initial | C5 fuse16+overlay | riblt | n/a | | | | | | |
| initial | C5 fuse16+overlay | replay | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | rbsr | ok | 2 | 258 | 7.16 MiB | 11.3 / 15930.5 | 16199.9 / 28841.9 | 0 |
| initial | C8a cuckoo over disk exact | real | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | riblt | ok | 1 | 446 | 16.86 MiB | 637.7 / 20010.6 | 21094.2 / 42948.2 | 0 |
| initial | C8a cuckoo over disk exact | replay | n/a | | | | | | |
## rep=1 n=8000000
# repair --n 8000000 --specs c1,c6a,c6b,c3,c4,c5,c8a --scenarios gap1,gap1k,gap100k,restart
| gap1 | C1 exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | real | ok | 1 | 4 | 6.4 KiB | 0.0 / 0.2 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 657 B | 758.6 / 779.9 | 1539.6 / 1588.6 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 657 B | 756.5 / 867.8 | 1625.3 / 1674.3 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 3 | 7.4 KiB | 0.1 / 0.1 | 3.2 / 150.2 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 4 | 66.5 KiB | 0.2 / 0.4 | 4.6 / 200.6 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 657 B | 757.3 / 792.1 | 1550.4 / 1599.4 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.6 / 5.8 | 31.4 / 1060.4 | 0 |
| gap1k | C1 exact | real | ok | 1 | 21 | 852.2 KiB | 0.0 / 10.7 | 31.7 / 1060.7 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 28.1 KiB | 1764.6 / 1783.6 | 3554.2 / 3848.2 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.8 / 30.4 | 56.1 / 1085.1 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 28.1 KiB | 1758.9 / 1873.5 | 3638.5 / 3932.5 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 2.9 | 3.9 / 52.9 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 26 | 1.25 MiB | 3.1 / 5.0 | 34.1 / 1308.1 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.2 / 7.8 | 44.9 / 1710.9 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.2 / 9.5 | 46.7 / 1712.7 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 2 | 400 | 21.71 MiB | 22.7 / 93.1 | 515.9 / 20115.9 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.6 / 37.6 | 63.3 / 1092.3 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 28.1 KiB | 1761.9 / 1797.0 | 3564.9 / 3858.9 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap100k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11205.4 / 13790.9 | 76196.3 / 2584996.3 | 119 |
| gap100k | C1 exact | real | FAILED | 50 | 12800 | 517.41 MiB | 0.0 / 6187.3 | 18987.3 / 646187.3 | 119 |
| gap100k | C1 exact | riblt | ok | 1 | 50 | 1.70 MiB | 2783.2 / 2875.5 | 5708.7 / 8158.7 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 24.2 | 41.4 / 874.4 | 0 |
| gap100k | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11365.9 / 110473.8 | 173039.6 / 2681839.6 | 119 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 50 | 1.70 MiB | 2786.2 / 3205.7 | 6041.9 / 8491.9 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 262.7 | 279.8 / 1112.8 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 4 | 896 | 49.06 MiB | 61.7 / 130.6 | 1088.3 / 44992.3 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 30.5 | 47.5 / 880.5 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 4 | 983 | 55.35 MiB | 52.9 / 223.3 | 1259.2 / 49426.2 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 15.6 | 32.7 / 865.7 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 983 | 55.35 MiB | 53.4 / 273.2 | 1309.6 / 49476.6 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 20.0 | 37.0 / 870.0 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 5 | 1038 | 56.36 MiB | 58.8 / 245.1 | 1341.8 / 52203.8 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 4.0 | 21.0 / 854.0 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11502.8 / 80911.6 | 143614.4 / 2652414.4 | 119 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 50 | 1.70 MiB | 2784.2 / 12495.5 | 15329.7 / 17779.7 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 10215.8 | 10232.8 / 11065.8 | 0 |
| restart | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11069.7 / 13701.6 | 75971.3 / 2584771.3 | 208 |
| restart | C1 exact | real | FAILED | 50 | 12800 | 516.82 MiB | 0.0 / 6018.3 | 18818.3 / 646018.3 | 208 |
| restart | C1 exact | riblt | ok | 1 | 76 | 2.70 MiB | 2982.0 / 3115.7 | 6173.7 / 9897.7 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11220.9 / 109435.7 | 171856.7 / 2680656.7 | 208 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 76 | 2.70 MiB | 2987.2 / 3588.9 | 6652.1 / 10376.1 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 5 | 1041 | 56.99 MiB | 69.1 / 150.9 | 1261.0 / 52270.0 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 4 | 1024 | 57.69 MiB | 54.7 / 233.5 | 1312.2 / 51488.2 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 4 | 1024 | 57.69 MiB | 55.2 / 284.4 | 1363.6 / 51539.6 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 5 | 1039 | 56.35 MiB | 55.5 / 241.4 | 1335.9 / 52246.9 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11320.0 / 82725.2 | 145245.2 / 2654045.2 | 208 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 76 | 2.70 MiB | 2982.4 / 17829.9 | 20888.3 / 24612.3 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
## rep=2 n=1000000
# repair --n 1000000 --specs c8a,c5,c4,c3,c6b,c6a,c1 --scenarios gap1,gap1k,gap100k,restart,initial
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 649 B | 95.4 / 97.0 | 193.5 / 242.5 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 1 | 31.2 KiB | 0.1 / 0.2 | 1.2 / 50.2 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 2 | 6.3 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 649 B | 95.3 / 107.5 | 203.8 / 252.8 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C1 exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.0 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | real | ok | 1 | 3 | 5.9 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 649 B | 95.5 / 94.5 | 191.0 / 240.0 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.4 / 18.7 | 34.2 / 671.2 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 27.2 KiB | 220.5 / 222.9 | 449.4 / 743.4 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 1 | 250 | 7.01 MiB | 10.5 / 27.1 | 287.6 / 12537.6 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 33 | 1.73 MiB | 2.1 / 7.9 | 43.0 / 1660.0 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 33 | 1.73 MiB | 1.9 / 6.5 | 41.4 / 1658.4 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 11 | 578.5 KiB | 1.0 / 1.7 | 13.7 / 552.7 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.3 / 16.6 | 31.9 / 668.9 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 27.2 KiB | 220.8 / 235.3 | 462.1 / 756.1 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 1.4 | 2.4 / 51.4 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.6 / 3.4 | 19.0 / 656.0 | 0 |
| gap1k | C1 exact | real | ok | 1 | 13 | 544.8 KiB | 0.0 / 6.2 | 19.2 / 656.2 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 27.2 KiB | 220.8 / 220.8 | 447.6 / 741.6 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.9 / 2716.9 | 2900.8 / 10936.8 | 0 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 49 | 1.66 MiB | 349.9 / 2578.3 | 2977.2 / 5378.2 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 3858.9 | 3875.0 / 4659.0 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.7 / 28.6 | 295.3 / 12839.3 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.3 | 19.3 / 803.3 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.98 MiB | 7.4 / 36.4 | 187.8 / 7243.8 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 7.9 | 24.0 / 808.0 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.98 MiB | 6.9 / 29.5 | 180.5 / 7236.5 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.9 | 19.9 / 803.9 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 1 | 147 | 8.05 MiB | 6.2 / 19.6 | 172.9 / 7375.9 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 6.6 | 22.7 / 806.7 | 0 |
| gap100k | C6a EF-coded exact | rbsr | ok | 1 | 164 | 8.20 MiB | 18.6 / 202.9 | 385.5 / 8421.5 | 0 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 49 | 1.66 MiB | 349.6 / 538.0 | 936.6 / 3337.6 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 130.7 | 146.8 / 930.8 | 0 |
| gap100k | C1 exact | rbsr | ok | 1 | 164 | 8.20 MiB | 18.9 / 43.6 | 226.5 / 8262.5 | 0 |
| gap100k | C1 exact | real | ok | 1 | 164 | 8.16 MiB | 0.0 / 63.6 | 227.6 / 8263.6 | 0 |
| gap100k | C1 exact | riblt | ok | 1 | 49 | 1.66 MiB | 349.0 / 401.0 | 798.9 / 3199.9 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 4.2 | 20.2 / 804.2 | 0 |
| restart | C8a cuckoo over disk exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.6 / 596.8 | 690.4 / 4659.4 | 0 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.6 / 429.1 | 742.7 / 1477.7 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.7 / 28.4 | 295.2 / 12839.2 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.92 MiB | 7.3 / 36.2 | 187.5 / 7243.5 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.92 MiB | 6.8 / 29.2 | 180.0 / 7236.0 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 1 | 104 | 5.69 MiB | 5.2 / 14.1 | 123.3 / 5219.3 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | ok | 1 | 81 | 3.81 MiB | 11.6 / 109.6 | 202.2 / 4171.2 | 0 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 15 | 367.1 KiB | 297.8 / 347.3 | 660.1 / 1395.1 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C1 exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.4 / 21.7 | 115.1 / 4084.1 | 0 |
| restart | C1 exact | real | ok | 1 | 81 | 3.80 MiB | 0.0 / 35.2 | 116.2 / 4085.2 | 0 |
| restart | C1 exact | riblt | ok | 1 | 15 | 367.1 KiB | 297.7 / 307.0 | 619.7 / 1354.7 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | rbsr | ok | 2 | 258 | 7.16 MiB | 11.0 / 15885.8 | 16154.8 / 28796.8 | 0 |
| initial | C8a cuckoo over disk exact | real | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | riblt | ok | 1 | 446 | 16.86 MiB | 642.2 / 19941.2 | 21029.4 / 42883.4 | 0 |
| initial | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| initial | C5 fuse16+overlay | rbsr | ok | 2 | 258 | 7.16 MiB | 10.8 / 28.4 | 297.2 / 12939.2 | 0 |
| initial | C5 fuse16+overlay | real | n/a | | | | | | |
| initial | C5 fuse16+overlay | riblt | n/a | | | | | | |
| initial | C5 fuse16+overlay | replay | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.7 / 32.5 | 301.2 / 12943.2 | 0 |
| initial | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| initial | C3 cuckoo f=16 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.6 / 26.0 | 294.6 / 12936.6 | 0 |
| initial | C3 cuckoo f=16 | real | n/a | | | | | | |
| initial | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| initial | C3 cuckoo f=16 | replay | n/a | | | | | | |
| initial | C6b suffix w=32 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.0 / 14.1 | 282.1 / 12924.1 | 0 |
| initial | C6b suffix w=32 | real | n/a | | | | | | |
| initial | C6b suffix w=32 | riblt | n/a | | | | | | |
| initial | C6b suffix w=32 | replay | n/a | | | | | | |
| initial | C6a EF-coded exact | rbsr | ok | 2 | 258 | 7.16 MiB | 9.9 / 46.0 | 313.9 / 12955.9 | 0 |
| initial | C6a EF-coded exact | real | n/a | | | | | | |
| initial | C6a EF-coded exact | riblt | ok | 1 | 446 | 16.86 MiB | 642.6 / 1990.0 | 3078.6 / 24932.6 | 0 |
| initial | C6a EF-coded exact | replay | n/a | | | | | | |
| initial | C1 exact | rbsr | ok | 2 | 258 | 7.16 MiB | 10.6 / 28.3 | 296.9 / 12938.9 | 0 |
| initial | C1 exact | real | n/a | | | | | | |
| initial | C1 exact | riblt | ok | 1 | 446 | 16.86 MiB | 641.3 / 613.8 | 1701.1 / 23555.1 | 0 |
| initial | C1 exact | replay | n/a | | | | | | |
## rep=2 n=8000000
# repair --n 8000000 --specs c8a,c5,c4,c3,c6b,c6a,c1 --scenarios gap1,gap1k,gap100k,restart
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 657 B | 756.1 / 788.7 | 1545.9 / 1594.9 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 4 | 66.5 KiB | 0.2 / 0.3 | 4.6 / 200.6 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 3 | 7.4 KiB | 0.1 / 0.1 | 3.2 / 150.2 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 657 B | 756.4 / 863.7 | 1621.1 / 1670.1 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C1 exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | real | ok | 1 | 4 | 6.4 KiB | 0.0 / 0.2 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 657 B | 757.7 / 771.3 | 1530.0 / 1579.0 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.8 / 38.3 | 64.1 / 1093.1 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 28.1 KiB | 1762.9 / 1800.6 | 3569.5 / 3863.5 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 2 | 400 | 21.71 MiB | 22.7 / 94.3 | 517.0 / 20117.0 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.2 / 9.5 | 46.7 / 1712.7 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.1 / 7.8 | 44.9 / 1710.9 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 26 | 1.25 MiB | 3.1 / 4.9 | 34.0 / 1308.0 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.6 / 30.3 | 55.9 / 1084.9 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 28.1 KiB | 1765.3 / 1877.3 | 3648.6 / 3942.6 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 2.9 | 3.9 / 52.9 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.6 / 5.9 | 31.5 / 1060.5 | 0 |
| gap1k | C1 exact | real | ok | 1 | 21 | 852.2 KiB | 0.0 / 10.6 | 31.6 / 1060.6 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 28.1 KiB | 1765.5 / 1780.4 | 3551.9 / 3845.9 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11340.9 / 80030.5 | 142571.4 / 2651371.4 | 119 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 50 | 1.70 MiB | 2785.4 / 12422.5 | 15257.9 / 17707.9 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 10291.9 | 10309.0 / 11142.0 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 5 | 1038 | 56.36 MiB | 58.0 / 243.8 | 1339.8 / 52201.8 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 4.0 | 21.0 / 854.0 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 983 | 55.35 MiB | 55.8 / 274.0 | 1312.8 / 49479.8 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 21.3 | 38.4 / 871.4 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 4 | 983 | 55.35 MiB | 54.9 / 221.3 | 1259.2 / 49426.2 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 14.5 | 31.5 / 864.5 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 4 | 896 | 49.06 MiB | 62.2 / 130.0 | 1088.1 / 44992.1 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 17 | 768.5 KiB | 0.0 / 32.2 | 49.2 / 882.2 | 0 |
| gap100k | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11251.2 / 109800.7 | 172251.9 / 2681051.9 | 119 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 50 | 1.70 MiB | 2784.0 / 3205.5 | 6039.5 / 8489.5 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 262.4 | 279.5 / 1112.5 | 0 |
| gap100k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11076.6 / 13832.9 | 76109.5 / 2584909.5 | 119 |
| gap100k | C1 exact | real | FAILED | 50 | 12800 | 517.41 MiB | 0.0 / 6134.7 | 18934.7 / 646134.7 | 119 |
| gap100k | C1 exact | riblt | ok | 1 | 50 | 1.70 MiB | 2783.5 / 2874.8 | 5708.2 / 8158.2 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 24.4 | 41.5 / 874.5 | 0 |
| restart | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11188.0 / 83748.6 | 146136.6 / 2654936.6 | 208 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 76 | 2.70 MiB | 2985.3 / 17809.2 | 20870.5 / 24594.5 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 5 | 1039 | 56.35 MiB | 58.6 / 243.7 | 1341.3 / 52252.3 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 4 | 1024 | 57.69 MiB | 58.4 / 284.2 | 1366.6 / 51542.6 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 4 | 1024 | 57.69 MiB | 57.4 / 232.2 | 1313.6 / 51489.6 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 5 | 1041 | 56.99 MiB | 69.3 / 149.8 | 1260.1 / 52269.1 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11106.4 / 110190.8 | 172497.1 / 2681297.1 | 208 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 76 | 2.70 MiB | 2982.5 / 3593.1 | 6651.6 / 10375.6 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 10918.3 / 13925.2 | 76043.6 / 2584843.6 | 208 |
| restart | C1 exact | real | FAILED | 50 | 12800 | 516.82 MiB | 0.0 / 6010.4 | 18810.4 / 646010.4 | 208 |
| restart | C1 exact | riblt | ok | 1 | 76 | 2.70 MiB | 2984.6 / 3120.0 | 6180.6 / 9904.6 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
## rep=3 n=1000000
# repair --n 1000000 --specs c1,c6a,c6b,c3,c4,c5,c8a --scenarios gap1,gap1k,gap100k,restart,initial
| gap1 | C1 exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.0 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | real | ok | 1 | 3 | 5.9 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 649 B | 95.0 / 95.3 | 191.3 / 240.3 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 649 B | 94.6 / 105.7 | 201.3 / 250.3 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 2 | 6.3 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 1 | 31.2 KiB | 0.1 / 0.2 | 1.2 / 50.2 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 649 B | 94.8 / 96.8 | 192.6 / 241.6 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.5 / 3.4 | 18.9 / 655.9 | 0 |
| gap1k | C1 exact | real | ok | 1 | 13 | 544.8 KiB | 0.0 / 6.1 | 19.1 / 656.1 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 27.2 KiB | 220.5 / 220.7 | 447.3 / 741.3 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.5 / 16.5 | 32.0 / 669.0 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 27.2 KiB | 220.2 / 234.6 | 460.8 / 754.8 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 1.4 | 2.4 / 51.4 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 11 | 578.5 KiB | 1.2 / 1.7 | 13.9 / 552.9 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 33 | 1.73 MiB | 2.0 / 6.5 | 41.5 / 1658.5 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 33 | 1.73 MiB | 1.8 / 7.9 | 42.7 / 1659.7 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 1 | 250 | 7.01 MiB | 9.6 / 26.8 | 286.4 / 12536.4 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.3 / 18.3 | 33.6 / 670.6 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 27.2 KiB | 220.8 / 223.3 | 450.1 / 744.1 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap100k | C1 exact | rbsr | ok | 1 | 164 | 8.20 MiB | 18.8 / 43.7 | 226.5 / 8262.5 | 0 |
| gap100k | C1 exact | real | ok | 1 | 164 | 8.16 MiB | 0.0 / 64.1 | 228.1 / 8264.1 | 0 |
| gap100k | C1 exact | riblt | ok | 1 | 49 | 1.66 MiB | 348.9 / 399.1 | 797.0 / 3198.0 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 4.2 | 20.2 / 804.2 | 0 |
| gap100k | C6a EF-coded exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.0 / 202.3 | 385.3 / 8421.3 | 0 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 49 | 1.66 MiB | 349.1 / 534.1 | 932.2 / 3333.2 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 131.2 | 147.3 / 931.3 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 1 | 147 | 8.05 MiB | 7.4 / 19.8 | 174.2 / 7377.2 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 6.3 | 22.3 / 806.3 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.98 MiB | 7.0 / 29.9 | 180.9 / 7236.9 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.9 | 19.9 / 803.9 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.98 MiB | 6.7 / 36.4 | 187.1 / 7243.1 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 7.9 | 23.9 / 807.9 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 9.5 / 27.8 | 293.3 / 12837.3 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.2 | 19.2 / 803.2 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.8 / 2709.6 | 2893.4 / 10929.4 | 0 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 49 | 1.66 MiB | 349.0 / 2579.1 | 2977.1 / 5378.1 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3856.3 | 3872.4 / 4656.4 | 0 |
| restart | C1 exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.4 / 22.0 | 115.4 / 4084.4 | 0 |
| restart | C1 exact | real | ok | 1 | 81 | 3.80 MiB | 0.0 / 36.0 | 117.0 / 4086.0 | 0 |
| restart | C1 exact | riblt | ok | 1 | 15 | 367.1 KiB | 297.4 / 308.4 | 620.8 / 1355.8 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.7 / 108.6 | 202.3 / 4171.3 | 0 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.0 / 346.9 | 659.9 / 1394.9 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 1 | 104 | 5.69 MiB | 6.3 / 14.1 | 124.4 / 5220.4 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.92 MiB | 7.3 / 29.7 | 181.0 / 7237.0 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.92 MiB | 6.7 / 36.2 | 187.0 / 7243.0 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.0 / 27.6 | 293.6 / 12837.6 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.1 / 591.0 | 684.2 / 4653.2 | 0 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.0 / 411.1 | 724.2 / 1459.2 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| initial | C1 exact | rbsr | ok | 2 | 258 | 7.16 MiB | 10.9 / 27.9 | 296.9 / 12938.9 | 0 |
| initial | C1 exact | real | n/a | | | | | | |
| initial | C1 exact | riblt | ok | 1 | 446 | 16.86 MiB | 643.4 / 609.1 | 1698.5 / 23552.5 | 0 |
| initial | C1 exact | replay | n/a | | | | | | |
| initial | C6a EF-coded exact | rbsr | ok | 2 | 258 | 7.16 MiB | 10.9 / 45.0 | 313.9 / 12955.9 | 0 |
| initial | C6a EF-coded exact | real | n/a | | | | | | |
| initial | C6a EF-coded exact | riblt | ok | 1 | 446 | 16.86 MiB | 641.9 / 1979.9 | 3067.8 / 24921.8 | 0 |
| initial | C6a EF-coded exact | replay | n/a | | | | | | |
| initial | C6b suffix w=32 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.8 / 13.4 | 282.2 / 12924.2 | 0 |
| initial | C6b suffix w=32 | real | n/a | | | | | | |
| initial | C6b suffix w=32 | riblt | n/a | | | | | | |
| initial | C6b suffix w=32 | replay | n/a | | | | | | |
| initial | C3 cuckoo f=16 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.3 / 25.1 | 293.4 / 12935.4 | 0 |
| initial | C3 cuckoo f=16 | real | n/a | | | | | | |
| initial | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| initial | C3 cuckoo f=16 | replay | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.2 / 31.7 | 299.9 / 12941.9 | 0 |
| initial | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| initial | C5 fuse16+overlay | rbsr | ok | 2 | 258 | 7.16 MiB | 10.0 / 27.6 | 295.6 / 12937.6 | 0 |
| initial | C5 fuse16+overlay | real | n/a | | | | | | |
| initial | C5 fuse16+overlay | riblt | n/a | | | | | | |
| initial | C5 fuse16+overlay | replay | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | rbsr | ok | 2 | 258 | 7.16 MiB | 11.2 / 15851.1 | 16120.3 / 28762.3 | 0 |
| initial | C8a cuckoo over disk exact | real | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | riblt | ok | 1 | 446 | 16.86 MiB | 641.8 / 20044.0 | 21131.8 / 42985.8 | 0 |
| initial | C8a cuckoo over disk exact | replay | n/a | | | | | | |
## rep=3 n=8000000
# repair --n 8000000 --specs c1,c6a,c6b,c3,c4,c5,c8a --scenarios gap1,gap1k,gap100k,restart
| gap1 | C1 exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | real | ok | 1 | 4 | 6.4 KiB | 0.0 / 0.2 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 657 B | 758.5 / 778.8 | 1538.3 / 1587.3 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 657 B | 757.2 / 869.0 | 1627.2 / 1676.2 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 3 | 7.4 KiB | 0.1 / 0.1 | 3.2 / 150.2 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 4 | 66.5 KiB | 0.2 / 0.3 | 4.6 / 200.6 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 657 B | 755.7 / 790.7 | 1547.4 / 1596.4 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.6 / 5.9 | 31.5 / 1060.5 | 0 |
| gap1k | C1 exact | real | ok | 1 | 21 | 852.2 KiB | 0.0 / 10.7 | 31.7 / 1060.7 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 28.1 KiB | 1764.0 / 1785.5 | 3555.4 / 3849.4 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.7 / 30.2 | 55.8 / 1084.8 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 28.1 KiB | 1769.7 / 1862.9 | 3638.7 / 3932.7 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 2.9 | 3.9 / 52.9 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 26 | 1.25 MiB | 2.9 / 4.8 | 33.7 / 1307.7 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.4 | 1.5 / 50.5 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.1 / 7.7 | 44.8 / 1710.8 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.1 / 9.4 | 46.5 / 1712.5 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.4 | 1.4 / 50.4 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 2 | 400 | 21.71 MiB | 22.8 / 92.9 | 515.7 / 20115.7 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.5 / 37.9 | 63.4 / 1092.4 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 28.1 KiB | 1767.6 / 1782.1 | 3555.7 / 3849.7 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap100k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11033.2 / 13757.6 | 75990.8 / 2584790.8 | 119 |
| gap100k | C1 exact | real | FAILED | 50 | 12800 | 517.41 MiB | 0.0 / 6161.1 | 18961.1 / 646161.1 | 119 |
| gap100k | C1 exact | riblt | ok | 1 | 50 | 1.70 MiB | 2788.7 / 2878.4 | 5717.1 / 8167.1 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 23.2 | 40.3 / 873.3 | 0 |
| gap100k | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11237.2 / 110387.3 | 172824.5 / 2681624.5 | 119 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 50 | 1.70 MiB | 2789.4 / 3190.2 | 6029.6 / 8479.6 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 262.3 | 279.4 / 1112.4 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 4 | 896 | 49.06 MiB | 61.4 / 131.1 | 1088.5 / 44992.5 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 30.3 | 47.3 / 880.3 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 4 | 983 | 55.35 MiB | 53.7 / 223.0 | 1259.7 / 49426.7 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 13.3 | 30.3 / 863.3 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 983 | 55.35 MiB | 54.0 / 273.0 | 1310.0 / 49477.0 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 20.3 | 37.4 / 870.4 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 5 | 1038 | 56.36 MiB | 56.7 / 244.6 | 1339.2 / 52201.2 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 3.9 | 20.9 / 853.9 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11365.2 / 81420.3 | 143985.5 / 2652785.5 | 119 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 50 | 1.70 MiB | 2790.2 / 12406.5 | 15246.7 / 17696.7 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 10216.0 | 10233.0 / 11066.0 | 0 |
| restart | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 10872.7 / 13659.0 | 75731.6 / 2584531.6 | 208 |
| restart | C1 exact | real | FAILED | 50 | 12800 | 516.82 MiB | 0.0 / 6029.7 | 18829.7 / 646029.7 | 208 |
| restart | C1 exact | riblt | ok | 1 | 76 | 2.70 MiB | 2983.7 / 3118.7 | 6178.4 / 9902.4 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11072.1 / 109507.7 | 171779.7 / 2680579.7 | 208 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 76 | 2.70 MiB | 2991.6 / 3575.5 | 6643.0 / 10367.0 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 5 | 1041 | 56.99 MiB | 67.7 / 148.7 | 1257.4 / 52266.4 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 4 | 1024 | 57.69 MiB | 54.2 / 231.6 | 1309.8 / 51485.8 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 4 | 1024 | 57.69 MiB | 55.1 / 283.6 | 1362.7 / 51538.7 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 5 | 1039 | 56.35 MiB | 55.5 / 241.9 | 1336.4 / 52247.4 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11193.2 / 83246.9 | 145640.1 / 2654440.1 | 208 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 76 | 2.70 MiB | 2985.1 / 17768.2 | 20829.4 / 24553.4 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
## rep=4 n=1000000
# repair --n 1000000 --specs c8a,c5,c4,c3,c6b,c6a,c1 --scenarios gap1,gap1k,gap100k,restart,initial
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 649 B | 95.3 / 98.1 | 194.4 / 243.4 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 1 | 31.2 KiB | 0.1 / 0.2 | 1.2 / 50.2 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.5 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 2 | 6.3 KiB | 0.0 / 0.0 | 2.1 / 100.1 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 649 B | 95.1 / 107.6 | 203.7 / 252.7 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C1 exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.0 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | real | ok | 1 | 3 | 5.9 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 649 B | 95.2 / 94.4 | 190.6 / 239.6 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.4 / 18.4 | 33.9 / 670.9 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 27.2 KiB | 221.4 / 223.0 | 450.5 / 744.5 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 1 | 250 | 7.01 MiB | 10.0 / 28.0 | 288.0 / 12538.0 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 33 | 1.73 MiB | 2.0 / 8.1 | 43.1 / 1660.1 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 33 | 1.73 MiB | 1.8 / 6.6 | 41.4 / 1658.4 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 11 | 578.5 KiB | 1.0 / 1.7 | 13.7 / 552.7 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.3 / 16.8 | 32.1 / 669.1 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 27.2 KiB | 221.0 / 235.6 | 462.6 / 756.6 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 1.4 | 2.4 / 51.4 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.5 / 3.3 | 18.9 / 655.9 | 0 |
| gap1k | C1 exact | real | ok | 1 | 13 | 544.8 KiB | 0.0 / 6.0 | 19.0 / 656.0 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 27.2 KiB | 221.0 / 220.9 | 447.9 / 741.9 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.6 / 2718.3 | 2901.9 / 10937.9 | 0 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 49 | 1.66 MiB | 351.2 / 2579.9 | 2980.0 / 5381.0 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 3886.7 | 3902.7 / 4686.7 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.7 / 28.6 | 295.3 / 12839.3 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.3 | 19.3 / 803.3 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.98 MiB | 7.6 / 36.9 | 188.5 / 7244.5 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 7.8 | 23.9 / 807.9 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.98 MiB | 6.9 / 29.9 | 180.8 / 7236.8 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 3.9 | 19.9 / 803.9 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 1 | 147 | 8.05 MiB | 6.4 / 19.6 | 173.0 / 7376.0 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 6.4 | 22.4 / 806.4 | 0 |
| gap100k | C6a EF-coded exact | rbsr | ok | 1 | 164 | 8.20 MiB | 18.4 / 203.1 | 385.5 / 8421.5 | 0 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 49 | 1.66 MiB | 350.4 / 540.0 | 939.4 / 3340.4 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 130.9 | 146.9 / 930.9 | 0 |
| gap100k | C1 exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.1 / 43.4 | 226.5 / 8262.5 | 0 |
| gap100k | C1 exact | real | ok | 1 | 164 | 8.16 MiB | 0.0 / 64.4 | 228.4 / 8264.4 | 0 |
| gap100k | C1 exact | riblt | ok | 1 | 49 | 1.66 MiB | 350.4 / 399.2 | 798.6 / 3199.6 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 16 | 766.1 KiB | 0.0 / 4.2 | 20.3 / 804.3 | 0 |
| restart | C8a cuckoo over disk exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.7 / 612.7 | 706.4 / 4675.4 | 0 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.1 / 431.7 | 744.8 / 1479.8 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.7 / 27.8 | 294.5 / 12838.5 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.92 MiB | 7.4 / 36.2 | 187.6 / 7243.6 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.92 MiB | 8.0 / 30.0 | 182.0 / 7238.0 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 1 | 104 | 5.69 MiB | 5.7 / 14.1 | 123.8 / 5219.8 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | ok | 1 | 81 | 3.81 MiB | 11.7 / 108.3 | 201.0 / 4170.0 | 0 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.5 / 347.6 | 661.1 / 1396.1 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C1 exact | rbsr | ok | 1 | 81 | 3.81 MiB | 12.5 / 21.6 | 115.1 / 4084.1 | 0 |
| restart | C1 exact | real | ok | 1 | 81 | 3.80 MiB | 0.0 / 35.0 | 116.0 / 4085.0 | 0 |
| restart | C1 exact | riblt | ok | 1 | 15 | 367.1 KiB | 298.8 / 308.5 | 622.2 / 1357.2 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | rbsr | ok | 2 | 258 | 7.16 MiB | 11.3 / 15847.0 | 16116.4 / 28758.4 | 0 |
| initial | C8a cuckoo over disk exact | real | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | riblt | ok | 1 | 446 | 16.86 MiB | 644.2 / 19983.4 | 21073.7 / 42927.7 | 0 |
| initial | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| initial | C5 fuse16+overlay | rbsr | ok | 2 | 258 | 7.16 MiB | 10.7 / 27.7 | 296.4 / 12938.4 | 0 |
| initial | C5 fuse16+overlay | real | n/a | | | | | | |
| initial | C5 fuse16+overlay | riblt | n/a | | | | | | |
| initial | C5 fuse16+overlay | replay | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.5 / 32.0 | 300.6 / 12942.6 | 0 |
| initial | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| initial | C3 cuckoo f=16 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.4 / 25.3 | 293.7 / 12935.7 | 0 |
| initial | C3 cuckoo f=16 | real | n/a | | | | | | |
| initial | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| initial | C3 cuckoo f=16 | replay | n/a | | | | | | |
| initial | C6b suffix w=32 | rbsr | ok | 2 | 258 | 7.16 MiB | 10.1 / 13.5 | 281.7 / 12923.7 | 0 |
| initial | C6b suffix w=32 | real | n/a | | | | | | |
| initial | C6b suffix w=32 | riblt | n/a | | | | | | |
| initial | C6b suffix w=32 | replay | n/a | | | | | | |
| initial | C6a EF-coded exact | rbsr | ok | 2 | 258 | 7.16 MiB | 9.9 / 45.5 | 313.4 / 12955.4 | 0 |
| initial | C6a EF-coded exact | real | n/a | | | | | | |
| initial | C6a EF-coded exact | riblt | ok | 1 | 446 | 16.86 MiB | 644.1 / 1978.3 | 3068.4 / 24922.4 | 0 |
| initial | C6a EF-coded exact | replay | n/a | | | | | | |
| initial | C1 exact | rbsr | ok | 2 | 258 | 7.16 MiB | 10.8 / 27.8 | 296.5 / 12938.5 | 0 |
| initial | C1 exact | real | n/a | | | | | | |
| initial | C1 exact | riblt | ok | 1 | 446 | 16.86 MiB | 642.1 / 609.2 | 1697.3 / 23551.3 | 0 |
| initial | C1 exact | replay | n/a | | | | | | |
## rep=4 n=8000000
# repair --n 8000000 --specs c8a,c5,c4,c3,c6b,c6a,c1 --scenarios gap1,gap1k,gap100k,restart
| gap1 | C8a cuckoo over disk exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 657 B | 754.2 / 787.0 | 1542.2 / 1591.2 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C5 fuse16+overlay | rbsr | ok | 1 | 4 | 66.5 KiB | 0.2 / 0.4 | 4.6 / 200.6 | 0 |
| gap1 | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1 | C5 fuse16+overlay | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C4 quotient eps=2^-14 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1 | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C3 cuckoo f=16 | rbsr | ok | 1 | 2 | 7.7 KiB | 0.1 / 0.1 | 2.2 / 100.2 | 0 |
| gap1 | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1 | C3 cuckoo f=16 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6b suffix w=32 | rbsr | ok | 1 | 3 | 7.4 KiB | 0.1 / 0.1 | 3.2 / 150.2 | 0 |
| gap1 | C6b suffix w=32 | real | n/a | | | | | | |
| gap1 | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1 | C6b suffix w=32 | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 657 B | 753.6 / 862.4 | 1617.0 / 1666.0 | 0 |
| gap1 | C6a EF-coded exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C1 exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | real | ok | 1 | 4 | 6.4 KiB | 0.0 / 0.2 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 657 B | 754.0 / 772.6 | 1527.6 / 1576.6 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.7 / 38.0 | 63.7 / 1092.7 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 28.1 KiB | 1756.4 / 1794.1 | 3556.5 / 3850.5 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 2 | 400 | 21.71 MiB | 22.6 / 94.5 | 517.2 / 20117.2 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.2 / 9.5 | 46.7 / 1712.7 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.0 / 7.8 | 44.8 / 1710.8 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 26 | 1.25 MiB | 3.1 / 4.9 | 34.0 / 1308.0 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.7 / 30.1 | 55.8 / 1084.8 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 28.1 KiB | 1756.1 / 1871.0 | 3633.1 / 3927.1 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 2.9 | 3.9 / 52.9 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.6 / 5.8 | 31.4 / 1060.4 | 0 |
| gap1k | C1 exact | real | ok | 1 | 21 | 852.2 KiB | 0.0 / 10.6 | 31.6 / 1060.6 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 28.1 KiB | 1758.8 / 1775.9 | 3540.7 / 3834.7 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11383.6 / 81444.0 | 144027.7 / 2652827.7 | 119 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 50 | 1.70 MiB | 2783.0 / 12417.4 | 15250.3 / 17700.3 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 10212.7 | 10229.8 / 11062.8 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 5 | 1038 | 56.36 MiB | 58.2 / 244.4 | 1340.6 / 52202.6 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 3.9 | 21.0 / 854.0 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 983 | 55.35 MiB | 56.5 / 272.6 | 1312.1 / 49479.1 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 22.0 | 39.1 / 872.1 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 4 | 983 | 55.35 MiB | 53.1 / 221.0 | 1257.1 / 49424.1 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 13.4 | 30.4 / 863.4 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 4 | 896 | 49.06 MiB | 60.8 / 129.6 | 1086.4 / 44990.4 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 33.8 | 50.8 / 883.8 | 0 |
| gap100k | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11287.0 / 109826.6 | 172313.7 / 2681113.7 | 119 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 50 | 1.70 MiB | 2787.5 / 3212.1 | 6049.6 / 8499.6 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 258.7 | 275.7 / 1108.7 | 0 |
| gap100k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11080.2 / 13807.8 | 76087.9 / 2584887.9 | 119 |
| gap100k | C1 exact | real | FAILED | 50 | 12800 | 517.41 MiB | 0.0 / 6202.5 | 19002.5 / 646202.5 | 119 |
| gap100k | C1 exact | riblt | ok | 1 | 50 | 1.70 MiB | 2785.1 / 2875.6 | 5710.6 / 8160.6 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 17 | 768.5 KiB | 0.2 / 23.9 | 41.0 / 874.0 | 0 |
| restart | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11208.3 / 82793.6 | 145201.9 / 2654001.9 | 208 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 76 | 2.70 MiB | 2980.8 / 17804.2 | 20860.9 / 24584.9 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 5 | 1039 | 56.35 MiB | 58.0 / 244.1 | 1341.1 / 52252.1 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 4 | 1024 | 57.69 MiB | 57.4 / 287.2 | 1368.6 / 51544.6 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 4 | 1024 | 57.69 MiB | 56.5 / 233.2 | 1313.6 / 51489.6 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 5 | 1041 | 56.99 MiB | 69.8 / 150.3 | 1261.1 / 52270.1 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11126.7 / 109754.5 | 172081.1 / 2680881.1 | 208 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 76 | 2.70 MiB | 2985.8 / 3593.9 | 6655.6 / 10379.6 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 10963.2 / 13742.0 | 75905.2 / 2584705.2 | 208 |
| restart | C1 exact | real | FAILED | 50 | 12800 | 516.82 MiB | 0.0 / 6057.8 | 18857.8 / 646057.8 | 208 |
| restart | C1 exact | riblt | ok | 1 | 76 | 2.70 MiB | 2983.5 / 3123.3 | 6182.7 / 9906.7 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
