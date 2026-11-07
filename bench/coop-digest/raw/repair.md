| scenario | candidate | method | result | sessions | rounds | wire bytes | owner / receiver CPU ms | derived wall ms at 1 ms / 50 ms RTT (CPU + rounds x RTT) | false negatives |
## n=1000000
# repair --n 1000000
| gap1 | C1 exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.0 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | real | ok | 1 | 3 | 5.9 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 649 B | 96.0 / 96.5 | 193.4 / 242.4 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 3 | 6.5 KiB | 0.0 / 0.1 | 3.1 / 150.1 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 649 B | 95.5 / 106.9 | 203.4 / 252.4 | 0 |
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
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 649 B | 96.2 / 98.2 | 195.4 / 244.4 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.6 / 3.5 | 19.2 / 656.2 | 0 |
| gap1k | C1 exact | real | ok | 1 | 13 | 544.8 KiB | 0.0 / 6.7 | 19.7 / 656.7 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 27.2 KiB | 222.3 / 223.0 | 451.4 / 745.4 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.8 / 17.0 | 32.7 / 669.7 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 27.2 KiB | 221.7 / 236.5 | 464.1 / 758.1 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 1.5 | 2.5 / 51.5 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 11 | 578.5 KiB | 1.3 / 1.7 | 14.0 / 553.0 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 33 | 1.73 MiB | 2.2 / 6.6 | 41.9 / 1658.9 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 33 | 1.73 MiB | 2.3 / 8.2 | 43.5 / 1660.5 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.2 | 1.2 / 50.2 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 1 | 250 | 7.01 MiB | 10.6 / 27.2 | 287.8 / 12537.8 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 13 | 547.4 KiB | 2.8 / 19.0 | 34.7 / 671.7 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 27.2 KiB | 222.7 / 225.7 | 454.4 / 748.4 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap100k | C1 exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.7 / 44.9 | 228.6 / 8264.6 | 0 |
| gap100k | C1 exact | real | ok | 1 | 164 | 8.16 MiB | 0.0 / 65.6 | 229.6 / 8265.6 | 0 |
| gap100k | C1 exact | riblt | ok | 1 | 49 | 1.66 MiB | 352.8 / 408.9 | 810.7 / 3211.7 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 6.1 | 22.2 / 806.2 | 0 |
| gap100k | C6a EF-coded exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.9 / 205.4 | 389.3 / 8425.3 | 0 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 49 | 1.66 MiB | 352.0 / 553.8 | 954.9 / 3355.9 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 137.3 | 153.4 / 937.4 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 1 | 147 | 8.05 MiB | 7.8 / 20.4 | 175.2 / 7378.2 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 7.2 | 23.3 / 807.3 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.98 MiB | 8.3 / 31.2 | 183.5 / 7239.5 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 4.8 | 20.8 / 804.8 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.98 MiB | 8.3 / 38.0 | 190.2 / 7246.2 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 9.9 | 26.0 / 810.0 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 11.0 / 28.3 | 295.3 / 12839.3 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 3.4 | 19.5 / 803.5 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | ok | 1 | 164 | 8.20 MiB | 21.2 / 1979.7 | 2164.8 / 10200.8 | 0 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 49 | 1.66 MiB | 354.0 / 1012.6 | 1415.5 / 3816.5 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 16 | 766.1 KiB | 0.1 / 1906.9 | 1923.0 / 2707.0 | 0 |
| burst100k | C1 exact | rbsr | ok | 1 | 164 | 8.20 MiB | 19.6 / 44.7 | 228.3 / 8264.3 | 0 |
| burst100k | C1 exact | real | ok | 1 | 164 | 8.16 MiB | 0.0 / 65.3 | 229.3 / 8265.3 | 0 |
| burst100k | C1 exact | riblt | ok | 1 | 49 | 1.66 MiB | 351.8 / 407.4 | 808.1 / 3209.1 | 0 |
| burst100k | C1 exact | replay | n/a | | | | | | |
| burst100k | C6a EF-coded exact | rbsr | ok | 1 | 164 | 8.20 MiB | 20.0 / 205.1 | 389.1 / 8425.1 | 0 |
| burst100k | C6a EF-coded exact | real | n/a | | | | | | |
| burst100k | C6a EF-coded exact | riblt | ok | 1 | 49 | 1.66 MiB | 351.8 / 555.3 | 956.1 / 3357.1 | 0 |
| burst100k | C6a EF-coded exact | replay | n/a | | | | | | |
| burst100k | C6b suffix w=32 | rbsr | ok | 1 | 147 | 8.05 MiB | 7.8 / 20.4 | 175.3 / 7378.3 | 0 |
| burst100k | C6b suffix w=32 | real | n/a | | | | | | |
| burst100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| burst100k | C6b suffix w=32 | replay | n/a | | | | | | |
| burst100k | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.98 MiB | 8.4 / 31.2 | 183.6 / 7239.6 | 0 |
| burst100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| burst100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| burst100k | C3 cuckoo f=16 | replay | n/a | | | | | | |
| burst100k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.98 MiB | 8.3 / 37.7 | 190.0 / 7246.0 | 0 |
| burst100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| burst100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| burst100k | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| burst100k | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.9 / 28.2 | 295.1 / 12839.1 | 0 |
| burst100k | C5 fuse16+overlay | real | n/a | | | | | | |
| burst100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| burst100k | C5 fuse16+overlay | replay | n/a | | | | | | |
| burst100k | C8a cuckoo over disk exact | rbsr | ok | 1 | 164 | 8.20 MiB | 21.2 / 2084.2 | 2269.4 / 10305.4 | 0 |
| burst100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| burst100k | C8a cuckoo over disk exact | riblt | ok | 1 | 49 | 1.66 MiB | 351.0 / 1055.7 | 1455.7 / 3856.7 | 0 |
| burst100k | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| restart | C1 exact | rbsr | ok | 1 | 81 | 3.81 MiB | 13.4 / 23.9 | 118.3 / 4087.3 | 0 |
| restart | C1 exact | real | ok | 1 | 81 | 3.80 MiB | 0.0 / 37.8 | 118.8 / 4087.8 | 0 |
| restart | C1 exact | riblt | ok | 1 | 15 | 367.1 KiB | 299.6 / 311.2 | 625.8 / 1360.8 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | ok | 1 | 81 | 3.81 MiB | 13.6 / 110.8 | 205.4 / 4174.4 | 0 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 15 | 367.1 KiB | 300.3 / 352.0 | 667.3 / 1402.3 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 1 | 104 | 5.69 MiB | 6.7 / 14.9 | 125.6 / 5221.6 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 1 | 144 | 7.92 MiB | 7.9 / 30.5 | 182.4 / 7238.4 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 1 | 144 | 7.92 MiB | 7.9 / 37.3 | 189.2 / 7245.2 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 1 | 256 | 7.18 MiB | 10.6 / 28.0 | 294.6 / 12838.6 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | rbsr | ok | 1 | 81 | 3.81 MiB | 13.8 / 1187.7 | 1282.4 / 5251.4 | 0 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 15 | 367.1 KiB | 299.6 / 977.5 | 1292.0 / 2027.0 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| initial | C1 exact | rbsr | ok | 2 | 258 | 7.16 MiB | 11.1 / 29.3 | 298.4 / 12940.4 | 0 |
| initial | C1 exact | real | n/a | | | | | | |
| initial | C1 exact | riblt | ok | 1 | 446 | 16.86 MiB | 650.1 / 660.9 | 1757.0 / 23611.0 | 0 |
| initial | C1 exact | replay | n/a | | | | | | |
| initial | C6a EF-coded exact | rbsr | ok | 2 | 258 | 7.16 MiB | 11.1 / 46.9 | 316.0 / 12958.0 | 0 |
| initial | C6a EF-coded exact | real | n/a | | | | | | |
| initial | C6a EF-coded exact | riblt | ok | 1 | 446 | 16.86 MiB | 650.2 / 2162.7 | 3258.9 / 25112.9 | 0 |
| initial | C6a EF-coded exact | replay | n/a | | | | | | |
| initial | C6b suffix w=32 | rbsr | ok | 2 | 258 | 7.16 MiB | 11.3 / 14.5 | 283.7 / 12925.7 | 0 |
| initial | C6b suffix w=32 | real | n/a | | | | | | |
| initial | C6b suffix w=32 | riblt | n/a | | | | | | |
| initial | C6b suffix w=32 | replay | n/a | | | | | | |
| initial | C3 cuckoo f=16 | rbsr | ok | 2 | 258 | 7.16 MiB | 11.5 / 26.5 | 296.0 / 12938.0 | 0 |
| initial | C3 cuckoo f=16 | real | n/a | | | | | | |
| initial | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| initial | C3 cuckoo f=16 | replay | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | rbsr | ok | 2 | 258 | 7.16 MiB | 11.3 / 33.7 | 303.0 / 12945.0 | 0 |
| initial | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| initial | C5 fuse16+overlay | rbsr | ok | 2 | 258 | 7.16 MiB | 11.0 / 29.0 | 297.9 / 12939.9 | 0 |
| initial | C5 fuse16+overlay | real | n/a | | | | | | |
| initial | C5 fuse16+overlay | riblt | n/a | | | | | | |
| initial | C5 fuse16+overlay | replay | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | rbsr | ok | 2 | 258 | 7.16 MiB | 16.2 / 7836.4 | 8110.6 / 20752.6 | 0 |
| initial | C8a cuckoo over disk exact | real | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | riblt | ok | 1 | 446 | 16.86 MiB | 653.3 / 10416.6 | 11515.8 / 33369.8 | 0 |
| initial | C8a cuckoo over disk exact | replay | n/a | | | | | | |
## n=8000000
# repair --n 8000000
| gap1 | C1 exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.3 / 200.3 | 0 |
| gap1 | C1 exact | real | ok | 1 | 4 | 6.4 KiB | 0.0 / 0.2 | 4.2 / 200.2 | 0 |
| gap1 | C1 exact | riblt | ok | 1 | 1 | 657 B | 894.9 / 814.8 | 1710.8 / 1759.8 | 0 |
| gap1 | C1 exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1 | C6a EF-coded exact | rbsr | ok | 1 | 4 | 7.2 KiB | 0.1 / 0.1 | 4.2 / 200.2 | 0 |
| gap1 | C6a EF-coded exact | real | n/a | | | | | | |
| gap1 | C6a EF-coded exact | riblt | ok | 1 | 1 | 657 B | 764.4 / 883.5 | 1649.0 / 1698.0 | 0 |
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
| gap1 | C8a cuckoo over disk exact | riblt | ok | 1 | 1 | 657 B | 769.8 / 807.7 | 1578.5 / 1627.5 | 0 |
| gap1 | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 249 B | 0.0 / 0.0 | 1.0 / 50.0 | 0 |
| gap1k | C1 exact | rbsr | ok | 1 | 21 | 856.4 KiB | 4.9 / 6.1 | 32.0 / 1061.0 | 0 |
| gap1k | C1 exact | real | ok | 1 | 21 | 852.2 KiB | 0.0 / 11.1 | 32.1 / 1061.1 | 0 |
| gap1k | C1 exact | riblt | ok | 1 | 6 | 28.1 KiB | 1783.9 / 1804.9 | 3594.8 / 3888.8 | 0 |
| gap1k | C1 exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C6a EF-coded exact | rbsr | ok | 1 | 21 | 856.4 KiB | 5.2 / 31.2 | 57.4 / 1086.4 | 0 |
| gap1k | C6a EF-coded exact | real | n/a | | | | | | |
| gap1k | C6a EF-coded exact | riblt | ok | 1 | 6 | 28.1 KiB | 1776.9 / 1894.8 | 3677.7 / 3971.7 | 0 |
| gap1k | C6a EF-coded exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 2.9 | 3.9 / 52.9 | 0 |
| gap1k | C6b suffix w=32 | rbsr | ok | 1 | 26 | 1.25 MiB | 3.2 / 5.2 | 34.4 / 1308.4 | 0 |
| gap1k | C6b suffix w=32 | real | n/a | | | | | | |
| gap1k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap1k | C6b suffix w=32 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.7 | 1.7 / 50.7 | 0 |
| gap1k | C3 cuckoo f=16 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.2 / 7.9 | 45.1 / 1711.1 | 0 |
| gap1k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap1k | C3 cuckoo f=16 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.3 | 1.3 / 50.3 | 0 |
| gap1k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 34 | 1.83 MiB | 3.3 / 9.6 | 46.9 / 1712.9 | 0 |
| gap1k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap1k | C4 quotient eps=2^-14 | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.4 | 1.4 / 50.4 | 0 |
| gap1k | C5 fuse16+overlay | rbsr | ok | 2 | 400 | 21.71 MiB | 23.3 / 95.0 | 518.4 / 20118.4 | 0 |
| gap1k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap1k | C5 fuse16+overlay | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.1 | 1.1 / 50.1 | 0 |
| gap1k | C8a cuckoo over disk exact | rbsr | ok | 1 | 21 | 856.4 KiB | 5.1 / 40.2 | 66.3 / 1095.3 | 0 |
| gap1k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap1k | C8a cuckoo over disk exact | riblt | ok | 1 | 6 | 28.1 KiB | 1770.5 / 1805.5 | 3582.0 / 3876.0 | 0 |
| gap1k | C8a cuckoo over disk exact | replay | ok | 1 | 1 | 8.2 KiB | 0.0 / 0.5 | 1.5 / 50.5 | 0 |
| gap100k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11407.8 / 14113.3 | 76721.2 / 2585521.2 | 119 |
| gap100k | C1 exact | real | FAILED | 50 | 12800 | 517.41 MiB | 0.0 / 6392.3 | 19192.3 / 646392.3 | 119 |
| gap100k | C1 exact | riblt | ok | 1 | 50 | 1.70 MiB | 2945.7 / 3011.7 | 6007.4 / 8457.4 | 0 |
| gap100k | C1 exact | replay | ok | 1 | 17 | 768.5 KiB | 0.2 / 27.8 | 44.9 / 877.9 | 0 |
| gap100k | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 12410.8 / 114721.8 | 178332.6 / 2687132.6 | 119 |
| gap100k | C6a EF-coded exact | real | n/a | | | | | | |
| gap100k | C6a EF-coded exact | riblt | ok | 1 | 50 | 1.70 MiB | 3219.2 / 3440.2 | 6709.4 / 9159.4 | 0 |
| gap100k | C6a EF-coded exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 295.7 | 312.8 / 1145.8 | 0 |
| gap100k | C6b suffix w=32 | rbsr | ok | 4 | 896 | 49.06 MiB | 63.4 / 146.5 | 1105.9 / 45009.9 | 0 |
| gap100k | C6b suffix w=32 | real | n/a | | | | | | |
| gap100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap100k | C6b suffix w=32 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 46.5 | 63.6 / 896.6 | 0 |
| gap100k | C3 cuckoo f=16 | rbsr | ok | 4 | 983 | 55.35 MiB | 66.6 / 271.0 | 1320.6 / 49487.6 | 0 |
| gap100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap100k | C3 cuckoo f=16 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 21.0 | 38.1 / 871.1 | 0 |
| gap100k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 983 | 55.35 MiB | 59.5 / 295.5 | 1338.0 / 49505.0 | 0 |
| gap100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap100k | C4 quotient eps=2^-14 | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 25.5 | 42.6 / 875.6 | 0 |
| gap100k | C5 fuse16+overlay | rbsr | ok | 5 | 1038 | 56.36 MiB | 69.1 / 258.7 | 1365.9 / 52227.9 | 0 |
| gap100k | C5 fuse16+overlay | real | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| gap100k | C5 fuse16+overlay | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 4.3 | 21.4 / 854.4 | 0 |
| gap100k | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11969.3 / 84219.9 | 147389.3 / 2656189.3 | 119 |
| gap100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| gap100k | C8a cuckoo over disk exact | riblt | ok | 1 | 50 | 1.70 MiB | 2809.3 / 7288.3 | 10147.6 / 12597.6 | 0 |
| gap100k | C8a cuckoo over disk exact | replay | ok | 1 | 17 | 768.5 KiB | 0.1 / 5021.3 | 5038.3 / 5871.3 | 0 |
| burst100k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 13828.3 / 17055.5 | 82083.8 / 2590883.8 | 149 |
| burst100k | C1 exact | real | FAILED | 50 | 12800 | 517.41 MiB | 0.0 / 7485.1 | 20285.1 / 647485.1 | 149 |
| burst100k | C1 exact | riblt | ok | 1 | 50 | 1.70 MiB | 3108.0 / 3195.8 | 6353.8 / 8803.8 | 0 |
| burst100k | C1 exact | replay | n/a | | | | | | |
| burst100k | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 12603.5 / 114582.5 | 178386.0 / 2687186.0 | 149 |
| burst100k | C6a EF-coded exact | real | n/a | | | | | | |
| burst100k | C6a EF-coded exact | riblt | ok | 1 | 50 | 1.70 MiB | 2964.9 / 3447.0 | 6461.9 / 8911.9 | 0 |
| burst100k | C6a EF-coded exact | replay | n/a | | | | | | |
| burst100k | C6b suffix w=32 | rbsr | ok | 4 | 896 | 49.06 MiB | 65.7 / 147.6 | 1109.3 / 45013.3 | 0 |
| burst100k | C6b suffix w=32 | real | n/a | | | | | | |
| burst100k | C6b suffix w=32 | riblt | n/a | | | | | | |
| burst100k | C6b suffix w=32 | replay | n/a | | | | | | |
| burst100k | C3 cuckoo f=16 | rbsr | ok | 4 | 983 | 55.36 MiB | 58.5 / 237.6 | 1279.1 / 49446.1 | 0 |
| burst100k | C3 cuckoo f=16 | real | n/a | | | | | | |
| burst100k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| burst100k | C3 cuckoo f=16 | replay | n/a | | | | | | |
| burst100k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 983 | 55.36 MiB | 57.2 / 296.9 | 1337.1 / 49504.1 | 0 |
| burst100k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| burst100k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| burst100k | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| burst100k | C5 fuse16+overlay | rbsr | ok | 5 | 1038 | 56.36 MiB | 64.5 / 268.3 | 1370.8 / 52232.8 | 0 |
| burst100k | C5 fuse16+overlay | real | n/a | | | | | | |
| burst100k | C5 fuse16+overlay | riblt | n/a | | | | | | |
| burst100k | C5 fuse16+overlay | replay | n/a | | | | | | |
| burst100k | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11607.2 / 81763.0 | 144570.2 / 2653370.2 | 149 |
| burst100k | C8a cuckoo over disk exact | real | n/a | | | | | | |
| burst100k | C8a cuckoo over disk exact | riblt | ok | 1 | 50 | 1.70 MiB | 2787.6 / 7253.6 | 10091.2 / 12541.2 | 0 |
| burst100k | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| restart | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11212.3 / 14058.6 | 76471.0 / 2585271.0 | 208 |
| restart | C1 exact | real | FAILED | 50 | 12800 | 516.82 MiB | 0.0 / 6196.6 | 18996.6 / 646196.6 | 208 |
| restart | C1 exact | riblt | ok | 1 | 76 | 2.70 MiB | 3025.9 / 3174.3 | 6276.2 / 10000.2 | 0 |
| restart | C1 exact | replay | n/a | | | | | | |
| restart | C6a EF-coded exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11435.5 / 110925.8 | 173561.3 / 2682361.3 | 208 |
| restart | C6a EF-coded exact | real | n/a | | | | | | |
| restart | C6a EF-coded exact | riblt | ok | 1 | 76 | 2.70 MiB | 2985.0 / 3594.9 | 6655.8 / 10379.8 | 0 |
| restart | C6a EF-coded exact | replay | n/a | | | | | | |
| restart | C6b suffix w=32 | rbsr | ok | 5 | 1041 | 56.99 MiB | 68.7 / 149.3 | 1259.0 / 52268.0 | 0 |
| restart | C6b suffix w=32 | real | n/a | | | | | | |
| restart | C6b suffix w=32 | riblt | n/a | | | | | | |
| restart | C6b suffix w=32 | replay | n/a | | | | | | |
| restart | C3 cuckoo f=16 | rbsr | ok | 4 | 1024 | 57.69 MiB | 56.4 / 232.0 | 1312.5 / 51488.5 | 0 |
| restart | C3 cuckoo f=16 | real | n/a | | | | | | |
| restart | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| restart | C3 cuckoo f=16 | replay | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | rbsr | ok | 4 | 1024 | 57.69 MiB | 56.6 / 285.1 | 1365.7 / 51541.7 | 0 |
| restart | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| restart | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| restart | C5 fuse16+overlay | rbsr | ok | 5 | 1039 | 56.35 MiB | 55.7 / 240.4 | 1335.1 / 52246.1 | 0 |
| restart | C5 fuse16+overlay | real | n/a | | | | | | |
| restart | C5 fuse16+overlay | riblt | n/a | | | | | | |
| restart | C5 fuse16+overlay | replay | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11359.9 / 82796.6 | 145356.6 / 2654156.6 | 208 |
| restart | C8a cuckoo over disk exact | real | n/a | | | | | | |
| restart | C8a cuckoo over disk exact | riblt | ok | 1 | 76 | 2.70 MiB | 3028.3 / 12538.6 | 15642.9 / 19366.9 | 0 |
| restart | C8a cuckoo over disk exact | replay | n/a | | | | | | |
| initial | C1 exact | rbsr | ok | 5 | 1037 | 56.68 MiB | 54.2 / 269.7 | 1360.8 / 52173.8 | 0 |
| initial | C1 exact | real | n/a | | | | | | |
| initial | C1 exact | riblt | n/a | | | | | | |
| initial | C1 exact | replay | n/a | | | | | | |
| initial | C6a EF-coded exact | rbsr | ok | 5 | 1037 | 56.68 MiB | 55.8 / 345.7 | 1438.5 / 52251.5 | 0 |
| initial | C6a EF-coded exact | real | n/a | | | | | | |
| initial | C6a EF-coded exact | riblt | n/a | | | | | | |
| initial | C6a EF-coded exact | replay | n/a | | | | | | |
| initial | C6b suffix w=32 | rbsr | ok | 5 | 1037 | 56.68 MiB | 53.1 / 124.4 | 1214.6 / 52027.6 | 0 |
| initial | C6b suffix w=32 | real | n/a | | | | | | |
| initial | C6b suffix w=32 | riblt | n/a | | | | | | |
| initial | C6b suffix w=32 | replay | n/a | | | | | | |
| initial | C3 cuckoo f=16 | rbsr | ok | 5 | 1034 | 57.63 MiB | 54.4 / 232.6 | 1321.0 / 51987.0 | 0 |
| initial | C3 cuckoo f=16 | real | n/a | | | | | | |
| initial | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| initial | C3 cuckoo f=16 | replay | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | rbsr | ok | 5 | 1034 | 57.63 MiB | 54.8 / 286.3 | 1375.2 / 52041.2 | 0 |
| initial | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| initial | C4 quotient eps=2^-14 | replay | n/a | | | | | | |
| initial | C5 fuse16+overlay | rbsr | ok | 5 | 1043 | 55.94 MiB | 55.7 / 244.3 | 1343.0 / 52450.0 | 0 |
| initial | C5 fuse16+overlay | real | n/a | | | | | | |
| initial | C5 fuse16+overlay | riblt | n/a | | | | | | |
| initial | C5 fuse16+overlay | replay | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | rbsr | ok | 5 | 1037 | 56.68 MiB | 66.1 / 420779.0 | 421882.2 / 472695.2 | 0 |
| initial | C8a cuckoo over disk exact | real | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | riblt | n/a | | | | | | |
| initial | C8a cuckoo over disk exact | replay | n/a | | | | | | |
## n=4000000 (gap sweep)
# repair --n 4000000 --specs c1,c6b,c3,c4 --scenarios gap10k,gap20k,gap30k,gap50k,gap75k --methods real,rbsr,riblt,replay
| gap10k | C1 exact | real | ok | 1 | 136 | 5.21 MiB | 0.0 / 68.6 | 204.6 / 6868.6 | 0 |
| gap10k | C1 exact | rbsr | ok | 1 | 136 | 5.24 MiB | 29.2 / 38.5 | 203.7 / 6867.7 | 0 |
| gap10k | C1 exact | riblt | ok | 1 | 11 | 211.8 KiB | 1159.0 / 1164.1 | 2334.2 / 2873.2 | 0 |
| gap10k | C1 exact | replay | ok | 1 | 2 | 77.4 KiB | 0.0 / 1.9 | 4.0 / 102.0 | 0 |
| gap10k | C6b suffix w=32 | real | n/a | | | | | | |
| gap10k | C6b suffix w=32 | rbsr | ok | 1 | 113 | 5.73 MiB | 13.8 / 22.8 | 149.6 / 5686.6 | 0 |
| gap10k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap10k | C6b suffix w=32 | replay | ok | 1 | 2 | 77.4 KiB | 0.0 / 3.8 | 5.8 / 103.8 | 0 |
| gap10k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap10k | C3 cuckoo f=16 | rbsr | ok | 1 | 248 | 13.88 MiB | 18.5 / 59.6 | 326.1 / 12478.1 | 0 |
| gap10k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap10k | C3 cuckoo f=16 | replay | ok | 1 | 2 | 77.4 KiB | 0.0 / 2.2 | 4.2 / 102.2 | 0 |
| gap10k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap10k | C4 quotient eps=2^-14 | rbsr | ok | 1 | 248 | 13.88 MiB | 18.4 / 71.8 | 338.2 / 12490.2 | 0 |
| gap10k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap10k | C4 quotient eps=2^-14 | replay | ok | 1 | 2 | 77.4 KiB | 0.0 / 2.8 | 4.9 / 102.9 | 0 |
| gap20k | C1 exact | real | ok | 1 | 228 | 8.65 MiB | 0.0 / 110.1 | 338.1 / 11510.1 | 0 |
| gap20k | C1 exact | rbsr | ok | 1 | 228 | 8.70 MiB | 47.4 / 62.3 | 337.7 / 11509.7 | 0 |
| gap20k | C1 exact | riblt | ok | 1 | 15 | 368.6 KiB | 1226.4 / 1251.0 | 2492.5 / 3227.5 | 0 |
| gap20k | C1 exact | replay | ok | 1 | 4 | 154.8 KiB | 0.0 / 3.9 | 7.9 / 203.9 | 0 |
| gap20k | C6b suffix w=32 | real | n/a | | | | | | |
| gap20k | C6b suffix w=32 | rbsr | ok | 1 | 183 | 9.57 MiB | 19.6 / 36.8 | 239.4 / 9206.4 | 0 |
| gap20k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap20k | C6b suffix w=32 | replay | ok | 1 | 4 | 154.8 KiB | 0.0 / 7.6 | 11.6 / 207.6 | 0 |
| gap20k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap20k | C3 cuckoo f=16 | rbsr | ok | 2 | 381 | 21.32 MiB | 27.0 / 92.5 | 500.5 / 19169.5 | 0 |
| gap20k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap20k | C3 cuckoo f=16 | replay | ok | 1 | 4 | 154.8 KiB | 0.0 / 3.7 | 7.7 / 203.7 | 0 |
| gap20k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap20k | C4 quotient eps=2^-14 | rbsr | ok | 2 | 381 | 21.32 MiB | 27.4 / 113.4 | 521.8 / 19190.8 | 0 |
| gap20k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap20k | C4 quotient eps=2^-14 | replay | ok | 1 | 4 | 154.8 KiB | 0.0 / 4.9 | 8.9 / 204.9 | 0 |
| gap30k | C1 exact | real | ok | 3 | 547 | 21.26 MiB | 0.0 / 257.3 | 804.3 / 27607.3 | 0 |
| gap30k | C1 exact | rbsr | ok | 3 | 547 | 21.37 MiB | 113.3 / 147.3 | 807.6 / 27610.6 | 0 |
| gap30k | C1 exact | riblt | ok | 1 | 19 | 525.4 KiB | 1265.1 / 1293.8 | 2577.9 / 3508.9 | 0 |
| gap30k | C1 exact | replay | ok | 1 | 5 | 232.2 KiB | 0.0 / 5.5 | 10.5 / 255.5 | 0 |
| gap30k | C6b suffix w=32 | real | n/a | | | | | | |
| gap30k | C6b suffix w=32 | rbsr | ok | 1 | 239 | 12.70 MiB | 22.7 / 42.2 | 303.9 / 12014.9 | 0 |
| gap30k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap30k | C6b suffix w=32 | replay | ok | 1 | 5 | 232.2 KiB | 0.0 / 10.8 | 15.8 / 260.8 | 0 |
| gap30k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap30k | C3 cuckoo f=16 | rbsr | ok | 2 | 450 | 25.18 MiB | 30.5 / 108.0 | 588.5 / 22638.5 | 0 |
| gap30k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap30k | C3 cuckoo f=16 | replay | ok | 1 | 5 | 232.2 KiB | 0.0 / 6.0 | 11.0 / 256.0 | 0 |
| gap30k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap30k | C4 quotient eps=2^-14 | rbsr | ok | 2 | 450 | 25.18 MiB | 30.9 / 134.1 | 615.1 / 22665.1 | 0 |
| gap30k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap30k | C4 quotient eps=2^-14 | replay | ok | 1 | 5 | 232.2 KiB | 0.0 / 7.5 | 12.5 / 257.5 | 0 |
| gap50k | C1 exact | real | FAILED | 50 | 12800 | 515.86 MiB | 0.0 / 5944.2 | 18744.2 / 645944.2 | 126 |
| gap50k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.02 GiB | 10527.3 / 12977.9 | 74705.2 / 2583505.2 | 126 |
| gap50k | C1 exact | riblt | ok | 1 | 28 | 878.3 KiB | 1293.5 / 1333.8 | 2655.2 / 4027.2 | 0 |
| gap50k | C1 exact | replay | ok | 1 | 9 | 385.8 KiB | 0.0 / 8.1 | 17.2 / 458.2 | 0 |
| gap50k | C6b suffix w=32 | real | n/a | | | | | | |
| gap50k | C6b suffix w=32 | rbsr | ok | 2 | 339 | 18.15 MiB | 29.8 / 57.4 | 426.2 / 17037.2 | 0 |
| gap50k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap50k | C6b suffix w=32 | replay | ok | 1 | 9 | 385.8 KiB | 0.0 / 10.7 | 19.7 / 460.7 | 0 |
| gap50k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap50k | C3 cuckoo f=16 | rbsr | ok | 2 | 511 | 28.61 MiB | 32.9 / 118.8 | 662.7 / 25701.7 | 0 |
| gap50k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap50k | C3 cuckoo f=16 | replay | ok | 1 | 9 | 385.8 KiB | 0.0 / 6.6 | 15.6 / 456.6 | 0 |
| gap50k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap50k | C4 quotient eps=2^-14 | rbsr | ok | 2 | 511 | 28.61 MiB | 33.2 / 145.5 | 689.7 / 25728.7 | 0 |
| gap50k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap50k | C4 quotient eps=2^-14 | replay | ok | 1 | 9 | 385.8 KiB | 0.0 / 9.0 | 18.0 / 459.0 | 0 |
| gap75k | C1 exact | real | FAILED | 50 | 12800 | 515.22 MiB | 0.0 / 5686.9 | 18486.9 / 645686.9 | 185 |
| gap75k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.02 GiB | 10126.8 / 12661.6 | 73988.4 / 2582788.4 | 185 |
| gap75k | C1 exact | riblt | ok | 1 | 39 | 1.28 MiB | 1352.6 / 1405.6 | 2797.3 / 4708.3 | 0 |
| gap75k | C1 exact | replay | ok | 1 | 13 | 578.6 KiB | 0.1 / 10.4 | 23.4 / 660.4 | 0 |
| gap75k | C6b suffix w=32 | real | n/a | | | | | | |
| gap75k | C6b suffix w=32 | rbsr | ok | 2 | 425 | 22.84 MiB | 35.1 / 69.7 | 529.8 / 21354.8 | 0 |
| gap75k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap75k | C6b suffix w=32 | replay | ok | 1 | 13 | 578.6 KiB | 0.0 / 11.5 | 24.6 / 661.6 | 0 |
| gap75k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap75k | C3 cuckoo f=16 | rbsr | ok | 3 | 531 | 29.72 MiB | 34.2 / 124.2 | 689.4 / 26708.4 | 0 |
| gap75k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap75k | C3 cuckoo f=16 | replay | ok | 1 | 13 | 578.6 KiB | 0.0 / 6.7 | 19.7 / 656.7 | 0 |
| gap75k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap75k | C4 quotient eps=2^-14 | rbsr | ok | 3 | 531 | 29.72 MiB | 34.6 / 151.3 | 717.0 / 26736.0 | 0 |
| gap75k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap75k | C4 quotient eps=2^-14 | replay | ok | 1 | 13 | 578.6 KiB | 0.0 / 11.1 | 24.1 / 661.1 | 0 |
## n=8000000 (gap sweep)
# repair --n 8000000 --specs c1,c6b,c3,c4 --scenarios gap10k,gap20k,gap30k,gap50k,gap75k --methods real,rbsr,riblt,replay
| gap10k | C1 exact | real | ok | 1 | 136 | 5.45 MiB | 0.0 / 72.9 | 208.9 / 6872.9 | 0 |
| gap10k | C1 exact | rbsr | ok | 1 | 136 | 5.48 MiB | 30.1 / 39.8 | 206.0 / 6870.0 | 0 |
| gap10k | C1 exact | riblt | ok | 1 | 11 | 213.4 KiB | 2265.3 / 2293.6 | 4569.9 / 5108.9 | 0 |
| gap10k | C1 exact | replay | ok | 1 | 2 | 77.5 KiB | 0.0 / 2.7 | 4.7 / 102.7 | 0 |
| gap10k | C6b suffix w=32 | real | n/a | | | | | | |
| gap10k | C6b suffix w=32 | rbsr | ok | 1 | 182 | 9.64 MiB | 16.5 / 29.9 | 228.4 / 9146.4 | 0 |
| gap10k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap10k | C6b suffix w=32 | replay | ok | 1 | 2 | 77.5 KiB | 0.0 / 4.1 | 6.1 / 104.1 | 0 |
| gap10k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap10k | C3 cuckoo f=16 | rbsr | ok | 2 | 275 | 15.41 MiB | 18.8 / 63.8 | 357.7 / 13832.7 | 0 |
| gap10k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap10k | C3 cuckoo f=16 | replay | ok | 1 | 2 | 77.5 KiB | 0.0 / 1.9 | 3.9 / 101.9 | 0 |
| gap10k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap10k | C4 quotient eps=2^-14 | rbsr | ok | 2 | 275 | 15.41 MiB | 18.9 / 77.6 | 371.6 / 13846.6 | 0 |
| gap10k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap10k | C4 quotient eps=2^-14 | replay | ok | 1 | 2 | 77.5 KiB | 0.0 / 2.6 | 4.6 / 102.6 | 0 |
| gap20k | C1 exact | real | ok | 1 | 229 | 9.13 MiB | 0.0 / 123.2 | 352.2 / 11573.2 | 0 |
| gap20k | C1 exact | rbsr | ok | 1 | 229 | 9.18 MiB | 51.8 / 70.0 | 350.8 / 11571.8 | 0 |
| gap20k | C1 exact | riblt | ok | 1 | 15 | 370.4 KiB | 2378.7 / 2412.2 | 4805.9 / 5540.9 | 0 |
| gap20k | C1 exact | replay | ok | 1 | 4 | 154.9 KiB | 0.0 / 5.1 | 9.1 / 205.1 | 0 |
| gap20k | C6b suffix w=32 | real | n/a | | | | | | |
| gap20k | C6b suffix w=32 | rbsr | ok | 2 | 319 | 17.20 MiB | 25.9 / 49.2 | 394.2 / 16025.2 | 0 |
| gap20k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap20k | C6b suffix w=32 | replay | ok | 1 | 4 | 154.9 KiB | 0.0 / 6.8 | 10.8 / 206.8 | 0 |
| gap20k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap20k | C3 cuckoo f=16 | rbsr | ok | 2 | 474 | 26.70 MiB | 30.2 / 109.6 | 613.8 / 23839.8 | 0 |
| gap20k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap20k | C3 cuckoo f=16 | replay | ok | 1 | 4 | 154.9 KiB | 0.0 / 3.1 | 7.1 / 203.1 | 0 |
| gap20k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap20k | C4 quotient eps=2^-14 | rbsr | ok | 2 | 474 | 26.70 MiB | 30.0 / 132.9 | 636.9 / 23862.9 | 0 |
| gap20k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap20k | C4 quotient eps=2^-14 | replay | ok | 1 | 4 | 154.9 KiB | 0.0 / 4.7 | 8.7 / 204.7 | 0 |
| gap30k | C1 exact | real | ok | 3 | 611 | 24.57 MiB | 0.0 / 318.3 | 929.3 / 30868.3 | 0 |
| gap30k | C1 exact | rbsr | ok | 3 | 611 | 24.69 MiB | 137.8 / 179.4 | 928.2 / 30867.2 | 0 |
| gap30k | C1 exact | riblt | ok | 1 | 19 | 527.3 KiB | 2461.6 / 2499.9 | 4980.5 / 5911.5 | 0 |
| gap30k | C1 exact | replay | ok | 1 | 5 | 232.3 KiB | 0.0 / 7.2 | 12.2 / 257.2 | 0 |
| gap30k | C6b suffix w=32 | real | n/a | | | | | | |
| gap30k | C6b suffix w=32 | rbsr | ok | 2 | 435 | 23.61 MiB | 33.7 / 66.2 | 535.0 / 21850.0 | 0 |
| gap30k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap30k | C6b suffix w=32 | replay | ok | 1 | 5 | 232.3 KiB | 0.0 / 10.2 | 15.2 / 260.2 | 0 |
| gap30k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap30k | C3 cuckoo f=16 | rbsr | ok | 3 | 621 | 34.97 MiB | 38.3 / 143.8 | 803.1 / 31232.1 | 0 |
| gap30k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap30k | C3 cuckoo f=16 | replay | ok | 1 | 5 | 232.3 KiB | 0.0 / 7.1 | 12.1 / 257.1 | 0 |
| gap30k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap30k | C4 quotient eps=2^-14 | rbsr | ok | 3 | 621 | 34.97 MiB | 38.3 / 175.4 | 834.7 / 31263.7 | 0 |
| gap30k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap30k | C4 quotient eps=2^-14 | replay | ok | 1 | 5 | 232.3 KiB | 0.0 / 9.5 | 14.5 / 259.5 | 0 |
| gap50k | C1 exact | real | FAILED | 50 | 12800 | 518.54 MiB | 0.0 / 6491.5 | 19291.5 / 646491.5 | 55 |
| gap50k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.04 GiB | 11689.0 / 14336.1 | 77225.1 / 2586025.1 | 55 |
| gap50k | C1 exact | riblt | ok | 1 | 28 | 880.2 KiB | 2604.5 / 2664.1 | 5296.5 / 6668.5 | 0 |
| gap50k | C1 exact | replay | ok | 1 | 9 | 385.9 KiB | 0.0 / 14.1 | 23.1 / 464.1 | 0 |
| gap50k | C6b suffix w=32 | real | n/a | | | | | | |
| gap50k | C6b suffix w=32 | rbsr | ok | 3 | 620 | 33.84 MiB | 45.4 / 96.7 | 762.1 / 31142.1 | 0 |
| gap50k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap50k | C6b suffix w=32 | replay | ok | 1 | 9 | 385.9 KiB | 0.0 / 23.9 | 33.0 / 474.0 | 0 |
| gap50k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap50k | C3 cuckoo f=16 | rbsr | ok | 4 | 809 | 45.51 MiB | 46.2 / 187.1 | 1042.3 / 40683.3 | 0 |
| gap50k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap50k | C3 cuckoo f=16 | replay | ok | 1 | 9 | 385.9 KiB | 0.0 / 12.1 | 21.1 / 462.1 | 0 |
| gap50k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap50k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 809 | 45.51 MiB | 46.2 / 227.8 | 1083.0 / 40724.0 | 0 |
| gap50k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap50k | C4 quotient eps=2^-14 | replay | ok | 1 | 9 | 385.9 KiB | 0.0 / 13.5 | 22.6 / 463.6 | 0 |
| gap75k | C1 exact | real | FAILED | 50 | 12800 | 517.87 MiB | 0.0 / 6357.1 | 19157.1 / 646357.1 | 98 |
| gap75k | C1 exact | rbsr | FAILED | 200 | 51200 | 2.03 GiB | 11526.3 / 14328.6 | 77054.9 / 2585854.9 | 98 |
| gap75k | C1 exact | riblt | ok | 1 | 39 | 1.28 MiB | 2720.3 / 2798.0 | 5557.3 / 7468.3 | 0 |
| gap75k | C1 exact | replay | ok | 1 | 13 | 578.8 KiB | 0.1 / 20.7 | 33.8 / 670.8 | 0 |
| gap75k | C6b suffix w=32 | real | n/a | | | | | | |
| gap75k | C6b suffix w=32 | rbsr | ok | 4 | 784 | 42.84 MiB | 56.8 / 118.4 | 959.2 / 39375.2 | 0 |
| gap75k | C6b suffix w=32 | riblt | n/a | | | | | | |
| gap75k | C6b suffix w=32 | replay | ok | 1 | 13 | 578.8 KiB | 0.1 / 34.3 | 47.3 / 684.3 | 0 |
| gap75k | C3 cuckoo f=16 | real | n/a | | | | | | |
| gap75k | C3 cuckoo f=16 | rbsr | ok | 4 | 927 | 52.20 MiB | 53.7 / 213.0 | 1193.7 / 46616.7 | 0 |
| gap75k | C3 cuckoo f=16 | riblt | n/a | | | | | | |
| gap75k | C3 cuckoo f=16 | replay | ok | 1 | 13 | 578.8 KiB | 0.0 / 16.4 | 29.5 / 666.5 | 0 |
| gap75k | C4 quotient eps=2^-14 | real | n/a | | | | | | |
| gap75k | C4 quotient eps=2^-14 | rbsr | ok | 4 | 927 | 52.20 MiB | 53.9 / 260.0 | 1240.9 / 46663.9 | 0 |
| gap75k | C4 quotient eps=2^-14 | riblt | n/a | | | | | | |
| gap75k | C4 quotient eps=2^-14 | replay | ok | 1 | 13 | 578.8 KiB | 0.1 / 22.2 | 35.2 / 672.2 | 0 |
