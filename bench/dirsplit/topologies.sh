#!/bin/bash
# The matrix's topologies: LAYOUT S3_NEAR S3_FAR RTT_LOCAL RTT_REMOTE.
# Site 0 holds the root (and is "near S3"); see README.md.
topology() {
    case $1 in
    T1) echo "same 20 20 0.5 0.5" ;;     # one AZ with S3
    T2) echo "same 80 80 0.5 0.5" ;;     # one AZ, S3 in another region
    T3) echo "half 20 80 0.5 60" ;;      # root + half the writers by S3, half remote
    T4) echo "each 80 80 0.5 60" ;;      # every node in its own region, S3 far
    T5) echo "rootfar 20 80 0.5 60" ;;   # writers together, the root (and S3) remote
    *) echo "unknown topology $1" >&2; return 1 ;;
    esac
}
