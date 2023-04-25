#!/usr/bin/env bash

set -e
for i in {1..500}; do
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhlist -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhmlist -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhhslist -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhashmap -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dnmtree -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -defrbtree -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dbonsaitree -i3 -t60 -r10 -g1 -mhp_pp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dskiplist -i3 -t60 -r10 -g1 -mhp_pp
done
