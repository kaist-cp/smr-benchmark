#!/usr/bin/env bash

set -e
for i in {1..500}; do
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dh-list -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhm-list -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhhs-list -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dhash-map -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dnm-tree -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -defrb-tree -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dbonsai-tree -i3 -t60 -r10 -g1 -mhp-sharp
    RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin smr-benchmark --target x86_64-unknown-linux-gnu --features sanitize -- -dskip-list -i3 -t60 -r10 -g1 -mhp-sharp
done
