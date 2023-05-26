#!/usr/bin/env bash

# TODO: ignore stack buffer overflow or enlarge the stack limit.

set -e
for i in {1..100}; do
    RUST_BACKTRACE=1 RUSTFLAGS="-g -Z sanitizer=address" cargo test --release --target x86_64-unknown-linux-gnu --features sanitize -- smoke_harris_read --exact --test-threads 1 --nocapture
done
