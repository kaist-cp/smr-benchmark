#!/usr/bin/env bash

set -e
for i in {1..100}; do
    RUST_BACKTRACE=1 RUSTFLAGS="-g -Z sanitizer=address" cargo test --target x86_64-unknown-linux-gnu --features sanitize -- smoke_harris --test-threads 1
done
