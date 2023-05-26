#!/usr/bin/env bash

set -e
for i in {1..100}; do
    RUST_BACKTRACE=1 RUSTFLAGS="-g" cargo test --release -- smoke_harris --test-threads 1 --nocapture
done
