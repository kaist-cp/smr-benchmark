#!/usr/bin/env bash

set -e
for i in {1..500}; do
    # RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" 
    cargo run --features sanitize -- -dbonsaitree -i2 -t30 -r10 -p10 -n1 -mpebr
    # --target x86_64-unknown-linux-gnu 
done
