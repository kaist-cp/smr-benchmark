#!/usr/bin/env bash

set -e
for i in {1..200}; do
    RUSTFLAGS="-Z sanitizer=address" cargo test --release --target x86_64-unknown-linux-gnu -- single_node
    RUSTFLAGS="-Z sanitizer=address" cargo test --release --target x86_64-unknown-linux-gnu -- double_node
done
