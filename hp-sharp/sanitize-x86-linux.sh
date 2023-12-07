#!/usr/bin/env bash

set -e
for i in {1..2000}; do
    RUSTFLAGS="-Z sanitizer=address" cargo test --profile=release-with-debug --target x86_64-unknown-linux-gnu -- double_node
done
