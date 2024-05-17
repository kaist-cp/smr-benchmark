#!/usr/bin/env bash

set -e

for i in {1..300}; do
    cargo test --release -- list_alter
done

for i in {1..2000}; do
    RUSTFLAGS="-Z sanitizer=address" cargo test --profile=release-simple --target x86_64-unknown-linux-gnu -- list_alter
done
