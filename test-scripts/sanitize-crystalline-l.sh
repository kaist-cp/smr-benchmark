#!/usr/bin/env bash

export RUST_BACKTRACE=1 RUSTFLAGS='-Z sanitizer=address' 

he="cargo run --bin crystalline-l --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "

set -e
for i in {1..5000}; do
    $he -dh-list -i3 -t48 -r10 -g1
    $he -dhm-list -i3 -t48 -r10 -g1
    $he -dhhs-list -i3 -t48 -r10 -g1
done
