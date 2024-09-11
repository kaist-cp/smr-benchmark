#!/usr/bin/env bash

export RUST_BACKTRACE=1 RUSTFLAGS='-Z sanitizer=address' 

hps="cargo run --bin hp --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "

set -e
for i in {1..5000}; do
    $hps -dh-list -i3 -t256 -r10 -g1
done
