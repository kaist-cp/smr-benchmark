#!/usr/bin/env bash

export RUST_BACKTRACE=1 RUSTFLAGS='-Z sanitizer=address' 

hps="cargo run --bin hp-brcu --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "
hps0="cargo run --bin hp-rcu --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "

set -e
for i in {1..5000}; do
    $hps -dh-list -i3 -t128 -r10 -g1
    $hps -dhm-list -i3 -t128 -r10 -g1
    $hps -dhhs-list -i3 -t128 -r10 -g1
    $hps -dhash-map -i3 -t256 -r10 -g1
    $hps -dnm-tree -i3 -t256 -r10 -g1
    $hps -dskip-list -i3 -t256 -r10 -g1

    $hps0 -dh-list -i3 -t128 -r10 -g1
    $hps0 -dhm-list -i3 -t128 -r10 -g1
    $hps0 -dhhs-list -i3 -t128 -r10 -g1
    $hps0 -dhash-map -i3 -t256 -r10 -g1
    $hps0 -dnm-tree -i3 -t256 -r10 -g1
    $hps0 -dskip-list -i3 -t256 -r10 -g1
done
