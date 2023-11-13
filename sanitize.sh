#!/usr/bin/env bash

export RUST_BACKTRACE=1 RUSTFLAGS='-Z sanitizer=address' 

circ_ebr="cargo run --bin circ-ebr --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "
circ_hp="cargo run --bin circ-hp --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "
cdrc_ebr="cargo run --bin cdrc-ebr --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "
cdrc_hp="cargo run --bin cdrc-hp --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "
double_link="cargo run --bin double_link --profile=release-simple --target x86_64-unknown-linux-gnu --features sanitize -- "

set -e
for i in {1..200}; do
    $cdrc_hp -dh-list -i3 -t128 -r10 -g1
    $cdrc_hp -dhm-list -i3 -t128 -r10 -g1
    $cdrc_hp -dhhs-list -i3 -t128 -r10 -g1
    $cdrc_hp -dhash-map -i3 -t128 -r10 -g1
    $cdrc_hp -dnm-tree -i3 -t128 -r10 -g1
    $cdrc_hp -dskip-list -i3 -t128 -r10 -g1
    $cdrc_hp -defrb-tree -i3 -t128 -r10 -g1

    $cdrc_ebr -dh-list -i3 -t128 -r10 -g1
    $cdrc_ebr -dhm-list -i3 -t128 -r10 -g1
    $cdrc_ebr -dhhs-list -i3 -t128 -r10 -g1
    $cdrc_ebr -dhash-map -i3 -t128 -r10 -g1
    $cdrc_ebr -dnm-tree -i3 -t128 -r10 -g1
    $cdrc_ebr -dskip-list -i3 -t128 -r10 -g1
    $cdrc_ebr -defrb-tree -i3 -t128 -r10 -g1

    $circ_hp -dh-list -i3 -t128 -r10 -g1
    $circ_hp -dhm-list -i3 -t128 -r10 -g1
    $circ_hp -dhhs-list -i3 -t128 -r10 -g1
    $circ_hp -dhash-map -i3 -t128 -r10 -g1
    $circ_hp -dnm-tree -i3 -t128 -r10 -g1
    $circ_hp -dskip-list -i3 -t128 -r10 -g1
    $circ_hp -defrb-tree -i3 -t128 -r10 -g1

    export ASAN_OPTIONS=detect_leaks=0
    $circ_ebr -dh-list -i3 -t128 -r10 -g1
    $circ_ebr -dhm-list -i3 -t128 -r10 -g1
    $circ_ebr -dhhs-list -i3 -t128 -r10 -g1
    $circ_ebr -dhash-map -i3 -t128 -r10 -g1
    $circ_ebr -dnm-tree -i3 -t128 -r10 -g1
    $circ_ebr -dskip-list -i3 -t128 -r10 -g1
    $circ_ebr -defrb-tree -i3 -t128 -r10 -g1

    $double_link -t128 -mcirc-ebr
    export ASAN_OPTIONS=detect_leaks=1
    $double_link -t128 -mcirc-hp
    $double_link -t128 -mcdrc-ebr
    $double_link -t128 -mcdrc-hp
done
