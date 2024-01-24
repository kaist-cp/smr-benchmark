#!/usr/bin/env bash

vbr="cargo run --bin vbr --release -- "

set -e
for i in {1..5000}; do
    $vbr -dskip-list -i3 -t256 -r100000 -g0
done
