#!/usr/bin/env bash

set -e

for i in {1..3000}; do
    cargo test --release -- vbr
done
