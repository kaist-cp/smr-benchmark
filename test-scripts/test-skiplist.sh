#!/usr/bin/env bash

set -e

for i in {1..500}; do
    echo $i
    cargo test --release -- skip_list
done
