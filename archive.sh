#!/bin/bash
git submodule update --init --recursive
cargo clean
cd crossbeam-ebr; cargo clean; cd ..
cd crossbeam-pebr; cargo clean; cd ..

git-archive-all pebr-benchmark.zip

rm -- "$0"
