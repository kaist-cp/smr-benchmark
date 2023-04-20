#!/bin/bash
git submodule update --init --recursive
cargo clean
cd crossbeam-ebr; cargo clean; cd ..
cd crossbeam-pebr; cargo clean; cd ..
cd cdrc-rs; cargo clean; cd ..
cd hp_pp; cargo clean; cd ..
cd nbr-rs; cargo clean; cd ..

git-archive-all smr-benchmark.zip

rm -- "$0"
