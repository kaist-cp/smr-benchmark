#!/usr/bin/env bash

git submodule update --init --recursive
cargo clean
cd cdrc-rs; cargo clean; cd ..
cd crossbeam-ebr; cargo clean; cd ..
cd crossbeam-pebr; cargo clean; cd ..
cd hp_pp; cargo clean; cd ..
cd hp-sharp; cargo clean; cd ..
cd nbr-rs; cargo clean; cd ..
cd vbr-rs; cargo clean; cd ..

git-archive-all smr-benchmark.zip

rm -- "$0"
