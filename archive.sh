#!/bin/bash
set -e

ORIGIN=$(pwd)
rm -rf archive
mkdir archive

printf "[1/3] Setting up a temporary working directory... (../benchmark)\n"
cp -rT $ORIGIN $ORIGIN/../benchmark
cd $ORIGIN/../benchmark

git submodule update --init --recursive
cargo clean
cd cdrc-rs; cargo clean; cd ..
cd circ; cargo clean; cd ..
cd crossbeam-ebr; cargo clean; cd ..
cd hp_pp; cargo clean; cd ..
cd hp-brcu; cargo clean; cd ..
cd nbr-rs; cargo clean; cd ..
cd vbr-rs; cargo clean; cd ..

rm -rf archive
rm -f .gitattributes .gitignore .gitmodules .gitlab-ci.yml \
      .python-version archive.sh circ-docker.tar.gz

(
    find . -type d -name ".git" && \
    find . -name ".gitignore" && \
    find . -name ".gitmodules" \
) | xargs rm -rf

# `benchmark.zip`
cd ..
zip -r $ORIGIN/archive/benchmark.zip benchmark
cd benchmark

printf "[2/3] Building a docker image...\n"
rm -rf paper-results
sudo docker build -t circ:v1 .

# `circ-docker.tar.gz`
printf "[3/3] Saving the built image... (%s)\n" $ORIGIN/archive/circ-docker.tar.gz
sudo docker save circ:v1 | gzip > $ORIGIN/archive/circ-docker.tar.gz

cd $ORIGIN
rm -rf $ORIGIN/../benchmark/
