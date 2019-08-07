#!/bin/bash
git submodule update --init --recursive
cargo clean
cd crossbeam-ebr; cargo clean; cd ..
cd crossbeam-pebr; cargo clean; cd ..

# for all .rs files, find comments like `TODO(@author)` and anonymize
find . -name *.rs -print0 | xargs -0 sed -i -e 's/\([A-Z]\+\)(@\?[a-zA-Z]\+)/\1(anonymized)/g'
find . -name *.toml -print0 | xargs -0 sed -i -e 's/.*eehoon.*//g'
find . -name *.md -print0 | xargs -0 sed -i -e 's/.*eehoon.*//g'
# ^ this doesn't work on this file's last line ??????
sed -i -e  's/.*eehoon.*//g' crossbeam-pebr/crossbeam-epoch/deps/membarrier-rs/CHANGELOG.md

grep -i "kaist" . -r --exclude-dir=.git --color=always
grep -i "jeehoon" . -r --exclude-dir=.git --color=always
grep -i "jaehwang" . -r --exclude-dir=.git --color=always

pandoc README.md -o README.html

git-archive-all --extra README.html pebr-benchmark.zip

rm -- "$0"
