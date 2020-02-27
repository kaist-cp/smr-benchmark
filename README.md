# A Marriage of Pointer- and Epoch-Based Reclamation

This is the artifact for

Jeehoon Kang and Jaehwang Jung, A Marriage of Pointer- and Epoch-Based Reclamation, PLDI 2020.

The latest developments are on <https://github.com/kaist-cp/pebr-benchmark>.

## Summary
On Ubuntu 18.04,

```
sudo apt install build-essential python3-pip
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
pip3 install --user pandas plotnine
python3 bench.py
python3 plot.py
```

## Dependencies

* Linux >= 4.14 for [`MEMBARRIER_CMD_PRIVATE_EXPEDITED` and
  `MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED`](http://man7.org/linux/man-pages/man2/membarrier.2.html),
  used in the implementation of PEBR.
    * **IMPORTANT**: Docker disables this feature by default. To enable, please use
      ```
      --security-opt seccomp:unconfined
      ```
      option when launching Docker.

* [`rustup`](https://rustup.rs/) for building the implementation of NR, EBR, PEBR and data structures
    * Rust requires GCC for linking in Linux.

* Python >= 3.6, pandas and plotnine for benchmark runner and plotting scripts

## Usage

To build the benchmark,

```
git submodule update --init --recursive # not needed if you got the archived source code
cargo build --release                   # remove --release for debug build
```

To run a single test,

```
./target/release/pebr-benchmark -d <data structure> -m <reclamation scheme> -t <threads>
```

where

* data structure: HList, HMList, HHSList, HashMap, NMTree, BonsaiTree
* reclamation scheme: NR, EBR, PEBR

For detailed usage information,

```
./target/release/pebr-benchmark -h
```

To run the entire benchmark,

```
python3 bench.py
```

This takes several hours and creates raw CSV data under `./results/`.

To generate plots,

```
python3 plot.py
```

This creates plots presented in the paper under `./results/`.


## Debug

We used `./sanitize.sh` to debug our implementation. This script runs the
benchmark with [LLVM address sanitizer for
Rust](https://github.com/japaric/rust-san) and uses parameters that impose high
stress on PEBR by triggering more frequent ejection.

Note that sanitizer may report memory leaks when used against `-m EBR`.
This is because of a minor bug in original Crossbeam but it doesn't affect performance of our benchmark.


## Project structure

* `./crossbeam-pebr` is the fork of
  [Crossbeam](https://github.com/crossbeam-rs/crossbeam) that implements PEBR.
  The main implementation of PEBR lies under
  `./crossbeam-pebr/crossbeam-epoch`.

* `./crossbeam-ebr` is the original Crossbeam source code.

* `./src` contains the benchmark driver (`./src/main.rs`) and the
  implementation of data structures based on PEBR (`./src/pebr/`) and
  original Crossbeam (`./src/ebr/`).


## Results

`./paper-results` contains the raw results and graphs used in the paper.

## Note
* On Windows, the benchmark uses the default memory allocator instead of
  jemalloc since [the Rust library for
  jemalloc](https://crates.io/crates/jemallocator) does not support Windows.

* The benchmark run by `./sanitize.sh` will generate inaccurate memory usage
  report since it uses the default memory allocator instead of jemalloc. The
  memory tracker relies on jemalloc's functionalities which doesn't keep track
  of allocations by the default allocator.

