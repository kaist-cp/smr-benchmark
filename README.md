# A Marriage of Pointer- and Epoch-Based Reclamation

This is the supplementary material for PPoPP 2020 submission #100.

## Dependencies

* [`rustup`](https://rustup.rs/) for building the implementation of NR, EBR, PEBR and data structures

* Linux >= 4.14 for [`MEMBARRIER_CMD_PRIVATE_EXPEDITED` and `MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED`](http://man7.org/linux/man-pages/man2/membarrier.2.html), used in the implementation of PEBR.

* Python >= 3.6 and pip3 for benchmark and plotting scripts

    ```
    pip3 install --user pandas plotnine
    ```

## Build

```sh
git submodule update --init --recursive # not needed if you got the archived source code
cargo build --release
```

## Run

To run a single test,

```sh
./target/release/pebr-benchmark -d <data structure> -m <reclamation scheme> -t <threads>
```

For detailed usage information,

```sh
./target/release/pebr-benchmark -h
```

To run the entire benchmark, 

```sh
python3 bench.py
```

This takes several hours and creates raw csv data under `./results/`.


To generate plots presented in the paper,

```sh
python3 plot.py
```


## Debug

We used `./sanitize.sh` to check that our implementation does not have bugs.
This script runs the benchmark with [LLVM address sanitizer for Rust](https://github.com/japaric/rust-san)
and uses parameters that impose high stress on PEBR by triggering more frequent ejection.

Note that it may report memory leaks when used against `-m ebr`.
This is because of a minor bug in original Crossbeam but it doesn't affect performance of our benchmark.

Benchmark run by this script will generate inaccurate memory usage report since it uses glibc `malloc()` as the global allocator instead of jemalloc.
The memory tracker relies on jemalloc's functionalities which doesn't keep track of allocations by glibc.


## Project structure

 * `./crossbeam-pebr` is a fork of [Crossbeam](https://github.com/crossbeam-rs/crossbeam) that implements PEBR presented in the paper. The main implementation of PEBR lies under `./crossbeam-pebr/crossbeam-epoch`.

 * `./crossbeam-ebr` is the original Crossbeam source code.

 * `./src` contains the benchmark driver (`./src/main.rs`) and the implementation of 4 data structures based on PEBR (`./src/pebr/`) and original Crossbeam (`./src/ebr/`).


