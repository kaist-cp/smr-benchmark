# A Marriage of Pointer- and Epoch-Based Reclamation

## Dependencies

* [`rustup`](https://rustup.rs/) for building the implementation of PEBR and benchmarks
* Python 3.6 and pip3 for benchmark and plotting scripts

    ```
    pip3 install --user pandas plotnine
    ```

## Build

```sh
git submodule update --init  # not needed if you got the archived source code
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

This takes few hours and creates raw csv data under `./results/`.


To generate plots presented in the paper,

```sh
python3 plot.py
```

## Project structure

 * `./crossbeam-pebr` is a fork of [Crossbeam](https://github.com/crossbeam-rs/crossbeam) that implements PEBR presented in the paper. The main implementation of PEBR lies under `./crossbeam-pebr/crossbeam-epoch`.

 * `./crossbeam-ebr` is the original Crossbeam source code.

 * `./src` contains the .


