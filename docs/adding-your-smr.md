# Tl;dr

To add a new benchmark in `smr-benchmark`, you need to complete the following three tasks:

1. (If necessary) Implement your SMR in `./smrs` and define its dependency in `./Cargo.toml`.  
2. Implement data structures in `./src/ds_impl/<your_smr>`.  
3. Write the benchmark driver (a standalone binary for your SMR) in `./src/bin/<your_smr>.rs`

# Details

### Implementing Your SMR

Implement your SMR as a package in `./smrs` and define its dependency in `./Cargo.toml`. Alternatively, you can directly specify the dependency in `Cargo.toml` using the crate.io registry or a GitHub repository.

* [Example 1 (CIRC; Implemented manually)](https://github.com/kaist-cp/smr-benchmark/tree/main/smrs/circ)  
* [Example 2 (`crossbeam-epoch`; Imported from GitHub)](https://github.com/kaist-cp/smr-benchmark/blob/main/Cargo.toml\#L40-L43)

### Implementing Data Structures

Implement data structures in `./src/ds_impl/<your_smr>`. Follow this simple convention for the directory structure:

* Define a common trait for your `ConcurrentMap` implementation and its stress test `smoke` in `concurrent_map.rs` ([Example](https://github.com/kaist-cp/smr-benchmark/blob/main/src/ds\_impl/ebr/concurrent\_map.rs)).  
* Implement your data structures according to the predefined `ConcurrentMap` trait ([Example](https://github.com/kaist-cp/smr-benchmark/blob/main/src/ds\_impl/ebr/list.rs\#L387-L412)), and include a test function that invokes `smoke` internally ([Example](https://github.com/kaist-cp/smr-benchmark/blob/main/src/ds\_impl/ebr/list.rs\#L485-L498)).

There is currently no convention for other types of data structures, such as queues.

### Writing the Benchmark Driver

The benchmark driver for map data structures (a standalone binary for your SMR) is located at `./src/bin/<your_smr>.rs` ([Example](https://github.com/kaist-cp/smr-benchmark/blob/main/src/bin/ebr.rs)). This will mostly be boilerplate code, so you should be able to write it easily by referring to existing examples.

Afterward, you can run a benchmark by:

```
cargo run --release --bin <your-smr> -- \
    -d <data-structure>                 \
    -t <threads>                        \
    -g <get-rate>                       \
    -r <key-range>                      \
    -i <time-interval-to-run-seconds>
```

Please refer to `README.md` or `cargo run --bin <your-smr> -- -h`.

# Small Notes

* To compile the suite, an **x86 Ubuntu** machine is required.  
  * This is because some SMRs depend on x86 Ubuntu (e.g., PEBR and HP++ use a modified `membarrier`, which is specifically optimized for x86 Ubuntu).  
  * By removing these dependencies, you can compile and run the suite on other environments (e.g., AArch64 macOS).
