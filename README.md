# SMR Benchmark: A Microbenchmark Suite for Concurrent Safe Memory Reclamation Schemes

[![CI](https://github.com/kaist-cp/smr-benchmark/actions/workflows/test.yml/badge.svg)](https://github.com/kaist-cp/smr-benchmark/actions/workflows/test.yml)
[![Docs](https://github.com/kaist-cp/smr-benchmark/actions/workflows/doc.yml/badge.svg)](https://kaist-cp.github.io/smr-benchmark)

This is the artifact for:

* Jaehwang Jung, Jeonghyeon Kim, Matthew J. Parkinson and Jeehoon Kang, Concurrent Immediate Reference Counting, PLDI 2024 \[15\].
* Jeonghyeon Kim, Jaehwang Jung and Jeehoon Kang, Expediting Hazard Pointers with Bounded RCU Critical Sections, SPAA 2024 \[14\].
* Jaehwang Jung, Janggun Lee, Jeonghyeon Kim and Jeehoon Kang, Applying Hazard Pointers to More Concurrent Data Structures, SPAA 2023 \[11\].
* Jeehoon Kang and Jaehwang Jung, A Marriage of Pointer- and Epoch-Based Reclamation, PLDI 2020 \[8\].

## Summary
On Ubuntu 22.04,

```bash
sudo apt install build-essential python3-pip clang
# Install the Rust compiler and register its executable path.
curl https://sh.rustup.rs -sSf | bash -s -- -y
pip3 install --user pandas plotnine

cargo build --release

# To run a single map data structure benchmark (see later sections for details),
./target/release/<reclamation-scheme> -d <data-structure> -t <threads> -g <get-rate> -r <key-range> -i <time-interval-to-run-seconds>

# For dedicated experiment scripts (e.g., `experiment.sh`),
# see subdirectories in `bench-scripts`.
```

To add your own implementation of SMR and data structures, please refer to [this guide](./docs/adding-your-smr.md).

## Getting Started Guide

We recommend the following environment:

* OS: Ubuntu 22.04
* Architecture: x86-64
* Python: 3.10.12
* Rustup: 1.26.0

### Project Structure

* `smrs`: Reclamation schemes except EBR and PEBR.
  * `hp_pp`: An implementation of the original HP \[9,10\] and HP++ \[11\].
  * `hp-brcu`: An implementation of HP-RCU and HP-BRCU schemes \[14\].
  * `nbr-rs`: An implementation of NBR+ \[13\] with signal optimization.
  * `vbr-rs`: An implementation of VBR \[16\].
  * `cdrc-rs`: An implementation of CDRC \[12\].
  * `circ`: An implementation of CIRC \[15\].
* `src`: An implementaion of the benchmark suite.
  * `bin`: Benchmark drivers for each SMR.
  * `ds_impl`: Implementations of data structures based on each SMR.

For the implementation of EBR and PEBR, please refer to our dedicated repository [kaist-cp/crossbeam](https://github.com/kaist-cp/crossbeam).

* [`crossbeam-ebr`](https://github.com/kaist-cp/crossbeam/tree/smr-benchmark): The original Crossbeam source code implementing EBR \[1,7\].
* [`crossbeam-pebr`](https://github.com/kaist-cp/crossbeam/tree/pebr): The fork of [Crossbeam](https://github.com/crossbeam-rs/crossbeam) that implements PEBR \[8\] of which the main implementation lies under `./crossbeam-pebr/crossbeam-epoch`.

### Setting up the Environment

The benchmark requires Rust (for building the implementation of reclamation schemes and data structures), Python (for generating the figures), and other essential build tools.

#### Install Rust and Build Tools

Install build tools by simply running:

```bash
# `clang` is necessary to compile NBR and HP-BRCU.
sudo apt install build-essential clang
# Install the Rust compiler and register its executable path.
curl https://sh.rustup.rs -sSf | bash -s -- -y
```

#### Install Python Packages

Some Python packages are necessary to execute end-to-end experiment scripts and generate plots. To install them locally, simply run:

```bash
sudo apt install python3-pip
pip3 install --user pandas plotnine
```

However, in some cases, you might encounter an issue due to the conflicts on the versions of Python packages. If this is the case, We suggest using `pyenv` to set up the Python environment to minimize potential conflicts with the pre-installed system Python environment.

```bash
# Install the system Python3 and all dependencies for `pyenv`.
sudo apt update && sudo apt install -y \
  build-essential python3 python3-pip curl \
  libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl \
  libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# Install `pyenv`. If you already have `pyenv` in your system,
# skip this lines and set up `bench-venv`.
curl https://pyenv.run | bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc
source ~/.bashrc

# Set up the `bench-venv` environment to generate figures.
pyenv install 3.10.12
pyenv virtualenv 3.10.12 bench-venv
pyenv local bench-venv
pip3 install -r requirements.txt

# Install the Rust compiler and register its executable path.
curl https://sh.rustup.rs -sSf | bash -s -- -y
```

#### Build Binaries

After installing the necessary dependencies properly, build the benchmark binaries with the following command:

```bash
cargo build --release
```

### Basic Testing

```bash
cargo test --release -- --test-threads=1
```

After successful testing, outputs like the following appear, printing `ok` for each test.

```text
$ cargo test --release -- --test-threads=1
    Finished release [optimized] target(s) in 0.05s
     Running unittests src/lib.rs (target/release/deps/smr_benchmark-c42d60ebe9d214bd)

running 72 tests
test ds_impl::cdrc::bonsai_tree::tests::smoke_bonsai_tree_ebr ... ok
test ds_impl::cdrc::bonsai_tree::tests::smoke_bonsai_tree_hp ... ok
test ds_impl::cdrc::double_link::test::simple_all ... ok
test ds_impl::cdrc::double_link::test::smoke_all ... ok
... (omitted) ...

test result: ok. 72 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 25.24s
... (omitted) ...

   Doc-tests smr-benchmark

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

### Running a Single Benchmark

Even with the small configuration, the end-to-end benchmark scripts in `bench-scripts` would take several hours. You can run a single benchmark by directly executing the benchmark binaries.

#### Map Data Structures

To run a single map data structure benchmark,

```bash
./target/release/<reclamation-scheme> -d <data-structure> -t <threads> -g <get-rate> -r <key-range> -i <time-interval-to-run-seconds>
```

where

* Data structure
  * `h-list`: Harris's linked list \[1\]
  * `hm-list`: Harris-Michael linked list \[2\]
  * `hhs-list`: Harris’s list with wait-free get() method (HP not applicable) \[3\]
  * `hash-map`: Chaining hash table using HMList (for HP) or HHSList (for others) for each bucket \[2\]
  * `nm-tree`: Natarajan- Mittal tree (HP not applicable) \[4\]
  * `skip-list`: lock-free skiplist by Herlihy and Shavit, with wait-free get() for schemes other than HP \[3\]
  * `bonsai-tree`: A non-blocking variant of Bonsai tree \[5\]
  * `efrb-tree`: Ellen et al. ’s tree \[6\]
  * `elim-ab-tree`: An (a,b) tree with elimination \[17\]
* Reclamation scheme
  * `nr`: A baseline that does not reclaim memory
  * `ebr`: Epoch-based RCU \[1,7\]
  * `pebr`: Pointer- and epoch-based reclamation \[8\]
  * `hp`: Hazard pointer with asymmetric fence optimization \[9,10\]
  * `hp-pp`: An extension to hazard pointers that support optimistic traversal \[11\]
  * `nbr`: Neutralization based reclamation \[13\] with signal optimization (NBR+) with a moderate reclamation threshold (256)
  * `nbr-large`: NBR+ \[13\] with a large reclamation threshold (8192)
  * `hp-rcu`: An extension of HP scheme with RCU-expedited traversals \[14\]
  * `hp-brcu`: An extension of HP scheme with BRCU-expedited traversals \[14\]
  * `vbr`: Version based reclamation by Sheffi et al. \[16\]
  * `cdrc-ebr`: EBR flavor of CDRC \[12\]
  * `cdrc-ebr-flush`: EBR flavor of CDRC \[12\] flushing its local garbages on each operation
  * `cdrc-hp`: HP flavor of CDRC \[12\]
  * `circ-ebr`: EBR flavor of CIRC \[15\]
  * `circ-hp`: HP flavor of CIRC \[15\]
* Get rate
  * `0`: Write-only (Insert 50%, Remove 50%)
  * `1`: Read-write (Get 50%, Insert 25%, Remove 25%)
  * `2`: Read-intensive (Get 10%, Insert 45%, Remove 45%)
  * `3`: Read-only (Get 100%)

It runs a single map data structure benchmark with the given configuration, and measures the throughput (operations per second) and memory usage (bytes).

```text
$ ./target/release/circ-ebr -d nm-tree -t 64 -g 2 -r 10000 -i 10
nm-tree: 64 threads, n0, c1, E2, small bag
prefilling with 64 threads... prefilled... end
ops/s: 163839786, peak mem: 28.316 MiB, avg_mem: 4.970 MiB, peak garb: 99524, avg garb: 3589
```

For detailed usage information,

```sh
./target/release/<reclamation-scheme> -h
```

#### DoubleLink Queue

To run a single DoubleLink queue benchmark,

```sh
./target/release/double-link -t <threads> -m <reclamation-scheme> -i <time-interval-to-run-seconds>
```

where

* Reclamation scheme
  * `nr`: A baseline that does not reclaim memory
  * `ebr`: Epoch-based RCU
  * `hp`: Hazard pointer with asymmetric fence optimization
  * `cdrc-ebr`: EBR flavor of CDRC
  * `cdrc-ebr-flush`: EBR flavor of CDRC flushing its local garbage on each operation
  * `cdrc-hp`: HP flavor of CDRC
  * `circ-ebr`: EBR flavor of CIRC
  * `circ-hp`: HP flavor of CIRC

It runs a single DoubleLink queue benchmark with the given configuration, and measures the throughput (operations per second) and memory usage (bytes).

```text
$ ./target/release/double-link -t 64 -m circ-ebr -i 10          
circ-ebr: 64 threads
end
ops/s: 732030, peak mem: 219205664, avg_mem: 110476847
```

For detailed usage information,

```bash
./target/release/double-link -h
```

### Running the Entire Benchmark

To run the entire benchmark, execute `experiment.sh` script in `bench-scripts`. This takes several hours and creates raw CSV data and figures under `./results/`.

## Debug

We used AddressSanitizer to debug our implementation.
For example, a line below runs the benchmark with [LLVM address sanitizer for Rust](https://github.com/japaric/rust-san) and uses parameters that impose high stress on SMR by triggering more frequent retirements.

```text
RUST_BACKTRACE=1 RUSTFLAGS="-Z sanitizer=address" cargo run --bin <Reclamation scheme> --target x86_64-unknown-linux-gnu --features sanitize -- -d<Data structure> -i3 -t64 -r10 -g1
```

Note that sanitizer may report memory leaks when used against CIRC EBR. This is because we used high bits of pointers for epoch tagging purposes, but the AddressSanitizer does not recognize those tagged pointers.


## References

* \[1\] Timothy L. Harris. 2001. A Pragmatic Implementation of Non-Blocking Linked-Lists. In Proceedings of the 15th International Conference on Distributed Computing (DISC ’01). Springer-Verlag, Berlin, Heidelberg, 300–314.
* \[2\] Maged M. Michael. 2002. High Performance Dynamic Lock-Free Hash Tables and List-Based Sets. In Proceedings of the Fourteenth Annual ACM Symposium on Parallel Algorithms and Architectures (Winnipeg, Manitoba, Canada) (SPAA ’02). Association for Computing Machinery, New York, NY, USA, 73–82. <https://doi.org/10.1145/564870.564881>
* \[3\] Maurice Herlihy and Nir Shavit. 2012. The Art of Multiprocessor Programming, Revised Reprint (1st ed.). Morgan Kaufmann Publishers Inc., San Francisco, CA, USA.
* \[4\] Aravind Natarajan and Neeraj Mittal. 2014. Fast Concurrent Lock-Free Binary Search Trees. In Proceedings of the 19th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming (Orlando, Florida, USA) (PPoPP ’14). Association for Computing Machinery, New York, NY, USA, 317–328. <https://doi.org/10.1145/2555243.2555256>
* \[5\] Austin T. Clements, M. Frans Kaashoek, and Nickolai Zeldovich. 2012. Scalable Address Spaces Using RCU Balanced Trees. In Proceedings of the Seventeenth International Conference on Architectural Support for Programming Languages and Operating Systems (London, England, UK) (ASPLOS XVII). Association for Computing Machinery, New York, NY, USA, 199–210. <https://doi.org/10.1145/2150976.2150998>
* \[6\] Faith Ellen, Panagiota Fatourou, Eric Ruppert, and Franck van Breugel. 2010. Non-Blocking Binary Search Trees. In Proceedings of the 29th ACM SIGACT- SIGOPS Symposium on Principles of Distributed Computing (Zurich, Switzerland) (PODC ’10). Association for Computing Machinery, New York, NY, USA, 131–140. <https://doi.org/10.1145/1835698.1835736>
* \[7\] Keir Fraser. 2004. Practical lock-freedom. Ph. D. Dissertation.
* \[8\] Jeehoon Kang and Jaehwang Jung. 2020. A Marriage of Pointer- and Epoch-Based Reclamation. In Proceedings of the 41st ACM SIGPLAN Conference on Programming Language Design and Implementation (London, UK) (PLDI 2020). Association for Computing Machinery, New York, NY, USA, 314–328. <https://doi.org/10.1145/3385412.3385978>
* \[9\] Maged M. Michael. 2002. Safe Memory Reclamation for Dynamic Lock-Free Objects Using Atomic Reads and Writes. In Proceedings of the Twenty-First Annual Symposium on Principles of Distributed Computing (Monterey, California) (PODC ’02). Association for Computing Machinery, New York, NY, USA, 21–30. <https://doi.org/10.1145/571825.571829>
* \[10\] Maged M. Michael. 2004. Hazard Pointers: Safe Memory Reclamation for Lock-Free Objects. IEEE Trans. Parallel Distrib. Syst. 15, 6 (June 2004), 491–504. <https://doi.org/10.1109/TPDS.2004.8>
* \[11\] Jaehwang Jung, Janggun Lee, Jeonghyeon Kim, and Jeehoon Kang. 2023. Applying Hazard Pointers to More Concurrent Data Structures. In Proceedings of the 35th ACM Symposium on Parallelism in Algorithms and Architectures (SPAA '23). Association for Computing Machinery, New York, NY, USA, 213–226. <https://doi.org/10.1145/3558481.3591102>
* \[12\] Daniel Anderson, Guy E. Blelloch, and Yuanhao Wei. 2022. Turning Manual Concurrent Memory Reclamation into Automatic Reference Counting. In Proceedings of the 43rd ACM SIGPLAN International Conference on Programming Language Design and Implementation (San Diego, CA, USA) (PLDI 2022). Association for Computing Machinery, New York, NY, USA, 61–75. <https://doi.org/10.1145/3519939.3523730>
* \[13\] Ajay Singh, Trevor Brown, and Ali Mashtizadeh. 2021. NBR: Neutralization Based Reclamation. In Proceedings of the 26th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming (Virtual Event, Republic of Korea) (PPoPP ’21). Association for Computing Machinery, New York, NY, USA, 175–190. <https://doi.org/10.1145/3437801.3441625>
* \[14\] Jeonghyeon Kim, Jaehwang Jung, and Jeehoon Kang. 2024. Expediting Hazard Pointers with Bounded RCU Critical Sections. In Proceedings of the 36th ACM Symposium on Parallelism in Algorithms and Architectures (SPAA 2024), June 17–21, 2024, Nantes, France. ACM, New York, NY, USA, 34 pages. <https://doi.org/10.1145/3626183.3659941>
* \[15\] Jaehwang Jung, Jeonghyeon Kim, Matthew J. Parkinson, and Jeehoon Kang. 2024. Concurrent Immediate Reference Counting. Proc. ACM Program. Lang. 8, PLDI, Article 153 (June 2024), 24 pages. <https://doi.org/10.1145/3656383>
* \[16\] Gali Sheffi, Maurice Herlihy, and Erez Petrank. 2021. VBR: Version Based Reclamation. In Proceedings of the 33rd ACM Symposium on Parallelism in Algorithms and Architectures (Virtual Event, USA) (SPAA ’21). Association for Computing Machinery, New York, NY, USA, 443–445. <https://doi.org/10.1145/3409964.3461817>
* \[17\] Anubhav Srivastava and Trevor Brown. 2022. Elimination (a,b)-trees with fast, durable updates. In Proceedings of the 27th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming (PPoPP '22). Association for Computing Machinery, New York, NY, USA, 416–430. <https://doi.org/10.1145/3503221.3508441>
