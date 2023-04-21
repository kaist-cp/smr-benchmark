# `smr-benchmark`: A microbenchmark suite for concurrent memory reclamation schemes

This is the artifact for

* Jaehwang Jung, Janggun Lee, Jeonghyeon Kim and Jeehoon Kang, Applying Hazard Pointers to More Concurrent Data Structures, SPAA 2023.
* Jeehoon Kang and Jaehwang Jung, A Marriage of Pointer- and Epoch-Based Reclamation, PLDI 2020.

## Summary
On Ubuntu 22.04,

```
sudo apt install build-essential python3-pip clang
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh  # You may need to reload the shell after finishing this to use 'cargo'
pip3 install --user pandas plotnine

python3 bench.py
python3 bench-long-running.py
python3 plot.py
python3 plot-hhslist-hmlist.py
python3 plot-nmtree-efrbtree.py
python3 plot-long-running.py
```

## Dependencies

* Linux >= 4.14 for [`MEMBARRIER_CMD_PRIVATE_EXPEDITED` and
  `MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED`](http://man7.org/linux/man-pages/man2/membarrier.2.html),
  used in the implementation of PEBR, HP and HP++.
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
./target/release/smr-benchmark -d <Data structure> -m <Reclamation scheme> -t <Threads>
```

where

* Data structure
  * `HList`: Harris's linked list [1]
  * `HMList`: Harris-Michael linked list [2]
  * `HHSList`: Harris’s list [1] with wait-free get() method (HP not applicable) [3]
  * `HashMap`: Chaining hash table using HMList (for HP) or HHSList (for others) for each bucket [2]
  * `NMTree`: Natarajan- Mittal tree (HP not applicable) [4]
  * `BonsaiTree`: A non-blocking variant of Bonsai tree [5]
  * `SkipList`: lock-free skiplist by Herlihy and Shavit, with wait-free get() for schemes other than HP [3]
  * `EFRBTree`: Ellen et al. ’s tree [6]
* Reclamation scheme
  * `NR`: A baseline that does not reclaim memory
  * `EBR`: Epoch-based RCU [7][8]
  * `PEBR`: Pointer- and epoch-based reclamation [9]
  * `HP`: Hazard pointer with asymmetric fence optimization [10][11]
  * `HP_PP`: An extension to hazard pointers that supports optimistic traversal [12]
  * `CDRC_EBR`: EBR flavor of CDRC [13]
  * `NBR`: Neutralization based reclamation with signal optimization (NBR+) [14]

For detailed usage information,

```
./target/release/smr-benchmark -h
```

To run the entire benchmark,

```
python3 bench.py                # General throughput benchmarks
python3 bench-long-running.py   # Throughput of long-running read operations
```

This takes several hours and creates raw CSV data under `./results/`.

To generate plots,

```
python3 plot.py                   # General throughput plots
python3 plot-hhslist-hmlist.py    # HHSList with HP++ vs. HMList with HP
python3 plot-nmtree-efrbtree.py   # NMTree with HP++ vs. EFRBTree with HP
python3 plot-long-running.py      # Throughput of long-running read operations
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

* `./hp_pp` is an implementation of the original HP and our HP++.

* `./nbr-rs` is an implementation of NBR+ with signal optimizing.

* `./cdrc-rs` is an implementation of CDRC with Crossbeam EBR.

* `./src` contains the benchmark driver (`./src/main.rs`) and the
  implementation of data structures based on each SMR.
  
  * PEBR (`./src/pebr/`)

  * EBR with original Crossbeam (`./src/ebr/`).

  * HP (`./src/hp/`)

  * HP++ (`./src/hp_pp/`)

  * NBR (`./src/nbr`)

  * CDRC (`./src/cdrc`)


## Note
* On Windows, the benchmark uses the default memory allocator instead of
  jemalloc since [the Rust library for
  jemalloc](https://crates.io/crates/jemallocator) does not support Windows.

* The benchmark run by `./sanitize.sh` will generate inaccurate memory usage
  report since it uses the default memory allocator instead of jemalloc. The
  memory tracker relies on jemalloc's functionalities which doesn't keep track
  of allocations by the default allocator.


## References

* [1] Timothy L. Harris. 2001. A Pragmatic Implementation of Non-Blocking Linked-
Lists. In Proceedings of the 15th International Conference on Distributed Computing
(DISC ’01). Springer-Verlag, Berlin, Heidelberg, 300–314.
* [2] Maged M. Michael. 2002. High Performance Dynamic Lock-Free Hash Tables and List-Based Sets. In Proceedings of the Fourteenth Annual ACM Symposium on Parallel Algorithms and Architectures (Winnipeg, Manitoba, Canada) (SPAA ’02). Association for Computing Machinery, New York, NY, USA, 73–82. https: //doi.org/10.1145/564870.564881
* [3] Maurice Herlihy and Nir Shavit. 2012. The Art of Multiprocessor Programming, Revised Reprint (1st ed.). Morgan Kaufmann Publishers Inc., San Francisco, CA, USA.
* [4] Aravind Natarajan and Neeraj Mittal. 2014. Fast Concurrent Lock-Free Bi- nary Search Trees. In Proceedings of the 19th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming (Orlando, Florida, USA) (PPoPP ’14). Association for Computing Machinery, New York, NY, USA, 317–328. https://doi.org/10.1145/2555243.2555256
* [5] Austin T. Clements, M. Frans Kaashoek, and Nickolai Zeldovich. 2012. Scalable Address Spaces Using RCU Balanced Trees. In Proceedings of the Seventeenth International Conference on Architectural Support for Programming Languages and Operating Systems (London, England, UK) (ASPLOS XVII). Association for Computing Machinery, New York, NY, USA, 199–210. https://doi.org/10.1145/ 2150976.2150998
* [6] Faith Ellen, Panagiota Fatourou, Eric Ruppert, and Franck van Breugel. 2010. Non-Blocking Binary Search Trees. In Proceedings of the 29th ACM SIGACT- SIGOPS Symposium on Principles of Distributed Computing (Zurich, Switzerland) (PODC ’10). Association for Computing Machinery, New York, NY, USA, 131–140. https://doi.org/10.1145/1835698.1835736
* [7] Keir Fraser. 2004. Practical lock-freedom. Ph. D. Dissertation.
* [8] Timothy L. Harris. 2001. A Pragmatic Implementation of Non-Blocking Linked-
Lists. In Proceedings of the 15th International Conference on Distributed Computing
(DISC ’01). Springer-Verlag, Berlin, Heidelberg, 300–314.
* [9] Jeehoon Kang and Jaehwang Jung. 2020. A Marriage of Pointer- and Epoch-Based Reclamation. In Proceedings of the 41st ACM SIGPLAN Conference on Programming Language Design and Implementation (London, UK) (PLDI 2020). Association for Computing Machinery, New York, NY, USA, 314–328. https://doi.org/10.1145/ 3385412.3385978
* [10] Maged M. Michael. 2002. Safe Memory Reclamation for Dynamic Lock-Free Objects Using Atomic Reads and Writes. In Proceedings of the Twenty-First Annual Symposium on Principles of Distributed Computing (Monterey, California) (PODC ’02). Association for Computing Machinery, New York, NY, USA, 21–30. https: //doi.org/10.1145/571825.571829
* [11] Maged M. Michael. 2004. Hazard Pointers: Safe Memory Reclamation for Lock- Free Objects. IEEE Trans. Parallel Distrib. Syst. 15, 6 (June 2004), 491–504. https: //doi.org/10.1109/TPDS.2004.8
* [12] Jaehwang Jung, Janggun Lee, Jeonghyeon Kim and Jeehoon Kang, Applying Hazard Pointers to More Concurrent Data Structures, SPAA 2023.
* [13] Daniel Anderson, Guy E. Blelloch, and Yuanhao Wei. 2022. Turning Man- ual Concurrent Memory Reclamation into Automatic Reference Counting. In Proceedings of the 43rd ACM SIGPLAN International Conference on Program- ming Language Design and Implementation (San Diego, CA, USA) (PLDI 2022). Association for Computing Machinery, New York, NY, USA, 61–75. https: //doi.org/10.1145/3519939.3523730
* [14] Ajay Singh, Trevor Brown, and Ali Mashtizadeh. 2021. NBR: Neutralization Based Reclamation. In Proceedings of the 26th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming (Virtual Event, Republic of Korea) (PPoPP ’21). Association for Computing Machinery, New York, NY, USA, 175–190. https://doi.org/10.1145/3437801.3441625