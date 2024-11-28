use crossbeam_utils::thread::scope;
use rand::prelude::*;
use std::cmp::max;
use std::io::{stdout, Write};
use std::mem::ManuallyDrop;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Barrier};
use std::thread::available_parallelism;
use std::time::Instant;
use typenum::{Unsigned, U1, U4};

use smr_benchmark::config::map::{setup, BagSize, BenchWriter, Config, Op, OpsPerCs, Perf, DS};
use smr_benchmark::ds_impl::ebr::{
    BonsaiTreeMap, ConcurrentMap, EFRBTree, ElimABTree, HHSList, HList, HMList, HashMap, NMTreeMap,
    SkipList,
};

fn main() {
    let (config, output) = setup(
        Path::new(file!())
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap(),
    );
    match config.ops_per_cs {
        OpsPerCs::One => bench::<U1>(&config, output),
        OpsPerCs::Four => bench::<U4>(&config, output),
    }
}

fn bench<N: Unsigned>(config: &Config, output: BenchWriter) {
    println!("{}", config);
    let perf = match config.ds {
        DS::HList => bench_map::<HList<usize, usize>, N>(config, PrefillStrategy::Decreasing),
        DS::HMList => bench_map::<HMList<usize, usize>, N>(config, PrefillStrategy::Decreasing),
        DS::HHSList => bench_map::<HHSList<usize, usize>, N>(config, PrefillStrategy::Decreasing),
        DS::HashMap => bench_map::<HashMap<usize, usize>, N>(config, PrefillStrategy::Decreasing),
        DS::NMTree => bench_map::<NMTreeMap<usize, usize>, N>(config, PrefillStrategy::Random),
        DS::BonsaiTree => {
            // For Bonsai Tree, it would be faster to use a single thread to prefill.
            bench_map::<BonsaiTreeMap<usize, usize>, N>(config, PrefillStrategy::Decreasing)
        }
        DS::EFRBTree => bench_map::<EFRBTree<usize, usize>, N>(config, PrefillStrategy::Random),
        DS::SkipList => bench_map::<SkipList<usize, usize>, N>(config, PrefillStrategy::Decreasing),
        DS::ElimAbTree => bench_map::<ElimABTree<usize, usize>, N>(config, PrefillStrategy::Random),
    };
    output.write_record(config, &perf);
    println!("{}", perf);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefillStrategy {
    /// Inserts keys in a random order, with multiple threads.
    Random,
    /// Inserts keys in an increasing order, with a single thread.
    Decreasing,
}

impl PrefillStrategy {
    fn prefill<M: ConcurrentMap<usize, usize> + Send + Sync>(self, config: &Config, map: &M) {
        // Some data structures (e.g., Bonsai tree, Elim AB-Tree) need SMR's retirement
        // functionality even during insertions.
        let collector = &crossbeam_ebr::Collector::new();
        match self {
            PrefillStrategy::Random => {
                let threads = available_parallelism().map(|v| v.get()).unwrap_or(1);
                print!("prefilling with {threads} threads... ");
                stdout().flush().unwrap();
                scope(|s| {
                    for t in 0..threads {
                        s.spawn(move |_| {
                            let handle = collector.register();
                            let rng = &mut rand::thread_rng();
                            let count = config.prefill / threads
                                + if t < config.prefill % threads { 1 } else { 0 };
                            for _ in 0..count {
                                let key = config.key_dist.sample(rng);
                                let value = key;
                                map.insert(key, value, &handle.pin());
                            }
                        });
                    }
                })
                .unwrap();
                for _ in 0..config.prefill {}
            }
            PrefillStrategy::Decreasing => {
                let handle = collector.register();
                let rng = &mut rand::thread_rng();
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key;
                    map.insert(key, value, &handle.pin());
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }
}

fn bench_map<M: ConcurrentMap<usize, usize> + Send + Sync, N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> Perf {
    match config.bag_size {
        BagSize::Small => crossbeam_ebr::set_bag_capacity(512),
        BagSize::Large => crossbeam_ebr::set_bag_capacity(4096),
    }
    let map = &M::new();
    strategy.prefill(config, map);

    let collector = &crossbeam_ebr::Collector::new();

    let barrier = &Arc::new(Barrier::new(config.threads + config.aux_thread));
    let (ops_sender, ops_receiver) = mpsc::channel();
    let (mem_sender, mem_receiver) = mpsc::channel();

    scope(|s| {
        // sampling & interference thread
        if config.aux_thread > 0 {
            let mem_sender = mem_sender.clone();
            s.spawn(move |_| {
                let mut samples = 0usize;
                let mut acc = 0usize;
                let mut peak = 0usize;
                let mut garb_acc = 0usize;
                let mut garb_peak = 0usize;
                let handle = collector.register();
                barrier.clone().wait();

                let start = Instant::now();
                // Immediately drop if no non-coop else keep it and repin periodically.
                let mut guard = ManuallyDrop::new(handle.pin());
                if config.non_coop == 0 {
                    unsafe { ManuallyDrop::drop(&mut guard) };
                }
                let mut next_sampling = start + config.sampling_period;
                let mut next_repin = start + config.non_coop_period;
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        let garbages = crossbeam_ebr::GLOBAL_GARBAGE_COUNT.load(Ordering::Acquire);
                        garb_acc += garbages;
                        garb_peak = max(garb_peak, garbages);

                        next_sampling = now + config.sampling_period;
                    }
                    if now > next_repin {
                        (*guard).repin();
                        next_repin = now + config.non_coop_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.non_coop > 0 {
                    unsafe { ManuallyDrop::drop(&mut guard) };
                }

                if config.sampling {
                    mem_sender
                        .send((peak, acc / samples, garb_peak, garb_acc / samples))
                        .unwrap();
                } else {
                    mem_sender.send((0, 0, 0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0, 0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = &mut rand::thread_rng();
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, &guard);
                        }
                        Op::Insert => {
                            let value = key;
                            map.insert(key, value, &guard);
                        }
                        Op::Remove => {
                            map.remove(&key, &guard);
                        }
                    }
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        drop(guard);
                        guard = handle.pin();
                    }
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.threads {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, peak_garb, avg_garb) = mem_receiver.recv().unwrap();
    Perf {
        ops_per_sec,
        peak_mem,
        avg_mem,
        peak_garb,
        avg_garb,
    }
}
