use crossbeam_utils::thread::scope;
use hp_brcu::{global, set_bag_capacity, THREAD};
use rand::prelude::*;
use std::cmp::max;
use std::io::{stdout, Write};
use std::path::Path;
use std::sync::{mpsc, Arc, Barrier};
use std::thread::available_parallelism;
use std::time::Instant;

use smr_benchmark::config::map::{setup, BagSize, BenchWriter, Config, Op, Perf, DS};
use smr_benchmark::ds_impl::hp_brcu::{
    BonsaiTreeMap, ConcurrentMap, ElimABTree, HHSList, HList, HMList, HashMap, NMTreeMap, SkipList,
};

fn main() {
    unsafe { hp_brcu::set_rollback(false) };
    let (config, output) = setup(
        Path::new(file!())
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap(),
    );
    bench(&config, output)
}

fn bench(config: &Config, output: BenchWriter) {
    println!("{}", config);
    let perf = match config.ds {
        DS::HList => bench_map::<HList<usize, usize>>(config, PrefillStrategy::Decreasing),
        DS::HMList => bench_map::<HMList<usize, usize>>(config, PrefillStrategy::Decreasing),
        DS::HHSList => bench_map::<HHSList<usize, usize>>(config, PrefillStrategy::Decreasing),
        DS::HashMap => bench_map::<HashMap<usize, usize>>(config, PrefillStrategy::Decreasing),
        DS::NMTree => bench_map::<NMTreeMap<usize, usize>>(config, PrefillStrategy::Random),
        DS::SkipList => bench_map::<SkipList<usize, usize>>(config, PrefillStrategy::Decreasing),
        DS::BonsaiTree => {
            bench_map::<BonsaiTreeMap<usize, usize>>(config, PrefillStrategy::Decreasing)
        }
        DS::ElimAbTree => bench_map::<ElimABTree<usize, usize>>(config, PrefillStrategy::Random),
        _ => panic!("Unsupported(or unimplemented) data structure for HP-BRCU"),
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
        match self {
            PrefillStrategy::Random => {
                let threads = available_parallelism().map(|v| v.get()).unwrap_or(1);
                print!("prefilling with {threads} threads... ");
                stdout().flush().unwrap();
                scope(|s| {
                    for t in 0..threads {
                        s.spawn(move |_| {
                            hp_brcu::THREAD.with(|handle| {
                                let handle = &mut **handle.borrow_mut();
                                let output = &mut M::empty_output(handle);
                                let rng = &mut rand::thread_rng();
                                let count = config.prefill / threads
                                    + if t < config.prefill % threads { 1 } else { 0 };
                                for _ in 0..count {
                                    let key = config.key_dist.sample(rng);
                                    let value = key.clone();
                                    map.insert(key, value, output, handle);
                                }
                            });
                        });
                    }
                })
                .unwrap();
            }
            PrefillStrategy::Decreasing => {
                hp_brcu::THREAD.with(|handle| {
                    let handle = &mut **handle.borrow_mut();
                    let output = &mut M::empty_output(handle);
                    let rng = &mut rand::thread_rng();
                    let mut keys = Vec::with_capacity(config.prefill);
                    for _ in 0..config.prefill {
                        keys.push(config.key_dist.sample(rng));
                    }
                    keys.sort_by(|a, b| b.cmp(a));
                    for key in keys.drain(..) {
                        let value = key.clone();
                        map.insert(key, value, output, handle);
                    }
                });
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }
}

fn bench_map<M: ConcurrentMap<usize, usize> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
) -> Perf {
    // Note: It tries a collection after two bag flushes.
    match config.bag_size {
        BagSize::Small => set_bag_capacity(512),
        BagSize::Large => set_bag_capacity(4096),
    }
    let map = &M::new();
    strategy.prefill(config, map);

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + config.sampling_period;
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        let garbages = global().garbage_count();
                        garb_acc += garbages;
                        garb_peak = max(garb_peak, garbages);

                        next_sampling = now + config.sampling_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
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
                THREAD.with(|th| {
                    let handle = &mut **th.borrow_mut();
                    let mut ops: u64 = 0;
                    let mut rng = &mut rand::thread_rng();
                    let output = &mut M::empty_output(handle);
                    barrier.clone().wait();
                    let start = Instant::now();

                    while start.elapsed() < config.duration {
                        let key = config.key_dist.sample(rng);
                        match Op::OPS[config.op_dist.sample(&mut rng)] {
                            Op::Get => {
                                map.get(&key, output, handle);
                            }
                            Op::Insert => {
                                let value = key.clone();
                                map.insert(key, value, output, handle);
                            }
                            Op::Remove => {
                                map.remove(&key, output, handle);
                            }
                        }
                        ops += 1;
                    }
                    ops_sender.send(ops).unwrap();
                })
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
