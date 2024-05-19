use crossbeam_utils::thread::scope;
use rand::prelude::*;
use std::cmp::max;
use std::io::{stdout, Write};
use std::path::Path;
use std::sync::{mpsc, Arc, Barrier};
use std::thread::available_parallelism;
use std::time::Instant;

use smr_benchmark::config::map::{setup, BagSize, BenchWriter, Config, Op, Perf, DS};
use smr_benchmark::ds_impl::nbr::{ConcurrentMap, HHSList, HList, HashMap, NMTreeMap};

fn main() {
    let (config, output) = setup(
        Path::new(file!())
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap(),
    );
    bench(&config, output);
}

fn bench(config: &Config, output: BenchWriter) {
    println!("{}", config);
    let perf = match config.ds {
        DS::HList => bench_map::<HList<usize, usize>>(config, PrefillStrategy::Decreasing, 2),
        DS::HHSList => bench_map::<HHSList<usize, usize>>(config, PrefillStrategy::Decreasing, 2),
        DS::HashMap => bench_map::<HashMap<usize, usize>>(config, PrefillStrategy::Decreasing, 2),
        DS::NMTree => bench_map::<NMTreeMap<usize, usize>>(config, PrefillStrategy::Random, 4),
        _ => panic!("Unsupported(or unimplemented) data structure for NBR"),
    };
    output.write_record(config, &perf);
    println!("{}", perf);
}

fn extract_nbr_params(config: &Config) -> (usize, usize) {
    match config.bag_size {
        BagSize::Small => (256, 32),
        BagSize::Large => (8192, 1024),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefillStrategy {
    Random,
    Decreasing,
}

impl PrefillStrategy {
    fn prefill<M: ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
        max_hazptrs: usize,
    ) {
        match self {
            PrefillStrategy::Random => {
                let (bag_cap_pow2, lowatermark) = extract_nbr_params(config);
                let threads = available_parallelism().map(|v| v.get()).unwrap_or(1);
                let collector =
                    &nbr::Collector::new(threads, bag_cap_pow2, lowatermark, max_hazptrs);
                print!("prefilling with {threads} threads... ");
                stdout().flush().unwrap();
                scope(|s| {
                    for t in 0..threads {
                        s.spawn(move |_| {
                            let mut guard = collector.register();
                            let mut handle = M::handle(&mut guard);
                            let rng = &mut rand::thread_rng();
                            let count = config.prefill / threads
                                + if t < config.prefill % threads { 1 } else { 0 };
                            for _ in 0..count {
                                let key = config.key_dist.sample(rng);
                                let value = key.clone();
                                map.insert(key, value, &mut handle, &guard);
                            }
                        });
                    }
                })
                .unwrap();
            }
            PrefillStrategy::Decreasing => {
                let collector = &nbr::Collector::new(1, 256, 32, max_hazptrs);
                let mut guard = collector.register();
                let mut handle = M::handle(&mut guard);
                let rng = &mut rand::thread_rng();
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(key, value, &mut handle, &guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }
}

fn bench_map<M: ConcurrentMap<usize, usize> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
    max_hazptrs: usize,
) -> Perf {
    let (bag_cap_pow2, lowatermark) = extract_nbr_params(config);
    let map = &M::new();
    strategy.prefill(config, map, max_hazptrs);

    let collector = &nbr::Collector::new(config.threads, bag_cap_pow2, lowatermark, max_hazptrs);

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

                        let garbages = nbr::count_garbages();
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
                let mut ops: u64 = 0;
                let mut rng = &mut rand::thread_rng();
                let mut guard = collector.register();
                let mut handle = M::handle(&mut guard);
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, &mut handle, &guard);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, &mut handle, &guard);
                        }
                        Op::Remove => {
                            map.remove(&key, &mut handle, &guard);
                        }
                    }
                    ops += 1;
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
