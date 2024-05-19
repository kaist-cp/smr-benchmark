use cdrc::{hp_impl, set_counts_between_flush_hp, Cs, CsHP};
use crossbeam_utils::thread::scope;
use rand::prelude::*;
use std::cmp::max;
use std::io::{stdout, Write};
use std::path::Path;
use std::sync::{mpsc, Arc, Barrier};
use std::thread::available_parallelism;
use std::time::Instant;

use smr_benchmark::config::map::{setup, BagSize, BenchWriter, Config, Op, Perf, DS};
use smr_benchmark::ds_impl::cdrc::{
    BonsaiTreeMap, ConcurrentMap, HHSList, HList, HMList, HashMap, NMTreeMap, SkipList,
};

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
        DS::HList => bench_map::<HList<usize, usize, CsHP>>(config, PrefillStrategy::Decreasing),
        DS::HMList => bench_map::<HMList<usize, usize, CsHP>>(config, PrefillStrategy::Decreasing),
        DS::HHSList => {
            bench_map::<HHSList<usize, usize, CsHP>>(config, PrefillStrategy::Decreasing)
        }
        DS::HashMap => {
            bench_map::<HashMap<usize, usize, CsHP>>(config, PrefillStrategy::Decreasing)
        }
        DS::NMTree => bench_map::<NMTreeMap<usize, usize, CsHP>>(config, PrefillStrategy::Random),
        DS::SkipList => {
            bench_map::<SkipList<usize, usize, CsHP>>(config, PrefillStrategy::Decreasing)
        }
        DS::BonsaiTree => {
            bench_map::<BonsaiTreeMap<usize, usize, CsHP>>(config, PrefillStrategy::Random)
        }
        _ => panic!("Unsupported(or unimplemented) data structure for CDRC"),
    };
    output.write_record(config, &perf);
    println!("{}", perf);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefillStrategy {
    Random,
    Decreasing,
}

impl PrefillStrategy {
    fn prefill<M: ConcurrentMap<usize, usize, CsHP> + Send + Sync>(self, config: &Config, map: &M) {
        match self {
            PrefillStrategy::Random => {
                let threads = available_parallelism().map(|v| v.get()).unwrap_or(1);
                print!("prefilling with {threads} threads... ");
                stdout().flush().unwrap();
                scope(|s| {
                    for t in 0..threads {
                        s.spawn(move |_| {
                            let cs = unsafe { &Cs::unprotected() };
                            let output = &mut M::empty_output();
                            let rng = &mut rand::thread_rng();
                            let count = config.prefill / threads
                                + if t < config.prefill % threads { 1 } else { 0 };
                            for _ in 0..count {
                                let key = config.key_dist.sample(rng);
                                let value = key;
                                map.insert(key, value, output, cs);
                            }
                        });
                    }
                })
                .unwrap();
            }
            PrefillStrategy::Decreasing => {
                let cs = unsafe { &Cs::unprotected() };
                let output = &mut M::empty_output();
                let rng = &mut rand::thread_rng();
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key;
                    map.insert(key, value, output, cs);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }
}

fn bench_map<M: ConcurrentMap<usize, usize, CsHP> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
) -> Perf {
    set_counts_between_flush_hp(match config.bag_size {
        BagSize::Small => 64,
        BagSize::Large => 4096,
    });
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

                        let garb = hp_impl::DEFAULT_DOMAIN.num_garbages();
                        garb_acc += garb;
                        garb_peak = max(garb_peak, garb);

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
                barrier.clone().wait();
                let start = Instant::now();

                let output = &mut M::empty_output();
                let mut cs = CsHP::new();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, output, &cs);
                        }
                        Op::Insert => {
                            let value = key;
                            map.insert(key, value, output, &cs);
                        }
                        Op::Remove => {
                            map.remove(&key, output, &cs);
                        }
                    }
                    ops += 1;
                    cs.clear();
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
