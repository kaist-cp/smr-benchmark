extern crate clap;
extern crate crossbeam_ebr;
extern crate crossbeam_pebr;
extern crate csv;
extern crate pebr_benchmark;

use clap::{arg_enum, value_t, values_t, App, Arg, ArgMatches};
use crossbeam_utils::thread::scope;
use csv::Writer;
use rand::distributions::{Uniform, WeightedIndex};
use rand::prelude::*;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::sync::atomic::spin_loop_hint;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use pebr_benchmark::ebr;
use pebr_benchmark::pebr;

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum DS {
        List,
        HashMap,
        NMTree,
        BonsaiTree
    }
}

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum MM {
        NoMM,
        EBR,
        PEBR,
    }
}

#[derive(PartialEq, Debug)]
pub enum Op {
    Insert,
    Get,
    Remove,
}

impl Op {
    const OPS: [Op; 3] = [Op::Insert, Op::Get, Op::Remove];
}

// TODO op_dist arg
struct Config {
    dss: Vec<DS>,
    mms: Vec<MM>,
    threads: Vec<usize>,
    key_dist: Uniform<usize>,
    prefill: usize,
    interval: i64,
    duration: Duration,
    op_dist: WeightedIndex<i32>,
}

fn main() {
    // TODO: normal mode & lazy thread mode
    let matches = App::new("pebr_benchmark")
        .arg(
            Arg::with_name("data structure")
                .short("d")
                .value_name("DS")
                .possible_values(&DS::variants())
                .multiple(true)
                .case_insensitive(true)
                .help("Data structure(s)"),
        )
        .arg(
            Arg::with_name("memory manager")
                .short("m")
                .value_name("MM")
                .possible_values(&MM::variants())
                .multiple(true)
                .case_insensitive(true)
                .help("Memeory manager(s)"),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .value_name("THREADS")
                .takes_value(true)
                .multiple(true)
                .help(
                    "Numbers of threads to run. \
                     Use the deafult value (0) to run for [1,5,10,...100] threads",
                )
                .default_value("0"),
        )
        .arg(
            Arg::with_name("range")
                .short("r")
                .value_name("RANGE")
                .takes_value(true)
                .default_value("100000"),
        )
        .arg(
            Arg::with_name("prefill")
                .short("p")
                .value_name("PREFILL")
                .takes_value(true)
                .default_value("50000"),
        )
        .arg(
            Arg::with_name("interval")
                .short("i")
                .value_name("INTERVAL")
                .takes_value(true)
                .help("Time interval in seconds to run the benchmark")
                .default_value("10"),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .value_name("OUTPUT")
                .takes_value(true)
                .help(
                    "Output CSV filename. \
                     Appends the data if the file already exists.",
                )
                .default_value("results.csv"),
        )
        .get_matches();

    let (config, mut output) = setup(matches);
    bench_all(&config, &mut output);
}

fn setup(m: ArgMatches) -> (Config, Writer<File>) {
    let dss = values_t!(m, "data structure", DS).unwrap();
    let mms = values_t!(m, "memory manager", MM).unwrap();
    let range = value_t!(m, "range", usize).unwrap();
    let key_dist = Uniform::from(0..range);
    let prefill = value_t!(m, "prefill", usize).unwrap();
    let interval: i64 = value_t!(m, "interval", usize).unwrap().try_into().unwrap();
    let duration = Duration::from_secs(interval as u64);

    let threads = values_t!(m, "threads", usize).unwrap();
    let threads = if threads.len() == 1 && threads[0] == 0 {
        let mut threads: Vec<usize> = (0..101).step_by(5).collect();
        threads[0] = 1;
        threads
    } else {
        threads
    };

    // TODO use arg
    let op_weights = &[1, 0, 1];
    let op_dist = WeightedIndex::new(op_weights).unwrap();

    let output_name = m.value_of("output").unwrap();
    let output = match OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .open(output_name)
    {
        Ok(f) => csv::Writer::from_writer(f),
        Err(_) => {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(output_name)
                .unwrap();
            let mut output = csv::Writer::from_writer(f);
            output
                .write_record(&["mm", "ds", "threads", "throughput", "avg_unreclaimed"])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    let config = Config {
        dss,
        mms,
        threads,
        key_dist,
        prefill,
        interval,
        duration,
        op_dist,
    };
    (config, output)
}

fn bench_all(config: &Config, output: &mut Writer<File>) {
    for mm in &config.mms {
        for ds in &config.dss {
            for threads in &config.threads {
                println!("{} {} threads, {}", mm, threads, ds);
                let (ops_per_sec, avg_unreclaimed) = bench(config, mm, ds, *threads);
                println!("ops / sec = {}", ops_per_sec);
                println!("avg unreclaimed at each op: {}", avg_unreclaimed);
                output
                    .write_record(&[
                        mm.to_string(),
                        ds.to_string(),
                        threads.to_string(),
                        ops_per_sec.to_string(),
                        avg_unreclaimed.to_string(),
                    ])
                    .unwrap();
                output.flush().unwrap();
            }
        }
    }
}

fn bench(config: &Config, mm: &MM, ds: &DS, threads: usize) -> (i64, i64) {
    match mm {
        MM::NoMM => {
            return match ds {
                DS::List => bench_no_mm::<ebr::List<String, String>>(config, threads),
                DS::HashMap => bench_no_mm::<ebr::HashMap<String, String>>(config, threads),
                DS::NMTree => bench_no_mm::<ebr::NMTreeMap<String, String>>(config, threads),
                DS::BonsaiTree => {
                    bench_no_mm::<ebr::BonsaiTreeMap<String, String>>(config, threads)
                }
            };
        }
        MM::EBR => {
            return match ds {
                DS::List => bench_ebr::<ebr::List<String, String>>(config, threads),
                DS::HashMap => bench_ebr::<ebr::HashMap<String, String>>(config, threads),
                DS::NMTree => bench_ebr::<ebr::NMTreeMap<String, String>>(config, threads),
                DS::BonsaiTree => bench_ebr::<ebr::BonsaiTreeMap<String, String>>(config, threads),
            };
        }
        MM::PEBR => {
            return match ds {
                DS::List => bench_pebr::<pebr::List<String, String>>(config, threads),
                DS::HashMap => bench_pebr::<pebr::HashMap<String, String>>(config, threads),
                DS::NMTree => {
                    println!("Skip PEBR NMTree");
                    (0, 0)
                }
                DS::BonsaiTree => {
                    println!("Skip PEBR BonsaiTree");
                    (0, 0)
                }
            };
        }
    }
}

// TODO: too much duplication
fn bench_no_mm<M: ebr::ConcurrentMap<String, String> + Send + Sync>(
    config: &Config,
    threads: usize,
) -> (i64, i64) {
    let map = &M::new();

    for _ in 0..config.prefill {
        let mut rng = rand::thread_rng();
        let key = config.key_dist.sample(&mut rng).to_string();
        let value = key.clone();
        map.insert(key, value, unsafe { crossbeam_ebr::unprotected() });
    }

    println!("prefilled");

    let barrier = &Arc::new(Barrier::new(threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..threads {
            let sender = sender.clone();
            s.spawn(move |_| {
                let mut rng = rand::thread_rng();
                let c = barrier.clone();

                let mut ops: i64 = 0;

                c.wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, unsafe { crossbeam_ebr::unprotected() });
                        }
                        Op::Get => {
                            map.get(&key, unsafe { crossbeam_ebr::unprotected() });
                        }
                        Op::Remove => {
                            map.remove(&key, unsafe { crossbeam_ebr::unprotected() });
                        }
                    }
                    ops += 1;
                }

                sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();

    let mut ops = 0;
    for _ in 0..threads {
        let local_ops = receiver.recv().unwrap();
        ops += local_ops;
    }

    let ops_per_sec = ops / config.interval;
    (ops_per_sec, 0)
}

fn bench_ebr<M: ebr::ConcurrentMap<String, String> + Send + Sync>(
    config: &Config,
    threads: usize,
) -> (i64, i64) {
    let map = &M::new();

    let collector = &crossbeam_ebr::Collector::new();
    let main_handle = collector.register();

    for _ in 0..config.prefill {
        let mut rng = rand::thread_rng();
        let guard = main_handle.pin();
        let key = config.key_dist.sample(&mut rng).to_string();
        let value = key.clone();
        map.insert(key, value, &guard);
    }

    println!("prefilled");

    let barrier = &Arc::new(Barrier::new(threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..threads {
            let sender = sender.clone();
            s.spawn(move |_| {
                let mut rng = rand::thread_rng();
                let handle = collector.register();
                let c = barrier.clone();

                let mut ops: i64 = 0;
                // Add up unreclaimed block numbers at the beginning of each op and then divide by
                // total num of ops later.
                let mut unreclaimd_acc: i64 = 0;

                c.wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    let guard = handle.pin();
                    unreclaimd_acc += handle.retired_unreclaimed();
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, &guard);
                        }
                        Op::Get => {
                            map.get(&key, &guard);
                        }
                        Op::Remove => {
                            map.remove(&key, &guard);
                        }
                    }
                    ops += 1;
                }

                sender.send((ops, unreclaimd_acc)).unwrap();
            });
        }
    })
    .unwrap();

    let mut ops = 0;
    let mut unreclaimed_acc = 0;
    for _ in 0..threads {
        let (local_ops, local_unreclaimed_acc) = receiver.recv().unwrap();
        ops += local_ops;
        // First the local avg (w.r.t ops) of unreclaimed count.
        // Then, compute the avg (w.r.t threads) of them.
        unreclaimed_acc += local_unreclaimed_acc / local_ops;
    }

    let ops_per_sec = ops / config.interval;
    let threads: i64 = threads.try_into().unwrap();
    let avg_unreclaimed = unreclaimed_acc / threads;
    (ops_per_sec, avg_unreclaimed)
}

fn bench_pebr<M: pebr::ConcurrentMap<String, String> + Send + Sync>(
    config: &Config,
    threads: usize,
) -> (i64, i64) {
    let map = &M::new();

    let collector = &crossbeam_pebr::Collector::new();

    {
        let guard = unsafe { crossbeam_pebr::unprotected() };
        let mut handle = M::handle(guard);
        for _ in 0..config.prefill {
            let mut rng = rand::thread_rng();
            let key = config.key_dist.sample(&mut rng).to_string();
            let value = key.clone();
            map.insert(&mut handle, key, value, guard);
        }
    }

    println!("prefilled");

    let barrier = &Arc::new(Barrier::new(threads + 1));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        s.spawn(move |_| {
            let handle = collector.register();
            barrier.clone().wait();

            let start = Instant::now();

            let guard = handle.pin();
            while start.elapsed() < config.duration {
                spin_loop_hint();
            }
            drop(guard);
        });

        for _ in 0..threads {
            let sender = sender.clone();
            s.spawn(move |_| {
                let mut rng = rand::thread_rng();
                let handle = collector.register();
                let mut map_handle = M::handle(&handle.pin());
                let c = barrier.clone();

                let mut ops: i64 = 0;
                // Add up unreclaimed block numbers at the beginning of each op and then divide by
                // total num of ops later.
                let mut unreclaimd_acc: i64 = 0;

                c.wait();
                let start = Instant::now();

                // TODO: repin freq opt?
                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    if ops % 1 == 0 {
                        M::clear(&mut map_handle);
                        guard.repin();
                    }
                    unreclaimd_acc += handle.retired_unreclaimed();
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(&mut map_handle, key, value, &mut guard);
                        }
                        Op::Get => {
                            map.get(&mut map_handle, &key, &mut guard);
                        }
                        Op::Remove => {
                            map.remove(&mut map_handle, &key, &mut guard);
                        }
                    }
                    ops += 1;
                }

                sender.send((ops, unreclaimd_acc)).unwrap();
            });
        }
    })
    .unwrap();

    let mut ops = 0;
    let mut unreclaimed_acc = 0;
    for _ in 0..threads {
        let (local_ops, local_unreclaimed_acc) = receiver.recv().unwrap();
        ops += local_ops;
        // First the local avg (w.r.t ops) of unreclaimed count.
        // Then, compute the avg (w.r.t threads) of them.
        unreclaimed_acc += local_unreclaimed_acc / local_ops;
    }

    let ops_per_sec = ops / config.interval;
    let threads: i64 = threads.try_into().unwrap();
    let avg_unreclaimed = unreclaimed_acc / threads;
    (ops_per_sec, avg_unreclaimed)
}
