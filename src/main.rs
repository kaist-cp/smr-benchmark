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
use std::sync::{mpsc, Arc, Barrier};
use std::sync::atomic::spin_loop_hint;
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
    ds: DS,
    mm: MM,
    threads: usize,
    non_coop_threads: usize,
    // TODO ms
    non_coop_period: usize,
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
                .required(true)
                .case_insensitive(true)
                .help("Data structure(s)"),
        )
        .arg(
            Arg::with_name("memory manager")
                .short("m")
                .value_name("MM")
                .possible_values(&MM::variants())
                .required(true)
                .case_insensitive(true)
                .help("Memeory manager(s)"),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .value_name("THREADS")
                .takes_value(true)
                .required(true)
                .help("Numbers of threads to run."),
        )
        .arg(
            Arg::with_name("non-coop")
                .short("n")
                .value_name("NON_COOPERATIVE_THREADS, PERIOD")
                .takes_value(true)
                .number_of_values(2)
                .use_delimiter(true)
                .help("The number of non-cooperative threads and their length of dormant period")
                .default_value("0,0"),
        )
        .arg(
            Arg::with_name("range")
                .short("r")
                .value_name("RANGE")
                .takes_value(true)
                .help("Key range: [0..RANGE]")
                .default_value("100000"),
        )
        .arg(
            Arg::with_name("prefill")
                .short("p")
                .value_name("PREFILL")
                .takes_value(true)
                .help("The number of pre-inserted elements before starting")
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
                     Appends the data if the file already exists.\n\
                     [default: <DS>_results.csv]",
                ),
        )
        .get_matches();

    let (config, mut output) = setup(matches);
    bench(&config, &mut output);
}

fn setup(m: ArgMatches) -> (Config, Writer<File>) {
    let ds = value_t!(m, "data structure", DS).unwrap();
    let mm = value_t!(m, "memory manager", MM).unwrap();
    let threads = value_t!(m, "threads", usize).unwrap();
    let non_coop = values_t!(m, "non-coop", usize).unwrap();
    let range = value_t!(m, "range", usize).unwrap();
    let key_dist = Uniform::from(0..range);
    let prefill = value_t!(m, "prefill", usize).unwrap();
    let interval: i64 = value_t!(m, "interval", usize).unwrap().try_into().unwrap();
    let duration = Duration::from_secs(interval as u64);

    // TODO use arg
    let op_weights = &[1, 0, 1];
    let op_dist = WeightedIndex::new(op_weights).unwrap();

    let output_name = &m
        .value_of("output")
        .map_or(ds.to_string() + "_results.csv", |o| o.to_string());
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
                .write_record(&[
                    "ds",
                    "mm",
                    "threads",
                    "non_coop_threads",
                    "non_coop_period",
                    "throughput",
                    "avg_unreclaimed",
                ])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    let config = Config {
        ds,
        mm,
        threads,
        non_coop_threads: non_coop[0],
        non_coop_period: non_coop[1],
        key_dist,
        prefill,
        interval,
        duration,
        op_dist,
    };
    (config, output)
}

fn bench(config: &Config, output: &mut Writer<File>) {
    println!("{}: {}, {} threads", config.ds, config.mm, config.threads);
    let (ops_per_sec, avg_unreclaimed) = match config.mm {
        MM::NoMM => match config.ds {
            DS::List => bench_no_mm::<ebr::List<String, String>>(config),
            DS::HashMap => bench_no_mm::<ebr::HashMap<String, String>>(config),
            DS::NMTree => bench_no_mm::<ebr::NMTreeMap<String, String>>(config),
            DS::BonsaiTree => bench_no_mm::<ebr::BonsaiTreeMap<String, String>>(config),
        },
        MM::EBR => match config.ds {
            DS::List => bench_ebr::<ebr::List<String, String>>(config),
            DS::HashMap => bench_ebr::<ebr::HashMap<String, String>>(config),
            DS::NMTree => bench_ebr::<ebr::NMTreeMap<String, String>>(config),
            DS::BonsaiTree => bench_ebr::<ebr::BonsaiTreeMap<String, String>>(config),
        },
        MM::PEBR => match config.ds {
            DS::List => bench_pebr::<pebr::List<String, String>>(config),
            DS::HashMap => bench_pebr::<pebr::HashMap<String, String>>(config),
            DS::NMTree => {
                println!("Skip PEBR NMTree");
                return;
            }
            DS::BonsaiTree => {
                println!("Skip PEBR BonsaiTree");
                return;
            }
        },
    };
    output
        .write_record(&[
            config.ds.to_string(),
            config.mm.to_string(),
            config.threads.to_string(),
            config.non_coop_threads.to_string(),
            config.non_coop_period.to_string(),
            ops_per_sec.to_string(),
            avg_unreclaimed.to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
    println!("ops / sec = {}", ops_per_sec);
    println!("avg unreclaimed at each op: {}", avg_unreclaimed);
}

// TODO: too much duplication
fn bench_no_mm<M: ebr::ConcurrentMap<String, String> + Send + Sync>(config: &Config) -> (i64, i64) {
    let map = &M::new();

    for _ in 0..config.prefill {
        let mut rng = rand::thread_rng();
        let key = config.key_dist.sample(&mut rng).to_string();
        let value = key.clone();
        map.insert(key, value, unsafe { crossbeam_ebr::unprotected() });
    }

    println!("prefilled");

    let barrier = &Arc::new(Barrier::new(config.threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..config.threads {
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
    for _ in 0..config.threads {
        let local_ops = receiver.recv().unwrap();
        ops += local_ops;
    }

    let ops_per_sec = ops / config.interval;
    (ops_per_sec, 0)
}

fn bench_ebr<M: ebr::ConcurrentMap<String, String> + Send + Sync>(config: &Config) -> (i64, i64) {
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

    let barrier = &Arc::new(Barrier::new(config.threads + config.non_coop_threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..config.non_coop_threads {
            s.spawn(move |_| {
                let handle = collector.register();
                barrier.clone().wait();

                let start = Instant::now();

                let mut guard = handle.pin();
                let mut ops = 0;
                while start.elapsed() < config.duration {
                    spin_loop_hint();
                    ops += 1;

                    if ops % config.non_coop_period == 0 {
                        guard.repin();
                    }
                }
                drop(guard);
            });
        }
        for _ in 0..config.threads {
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
    for _ in 0..config.threads {
        let (local_ops, local_unreclaimed_acc) = receiver.recv().unwrap();
        ops += local_ops;
        // First the local avg (w.r.t ops) of unreclaimed count.
        // Then, compute the avg (w.r.t threads) of them.
        unreclaimed_acc += local_unreclaimed_acc / local_ops;
    }

    let ops_per_sec = ops / config.interval;
    let threads: i64 = config.threads.try_into().unwrap();
    let avg_unreclaimed = unreclaimed_acc / threads;
    (ops_per_sec, avg_unreclaimed)
}

fn bench_pebr<M: pebr::ConcurrentMap<String, String> + Send + Sync>(config: &Config) -> (i64, i64) {
    let map = &M::new();

    let collector = &crossbeam_pebr::Collector::new();

    let guard = unsafe { crossbeam_pebr::unprotected() };
    let mut handle = M::handle(guard);
    for _ in 0..config.prefill {
        let mut rng = rand::thread_rng();
        let key = config.key_dist.sample(&mut rng).to_string();
        let value = key.clone();
        map.insert(&mut handle, key, value, guard);
    }

    println!("prefilled");

    let barrier = &Arc::new(Barrier::new(config.threads + config.non_coop_threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..config.non_coop_threads {
            s.spawn(move |_| {
                let handle = collector.register();
                barrier.clone().wait();

                let start = Instant::now();

                let mut guard = handle.pin();
                let mut ops = 0;
                while start.elapsed() < config.duration {
                    spin_loop_hint();
                    ops += 1;

                    if ops % config.non_coop_period == 0 {
                        guard.repin();
                    }
                }
                drop(guard);
            });
        }
        for _ in 0..config.threads {
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
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    let mut guard = handle.pin();
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
                    M::clear(&mut map_handle);
                }

                sender.send((ops, unreclaimd_acc)).unwrap();
            });
        }
    })
    .unwrap();

    let mut ops = 0;
    let mut unreclaimed_acc = 0;
    for _ in 0..config.threads {
        let (local_ops, local_unreclaimed_acc) = receiver.recv().unwrap();
        ops += local_ops;
        // First the local avg (w.r.t ops) of unreclaimed count.
        // Then, compute the avg (w.r.t threads) of them.
        unreclaimed_acc += local_unreclaimed_acc / local_ops;
    }

    let ops_per_sec = ops / config.interval;
    let threads: i64 = config.threads.try_into().unwrap();
    let avg_unreclaimed = unreclaimed_acc / threads;
    (ops_per_sec, avg_unreclaimed)
}
