extern crate clap;
extern crate crossbeam_ebr;
extern crate crossbeam_pebr;
extern crate csv;
extern crate pebr_benchmark;

use clap::{arg_enum, value_t, App, Arg, ArgMatches};
use crossbeam_utils::thread::scope;
use csv::Writer;
use rand::distributions::{Uniform, WeightedIndex};
use rand::prelude::*;
use std::cmp::min;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};
use typenum::{Integer, P1, P4};

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
        NR,
        EBR,
        PEBR,
    }
}

pub enum OpsPerCs {
    One,
    Four,
}

#[derive(PartialEq, Debug)]
pub enum Op {
    Get,
    Insert,
    Remove,
}

impl Op {
    const OPS: [Op; 3] = [Op::Get, Op::Insert, Op::Remove];
}

struct Config {
    ds: DS,
    mm: MM,
    threads: usize,
    non_coop_threads: usize,
    non_coop_period: usize,
    get_rate: usize,
    key_dist: Uniform<usize>,
    prefill: usize,
    interval: i64,
    ops_per_cs: OpsPerCs,
    duration: Duration,
    op_dist: WeightedIndex<i32>,
}

fn main() {
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
                .takes_value(false)
                .multiple(true)
                .help(
                    "The degree of non-cooperation. \
                     -n for 10ms, -nn for inf",
                ),
        )
        .arg(
            Arg::with_name("get rate")
                .short("g")
                .takes_value(false)
                .multiple(true)
                .help(
                    "The proportion of `get`(read) operations. \
                     none: 0%, -g: 50%, -gg: 90%",
                ),
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
            Arg::with_name("ops per cs")
                .short("n")
                .value_name("OPS_PER_CS")
                .takes_value(true)
                .help("Operations per each critical section")
                .default_value("1"),
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
    match config.ops_per_cs {
        OpsPerCs::One => bench::<P1>(&config, &mut output),
        OpsPerCs::Four => bench::<P4>(&config, &mut output),
    }
}

fn setup(m: ArgMatches) -> (Config, Writer<File>) {
    let ds = value_t!(m, "data structure", DS).unwrap();
    let mm = value_t!(m, "memory manager", MM).unwrap();
    let threads = value_t!(m, "threads", usize).unwrap();
    let non_coop = m.occurrences_of("non-coop");
    let get_rate = min(2, m.occurrences_of("get rate")) as usize;
    let range = value_t!(m, "range", usize).unwrap();
    let key_dist = Uniform::from(0..range);
    let prefill = value_t!(m, "prefill", usize).unwrap();
    let interval: i64 = value_t!(m, "interval", usize).unwrap().try_into().unwrap();
    let ops_per_cs = match value_t!(m, "ops per cs", usize).unwrap() {
        1 => OpsPerCs::One,
        4 => OpsPerCs::Four,
        _ => panic!("ops_per_cs should be one or four"),
    };
    let duration = Duration::from_secs(interval as u64);

    let op_weights = match get_rate {
        0 => &[0, 1, 1],
        1 => &[2, 1, 1],
        _ => &[18, 1, 1],
    };
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
                    "get_rate",
                    "throughput",
                    // TODO peak mem
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
        non_coop_threads: min(non_coop, 1) as usize,
        non_coop_period: min(non_coop, 2) as usize,
        get_rate,
        key_dist,
        prefill,
        interval,
        ops_per_cs,
        duration,
        op_dist,
    };
    (config, output)
}

fn bench<N: Integer>(config: &Config, output: &mut Writer<File>) {
    println!("{}: {}, {} threads", config.ds, config.mm, config.threads);
    let ops_per_sec = match config.mm {
        MM::NR => match config.ds {
            DS::List => bench_nr::<ebr::List<String, String>>(config, PrefillStrategy::Decreasing),
            DS::HashMap => {
                bench_nr::<ebr::HashMap<String, String>>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_nr::<ebr::NMTreeMap<String, String>>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => {
                bench_nr::<ebr::BonsaiTreeMap<String, String>>(config, PrefillStrategy::Random)
            }
        },
        MM::EBR => match config.ds {
            DS::List => {
                bench_ebr::<ebr::List<String, String>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_ebr::<ebr::HashMap<String, String>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_ebr::<ebr::NMTreeMap<String, String>, N>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => {
                bench_ebr::<ebr::BonsaiTreeMap<String, String>, N>(config, PrefillStrategy::Random)
            }
        },
        MM::PEBR => match config.ds {
            DS::List => {
                bench_pebr::<pebr::List<String, String>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_pebr::<pebr::HashMap<String, String>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_pebr::<pebr::NMTreeMap<String, String>, N>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => bench_pebr::<pebr::BonsaiTreeMap<String, String>, N>(
                config,
                PrefillStrategy::Random,
            ),
        },
    };
    output
        .write_record(&[
            config.ds.to_string(),
            config.mm.to_string(),
            config.threads.to_string(),
            config.non_coop_threads.to_string(),
            config.non_coop_period.to_string(),
            config.get_rate.to_string(),
            ops_per_sec.to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
    println!("ops / sec = {}", ops_per_sec);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefillStrategy {
    Random,
    Decreasing,
}

impl PrefillStrategy {
    fn prefill_ebr<M: ebr::ConcurrentMap<String, String> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = unsafe { crossbeam_ebr::unprotected() };
        let mut rng = rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    let value = key.clone();
                    map.insert(key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(&mut rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for k in keys.drain(..) {
                    let key = k.to_string();
                    let value = key.clone();
                    map.insert(key, value, guard);
                }
            }
        }
        println!("prefilled");
    }

    fn prefill_pebr<M: pebr::ConcurrentMap<String, String> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = unsafe { crossbeam_pebr::unprotected() };
        let mut handle = M::handle(guard);
        let mut rng = rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    let value = key.clone();
                    map.insert(&mut handle, key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(&mut rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for k in keys.drain(..) {
                    let key = k.to_string();
                    let value = key.clone();
                    map.insert(&mut handle, key, value, guard);
                }
            }
        }
        println!("prefilled");
    }
}

// TODO: too much duplication
fn bench_nr<M: ebr::ConcurrentMap<String, String> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
) -> i64 {
    let map = &M::new();
    strategy.prefill_ebr(config, map);

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
                        Op::Get => {
                            map.get(&key, unsafe { crossbeam_ebr::unprotected() });
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, unsafe { crossbeam_ebr::unprotected() });
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
    ops_per_sec
}

fn bench_ebr<M: ebr::ConcurrentMap<String, String> + Send + Sync, N: Integer>(
    config: &Config,
    strategy: PrefillStrategy,
) -> i64 {
    let map = &M::new();
    strategy.prefill_ebr(config, map);

    let collector = &crossbeam_ebr::Collector::new();

    let barrier = &Arc::new(Barrier::new(config.threads + config.non_coop_threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..config.non_coop_threads {
            s.spawn(move |_| {
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                if config.non_coop_period == 1 {
                    while start.elapsed() < config.duration {
                        std::thread::sleep(Duration::from_millis(10));
                        guard.repin();
                    }
                } else if config.non_coop_period == 2 {
                    std::thread::sleep(Duration::from_millis(
                        (config.interval * 1000).try_into().unwrap(),
                    ));
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

                c.wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, &guard);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, &guard);
                        }
                        Op::Remove => {
                            map.remove(&key, &guard);
                        }
                    }
                    ops += 1;
                    if ops % N::to_i64() == 0 {
                        drop(guard);
                        guard = handle.pin();
                    }
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
    ops_per_sec
}

fn bench_pebr<M: pebr::ConcurrentMap<String, String> + Send + Sync, N: Integer>(
    config: &Config,
    strategy: PrefillStrategy,
) -> i64 {
    let map = &M::new();
    strategy.prefill_pebr(config, map);

    let collector = &crossbeam_pebr::Collector::new();

    let barrier = &Arc::new(Barrier::new(config.threads + config.non_coop_threads));
    let (sender, receiver) = mpsc::channel();

    scope(|s| {
        for _ in 0..config.non_coop_threads {
            s.spawn(move |_| {
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                if config.non_coop_period == 1 {
                    while start.elapsed() < config.duration {
                        std::thread::sleep(Duration::from_millis(10));
                        guard.repin();
                    }
                } else if config.non_coop_period == 2 {
                    std::thread::sleep(Duration::from_millis(
                        (config.interval * 1000).try_into().unwrap(),
                    ));
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

                c.wait();
                let start = Instant::now();

                // TODO: repin freq opt?
                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng).to_string();
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&mut map_handle, &key, &mut guard);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(&mut map_handle, key, value, &mut guard);
                        }
                        Op::Remove => {
                            map.remove(&mut map_handle, &key, &mut guard);
                        }
                    }
                    ops += 1;
                    if ops % N::to_i64() == 0 {
                        M::clear(&mut map_handle);
                        guard.repin();
                    }
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
    ops_per_sec
}
