extern crate clap;
extern crate csv;
extern crate pebr_benchmark;

use rand::distributions::{Uniform, WeightedIndex};
use rand::prelude::*;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use clap::{arg_enum, value_t, values_t, App, Arg, ArgMatches};
use crossbeam_utils::thread::scope;

use pebr_benchmark::bonsai_tree::BonsaiTreeMap;
use pebr_benchmark::concurrent_map::ConcurrentMap;
use pebr_benchmark::harris_michael_list::List;
use pebr_benchmark::michael_hash_map::HashMap;
use pebr_benchmark::natarajan_mittal_tree::NMTreeMap;

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum DS {
        List,
        HashMap,
        NMTree,
        BonsaiTree
    }
}

#[derive(PartialEq, Debug)]
pub enum MM {
    NoMM,
    EBR,
    PEBR,
}

#[derive(PartialEq, Debug)]
pub enum Op {
    Insert,
    Get,
    Remove,
}

// global config & run config
// TODO: setup should make Iterator of Configs
// TODO: Config.run(threads, MM)
// TODO op_dist arg
struct Config {
    dss: Vec<DS>,
    threads: Vec<usize>,
    range: usize,
    prefill: usize,
    interval: i64,
    duration: Duration,
    op_dist: WeightedIndex<i32>,
    output: csv::Writer<File>,
}

fn main() {
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

    let mut config = setup(matches);

    bench_all(&mut config);
}

fn setup(m: ArgMatches) -> Config {
    let dss = values_t!(m, "data structure", DS).unwrap();
    let range = value_t!(m, "range", usize).unwrap();
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
    Config {
        dss,
        threads,
        range,
        prefill,
        interval,
        duration,
        op_dist,
        output,
    }
}

fn bench_all(config: &mut Config) -> (i64, i64) {
    for threads in &config.threads {
        for ds in &config.dss {
            println!("{} threads, {:?}", threads, ds);
            let (ops_per_sec, avg_unreclaimed) = match ds {
                DS::List => bench::<List<String, String>>(config, *threads),
                DS::HashMap => bench::<HashMap<String, String>>(config, *threads),
                DS::NMTree => bench::<NMTreeMap<String, String>>(config, *threads),
                DS::BonsaiTree => bench::<BonsaiTreeMap<String, String>>(config, *threads),
            };
            println!("ops / sec = {}", ops_per_sec);
            println!("avg unreclaimed at each op: {}", avg_unreclaimed);
            config
                .output
                .write_record(&[
                    String::from("ebr"), // TODO do this properly
                    ds.to_string(),
                    threads.to_string(),
                    ops_per_sec.to_string(),
                    avg_unreclaimed.to_string(),
                ])
                .unwrap();
            config.output.flush().unwrap();
        }
    }
    (0, 0)
}

fn bench<M: ConcurrentMap<String, String> + Send + Sync>(
    config: &Config,
    threads: usize,
) -> (i64, i64) {
    let map = &M::new();

    let key_dist = &Uniform::from(0..config.range);
    let op_choices = &[Op::Insert, Op::Get, Op::Remove];

    let collector = &crossbeam_epoch::Collector::new();
    let main_handle = collector.register();

    for _ in 0..config.prefill {
        let mut rng = rand::thread_rng();
        let guard = main_handle.pin();
        let key = key_dist.sample(&mut rng).to_string();
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
                    let key = key_dist.sample(&mut rng).to_string();
                    let guard = handle.pin();
                    unreclaimd_acc += handle.retired_unreclaimed();
                    match op_choices[config.op_dist.sample(&mut rng)] {
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
