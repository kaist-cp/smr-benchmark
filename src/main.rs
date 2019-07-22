extern crate clap;
extern crate pebr_benchmark;

use rand::distributions::WeightedIndex;
use rand::prelude::*;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use clap::{arg_enum, value_t, App, Arg};
use crossbeam_utils::thread::scope;

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
        // TODO: BonsaiTree
    }
}

#[derive(PartialEq, Debug)]
pub enum Op {
    Insert,
    Get,
    Remove,
}

fn main() {
    let matches = App::new("pebr_benchmark")
        .arg(
            Arg::with_name("data structure")
                .short("d")
                .value_name("DS")
                .possible_values(&DS::variants())
                .case_insensitive(true)
                .help("Data structure"),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .value_name("THREADS")
                .takes_value(true)
                .default_value("1"),
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
        .get_matches();

    // TODO abstraction
    let ds = value_t!(matches, "data structure", DS).unwrap();
    let threads = value_t!(matches, "threads", usize).unwrap();
    let range = value_t!(matches, "range", usize).unwrap();
    let prefill = value_t!(matches, "prefill", usize).unwrap();
    let interval = value_t!(matches, "interval", u64).unwrap();
    let duration = Duration::from_secs(interval);

    let op_choices = &[Op::Insert, Op::Get, Op::Remove];
    // TODO use arg
    let op_weights = &[1, 0, 1];
    let dist = &WeightedIndex::new(op_weights).unwrap();

    let collector = &crossbeam_epoch::Collector::new();
    let main_handle = collector.register();

    let map: &Box<dyn ConcurrentMap<String, String> + Send + Sync> = &match ds {
        DS::List => Box::new(List::new()),
        DS::HashMap => Box::new(HashMap::with_capacity(30000)),
        DS::NMTree => Box::new(NMTreeMap::new()),
    };

    for _ in 0..prefill {
        let mut rng = rand::thread_rng();
        let guard = main_handle.pin();
        let key = rng.gen_range::<usize, usize, usize>(0, range).to_string();
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
                let mut ops = 0;
                c.wait();
                let start = Instant::now();
                while start.elapsed() < duration {
                    let key = rng.gen_range::<usize, usize, usize>(0, range).to_string();
                    let guard = handle.pin();
                    match op_choices[dist.sample(&mut rng)] {
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

                sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();

    let mut ops = 0;
    for _ in 0..threads {
        ops += receiver.recv().unwrap();
    }

    println!("ops / sec = {}", ops / interval);

    // TODO CSV output
}
