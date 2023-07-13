#[macro_use]
extern crate cfg_if;
extern crate clap;
extern crate csv;

extern crate crossbeam_ebr;
extern crate crossbeam_pebr;
extern crate smr_benchmark;

use ::hp_pp::DEFAULT_DOMAIN;
use clap::{value_parser, Arg, ArgMatches, Command, ValueEnum};
use crossbeam_utils::thread::scope;
use csv::Writer;
use rand::distributions::Uniform;
use rand::prelude::*;
use std::cmp::max;
use std::fmt;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{stdout, Write};
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};
use typenum::{Unsigned, U1};

use smr_benchmark::{cdrc, ebr};
use smr_benchmark::{hp, hp_pp, hp_sharp as hp_sharp_bench};
use smr_benchmark::{nbr, pebr};

#[derive(PartialEq, Debug, ValueEnum, Clone)]
#[allow(non_camel_case_types)]
pub enum MM {
    NR,
    EBR,
    PEBR,
    HP,
    HP_PP,
    CDRC_EBR,
    HP_SHARP,
    NBR,
}

pub enum OpsPerCs {
    One,
    Four,
}

impl fmt::Display for OpsPerCs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpsPerCs::One => write!(f, "1"),
            OpsPerCs::Four => write!(f, "4"),
        }
    }
}

struct Config {
    mm: MM,
    readers: usize,
    writers: usize,

    aux_thread: usize,
    aux_thread_period: Duration,
    sampling: bool,
    sampling_period: Duration,

    key_dist: Uniform<usize>,
    prefill: usize,
    interval: u64,
    duration: Duration,

    mem_sampler: MemSampler,
}

cfg_if! {
    if #[cfg(all(not(feature = "sanitize"), target_os = "linux"))] {
        extern crate tikv_jemalloc_ctl;
        struct MemSampler {
            epoch_mib: tikv_jemalloc_ctl::epoch_mib,
            allocated_mib: tikv_jemalloc_ctl::stats::allocated_mib,
        }
        impl MemSampler {
            pub fn new() -> Self {
                MemSampler {
                    epoch_mib: tikv_jemalloc_ctl::epoch::mib().unwrap(),
                    allocated_mib: tikv_jemalloc_ctl::stats::allocated::mib().unwrap(),
                }
            }
            pub fn sample(&self) -> usize {
                self.epoch_mib.advance().unwrap();
                self.allocated_mib.read().unwrap()
            }
        }
    } else {
        struct MemSampler {}
        impl MemSampler {
            pub fn new() -> Self {
                println!("NOTE: Memory usage benchmark is supported only for linux.");
                MemSampler {}
            }
            pub fn sample(&self) -> usize {
                0
            }
        }
    }
}

fn main() {
    let matches = Command::new("smr_benchmark")
        .arg(
            Arg::new("memory manager")
                .short('m')
                .value_parser(value_parser!(MM))
                .required(true)
                .ignore_case(true)
                .help("Memeory manager(s)"),
        )
        .arg(
            Arg::new("writers")
                .short('w')
                .value_parser(value_parser!(usize))
                .required(true)
                .help("Numbers of threads which perform only write operations."),
        )
        .arg(
            Arg::new("readers")
                .short('g')
                .value_parser(value_parser!(usize))
                .required(true)
                .help("Numbers of threads which perform only get operations."),
        )
        .arg(
            Arg::new("range")
                .short('r')
                .value_parser(value_parser!(usize))
                .help("Key range: [0..RANGE]")
                .default_value("100000"),
        )
        .arg(
            Arg::new("interval")
                .short('i')
                .value_parser(value_parser!(u64))
                .help("Time interval in seconds to run the benchmark")
                .default_value("10"),
        )
        .arg(
            Arg::new("sampling period")
                .short('s')
                .value_parser(value_parser!(u64))
                .help(
                    "The period to query jemalloc stats.allocated (ms). 0 for no sampling. \
                     Only supported on linux.",
                )
                .default_value("1"),
        )
        .arg(Arg::new("output").short('o').help(
            "Output CSV filename. \
                     Appends the data if the file already exists.\n\
                     [default: results/<DS>.csv]",
        ))
        .get_matches();

    let (config, mut output) = setup(matches);
    bench::<U1>(&config, &mut output);
}

fn setup(m: ArgMatches) -> (Config, Writer<File>) {
    let mm = m.get_one::<MM>("memory manager").cloned().unwrap();
    let writers = m.get_one::<usize>("writers").copied().unwrap();
    let readers = m.get_one::<usize>("readers").copied().unwrap();
    let range = m.get_one::<usize>("range").copied().unwrap();
    let prefill = range / 2;
    let key_dist = Uniform::from(0..range);
    let interval = m.get_one::<u64>("interval").copied().unwrap();
    let sampling_period = m.get_one::<u64>("sampling period").copied().unwrap();
    let sampling = sampling_period > 0 && cfg!(all(not(feature = "sanitize"), target_os = "linux"));
    let duration = Duration::from_secs(interval);

    assert!(
        readers >= 1,
        "The number of readers must be greater than zero!"
    );

    let output_name = m
        .get_one("output")
        .cloned()
        .unwrap_or("results/long-running.csv".to_string());
    create_dir_all("results").unwrap();
    let output = match OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .open(&output_name)
    {
        Ok(f) => csv::Writer::from_writer(f),
        Err(_) => {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&output_name)
                .unwrap();
            let mut output = csv::Writer::from_writer(f);
            // NOTE: `write_record` on `bench`
            output
                .write_record(&[
                    "mm",
                    "sampling_period",
                    "throughput",
                    "peak_mem",
                    "avg_mem",
                    "peak_garb",
                    "avg_garb",
                    "key_range",
                ])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    let mem_sampler = MemSampler::new();
    let config = Config {
        mm,
        writers,
        readers,

        aux_thread: if sampling { 1 } else { 0 },
        aux_thread_period: Duration::from_millis(1),
        sampling,
        sampling_period: Duration::from_millis(sampling_period),

        key_dist,
        prefill,
        interval,
        duration,

        mem_sampler,
    };
    (config, output)
}

fn bench<N: Unsigned>(config: &Config, output: &mut Writer<File>) {
    println!(
        "{}: {} writers, {} readers",
        config.mm.to_possible_value().unwrap().get_name(),
        config.writers,
        config.readers
    );
    let (ops_per_sec, peak_mem, avg_mem, peak_garb, avg_garb) = match config.mm {
        MM::NR => bench_map_nr(config, PrefillStrategy::Decreasing),
        MM::EBR => bench_map_ebr::<N>(config, PrefillStrategy::Decreasing),
        MM::PEBR => bench_map_pebr::<N>(config, PrefillStrategy::Decreasing),
        MM::HP => bench_map_hp(config, PrefillStrategy::Decreasing),
        MM::HP_PP => bench_map_hp_pp(config, PrefillStrategy::Decreasing),
        MM::CDRC_EBR => bench_map_cdrc::<cdrc_rs::GuardEBR, N>(config, PrefillStrategy::Decreasing),
        MM::HP_SHARP => bench_map_hp_sharp(config, PrefillStrategy::Decreasing),
        MM::NBR => bench_map_nbr(config, PrefillStrategy::Decreasing, 2),
    };
    output
        .write_record(&[
            config
                .mm
                .to_possible_value()
                .unwrap()
                .get_name()
                .to_string(),
            config.sampling_period.as_millis().to_string(),
            ops_per_sec.to_string(),
            peak_mem.to_string(),
            avg_mem.to_string(),
            peak_garb.to_string(),
            avg_garb.to_string(),
            (config.prefill * 2).to_string(),
        ])
        .unwrap();
    output.flush().unwrap();
    println!(
        "ops/s: {}, peak mem: {}, avg_mem: {}, peak garb: {}, avg garb: {}",
        ops_per_sec, peak_mem, avg_mem, peak_garb, avg_garb
    );
}

#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefillStrategy {
    Random,
    Decreasing,
}

impl PrefillStrategy {
    fn prefill_ebr<M: ebr::ConcurrentMap<usize, String> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = unsafe { crossbeam_ebr::unprotected() };
        let mut rng = rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(&mut rng);
                    let value = key.to_string();
                    map.insert(key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(&mut rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.to_string();
                    map.insert(key, value, guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_pebr<M: pebr::ConcurrentMap<usize, String> + Send + Sync>(
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
                    let key = config.key_dist.sample(&mut rng);
                    let value = key.to_string();
                    map.insert(&mut handle, key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(&mut rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.to_string();
                    map.insert(&mut handle, key, value, guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_hp<M: hp::ConcurrentMap<usize, String> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let mut handle = M::handle();
        let mut rng = rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(&mut rng);
                    let value = key.to_string();
                    map.insert(&mut handle, key, value);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(&mut rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.to_string();
                    map.insert(&mut handle, key, value);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_cdrc<
        Guard: cdrc_rs::AcquireRetire,
        M: cdrc::ConcurrentMap<usize, String, Guard> + Send + Sync,
    >(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = &Guard::handle();
        let mut rng = rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(&mut rng);
                    let value = key.to_string();
                    map.insert(key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(&mut rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.to_string();
                    map.insert(key, value, guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_hp_sharp<M: hp_sharp_bench::ConcurrentMap<usize, String> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        hp_sharp::HANDLE.with(|handle| {
            let handle = &mut **handle.borrow_mut();
            let output = &mut M::empty_output(handle);
            let mut rng = rand::thread_rng();
            match self {
                PrefillStrategy::Random => {
                    for _ in 0..config.prefill {
                        let key = config.key_dist.sample(&mut rng);
                        let value = key.to_string();
                        map.insert(key, value, output, handle);
                    }
                }
                PrefillStrategy::Decreasing => {
                    let mut keys = Vec::with_capacity(config.prefill);
                    for _ in 0..config.prefill {
                        keys.push(config.key_dist.sample(&mut rng));
                    }
                    keys.sort_by(|a, b| b.cmp(a));
                    for key in keys.drain(..) {
                        let value = key.to_string();
                        map.insert(key, value, output, handle);
                    }
                }
            }
            print!("prefilled... ");
            stdout().flush().unwrap();
        });
    }

    fn prefill_nbr<M: nbr::ConcurrentMap<usize, String> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = unsafe { nbr_rs::unprotected() };
        let mut rng = rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(&mut rng);
                    let value = key.to_string();
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
                    let key = k;
                    let value = key.to_string();
                    map.insert(key, value, guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }
}

fn bench_map_nr(config: &Config, strategy: PrefillStrategy) -> (u64, usize, usize, usize, usize) {
    use ebr::ConcurrentMap;
    let map = &ebr::HHSList::new();
    strategy.prefill_ebr(config, map);

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
    let (ops_sender, ops_receiver) = mpsc::channel();
    let (mem_sender, mem_receiver) = mpsc::channel();

    scope(|s| {
        if config.aux_thread > 0 {
            let mem_sender = mem_sender.clone();
            s.spawn(move |_| {
                assert!(config.sampling);
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

                        let garbages = crossbeam_ebr::GLOBAL_GARBAGE_COUNT.load(Ordering::Acquire);
                        garb_acc += garbages;
                        garb_peak = max(garb_peak, garbages);

                        next_sampling = now + config.sampling_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
                }
                mem_sender
                    .send((peak, acc / samples, garb_peak, garb_acc / samples))
                    .unwrap();
            });
        } else {
            mem_sender.send((0, 0, 0, 0)).unwrap();
        }

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                barrier.clone().wait();
                let start = Instant::now();

                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(key, value, unsafe { crossbeam_ebr::leaking() }));
                    } else {
                        let (key, value) = map.pop(unsafe { crossbeam_ebr::leaking() }).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&key, unsafe { crossbeam_ebr::leaking() });
                    ops += 1;
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_ebr<N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    use ebr::ConcurrentMap;
    let map = &ebr::HHSList::new();
    strategy.prefill_ebr(config, map);

    let collector = &crossbeam_ebr::Collector::new();

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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

                        let garbages = crossbeam_ebr::GLOBAL_GARBAGE_COUNT.load(Ordering::Acquire);
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

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(key, value, &guard));
                    } else {
                        let (key, value) = map.pop(&guard).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        drop(guard);
                        guard = handle.pin();
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&key, &guard);
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
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_pebr<N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    use pebr::ConcurrentMap;
    let map = &pebr::HHSList::new();
    strategy.prefill_pebr(config, map);

    let collector = &crossbeam_pebr::Collector::new();

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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

                        let garbages = crossbeam_pebr::GLOBAL_GARBAGE_COUNT.load(Ordering::Acquire);
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

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let handle = collector.register();
                let mut map_handle = pebr::HHSList::handle(&handle.pin());
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(&mut map_handle, key, value, &mut guard));
                    } else {
                        let (key, value) = map.pop(&mut map_handle, &mut guard).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        pebr::HHSList::clear(&mut map_handle);
                        guard.repin();
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                let handle = collector.register();
                let mut map_handle = pebr::HHSList::handle(&handle.pin());
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&mut map_handle, &key, &mut guard);
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        pebr::HHSList::clear(&mut map_handle);
                        guard.repin();
                    }
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_hp(config: &Config, strategy: PrefillStrategy) -> (u64, usize, usize, usize, usize) {
    use hp::ConcurrentMap;
    let map = &hp::HMList::new();
    strategy.prefill_hp(config, map);

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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

                        let garbages = DEFAULT_DOMAIN.num_garbages();
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

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                let mut map_handle = hp::HMList::<usize, String>::handle();
                barrier.clone().wait();
                let start = Instant::now();

                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(&mut map_handle, key, value));
                    } else {
                        let (key, value) = map.pop(&mut map_handle).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                let mut map_handle = hp::HMList::<usize, String>::handle();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&mut map_handle, &key);
                    ops += 1;
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_hp_pp(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    use hp::ConcurrentMap;
    let map = &hp_pp::HHSList::new();
    strategy.prefill_hp(config, map);

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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

                        let garbages = DEFAULT_DOMAIN.num_garbages();
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

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                let mut map_handle = hp_pp::HHSList::<usize, String>::handle();
                barrier.clone().wait();
                let start = Instant::now();

                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(&mut map_handle, key, value))
                    } else {
                        let (key, value) = map.pop(&mut map_handle).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                let mut map_handle = hp_pp::HHSList::<usize, String>::handle();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&mut map_handle, &key);
                    ops += 1;
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_cdrc<Guard: cdrc_rs::AcquireRetire, N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    use cdrc::ConcurrentMap;
    let map = &cdrc::HHSList::new();
    strategy.prefill_cdrc(config, map);

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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
                let mut _garb_acc = 0usize;
                let mut _garb_peak = 0usize;
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

                        // TODO: measure garbages for CDRC
                        // (Is it reasonable to measure garbages for reference counting?)

                        next_sampling = now + config.sampling_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender
                        .send((peak, acc / samples, _garb_peak, _garb_acc / samples))
                        .unwrap();
                } else {
                    mem_sender.send((0, 0, 0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0, 0, 0)).unwrap();
        }

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = Guard::handle();
                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(key, value, &guard));
                    } else {
                        let (key, value) = map.pop(&guard).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        drop(guard);
                        guard = Guard::handle();
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = Guard::handle();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&key, &guard);
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        drop(guard);
                        guard = Guard::handle();
                    }
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_hp_sharp(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    use hp_sharp_bench::concurrent_map::OutputHolder;
    use hp_sharp_bench::ConcurrentMap;
    let map = &hp_sharp_bench::HHSList::new();
    strategy.prefill_hp_sharp(config, map);

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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

                        let garbages = hp_sharp::GLOBAL_GARBAGE_COUNT.load(Ordering::Acquire);
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

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                hp_sharp::HANDLE.with(|handle| {
                    let handle = &mut **handle.borrow_mut();
                    let output =
                        &mut hp_sharp_bench::HHSList::<usize, String>::empty_output(handle);
                    barrier.clone().wait();
                    let start = Instant::now();

                    let mut acquired: Option<String> = None;
                    while start.elapsed() < config.duration {
                        if let Some(value) = acquired.take() {
                            assert!(map.insert(value.parse().unwrap(), value, output, handle))
                        } else {
                            assert!(map.pop(output, handle));
                            acquired = Some(output.output().clone());
                        }
                    }
                });
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                hp_sharp::HANDLE.with(|handle| {
                    let handle = &mut **handle.borrow_mut();
                    let output =
                        &mut hp_sharp_bench::HHSList::<usize, String>::empty_output(handle);
                    let mut ops: u64 = 0;
                    let mut rng = rand::thread_rng();
                    barrier.clone().wait();
                    let start = Instant::now();

                    while start.elapsed() < config.duration {
                        let key = config.key_dist.sample(&mut rng);
                        let _ = map.get(&key, output, handle);
                        ops += 1;
                    }

                    ops_sender.send(ops).unwrap();
                });
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_nbr(
    config: &Config,
    strategy: PrefillStrategy,
    max_hazptr_per_thread: usize,
) -> (u64, usize, usize, usize, usize) {
    use nbr::ConcurrentMap;
    let map = &nbr::HHSList::new();
    strategy.prefill_nbr(config, map);

    let collector = &nbr_rs::Collector::new(
        config.writers + config.readers,
        max_hazptr_per_thread,
    );

    let barrier = &Arc::new(Barrier::new(
        config.writers + config.readers + config.aux_thread,
    ));
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

                        let garbages = nbr_rs::count_garbages();
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

        // Spawn writer threads.
        for _ in 0..config.writers {
            s.spawn(move |_| {
                let guard = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut acquired = None;
                while start.elapsed() < config.duration {
                    if let Some((key, value)) = acquired.take() {
                        assert!(map.insert(key, value, &guard));
                    } else {
                        let (key, value) = map.pop(&guard).unwrap();
                        acquired = Some((key.clone(), value.clone()));
                    }
                }
            });
        }

        // Spawn reader threads.
        for _ in 0..config.readers {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let guard = collector.register();
                let mut ops: u64 = 0;
                let mut rng = rand::thread_rng();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(&mut rng);
                    let _ = map.get(&key, &guard);
                    ops += 1;
                }

                ops_sender.send(ops).unwrap();
            });
        }
    })
    .unwrap();
    println!("end");

    let mut ops = 0;
    for _ in 0..config.readers {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}
