extern crate clap;
extern crate csv;

extern crate crossbeam_ebr;
extern crate smr_benchmark;

use circ::Cs;
use clap::{value_parser, Arg, ArgMatches, Command, ValueEnum};
use crossbeam_utils::thread::scope;
use csv::Writer;
use rand::distributions::Uniform;
use rand::prelude::*;
use std::cmp::max;
use std::fs::{create_dir_all, File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{compiler_fence, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use smr_benchmark::{ds_impl, MemSampler};

#[derive(PartialEq, Debug, ValueEnum, Clone)]
#[allow(non_camel_case_types)]
pub enum MM {
    NR,
    EBR,
    HP,
    CDRC_EBR,
    CDRC_HP,
    CDRC_EBR_FLUSH,
    CDRC_HP_FLUSH,
    CIRC_EBR,
    CIRC_HP,
}

struct Config {
    mm: MM,
    threads: usize,
    aux_thread: usize,
    aux_thread_period: Duration,
    sampling: bool,
    interval: u64,
    duration: Duration,
    mem_sampler: MemSampler,
    key_dist: Uniform<usize>,
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
            Arg::new("threads")
                .short('t')
                .value_parser(value_parser!(usize))
                .required(true)
                .help("Numbers of threads which perform enqueue and dequeue."),
        )
        .arg(
            Arg::new("interval")
                .short('i')
                .value_parser(value_parser!(u64))
                .help("Time interval in seconds to run the benchmark")
                .default_value("10"),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .help("Output CSV filename. Appends the data if the file already exists."),
        )
        .get_matches();

    let (config, mut output) = setup(matches);
    bench(&config, output.as_mut());
}

fn setup(m: ArgMatches) -> (Config, Option<Writer<File>>) {
    let mm = m.get_one::<MM>("memory manager").cloned().unwrap();
    let threads = m.get_one::<usize>("threads").copied().unwrap();
    let interval = m.get_one::<u64>("interval").copied().unwrap();
    let sampling = cfg!(all(not(feature = "sanitize"), target_os = "linux"));
    let duration = Duration::from_secs(interval);

    assert!(
        threads >= 1,
        "The number of threads must be greater than zero!"
    );

    let output = m.get_one::<String>("output").map(|output_name| {
        let output_path = Path::new(output_name);
        let dir = output_path.parent().unwrap();
        create_dir_all(dir).unwrap();
        match OpenOptions::new().read(true).append(true).open(output_path) {
            Ok(f) => csv::Writer::from_writer(f),
            Err(_) => {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(output_path)
                    .unwrap();
                let mut output = csv::Writer::from_writer(f);
                // NOTE: `write_record` on `bench`
                output
                    .write_record([
                        "mm",
                        "threads",
                        "throughput",
                        "peak_mem",
                        "avg_mem",
                        "interval",
                    ])
                    .unwrap();
                output.flush().unwrap();
                output
            }
        }
    });
    let mem_sampler = MemSampler::new();
    let config = Config {
        mm,
        threads,
        aux_thread: if sampling { 1 } else { 0 },
        aux_thread_period: Duration::from_millis(1),
        sampling,
        interval,
        duration,
        mem_sampler,
        key_dist: Uniform::from(0..100000),
    };
    (config, output)
}

fn bench(config: &Config, output: Option<&mut Writer<File>>) {
    println!(
        "{}: {} threads",
        config.mm.to_possible_value().unwrap().get_name(),
        config.threads,
    );
    let (ops_per_sec, peak_mem, avg_mem) = match config.mm {
        MM::NR => bench_queue_nr(config),
        MM::EBR => bench_queue_ebr(config),
        MM::HP => bench_queue_hp(config),
        MM::CDRC_EBR => bench_queue_cdrc::<cdrc::CsEBR>(config),
        MM::CDRC_HP => bench_queue_cdrc::<cdrc::CsHP>(config),
        MM::CDRC_EBR_FLUSH => bench_queue_cdrc_flush::<cdrc::CsEBR>(config),
        MM::CDRC_HP_FLUSH => bench_queue_cdrc_flush::<cdrc::CsHP>(config),
        MM::CIRC_EBR => bench_queue_circ_ebr(config),
        MM::CIRC_HP => bench_queue_circ_hp(config),
    };
    if let Some(output) = output {
        output
            .write_record(&[
                config
                    .mm
                    .to_possible_value()
                    .unwrap()
                    .get_name()
                    .to_string(),
                config.threads.to_string(),
                ops_per_sec.to_string(),
                peak_mem.to_string(),
                avg_mem.to_string(),
                config.interval.to_string(),
            ])
            .unwrap();
        output.flush().unwrap();
    }
    println!(
        "ops/s: {}, peak mem: {}, avg_mem: {}",
        ops_per_sec, peak_mem, avg_mem
    );
}

fn bench_queue_nr(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::nr::DoubleLink::new();

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let item = config.key_dist.sample(rng).to_string();
                    queue.enqueue(item);
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue().unwrap();
                    compiler_fence(Ordering::SeqCst);

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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}

fn bench_queue_ebr(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::ebr::DoubleLink::new();
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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                let dist = Uniform::new(0, 100000);
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let item = dist.sample(rng).to_string();
                    queue.enqueue(item, &guard);
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue(&guard).unwrap();
                    compiler_fence(Ordering::SeqCst);

                    ops += 1;
                    drop(guard);
                    guard = handle.pin();
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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}

fn bench_queue_hp(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::hp::DoubleLink::new();

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                let dist = Uniform::new(0, 100000);
                let mut handle = ds_impl::hp::double_link::Handle::default();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let item = dist.sample(rng).to_string();
                    queue.enqueue(item, &mut handle);
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue(&mut handle).unwrap();
                    compiler_fence(Ordering::SeqCst);

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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}

fn bench_queue_cdrc<C: cdrc::Cs>(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::cdrc::DoubleLink::<_, C>::new();

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                let dist = Uniform::new(0, 100000);
                let mut holder = ds_impl::cdrc::double_link::Holder::new();
                barrier.clone().wait();
                let start = Instant::now();

                let mut cs = C::new();
                while start.elapsed() < config.duration {
                    let item = dist.sample(rng).to_string();
                    queue.enqueue(item, &mut holder, &cs);
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue(&mut holder, &cs).unwrap();
                    compiler_fence(Ordering::SeqCst);

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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}

fn bench_queue_cdrc_flush<C: cdrc::Cs>(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::cdrc::DoubleLink::<_, C>::new();

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                let dist = Uniform::new(0, 100000);
                let mut holder = ds_impl::cdrc::double_link::Holder::new();
                barrier.clone().wait();
                let start = Instant::now();

                let mut cs = C::new();
                while start.elapsed() < config.duration {
                    let item = dist.sample(rng).to_string();
                    queue.enqueue(item, &mut holder, &cs);
                    cs.eager_reclaim();
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue(&mut holder, &cs).unwrap();
                    cs.eager_reclaim();
                    compiler_fence(Ordering::SeqCst);

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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}

fn bench_queue_circ_ebr(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::circ_ebr::DoubleLink::new();

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                let dist = Uniform::new(0, 100000);
                barrier.clone().wait();
                let start = Instant::now();

                let mut cs = circ::CsEBR::new();
                while start.elapsed() < config.duration {
                    let item = dist.sample(rng).to_string();
                    queue.enqueue(item, &cs);
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue(&cs).unwrap();
                    compiler_fence(Ordering::SeqCst);

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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}

fn bench_queue_circ_hp(config: &Config) -> (u64, usize, usize) {
    let queue = ds_impl::circ_hp::DoubleLink::new();

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
                barrier.clone().wait();

                let start = Instant::now();
                let mut next_sampling = start + Duration::from_millis(1);
                while start.elapsed() < config.duration {
                    let now = Instant::now();
                    if now > next_sampling {
                        let allocated = config.mem_sampler.sample();
                        samples += 1;

                        acc += allocated;
                        peak = max(peak, allocated);

                        next_sampling = now + Duration::from_millis(1);
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.sampling {
                    mem_sender.send((peak, acc / samples)).unwrap();
                } else {
                    mem_sender.send((0, 0)).unwrap();
                }
            });
        } else {
            mem_sender.send((0, 0)).unwrap();
        }

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            let queue = &queue;
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let rng = &mut rand::thread_rng();
                let dist = Uniform::new(0, 100000);
                let mut holder = ds_impl::circ_hp::double_link::Holder::new();
                barrier.clone().wait();
                let start = Instant::now();

                let mut cs = Cs::new();
                while start.elapsed() < config.duration {
                    let item = dist.sample(rng).to_string();
                    queue.enqueue(item, &mut holder, &cs);
                    compiler_fence(Ordering::SeqCst);
                    queue.dequeue(&mut holder, &cs).unwrap();
                    compiler_fence(Ordering::SeqCst);

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
    let (peak_mem, avg_mem) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem)
}
