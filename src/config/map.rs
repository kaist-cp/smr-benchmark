use crate::MemSampler;
use clap::{value_parser, Arg, ArgAction, Command, ValueEnum};
use csv::Writer;
use rand::distributions::{Uniform, WeightedIndex};
use std::fmt;
use std::fs::{create_dir_all, File, OpenOptions};
use std::path::Path;
use std::time::Duration;

#[derive(PartialEq, Debug, ValueEnum, Clone)]
pub enum DS {
    HList,
    HMList,
    HHSList,
    HashMap,
    NMTree,
    BonsaiTree,
    EFRBTree,
    SkipList,
    ElimAbTree,
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

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum BagSize {
    Small,
    Large,
}

impl fmt::Display for BagSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BagSize::Small => write!(f, "small"),
            BagSize::Large => write!(f, "large"),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Op {
    Get,
    Insert,
    Remove,
}

impl Op {
    pub const OPS: [Op; 3] = [Op::Get, Op::Insert, Op::Remove];
}

#[derive(Clone, Copy, PartialEq)]
pub enum GetRate {
    WriteOnly = 0,
    ReadWrite = 1,
    ReadIntensive = 2,
    ReadOnly = 3,
}

pub struct Config {
    pub ds: DS,
    pub mm: String,
    pub threads: usize,
    pub bag_size: BagSize,

    pub aux_thread: usize,
    pub aux_thread_period: Duration,
    pub non_coop: u8,
    pub non_coop_period: Duration,
    pub sampling: bool,
    pub sampling_period: Duration,

    pub get_rate: GetRate,
    pub op_dist: WeightedIndex<i32>,
    pub key_dist: Uniform<usize>,
    pub prefill: usize,
    pub key_range: usize,
    pub interval: u64,
    pub duration: Duration,
    pub ops_per_cs: OpsPerCs,

    pub mem_sampler: MemSampler,
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} threads, n{}, c{}, g{}, {} bag",
            self.ds.to_possible_value().unwrap().get_name(),
            self.threads,
            self.non_coop,
            self.ops_per_cs,
            self.get_rate as u8,
            self.bag_size,
        )
    }
}

pub struct BenchWriter {
    output: Option<Writer<File>>,
}

#[derive(Clone)]
pub struct Perf {
    pub ops_per_sec: u64,
    pub peak_mem: usize,
    pub avg_mem: usize,
    pub peak_garb: usize,
    pub avg_garb: usize,
}

impl fmt::Display for Perf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ops/s: {}, peak mem: {}, avg_mem: {}, peak garb: {}, avg garb: {}",
            self.ops_per_sec,
            readable_bytes(self.peak_mem),
            readable_bytes(self.avg_mem),
            self.peak_garb,
            self.avg_garb
        )
    }
}

fn readable_bytes(num: usize) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    for (i, unit) in UNITS.iter().enumerate() {
        if num / 2usize.pow(i as u32 * 10) < 1000 {
            return format!("{:.3} {}", num as f64 / 2f64.powf(i as f64 * 10.0), unit);
        }
    }
    format!(
        "{:.3} {}",
        num as f64 / 2f64.powf((UNITS.len() - 1) as f64 * 10.0),
        UNITS.last().unwrap()
    )
}

impl BenchWriter {
    pub fn write_record(self, config: &Config, perf: &Perf) {
        if let Some(mut output) = self.output {
            output
                .write_record(&[
                    // chrono::Local::now().to_rfc3339(),
                    config
                        .ds
                        .to_possible_value()
                        .unwrap()
                        .get_name()
                        .to_string(),
                    config.mm.clone(),
                    config.threads.to_string(),
                    config.bag_size.to_string(),
                    config.sampling_period.as_millis().to_string(),
                    config.non_coop.to_string(),
                    (config.get_rate as u8).to_string(),
                    config.ops_per_cs.to_string(),
                    perf.ops_per_sec.to_string(),
                    perf.peak_mem.to_string(),
                    perf.avg_mem.to_string(),
                    perf.peak_garb.to_string(),
                    perf.avg_garb.to_string(),
                    config.key_range.to_string(),
                    config.interval.to_string(),
                ])
                .unwrap();
            output.flush().unwrap();
        }
    }
}

pub fn setup(mm: String) -> (Config, BenchWriter) {
    let m = Command::new(mm.clone())
        .arg(
            Arg::new("data structure")
                .short('d')
                .value_parser(value_parser!(DS))
                .required(true)
                .ignore_case(true)
                .help("Data structure(s)"),
        )
        .arg(
            Arg::new("threads")
                .short('t')
                .value_parser(value_parser!(usize))
                .required(true)
                .help("Numbers of threads to run."),
        )
        .arg(
            Arg::new("non-coop")
                .short('n')
                .help(
                    "The degree of non-cooperation (available on EBR and PEBR). \
                     1: 1ms, 2: 10ms, 3: stall",
                )
                .value_parser(value_parser!(u8).range(0..4))
                .default_value("0"),
        )
        .arg(
            Arg::new("get rate")
                .short('g')
                .help(
                    "The proportion of `get`(read) operations. \
                     0: 0%, 1: 50%, 2: 90%, 3: 100%",
                )
                .value_parser(value_parser!(u8).range(0..4))
                .default_value("0"),
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
        .arg(
            Arg::new("ops per cs")
                .short('c')
                .value_parser(["1", "4"])
                .help("Operations per each critical section")
                .default_value("1"),
        )
        .arg(
            Arg::new("bag size")
                .short('b')
                .value_parser(["small", "large"])
                .help("The size of deferred bag")
                .default_value("small"),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .help("Output CSV filename. Appends the data if the file already exists."),
        )
        .arg(
            Arg::new("dry run")
                .long("dry-run")
                .action(ArgAction::SetTrue)
                .help("Check whether the arguments are parsable, without running a benchmark"),
        )
        .get_matches();

    let ds = m.get_one::<DS>("data structure").cloned().unwrap();
    let threads = m.get_one::<usize>("threads").copied().unwrap();
    let bag_size = match m.get_one::<String>("bag size").unwrap().as_str() {
        "small" => BagSize::Small,
        "large" => BagSize::Large,
        _ => unreachable!("bag_size should be small or large"),
    };
    let non_coop = m.get_one::<u8>("non-coop").copied().unwrap();
    let get_rate = match m.get_one::<u8>("get rate").copied().unwrap() {
        0 => GetRate::WriteOnly,
        1 => GetRate::ReadWrite,
        2 => GetRate::ReadIntensive,
        3 => GetRate::ReadOnly,
        _ => unreachable!("get_rate is invalid"),
    };
    let key_range = m.get_one::<usize>("range").copied().unwrap();
    let prefill = key_range / 2;
    let key_dist = Uniform::from(0..key_range);
    let interval = m.get_one::<u64>("interval").copied().unwrap();
    let sampling_period = m.get_one::<u64>("sampling period").copied().unwrap();
    let sampling = sampling_period > 0 && cfg!(all(not(feature = "sanitize"), target_os = "linux"));
    let ops_per_cs = match m.get_one::<String>("ops per cs").unwrap().as_str() {
        "1" => OpsPerCs::One,
        "4" => OpsPerCs::Four,
        _ => unreachable!("ops_per_cs should be one or four"),
    };
    let duration = Duration::from_secs(interval);

    let op_weights = match get_rate {
        GetRate::WriteOnly => &[0, 1, 1],
        GetRate::ReadWrite => &[2, 1, 1],
        GetRate::ReadIntensive => &[18, 1, 1],
        GetRate::ReadOnly => &[1, 0, 0],
    };
    let op_dist = WeightedIndex::new(op_weights).unwrap();

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
                        // "timestamp",
                        "ds",
                        "mm",
                        "threads",
                        "bag_size",
                        "sampling_period",
                        "non_coop",
                        "get_rate",
                        "ops_per_cs",
                        "throughput",
                        "peak_mem",
                        "avg_mem",
                        "peak_garb",
                        "avg_garb",
                        "key_range",
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
        ds,
        mm,
        threads,
        bag_size,

        aux_thread: if sampling || non_coop > 0 { 1 } else { 0 },
        aux_thread_period: Duration::from_millis(1),
        non_coop,
        non_coop_period: match non_coop {
            1 => Duration::from_millis(1),
            2 => Duration::from_millis(10),
            // No repin if -n0 or -n3
            _ => Duration::from_secs(interval),
        },
        sampling,
        sampling_period: Duration::from_millis(sampling_period),

        get_rate,
        op_dist,
        key_dist,
        prefill,
        key_range,
        interval,
        duration,
        ops_per_cs,

        mem_sampler,
    };

    if m.get_flag("dry run") {
        std::process::exit(0);
    }

    (config, BenchWriter { output })
}
