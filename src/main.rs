#[macro_use]
extern crate cfg_if;
extern crate clap;
extern crate csv;

extern crate crossbeam_ebr;
extern crate crossbeam_pebr;
extern crate smr_benchmark;

use ::hp_pp::DEFAULT_DOMAIN;
use cdrc_rs::{Cs, CsEBR, CsHP};
use clap::{value_parser, Arg, ArgMatches, Command, ValueEnum};
use crossbeam_utils::thread::scope;
use csv::Writer;
use hp_sharp::GlobalHPSharp;
use rand::distributions::{Uniform, WeightedIndex};
use rand::prelude::*;
use std::cmp::max;
use std::fmt;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{stdout, Write};
use std::mem::ManuallyDrop;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};
use typenum::{Unsigned, U1, U4};

use smr_benchmark::nbr;
use smr_benchmark::nr;
use smr_benchmark::pebr;
use smr_benchmark::{cdrc, ebr};
use smr_benchmark::{cdrc_hp_sharp, hp, hp_sharp as hp_sharp_bench};
use smr_benchmark::{hp_pp, vbr};

const NBR_CAP: NBRConfig = NBRConfig {
    bag_cap_pow2: 256,
    lowatermark: 32,
};

const NBR_LARGE_CAP: NBRConfig = NBRConfig {
    bag_cap_pow2: 8192,
    lowatermark: 1024,
};

struct NBRConfig {
    bag_cap_pow2: usize,
    lowatermark: usize,
}

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
}

#[derive(PartialEq, Debug, ValueEnum, Clone)]
#[allow(non_camel_case_types)]
pub enum MM {
    NR,
    EBR,
    PEBR,
    HP,
    HP_PP,
    NBR,
    NBR_LARGE,
    CDRC_EBR,
    CDRC_HP,
    HP_SHARP,
    CDRC_HP_SHARP,
    VBR,
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

    aux_thread: usize,
    aux_thread_period: Duration,
    non_coop: u8,
    non_coop_period: Duration,
    sampling: bool,
    sampling_period: Duration,

    get_rate: u8,
    op_dist: WeightedIndex<i32>,
    key_dist: Uniform<usize>,
    prefill: usize,
    interval: u64,
    duration: Duration,
    ops_per_cs: OpsPerCs,

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
            Arg::new("data structure")
                .short('d')
                .value_parser(value_parser!(DS))
                .required(true)
                .ignore_case(true)
                .help("Data structure(s)"),
        )
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
                .help("Numbers of threads to run."),
        )
        .arg(
            Arg::new("non-coop")
                .short('n')
                .help(
                    "The degree of non-cooperation. \
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
        .arg(Arg::new("output").short('o').help(
            "Output CSV filename. \
                     Appends the data if the file already exists.\n\
                     [default: results/<DS>.csv]",
        ))
        .get_matches();

    let (config, mut output) = setup(matches);
    match config.ops_per_cs {
        OpsPerCs::One => bench::<U1>(&config, &mut output),
        OpsPerCs::Four => bench::<U4>(&config, &mut output),
    }
}

fn setup(m: ArgMatches) -> (Config, Writer<File>) {
    let ds = m.get_one::<DS>("data structure").cloned().unwrap();
    let mm = m.get_one::<MM>("memory manager").cloned().unwrap();
    let threads = m.get_one::<usize>("threads").copied().unwrap();
    let non_coop = m.get_one::<u8>("non-coop").copied().unwrap();
    let get_rate = m.get_one::<u8>("get rate").copied().unwrap();
    let range = m.get_one::<usize>("range").copied().unwrap();
    let prefill = range / 2;
    let key_dist = Uniform::from(0..range);
    let interval = m.get_one::<u64>("interval").copied().unwrap();
    let sampling_period = m.get_one::<u64>("sampling period").copied().unwrap();
    let sampling = sampling_period > 0 && cfg!(all(not(feature = "sanitize"), target_os = "linux"));
    let ops_per_cs = match m.get_one::<String>("ops per cs").unwrap().as_str() {
        "1" => OpsPerCs::One,
        "4" => OpsPerCs::Four,
        _ => panic!("ops_per_cs should be one or four"),
    };
    let duration = Duration::from_secs(interval);

    let op_weights = match get_rate {
        0 => &[0, 1, 1],
        1 => &[2, 1, 1],
        2 => &[18, 1, 1],
        _ => &[1, 0, 0],
    };
    let op_dist = WeightedIndex::new(op_weights).unwrap();

    let output_name = m.get_one::<String>("output").cloned().unwrap_or(format!(
        "results/{}.csv",
        ds.to_possible_value().unwrap().get_name()
    ));
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
                    // "timestamp",
                    "ds",
                    "mm",
                    "threads",
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
                ])
                .unwrap();
            output.flush().unwrap();
            output
        }
    };
    let mem_sampler = MemSampler::new();
    let config = Config {
        ds,
        mm,
        threads,

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
        interval,
        duration,
        ops_per_cs,

        mem_sampler,
    };
    (config, output)
}

fn bench<N: Unsigned>(config: &Config, output: &mut Writer<File>) {
    println!(
        "{}: {}, {} threads, n{}, c{}, g{}",
        config.ds.to_possible_value().unwrap().get_name(),
        config.mm.to_possible_value().unwrap().get_name(),
        config.threads,
        config.non_coop,
        config.ops_per_cs,
        config.get_rate
    );
    let (ops_per_sec, peak_mem, avg_mem, peak_garb, avg_garb) = match config.mm {
        MM::NR => match config.ds {
            DS::HList => {
                bench_map_nr::<nr::HList<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::HMList => {
                bench_map_nr::<nr::HMList<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::HHSList => {
                bench_map_nr::<nr::HHSList<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_map_nr::<nr::HashMap<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_map_nr::<nr::NMTreeMap<usize, usize>>(config, PrefillStrategy::Random)
            }
            DS::SkipList => {
                bench_map_nr::<nr::SkipList<usize, usize>>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => {
                bench_map_nr::<nr::BonsaiTreeMap<usize, usize>>(config, PrefillStrategy::Random)
            }
            DS::EFRBTree => {
                bench_map_nr::<nr::EFRBTree<usize, usize>>(config, PrefillStrategy::Random)
            }
        },
        MM::EBR => match config.ds {
            DS::HList => {
                bench_map_ebr::<ebr::HList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HMList => {
                bench_map_ebr::<ebr::HMList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HHSList => {
                bench_map_ebr::<ebr::HHSList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_map_ebr::<ebr::HashMap<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_map_ebr::<ebr::NMTreeMap<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => bench_map_ebr::<ebr::BonsaiTreeMap<usize, usize>, N>(
                config,
                PrefillStrategy::Random,
            ),
            DS::EFRBTree => {
                bench_map_ebr::<ebr::EFRBTree<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::SkipList => {
                bench_map_ebr::<ebr::SkipList<usize, usize>, N>(config, PrefillStrategy::Random)
            }
        },
        MM::PEBR => match config.ds {
            DS::HList => {
                bench_map_pebr::<pebr::HList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HMList => {
                bench_map_pebr::<pebr::HMList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HHSList => bench_map_pebr::<pebr::HHSList<usize, usize>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HashMap => bench_map_pebr::<pebr::HashMap<usize, usize>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::NMTree => {
                bench_map_pebr::<pebr::NMTreeMap<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => bench_map_pebr::<pebr::BonsaiTreeMap<usize, usize>, N>(
                config,
                PrefillStrategy::Random,
            ),
            DS::EFRBTree => {
                bench_map_pebr::<pebr::EFRBTree<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::SkipList => {
                bench_map_pebr::<pebr::SkipList<usize, usize>, N>(config, PrefillStrategy::Random)
            }
        },
        MM::HP => match config.ds {
            DS::HMList => {
                bench_map_hp::<hp::HMList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_map_hp::<hp::HashMap<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_map_hp::<hp::NMTreeMap<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::EFRBTree => {
                bench_map_hp::<hp::EFRBTree<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::SkipList => {
                bench_map_hp::<hp::SkipList<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => {
                bench_map_hp::<hp::BonsaiTreeMap<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            _ => panic!("Unsupported(or unimplemented) data structure for HP"),
        },
        MM::HP_PP => match config.ds {
            DS::HList => {
                bench_map_hp::<hp_pp::HList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HMList => {
                bench_map_hp::<hp_pp::HMList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HHSList => {
                bench_map_hp::<hp_pp::HHSList<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_map_hp::<hp_pp::HashMap<usize, usize>, N>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_map_hp::<hp_pp::NMTreeMap<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::EFRBTree => {
                bench_map_hp::<hp_pp::EFRBTree<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::SkipList => {
                bench_map_hp::<hp_pp::SkipList<usize, usize>, N>(config, PrefillStrategy::Random)
            }
            DS::BonsaiTree => bench_map_hp::<hp_pp::BonsaiTreeMap<usize, usize>, N>(
                config,
                PrefillStrategy::Random,
            ),
        },
        MM::NBR => match config.ds {
            DS::HList => bench_map_nbr::<nbr::HList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
                &NBR_CAP,
                2,
            ),
            DS::HHSList => bench_map_nbr::<nbr::HHSList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
                &NBR_CAP,
                2,
            ),
            DS::HashMap => bench_map_nbr::<nbr::HashMap<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
                &NBR_CAP,
                2,
            ),
            DS::NMTree => bench_map_nbr::<nbr::NMTreeMap<usize, usize>>(
                config,
                PrefillStrategy::Random,
                &NBR_CAP,
                4,
            ),
            _ => panic!("Unsupported(or unimplemented) data structure for NBR"),
        },
        MM::NBR_LARGE => match config.ds {
            DS::HList => bench_map_nbr::<nbr::HList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
                &NBR_LARGE_CAP,
                2,
            ),
            DS::HHSList => bench_map_nbr::<nbr::HHSList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
                &NBR_LARGE_CAP,
                2,
            ),
            DS::HashMap => bench_map_nbr::<nbr::HashMap<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
                &NBR_LARGE_CAP,
                2,
            ),
            DS::NMTree => bench_map_nbr::<nbr::NMTreeMap<usize, usize>>(
                config,
                PrefillStrategy::Random,
                &NBR_LARGE_CAP,
                4,
            ),
            _ => panic!("Unsupported(or unimplemented) data structure for NBR"),
        },
        MM::CDRC_EBR => match config.ds {
            DS::HList => bench_map_cdrc::<CsEBR, cdrc::HList<usize, usize, cdrc_rs::CsEBR>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HMList => bench_map_cdrc::<CsEBR, cdrc::HMList<usize, usize, cdrc_rs::CsEBR>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HHSList => bench_map_cdrc::<CsEBR, cdrc::HHSList<usize, usize, cdrc_rs::CsEBR>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HashMap => bench_map_cdrc::<CsEBR, cdrc::HashMap<usize, usize, cdrc_rs::CsEBR>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::NMTree => {
                bench_map_cdrc::<CsEBR, cdrc::NMTreeMap<usize, usize, cdrc_rs::CsEBR>, N>(
                    config,
                    PrefillStrategy::Random,
                )
            }
            DS::SkipList => {
                bench_map_cdrc::<CsEBR, cdrc::SkipList<usize, usize, cdrc_rs::CsEBR>, N>(
                    config,
                    PrefillStrategy::Random,
                )
            }
            DS::BonsaiTree => bench_map_cdrc::<
                CsEBR,
                cdrc::BonsaiTreeMap<usize, usize, cdrc_rs::CsEBR>,
                N,
            >(config, PrefillStrategy::Random),
            DS::EFRBTree => {
                bench_map_cdrc::<CsEBR, cdrc::EFRBTree<usize, usize, cdrc_rs::CsEBR>, N>(
                    config,
                    PrefillStrategy::Random,
                )
            }
        },
        MM::CDRC_HP => match config.ds {
            DS::HList => bench_map_cdrc::<CsHP, cdrc::HList<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HMList => bench_map_cdrc::<CsHP, cdrc::HMList<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HHSList => bench_map_cdrc::<CsHP, cdrc::HHSList<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HashMap => bench_map_cdrc::<CsHP, cdrc::HashMap<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::NMTree => bench_map_cdrc::<CsHP, cdrc::NMTreeMap<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Random,
            ),
            DS::SkipList => bench_map_cdrc::<CsHP, cdrc::SkipList<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Random,
            ),
            DS::BonsaiTree => bench_map_cdrc::<
                CsHP,
                cdrc::BonsaiTreeMap<usize, usize, cdrc_rs::CsHP>,
                N,
            >(config, PrefillStrategy::Random),
            DS::EFRBTree => bench_map_cdrc::<CsHP, cdrc::EFRBTree<usize, usize, cdrc_rs::CsHP>, N>(
                config,
                PrefillStrategy::Random,
            ),
        },
        MM::HP_SHARP => match config.ds {
            DS::HList => bench_map_hp_sharp::<hp_sharp_bench::HList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HMList => bench_map_hp_sharp::<hp_sharp_bench::HMList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HHSList => bench_map_hp_sharp::<hp_sharp_bench::HHSList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HashMap => bench_map_hp_sharp::<hp_sharp_bench::HashMap<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::NMTree => bench_map_hp_sharp::<hp_sharp_bench::NMTreeMap<usize, usize>>(
                config,
                PrefillStrategy::Random,
            ),
            DS::SkipList => bench_map_hp_sharp::<hp_sharp_bench::SkipList<usize, usize>>(
                config,
                PrefillStrategy::Random,
            ),
            _ => panic!("Unsupported(or unimplemented) data structure for HP#"),
        },
        MM::CDRC_HP_SHARP => match config.ds {
            DS::HList => bench_map_hp_sharp::<cdrc_hp_sharp::HList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HMList => bench_map_hp_sharp::<cdrc_hp_sharp::HMList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HHSList => bench_map_hp_sharp::<cdrc_hp_sharp::HHSList<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::HashMap => bench_map_hp_sharp::<cdrc_hp_sharp::HashMap<usize, usize>>(
                config,
                PrefillStrategy::Decreasing,
            ),
            DS::NMTree => bench_map_hp_sharp::<cdrc_hp_sharp::NMTreeMap<usize, usize>>(
                config,
                PrefillStrategy::Random,
            ),
            DS::SkipList => bench_map_hp_sharp::<cdrc_hp_sharp::SkipList<usize, usize>>(
                config,
                PrefillStrategy::Random,
            ),
            _ => panic!("Unsupported(or unimplemented) data structure for CDRC HP#"),
        },
        MM::VBR => match config.ds {
            DS::HList => {
                bench_map_vbr::<vbr::HList<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::HMList => {
                bench_map_vbr::<vbr::HMList<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::HHSList => {
                bench_map_vbr::<vbr::HHSList<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::HashMap => {
                bench_map_vbr::<vbr::HashMap<usize, usize>>(config, PrefillStrategy::Decreasing)
            }
            DS::NMTree => {
                bench_map_vbr::<vbr::NMTreeMap<usize, usize>>(config, PrefillStrategy::Random)
            }
            DS::SkipList => {
                bench_map_vbr::<vbr::SkipList<usize, usize>>(config, PrefillStrategy::Random)
            }
            _ => panic!("Unsupported(or unimplemented) data structure for VBR"),
        },
    };
    output
        .write_record(&[
            // chrono::Local::now().to_rfc3339(),
            config
                .ds
                .to_possible_value()
                .unwrap()
                .get_name()
                .to_string(),
            config
                .mm
                .to_possible_value()
                .unwrap()
                .get_name()
                .to_string(),
            config.threads.to_string(),
            config.sampling_period.as_millis().to_string(),
            config.non_coop.to_string(),
            config.get_rate.to_string(),
            config.ops_per_cs.to_string(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefillStrategy {
    Random,
    Decreasing,
}

impl PrefillStrategy {
    fn prefill_nr<M: nr::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(key, value);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(key, value);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_ebr<M: ebr::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = unsafe { crossbeam_ebr::unprotected() };
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(key, value, guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_pebr<M: pebr::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let guard = unsafe { crossbeam_pebr::unprotected() };
        let mut handle = M::handle(guard);
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(&mut handle, key, value, guard);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(&mut handle, key, value, guard);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_hp<M: hp::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let mut handle = M::handle();
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(&mut handle, key, value);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(&mut handle, key, value);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_nbr<M: nbr::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
        max_hazptrs: usize,
    ) {
        let collector = &nbr_rs::Collector::new(1, 256, 32, max_hazptrs);
        let mut guard = collector.register();
        let mut handle = M::handle(&mut guard);
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(key, value, &mut handle, &guard);
                }
            }
            PrefillStrategy::Decreasing => {
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

    fn prefill_cdrc<C: Cs, M: cdrc::ConcurrentMap<usize, usize, C> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        let output = &mut M::empty_output();
        let cs = unsafe { &C::unprotected() };
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(key, value, output, cs);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(key, value, output, cs);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }

    fn prefill_hp_sharp<M: hp_sharp_bench::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
    ) {
        hp_sharp::THREAD.with(|handle| {
            let handle = &mut **handle.borrow_mut();
            let output = &mut M::empty_output(handle);
            let rng = &mut rand::thread_rng();
            match self {
                PrefillStrategy::Random => {
                    for _ in 0..config.prefill {
                        let key = config.key_dist.sample(rng);
                        let value = key.clone();
                        map.insert(key, value, output, handle);
                    }
                }
                PrefillStrategy::Decreasing => {
                    let mut keys = Vec::with_capacity(config.prefill);
                    for _ in 0..config.prefill {
                        keys.push(config.key_dist.sample(rng));
                    }
                    keys.sort_by(|a, b| b.cmp(a));
                    for key in keys.drain(..) {
                        let value = key.clone();
                        map.insert(key, value, output, handle);
                    }
                }
            }
            print!("prefilled... ");
            stdout().flush().unwrap();
        });
    }

    fn prefill_vbr<M: vbr::ConcurrentMap<usize, usize> + Send + Sync>(
        self,
        config: &Config,
        map: &M,
        local: &M::Local,
    ) {
        let rng = &mut rand::thread_rng();
        match self {
            PrefillStrategy::Random => {
                for _ in 0..config.prefill {
                    let key = config.key_dist.sample(rng);
                    let value = key.clone();
                    map.insert(key, value, local);
                }
            }
            PrefillStrategy::Decreasing => {
                let mut keys = Vec::with_capacity(config.prefill);
                for _ in 0..config.prefill {
                    keys.push(config.key_dist.sample(rng));
                }
                keys.sort_by(|a, b| b.cmp(a));
                for key in keys.drain(..) {
                    let value = key.clone();
                    map.insert(key, value, local);
                }
            }
        }
        print!("prefilled... ");
        stdout().flush().unwrap();
    }
}

fn bench_map_nr<M: nr::ConcurrentMap<usize, usize> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_nr(config, map);

    let barrier = &Arc::new(Barrier::new(config.threads + config.aux_thread));
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

                        next_sampling = now + config.sampling_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
                }
                mem_sender.send((peak, acc / samples, 0, 0)).unwrap();
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

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value);
                        }
                        Op::Remove => {
                            map.remove(&key);
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
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_ebr<M: ebr::ConcurrentMap<usize, usize> + Send + Sync, N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_ebr(config, map);

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
                let mut garb_acc = 0usize;
                let mut garb_peak = 0usize;
                let handle = collector.register();
                barrier.clone().wait();

                let start = Instant::now();
                // Immediately drop if no non-coop else keep it and repin periodically.
                let mut guard = ManuallyDrop::new(handle.pin());
                if config.non_coop == 0 {
                    unsafe { ManuallyDrop::drop(&mut guard) };
                }
                let mut next_sampling = start + config.sampling_period;
                let mut next_repin = start + config.non_coop_period;
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
                    if now > next_repin {
                        (*guard).repin();
                        next_repin = now + config.non_coop_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.non_coop > 0 {
                    unsafe { ManuallyDrop::drop(&mut guard) };
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
                let handle = collector.register();
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
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
    for _ in 0..config.threads {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_pebr<M: pebr::ConcurrentMap<usize, usize> + Send + Sync, N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_pebr(config, map);

    let collector = &crossbeam_pebr::Collector::new();

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
                let handle = collector.register();
                barrier.clone().wait();

                let start = Instant::now();
                // Immediately drop if no non-coop else keep it and repin periodically.
                let mut guard = ManuallyDrop::new(handle.pin());
                if config.non_coop == 0 {
                    unsafe { ManuallyDrop::drop(&mut guard) };
                }
                let mut next_sampling = start + config.sampling_period;
                let mut next_repin = start + config.non_coop_period;
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
                    if now > next_repin {
                        (*guard).repin();
                        next_repin = now + config.non_coop_period;
                    }
                    std::thread::sleep(config.aux_thread_period);
                }

                if config.non_coop > 0 {
                    unsafe { ManuallyDrop::drop(&mut guard) };
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
                let handle = collector.register();
                let mut map_handle = M::handle(&handle.pin());
                barrier.clone().wait();
                let start = Instant::now();

                let mut guard = handle.pin();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
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
                    if ops % N::to_u64() == 0 {
                        M::clear(&mut map_handle);
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
    for _ in 0..config.threads {
        let local_ops = ops_receiver.recv().unwrap();
        ops += local_ops;
    }
    let ops_per_sec = ops / config.interval;
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_hp<M: hp::ConcurrentMap<usize, usize> + Send + Sync, N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_hp(config, map);

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

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = &mut rand::thread_rng();
                let mut map_handle = M::handle();
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&mut map_handle, &key);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(&mut map_handle, key, value);
                        }
                        Op::Remove => {
                            map.remove(&mut map_handle, &key);
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
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_nbr<M: nbr::ConcurrentMap<usize, usize> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
    nbr_config: &NBRConfig,
    max_hazptrs: usize,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_nbr(config, map, max_hazptrs);

    let collector = &nbr_rs::Collector::new(
        config.threads,
        nbr_config.bag_cap_pow2,
        nbr_config.lowatermark,
        max_hazptrs,
    );

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
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_cdrc<C: Cs, M: cdrc::ConcurrentMap<usize, usize, C> + Send + Sync, N: Unsigned>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_cdrc(config, map);

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

        for _ in 0..config.threads {
            let ops_sender = ops_sender.clone();
            s.spawn(move |_| {
                let mut ops: u64 = 0;
                let mut rng = &mut rand::thread_rng();
                barrier.clone().wait();
                let start = Instant::now();

                let output = &mut M::empty_output();
                let mut cs = C::new();
                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, output, &cs);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, output, &cs);
                        }
                        Op::Remove => {
                            map.remove(&key, output, &cs);
                        }
                    }
                    ops += 1;
                    if ops % N::to_u64() == 0 {
                        cs.clear();
                    }
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
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_hp_sharp<M: hp_sharp_bench::ConcurrentMap<usize, usize> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let map = &M::new();
    strategy.prefill_hp_sharp(config, map);

    let collector = &hp_sharp::Global::new();

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

                        let garbages = collector.garbage_count();
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
                let handle = &mut collector.register();
                let output = &mut M::empty_output(handle);
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, output, handle);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, output, handle);
                        }
                        Op::Remove => {
                            map.remove(&key, output, handle);
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
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}

fn bench_map_vbr<M: vbr::ConcurrentMap<usize, usize> + Send + Sync>(
    config: &Config,
    strategy: PrefillStrategy,
) -> (u64, usize, usize, usize, usize) {
    let global = &M::global(config.prefill * 2);
    let local = &M::local(global);
    let map = &M::new(local);
    strategy.prefill_vbr(config, map, local);

    let collector = &hp_sharp::Global::new();

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

                        let garbages = collector.garbage_count();
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
                let local = &M::local(global);
                barrier.clone().wait();
                let start = Instant::now();

                while start.elapsed() < config.duration {
                    let key = config.key_dist.sample(rng);
                    match Op::OPS[config.op_dist.sample(&mut rng)] {
                        Op::Get => {
                            map.get(&key, local);
                        }
                        Op::Insert => {
                            let value = key.clone();
                            map.insert(key, value, local);
                        }
                        Op::Remove => {
                            map.remove(&key, local);
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
    let (peak_mem, avg_mem, garb_peak, garb_avg) = mem_receiver.recv().unwrap();
    (ops_per_sec, peak_mem, avg_mem, garb_peak, garb_avg)
}
