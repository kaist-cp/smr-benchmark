extern crate clap;

use clap::{arg_enum, value_t, App, Arg, SubCommand};
use crossbeam_epoch::Guard;
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
        .get_matches();;
    let ds = value_t!(matches, "data structure", DS).unwrap();
    let map: Box<dyn ConcurrentMap<String, String>> = match ds {
        DS::List => Box::new(List::new()),
        DS::HashMap => Box::new(HashMap::with_capacity(30000)),
        DS::NMTree => Box::new(NMTreeMap::new()),
    };
}
