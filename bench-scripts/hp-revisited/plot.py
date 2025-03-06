import pandas as pd
import warnings
import os, math
import matplotlib
import matplotlib.pyplot as plt
from legends import *

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)

# avoid Type 3 fonts
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

# raw column names
THREADS = "threads"
THROUGHPUT = "throughput"
PEAK_MEM = "peak_mem"
AVG_GARB = "avg_garb"
PEAK_GARB = "peak_garb"

# legend
SMR_ONLY = "SMR\n"

HLIST = "h-list"
HMLIST = "hm-list"
HHSLIST = "hhs-list"
HASHMAP = "hash-map"
NMTREE = "nm-tree"
SKIPLIST = "skip-list"
ELIMABTREE = "elim-ab-tree"

FORMAL_NAMES = {
    HLIST: "HList",
    HMLIST: "HMList",
    HHSLIST: "HHSList",
    HASHMAP: "HashMap",
    NMTREE: "NMTree",
    SKIPLIST: "SkipList",
    ELIMABTREE: "ElimAbTree",
}

# DS with read-dominated bench & write-only bench
dss_all   = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, SKIPLIST, ELIMABTREE]
dss_read  = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, SKIPLIST, ELIMABTREE]
dss_write = [HLIST, HMLIST,          HASHMAP, NMTREE, SKIPLIST, ELIMABTREE]

WRITE, HALF, READ = "write", "half", "read"

cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = [1] + list(range(4, 33, 4))
elif cpu_count <= 64:
    ts = [1] + list(range(8, 129, 8))
else:
    ts = [1] + list(range(12, 193, 12))
n_map = {0: ''}

(label_size, xtick_size, ytick_size, marker_size) = (24, 20, 18, 20)

mm_order = [
    HP,
    HP_BRCU,
    EBR,
    HP_PP,
    PEBR,
    VBR,
    CDRC_HP,
    CIRC_HP,
    NR,
]

def plot_title(ds, bench):
    if bench == WRITE and ds == HLIST:
        return 'HList/HHSList'
    return FORMAL_NAMES[ds]

def key_ranges(ds):
    if ds in [HLIST, HMLIST, HHSLIST]:
        return [1000, 10000]
    else:
        return [100000, 100000000]

def range_to_str(kr: int):
    UNITS = ["K", "M", "B", "T"]
    for i in range(len(UNITS)):
        unit = pow(10, 3 * (i+1))
        div = kr // unit
        if div < 1000 or i == len(UNITS) - 1:
            return f"{div}{UNITS[i]}"

def draw(title, name, data, y_value, y_label=None, y_max=None, y_min=None):
    print(name)
    plt.figure(figsize=(10, 7))
    plt.title(title, fontsize=36, pad=15)

    for mm in sorted(list(set(data.mm)), key=lambda mm: len(mm_order) - mm_order.index(mm)):
        d = data[data.mm == mm].sort_values(by=[THREADS], axis=0)
        plt.plot(d[THREADS], d[y_value],
                 linewidth=3, markersize=marker_size, **line_shapes[mm], zorder=30)

    plt.xlabel("Threads", fontsize=label_size)
    plt.ylabel(y_label, fontsize=label_size)
    plt.yticks(fontsize=ytick_size)
    plt.xticks(ts, fontsize=xtick_size, rotation=90)
    plt.grid(alpha=0.5)

    if data.threads.max() >= cpu_count:
        left, right = plt.xlim()
        plt.axvspan(cpu_count, right, facecolor="#FF00000A")
        plt.xlim(left, right)

    y_max = min(y_max, data[y_value].max()) if y_max else data[y_value].max()
    y_min = max(y_min, data[y_value].min()) if y_min else data[y_value].min()
    y_margin = (y_max - y_min) * 0.05
    plt.ylim(y_min-y_margin, y_max+y_margin)

    plt.savefig(name, bbox_inches='tight')


def draw_throughput(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    data = data[data.non_coop == 0]
    y_label = 'Throughput (M op/s)'
    y_max = data.throughput.max() * 1.05
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_throughput.pdf',
         data, THROUGHPUT, y_label, y_max)


def draw_peak_garb(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    data = data[data.mm != CDRC_HP]
    data = data[data.mm != CIRC_HP]
    y_label = 'Peak unreclaimed nodes (×10⁴)'
    y_max = 0
    for cand in [HP_PP, HP_BRCU]:
        max_garb = data[data.mm == cand].peak_garb.max()
        if not math.isnan(max_garb):
            y_max = max(y_max, max_garb * 2)
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_peak_garb.pdf',
         data, PEAK_GARB, y_label, y_max)


def draw_peak_mem(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    y_label = 'Peak memory usage (MiB)'
    _d = data[~data[SMR_ONLY].isin([NR, EBR, VBR, CIRC_HP, CDRC_HP])]
    y_max = _d[_d.ds == ds].peak_mem.max()
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_peak_mem.pdf',
         data, PEAK_MEM, y_label, y_max)


raw_data = {}
# averaged data for write:read = 100:0, 50:50, 10:90
avg_data = { WRITE: {}, HALF: {}, READ: {} }

# preprocess
for ds in dss_all:
    data = pd.read_csv(f'{RESULTS_PATH}/' + ds + '.csv')

    data.throughput = data.throughput.map(lambda x: x / 1000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))
    data.peak_garb = data.peak_garb.map(lambda x: x / 10000)
    data.avg_garb = data.avg_garb.map(lambda x: x / 10000)
    data.mm = list(map(lambda tup: tup[0] if tup[1] == "small" else tup[0] + "-large", zip(data.mm, data.bag_size)))
    data = data.drop("bag_size", axis=1)
    data = data[data.mm.isin(SMRs)]

    raw_data[ds] = data.copy()

    # take average of each runs
    avg = data.groupby(['ds', 'mm', 'threads', 'non_coop', 'get_rate', 'key_range']).mean().reset_index()

    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMRs)
    avg.sort_values(by=SMR_ONLY, inplace=True)
    for i, bench in enumerate([WRITE, HALF, READ]):
        avg_data[bench][ds] = avg[avg.get_rate == i]

# 1. throughput graphs, 3 lines (SMR_ONLY) each.
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_throughput(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_throughput(avg_data[HALF], ds, HALF, kr)
        draw_throughput(avg_data[READ], ds, READ, kr)

# 2. peak garbage graph
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_peak_garb(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_peak_garb(avg_data[HALF], ds, HALF, kr)
        draw_peak_garb(avg_data[READ], ds, READ, kr)

# 3. peak memory graph
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_peak_mem(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_peak_mem(avg_data[HALF], ds, HALF, kr)
        draw_peak_mem(avg_data[READ], ds, READ, kr)
