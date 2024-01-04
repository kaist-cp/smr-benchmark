# type: ignore
import pandas as pd
from plotnine import *
import warnings
import os
import matplotlib
import math

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

FORMAL_NAMES = {
    HLIST: "HList",
    HMLIST: "HMList",
    HHSLIST: "HHSList",
    HASHMAP: "HashMap",
    NMTREE: "NMTree",
    SKIPLIST: "SkipList",
}

EBR = "ebr"
PEBR = "pebr"
NR = "nr"
HP = "hp"
HP_PP = "hp-pp"
NBR = "nbr"
NBR_LARGE = "nbr-large"
HP_SHARP = "hp-sharp"
HP_SHARP_0 = "hp-sharp-0"
VBR = "vbr"

# DS with read-dominated bench & write-only bench
dss_all   = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, SKIPLIST]
dss_read  = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, SKIPLIST]
dss_write = [HLIST, HMLIST,          HASHMAP, NMTREE, SKIPLIST]

WRITE, HALF, READ, READ_ONLY = "write", "half", "read", "read-only"

SMR_ONLYs = [NR, EBR, NBR, NBR_LARGE, HP_PP, HP, PEBR, HP_SHARP, HP_SHARP_0, VBR]

cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = [1] + list(range(4, 33, 4))
elif cpu_count <= 64:
    ts = [1] + list(range(8, 129, 8))
else:
    ts = [1] + list(range(12, 193, 12))
n_map = {0: ''}

# https://matplotlib.org/stable/api/markers_api.html
line_shapes = {
    NR: '.',
    EBR: 'o',
    HP: 'v',
    HP_PP: 'D',
    PEBR: "x",
    NBR: "p",
    NBR_LARGE: "H",
    HP_SHARP: "s",
    HP_SHARP_0: "P",
    VBR: "*",
}

line_colors = {
    NR: 'k',
    EBR: 'c',
    HP: 'hotpink',
    HP_PP: 'purple',
    PEBR: "y",
    NBR: "blue",
    NBR_LARGE: "indigo",
    HP_SHARP: "r",
    HP_SHARP_0: "green",
    VBR: "orange",
}

line_types = {
    NR: '-',
    EBR: 'dotted',
    HP: 'dashed',
    HP_PP: 'dashdot',
    PEBR: (5, (10, 3)),
    NBR: (5, (10, 3)),
    NBR_LARGE: (5, (10, 3)),
    HP_SHARP: (0, (3, 1, 1, 1)),
    HP_SHARP_0: (0, (3, 1, 1, 1)),
    VBR: (0, (2, 1)),
}

def filter_invalid_data(data, ds):
    if ds == NMTREE:
        data = data[data.mm != HP]
    if ds == HMLIST:
        data = data[data.mm != NBR]
        data = data[data.mm != NBR_LARGE]
    if ds == SKIPLIST:
        data = data[data.mm != NBR]
        data = data[data.mm != NBR_LARGE]
    return data

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

def draw(title, name, data, line_name, y_value, y_label=None, y_max=None, legend=False, y_log=False, y_min=None):
    print(name)
    p = ggplot(
            data,
            aes(x=THREADS, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Threads') + geom_point(size=7) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=ts) + \
        labs(title = title) + theme(plot_title = element_text(size=36))

    if y_log:
        p += scale_y_continuous(trans='log10')

    p += theme(
            axis_title_x=element_text(size=15),
            axis_text_x=element_text(size=14),
            axis_text_y=element_text(size=14))
    if y_label:
        p += ylab(y_label)
        p += theme(axis_title_y=element_text(size=14))
    else:
        p += theme(axis_title_y=element_blank())

    y_min = y_min if y_min != None else 0
    if y_max:
        p += coord_cartesian(ylim=(y_min, y_max))
    if legend:
        # HACK: `\n` at the end of legend title
        p += theme(legend_title=element_text(size=18, linespacing=1.5))
        p += theme(legend_key_size=15)
        p += theme(legend_text=element_text(size=18))
        p += theme(legend_entry_spacing=15)
    else:
        p += theme(legend_position='none')
    
    p += annotate(geom="rect", xmin=cpu_count, xmax=float("inf"), ymin=-float("inf"), ymax=float("inf"), fill = "#FF00000A")

    p.save(name, width=10, height=7, units="in")

def draw_throughput(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    data = data[data.non_coop == 0]
    data = filter_invalid_data(data, ds)
    y_label = 'Throughput (M op/s)'
    legend = False
    y_max = data.throughput.max() * 1.05
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_throughput.pdf',
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend)

def draw_mem(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    if ds == NMTREE:
        data = data[data.mm != HP]
    y_label = 'Peak memory usage (MiB)'
    y_max = None
    legend = False
    _d = data[~data[SMR_ONLY].isin([NR])]  # exclude NR and EBR stalled
    y_max = _d[_d.ds == ds].peak_mem.max() * 1.05
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_peak_mem.pdf',
         data, SMR_ONLY, PEAK_MEM, y_label, y_max, legend)

def draw_garb(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    data = filter_invalid_data(data, ds)
    y_label = 'Avg. Unreclaimed memory blocks (×10⁴)'
    legend = False

    nbr_max = data[data.mm == NBR].avg_garb.max()
    hp_pp_max = data[data.mm == HP_PP].avg_garb.max()
    y_max = max(0 if math.isnan(nbr_max) else nbr_max, 0 if math.isnan(hp_pp_max) else hp_pp_max) * 2

    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_avg_garb.pdf',
         data, SMR_ONLY, AVG_GARB, y_label, y_max, legend)


def draw_peak_garb(data, ds, bench, key_range):
    data = data[ds].copy()
    data = data[data.key_range == key_range]
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    data = filter_invalid_data(data, ds)
    y_label = 'Peak unreclaimed memory blocks (×10⁴)'
    legend = False

    nbr_max = data[data.mm == NBR].peak_garb.max()
    hp_pp_max = data[data.mm == HP_PP].peak_garb.max()
    y_max = max(0 if math.isnan(nbr_max) else nbr_max, 0 if math.isnan(hp_pp_max) else hp_pp_max) * 2

    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_{range_to_str(key_range)}_peak_garb.pdf',
         data, SMR_ONLY, PEAK_GARB, y_label, y_max, legend)


raw_data = {}
# averaged data for write:read = 100:0, 50:50, 10:90
avg_data = { WRITE: {}, HALF: {}, READ: {}, READ_ONLY: {} }

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
    data = data[data.mm.isin(SMR_ONLYs)]

    raw_data[ds] = data.copy()

    # take average of each runs
    avg = data.groupby(['ds', 'mm', 'threads', 'non_coop', 'get_rate', 'key_range']).mean().reset_index()

    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
    avg.sort_values(by=SMR_ONLY, inplace=True)
    for i, bench in enumerate([WRITE, HALF, READ, READ_ONLY]):
        avg_data[bench][ds] = avg[avg.get_rate == i]

# 1. throughput graphs, 3 lines (SMR_ONLY) each.
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_throughput(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_throughput(avg_data[HALF], ds, HALF, kr)
        draw_throughput(avg_data[READ], ds, READ, kr)

# 2. avg garbage graph, 7 lines (SMR_ONLY)
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_garb(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_garb(avg_data[HALF], ds, HALF, kr)
        draw_garb(avg_data[READ], ds, READ, kr)

# 3. peak mem graph
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_mem(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_mem(avg_data[HALF], ds, HALF, kr)
        draw_mem(avg_data[READ], ds, READ, kr)

# 4. peak garbage graph
for ds in dss_write:
    for kr in key_ranges(ds):
        draw_peak_garb(avg_data[WRITE], ds, WRITE, kr)
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_peak_garb(avg_data[HALF], ds, HALF, kr)
        draw_peak_garb(avg_data[READ], ds, READ, kr)

# 5. Read-only throughput
for ds in dss_read:
    for kr in key_ranges(ds):
        draw_throughput(avg_data[READ_ONLY], ds, READ_ONLY, kr)
