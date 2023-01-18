# type: ignore
import pandas as pd
from plotnine import *
import warnings
import os
import matplotlib
import math

RESULTS_PATH = "results"

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
SMR_I = "SMR, interf.\n"

HLIST = "HList"
HMLIST = "HMList"
HHSLIST = "HHSList"
HASHMAP = "HashMap"
NMTREE = "NMTree"
BONSAITREE = "BonsaiTree"
EFRBTREE = "EFRBTree"

EBR = "EBR"
PEBR = "PEBR"
NR = "NR"
HP = "HP"
HP_PP = "HP_PP"

# DS with read-dominated bench & write-only bench
dss_all   = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, EFRBTREE]
dss_read  = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, EFRBTREE]
dss_write = [HLIST, HMLIST,          HASHMAP, NMTREE, EFRBTREE]

WRITE, HALF, READ = "write", "half", "read"

SMR_ONLYs = [NR, EBR, HP, HP_PP]
SMR_Is = [NR, EBR, HP, HP_PP]

cpu_count = os.cpu_count()
ts = [1] + list(range(8, 145, 8))

n_map = {0: ''}

line_shapes = {
    NR: '.',
    EBR: 'o',
    HP: 'v',
    HP_PP: 'D'
}

line_colors = {
    NR: 'k',
    EBR: 'c',
    HP: 'hotpink',
    HP_PP: 'purple',
}

line_types = {
    NR: '-',
    EBR: '--',
    HP: '--',
    HP_PP: '--',
}

def plot_title(ds, bench):
    if bench == WRITE and ds == HLIST:
        return 'HList/HHSList'
    if ds == BONSAITREE:
        return 'Bonsai'
    return ds

def key_range(ds, large):
    if ds in ["HList", "HMList", "HHSList"]:
        if large:
            return 10000
        else:
            return 16
    else:
        if large:
            return 100000
        else:
            return 128

# line_name: SMR, SMR_I
def draw(title, name, data, line_name, y_value, y_label=None, y_max=None, legend=False, y_log=False, y_min=None):
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
    
    p += geom_rect(mapping=aes(xmin=64, xmax=float("inf"), ymin=-float("inf"), ymax=float("inf")), color = "#E0E0E0", alpha = 0.002)

    p.save(name, width=10, height=7, units="in")

def draw_throughput(data, ds, bench):
    data = data[ds].copy()
    data = data[data.key_range == key_range(ds, True)]
    data = data[data.non_coop == 0]
    if ds == NMTREE:
        data = data[data.mm != HP]
    y_label = 'Throughput (M op/s)' if ds in [HLIST, HASHMAP] else None
    legend = False
    y_max = data.throughput.max() * 1.05
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_throughput.pdf',
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend)

def draw_mem(data, ds, bench):
    data = data[ds].copy()
    data = data[data.key_range == key_range(ds, True)]
    if ds == NMTREE:
        data = data[data.mm != HP]
    y_label = 'Peak memory usage (MiB)' if ds in [HLIST, HASHMAP] else None
    y_max = None
    legend = False
    if ds == BONSAITREE:
        _d = data[~data[SMR_I].isin([NR])]  # exclude NR and EBR stalled
        max_threads = _d.threads.max()
        y_max = _d[_d.threads == max_threads].peak_mem.max() * 0.80
    elif ds in [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, EFRBTREE]:
        _d = data[~data[SMR_I].isin([NR])]  # exclude NR and EBR stalled
        y_max = _d[_d.ds == ds].peak_mem.max() * 1.05
    else:
        y_max = data.peak_mem.max() * 1.05
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_peak_mem.pdf',
         data, SMR_I, PEAK_MEM, y_label, y_max, legend)


def draw_garb_log(data, ds, bench):
    data = data[ds].copy()
    data = data[data.key_range == key_range(ds, True)]
    data = data[data.mm != "NR"]
    if ds == NMTREE:
        data = data[data.mm != HP]
    
    y_label = 'Avg. Unreclaimed memory blocks' if ds in [HLIST, HASHMAP] else None
    legend = False
    max_threads = data.threads.max()
    min_threads = data.threads.min()
    y_max = data[(data.threads == max_threads)].avg_garb.max()
    y_min = data[(data.threads == min_threads)].avg_garb.min()
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_avg_garb_log.pdf',
         data, SMR_ONLY, AVG_GARB, y_label, math.log10(y_max), legend, True, math.log10(y_min))


def draw_garb(data, ds, bench):
    data = data[ds].copy()
    data = data[data.key_range == key_range(ds, True)]
    data = data[data.mm != "NR"]
    if ds == NMTREE:
        data = data[data.mm != HP]
    y_label = 'Avg. Unreclaimed memory blocks'
    legend = False
    max_threads = data.threads.max()
    y_max = data[(data.threads == max_threads) & (data.mm == HP_PP)].avg_garb.max() * 1.7
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_avg_garb.pdf',
         data, SMR_ONLY, AVG_GARB, y_label, y_max, legend)


def draw_peak_garb_log(data, ds, bench):
    data = data[ds].copy()
    data = data[data.key_range == key_range(ds, True)]
    data = data[data.mm != "NR"]
    if ds == NMTREE:
        data = data[data.mm != HP]
    y_label = 'Peak unreclaimed memory blocks' if ds in [HLIST, HASHMAP] else None
    legend = False
    max_threads = data.threads.max()
    min_threads = data.threads.min()
    y_max = data[(data.threads == max_threads)].peak_garb.max()
    y_min = data[(data.threads == min_threads)].peak_garb.min()
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_peak_garb_log.pdf',
         data, SMR_ONLY, PEAK_GARB, y_label, math.log10(y_max), legend, True, math.log10(y_min))


def draw_peak_garb(data, ds, bench):
    data = data[ds].copy()
    data = data[data.key_range == key_range(ds, True)]
    data = data[data.mm != "NR"]
    if ds == NMTREE:
        data = data[data.mm != HP]
    y_label = 'Peak unreclaimed memory blocks'
    legend = False
    max_threads = data.threads.max()
    y_max = data[(data.threads == max_threads) & (data.mm == HP_PP)].peak_garb.max() * 2
    draw(plot_title(ds, bench), f'{RESULTS_PATH}/{ds}_{bench}_peak_garb.pdf',
         data, SMR_ONLY, PEAK_GARB, y_label, y_max, legend)


raw_data = {}
# averaged data for write:read = 100:0, 50:50, 10:90
avg_data = { WRITE: {}, HALF: {}, READ: {} }

# preprocess
for ds in dss_all:
    data = pd.read_csv(f'{RESULTS_PATH}/' + ds + '.csv')

    # for testing
    data = data[data.threads <= 80]

    data.throughput = data.throughput.map(lambda x: x / 1000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))

    # ignore -c4 data
    data = data[data.ops_per_cs == 1]
    # ignore -n1 data
    data = data[data.non_coop != 1]

    raw_data[ds] = data.copy()

    # take average of each runs
    avg = data.groupby(['ds', 'mm', 'threads', 'non_coop', 'get_rate', 'key_range']).mean().reset_index()

    # sort by SMR_I
    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
    avg[SMR_I] = pd.Categorical(avg.mm.map(str) + avg.non_coop.map(n_map), SMR_Is)
    avg.sort_values(by=SMR_I, inplace=True)
    for i, bench in enumerate([WRITE, HALF, READ]):
        avg_data[bench][ds] = avg[avg.get_rate == i]


# for ds in dss_all:
#     data = avg_data[HALF][ds].copy()
#     data = data[data.threads == 64]
#     hp = data[data.mm == "HP"]
#     hp_pp = data[data.mm == "HP_PP"]
#     ebr = data[data.mm == "EBR"]
#     print(ds)
#     hp = hp.mean().throughput
#     hp_pp = hp_pp.mean().throughput
#     ebr = ebr.mean().throughput
#     print(f"hp: {hp}, hp++: {hp_pp}, ebr: {ebr}")
#     print(f"((hp++) - (hp)) / (hp++) = {(hp_pp-hp) / hp_pp}")
#     print(f"((hp++) - (ebr)) / (hp++) = {(hp_pp-ebr) / hp_pp}")
#     print()
# exit(0)

# 1. throughput graphs, 3 lines (SMR_ONLY) each.
for ds in dss_write:
    draw_throughput(avg_data[WRITE], ds, WRITE)
for ds in dss_read:
    draw_throughput(avg_data[HALF], ds, HALF)
    draw_throughput(avg_data[READ], ds, READ)

# 2. avg garbage graph, 7 lines (SMR_ONLY)
for ds in dss_write:
    draw_garb(avg_data[WRITE], ds, WRITE)
    draw_garb_log(avg_data[WRITE], ds, WRITE)
for ds in dss_read:
    draw_garb(avg_data[HALF], ds, HALF)
    draw_garb(avg_data[READ], ds, READ)
    draw_garb_log(avg_data[HALF], ds, HALF)
    draw_garb_log(avg_data[READ], ds, READ)

# 3. peak mem graph
for ds in dss_write:
    draw_mem(avg_data[WRITE], ds, WRITE)
for ds in dss_read:
    draw_mem(avg_data[HALF], ds, HALF)
    draw_mem(avg_data[READ], ds, READ)

# 4. peak garbage graph
for ds in dss_write:
    draw_peak_garb(avg_data[WRITE], ds, WRITE)
    draw_peak_garb_log(avg_data[WRITE], ds, WRITE)
for ds in dss_read:
    draw_peak_garb(avg_data[HALF], ds, HALF)
    draw_peak_garb(avg_data[READ], ds, READ)
    draw_peak_garb_log(avg_data[HALF], ds, HALF)
    draw_peak_garb_log(avg_data[READ], ds, READ)