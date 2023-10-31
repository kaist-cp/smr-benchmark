# type: ignore
import pandas as pd
from plotnine import *
import warnings
import os
import matplotlib

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")

# raw column names
THREADS = "threads"
THROUGHPUT = "throughput"
PEAK_MEM = "peak_mem"
AVG_MEM = "avg_mem"

# legend
SMR_ONLY = "SMR\n"
SMR_I = "SMR, interf.\n"

HLIST = "h-list"
HMLIST = "hm-list"
HHSLIST = "hhs-list"
HASHMAP = "hash-map"
NMTREE = "nm-tree"
SKIPLIST = "skip-list"
DOUBLELINK = "double-link"

FORMAL_NAMES = {
    HLIST: "HList",
    HMLIST: "HMList",
    HHSLIST: "HHSList",
    HASHMAP: "HashMap",
    NMTREE: "NMTree",
    SKIPLIST: "SkipList",
    DOUBLELINK: "DoubleLink",
}

EBR = "ebr"
NR = "nr"
HP = "hp"
CDRC_EBR = "cdrc-ebr"
CDRC_HP = "cdrc-hp"
CIRC_EBR = "circ-ebr"
CIRC_HP = "circ-hp"

# DS with read-dominated bench & write-only bench
dss_all   = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, SKIPLIST]
dss_read  = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, SKIPLIST]
dss_write = [HLIST, HMLIST,          HASHMAP, NMTREE, SKIPLIST]

WRITE, HALF, READ = "write", "half", "read"

SMR_ONLYs = [NR, EBR, HP, CDRC_EBR, CDRC_HP, CIRC_EBR, CIRC_HP]
SMR_Is = [NR, EBR, HP, CDRC_EBR, CDRC_HP, CIRC_EBR, CIRC_HP]

cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = [1] + list(range(4, 33, 4))
elif cpu_count <= 64:
    ts = [1] + list(range(8, 129, 8))
else:
    ts = [1] + list(range(10, 151, 10))

# https://matplotlib.org/stable/api/markers_api.html
line_shapes = {
    NR: '.',
    EBR: 'o',
    HP: 'v',
    CDRC_EBR: "1",
    CDRC_HP: "2",
    CIRC_EBR: "X",
    CIRC_HP: "P",
}

# https://matplotlib.org/stable/gallery/color/named_colors.html
line_colors = {
    NR: 'k',
    EBR: 'c',
    HP: 'hotpink',
    CDRC_EBR: "green",
    CDRC_HP: "peru",
    CIRC_EBR: "blue",
    CIRC_HP: "purple",
}

line_types = {
    NR: '-',
    EBR: 'dotted',
    HP: 'dashed',
    CDRC_EBR: (5, (10, 3)),
    CDRC_HP: (5, (10, 3)),
    CIRC_EBR: (0, (3, 1)),
    CIRC_HP: (0, (3, 1)),
}

def filter_invalid_data(data, ds):
    if ds == NMTREE:
        data = data[data.mm != HP]
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
    
    p += annotate(geom="rect", xmin=cpu_count, xmax=float("inf"), ymin=-float("inf"), ymax=float("inf"), fill = "#FF00000A")

    p.save(name, width=10, height=7, units="in")

def draw_throughput(data, ds, bench, key_range):
    if key_range != None:
        data = data[data.key_range == key_range]
    data = filter_invalid_data(data, ds)
    y_label = 'Throughput (M op/s)'
    legend = True
    y_max = data.throughput.max() * 1.05
    props = "_".join(filter(lambda x: x != None, [
        ds,
        None if key_range == None else range_to_str(key_range),
        bench
    ]))
    name = f'{RESULTS_PATH}/{bench}/{props}_throughput.pdf'
    draw(plot_title(ds, bench), name,
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend)
    return name

def draw_peak_mem(data, ds, bench, key_range):
    if key_range != None:
        data = data[data.key_range == key_range]
    data = filter_invalid_data(data, ds)
    y_label = 'Peak memory usage (MiB)'
    y_max = None
    legend = True
    _d = data[~data[SMR_I].isin([NR])]  # exclude NR and EBR stalled
    y_max = _d[_d.ds == ds].peak_mem.max()
    y_min = _d[_d.ds == ds].peak_mem.min()
    y_interval = y_max - y_min
    y_max += y_interval * 0.05
    y_min = max(0, y_min - y_interval * 0.05)
    props = "_".join(filter(lambda x: x != None, [
        ds,
        None if key_range == None else range_to_str(key_range),
        bench
    ]))
    name = f'{RESULTS_PATH}/{bench}/{props}_peak_mem.pdf'
    draw(plot_title(ds, bench), name,
         data, SMR_I, PEAK_MEM, y_label, y_max, legend, y_min=y_min)
    return name

def draw_avg_mem(data, ds, bench, key_range):
    if key_range != None:
        data = data[data.key_range == key_range]
    data = filter_invalid_data(data, ds)
    y_label = 'Avg. memory usage (MiB)'
    y_max = None
    legend = True
    _d = data[~data[SMR_I].isin([NR])]  # exclude NR and EBR stalled
    y_max = _d[_d.ds == ds].avg_mem.max()
    y_min = _d[_d.ds == ds].avg_mem.min()
    y_interval = y_max - y_min
    y_max += y_interval * 0.05
    y_min = max(0, y_min - y_interval * 0.05)
    props = "_".join(filter(lambda x: x != None, [
        ds,
        None if key_range == None else range_to_str(key_range),
        bench
    ]))
    name = f'{RESULTS_PATH}/{bench}/{props}_avg_mem.pdf'
    draw(plot_title(ds, bench), name,
         data, SMR_I, AVG_MEM, y_label, y_max, legend, y_min=y_min)
    return name

def draw_task(descriptor):
    (kind, data, ds, bench, key_range) = descriptor
    if kind == THROUGHPUT:
        return draw_throughput(data, ds, bench, key_range)
    elif kind == PEAK_MEM:
        return draw_peak_mem(data, ds, bench, key_range)
    elif kind == AVG_MEM:
        return draw_avg_mem(data, ds, bench, key_range)
    else:
        raise Exception("invalid task")


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_rows', None)

    # avoid Type 3 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42


    for bench in [WRITE, HALF, READ]:
        os.makedirs(f'{RESULTS_PATH}/{bench}', exist_ok=True)

    n_map = {0: ''}

    raw_data = {}
    # averaged data for write:read = 100:0, 50:50, 10:90
    avg_data = { WRITE: {}, HALF: {}, READ: {} }

    # preprocess (map)
    for ds in dss_all:
        data = pd.read_csv(f'{RESULTS_PATH}/' + ds + '.csv')

        data.throughput = data.throughput.map(lambda x: x / 1_000_000)
        data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
        data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))

        raw_data[ds] = data.copy()

        # take average of each runs
        avg = data.groupby(['ds', 'mm', 'threads', 'non_coop', 'get_rate', 'key_range']).mean().reset_index()

        # sort by SMR_I
        avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
        avg[SMR_I] = pd.Categorical(avg.mm.map(str) + avg.non_coop.map(n_map), SMR_Is)
        avg.sort_values(by=SMR_I, inplace=True)
        for i, bench in enumerate([WRITE, HALF, READ]):
            avg_data[bench][ds] = avg[avg.get_rate == i]

    tasks = []

    # 1. throughput graphs, 3 lines (SMR_ONLY) each.
    for ds in dss_write:
        for kr in key_ranges(ds):
            tasks.append((THROUGHPUT, avg_data[WRITE][ds].copy(deep=True), ds, WRITE, kr))
    for ds in dss_read:
        for kr in key_ranges(ds):
            tasks.append((THROUGHPUT, avg_data[HALF][ds].copy(deep=True), ds, HALF, kr))
            tasks.append((THROUGHPUT, avg_data[READ][ds].copy(deep=True), ds, READ, kr))

    # 2. peak mem graph
    for ds in dss_write:
        for kr in key_ranges(ds):
            tasks.append((PEAK_MEM, avg_data[WRITE][ds].copy(deep=True), ds, WRITE, kr))
    for ds in dss_read:
        for kr in key_ranges(ds):
            tasks.append((PEAK_MEM, avg_data[HALF][ds].copy(deep=True), ds, HALF, kr))
            tasks.append((PEAK_MEM, avg_data[READ][ds].copy(deep=True), ds, READ, kr))

    # 3. avg mem graph
    for ds in dss_write:
        for kr in key_ranges(ds):
            tasks.append((AVG_MEM, avg_data[WRITE][ds].copy(deep=True), ds, WRITE, kr))
    for ds in dss_read:
        for kr in key_ranges(ds):
            tasks.append((AVG_MEM, avg_data[HALF][ds].copy(deep=True), ds, HALF, kr))
            tasks.append((AVG_MEM, avg_data[READ][ds].copy(deep=True), ds, READ, kr))

    # TODO: Parallelize generating plots.
    # Problem: generating with multiprocessing breaks some plots...
    #
    # from multiprocessing import Pool
    # with Pool(processes=os.cpu_count()) as pool:
    #     for name in pool.imap_unordered(draw_task, tasks):
    #         print(name)

    for task in tasks:
        print(draw_task(task))
