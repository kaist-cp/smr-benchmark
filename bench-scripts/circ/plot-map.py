# type: ignore
import pandas as pd
import warnings
import os, argparse
import matplotlib
import matplotlib.pyplot as plt

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

t_step, t_end = 0, 0
cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 12:
    t_step, t_end = 2, 16
elif cpu_count <= 24:
    t_step, t_end = 4, 32
elif cpu_count <= 64:
    t_step, t_end = 8, 128
else:
    t_step, t_end = 8, 192

parser = argparse.ArgumentParser()
parser.add_argument("-e", "--end", dest="end", type=int, default=t_end,
                    help="the maximum number in a sequence of the number of threads")
parser.add_argument("-t", "--step", dest="step", type=int, default=t_step,
                    help="the interval between adjacent pair in a sequence of the number of threads")
args = parser.parse_args()
t_end = args.end
t_step = args.step

ts = [1] + list(range(t_step, t_end + 1, t_step))
(width, height) = (10, 7) if len(ts) < 18 else (14, 10)
(label_size, xtick_size, ytick_size, marker_size) = (28, 22, 18, 18)

color_triple = ["#E53629", "#2CD23E", "#4149C3"]
face_alpha = "DF"

line_shapes = {
    NR: {
        "marker": ".",
        "color": "k",
        "linestyle": "-",
    },
    EBR: {
        "marker": "o",
        "color": color_triple[0],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[0] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "-",
    },
    CDRC_EBR: {
        "marker": "o",
        "color": color_triple[1],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[1] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dotted",
    },
    CIRC_EBR: {
        "marker": "o",
        "color": color_triple[2],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[2] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dashed",
    },
    HP: {
        "marker": "v",
        "color": color_triple[0],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[0] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "-",
    },
    CDRC_HP: {
        "marker": "v",
        "color": color_triple[1],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[1] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dotted",
    },
    CIRC_HP: {
        "marker": "v",
        "color": color_triple[2],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[2] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dashed",
    },
}

mm_order = {
    NR: 1,
    EBR: 6,
    CDRC_EBR: 2,
    CIRC_EBR: 4,
    HP: 7,
    CDRC_HP: 3,
    CIRC_HP: 5,
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


def draw(title, name, data, y_value, y_label, y_max=None, y_min=None):
    plt.figure(figsize=(width, height))
    plt.title(title, fontsize=36, pad=15)

    for mm in sorted(list(set(data.mm)), key=lambda mm: mm_order[mm]):
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
    if key_range != None:
        data = data[data.key_range == key_range]
    data = filter_invalid_data(data, ds)
    y_label = 'Throughput (M op/s)'
    props = "_".join(filter(lambda x: x != None, [
        ds,
        None if key_range == None else range_to_str(key_range),
        bench
    ]))
    name = f'{RESULTS_PATH}/{bench}/{props}_throughput.pdf'
    draw(plot_title(ds, bench), name, data, THROUGHPUT, y_label)
    return name

def draw_peak_mem(data, ds, bench, key_range):
    if key_range != None:
        data = data[data.key_range == key_range]
    data = filter_invalid_data(data, ds)
    y_label = 'Peak memory usage (MiB)'
    _d = data[~data[SMR_I].isin([NR])]  # exclude NR and EBR stalled
    y_max = _d[_d.ds == ds].peak_mem.max()
    y_min = _d[_d.ds == ds].peak_mem.min()
    props = "_".join(filter(lambda x: x != None, [
        ds,
        None if key_range == None else range_to_str(key_range),
        bench
    ]))
    name = f'{RESULTS_PATH}/{bench}/{props}_peak_mem.pdf'

    # Use GB for except list data structures
    if not (ds in [HLIST, HMLIST, HHSLIST]):
        y_label = 'Peak memory usage (GiB)'
        y_max /= 1024
        y_min /= 1024
        data.peak_mem /= 1024

    draw(plot_title(ds, bench), name, data, PEAK_MEM, y_label, y_max=y_max, y_min=y_min)
    return name

def draw_avg_mem(data, ds, bench, key_range):
    if key_range != None:
        data = data[data.key_range == key_range]
    data = filter_invalid_data(data, ds)
    y_label = 'Avg. memory usage (MiB)'
    _d = data[~data[SMR_I].isin([NR])]  # exclude NR and EBR stalled
    y_max = _d[_d.ds == ds].avg_mem.max()
    y_min = _d[_d.ds == ds].avg_mem.min()
    props = "_".join(filter(lambda x: x != None, [
        ds,
        None if key_range == None else range_to_str(key_range),
        bench
    ]))
    name = f'{RESULTS_PATH}/{bench}/{props}_avg_mem.pdf'

    # Use GB for except list data structures
    if not (ds in [HLIST, HMLIST, HHSLIST]):
        y_label = 'Peak memory usage (GiB)'
        y_max /= 1024
        y_min /= 1024
        data.avg_mem /= 1024

    draw(plot_title(ds, bench), name, data, AVG_MEM, y_label, y_max=y_max, y_min=y_min)
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

        data = data.drop(['bag_size'], axis=1, errors='ignore')
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
