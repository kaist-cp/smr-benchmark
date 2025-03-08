import pandas as pd
import warnings
import os, math
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as colors
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

EFRBTREE = "efrb-tree"
NMTREE = "nm-tree"

# DS with read-dominated bench & write-only bench
dss_all   = [NMTREE, EFRBTREE]

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

SMRs = [HP]
COMBs = [f"{HP}_{NMTREE}", f"{HP}_{EFRBTREE}"]

NMTREE_SHAPE = line_shapes[HP]
EFRBTREE_SHAPE = line_shapes[PESSIM_HP]

def plot_title(bench):
    return 'NMTree v.s. EFRBTree'

def range_to_str(kr: int):
    UNITS = ["K", "M", "B", "T"]
    for i in range(len(UNITS)):
        unit = pow(10, 3 * (i+1))
        div = kr // unit
        if div < 1000 or i == len(UNITS) - 1:
            return f"{div}{UNITS[i]}"

def draw(title, name, data, y_value, y_label=None, y_max=None, y_from_zero=False):
    print(name)
    plt.figure(figsize=(10, 7))
    plt.title(title, fontsize=36, pad=15)

    d = data[data.mm_ds == f"{HP}_{NMTREE}"].sort_values(by=[THREADS], axis=0)
    h1, = plt.plot(d[THREADS], d[y_value], label="HP; NMTREE",
             linewidth=3, markersize=marker_size, **NMTREE_SHAPE, zorder=30)
    
    d = data[data.mm_ds == f"{HP}_{EFRBTREE}"].sort_values(by=[THREADS], axis=0)
    h2, = plt.plot(d[THREADS], d[y_value], label="HP; EFRBTREE",
             linewidth=3, markersize=marker_size, **EFRBTREE_SHAPE, zorder=30)

    # plt.legend(handles=[h1, h2], fontsize=label_size, loc="lower right")

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
    y_min = 0 if y_from_zero else data[y_value].min()
    y_margin = (y_max - y_min) * 0.05
    plt.ylim(y_min, y_max+y_margin)

    plt.savefig(name, bbox_inches='tight')


def draw_throughput(data, bench):
    data = data.copy()
    y_label = 'Throughput (M op/s)'
    y_max = data.throughput.max() * 1.05
    draw(plot_title(bench), f'{RESULTS_PATH}/hp-trees_{bench}_throughput.pdf',
         data, THROUGHPUT, y_label, y_max, True)


def draw_peak_garb(data, bench):
    data = data.copy()
    y_label = 'Peak unreclaimed nodes (×10⁴)'
    y_max = data.peak_garb.max() * 1.05
    draw(plot_title(bench), f'{RESULTS_PATH}/hp-trees_{bench}_peak_garb.pdf',
         data, PEAK_GARB, y_label, y_max)


raw_data = {}
# averaged data for write:read = 100:0, 50:50, 10:90
avg_data = { WRITE: {}, HALF: {}, READ: {} }

# preprocess
csvs = []
for ds in dss_all:
    csvs.append(pd.read_csv(f'{RESULTS_PATH}/' + ds + '.csv'))

data = pd.concat(csvs)
data.throughput = data.throughput.map(lambda x: x / 1000_000)
data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))
data.peak_garb = data.peak_garb.map(lambda x: x / 10000)
data.avg_garb = data.avg_garb.map(lambda x: x / 10000)
data["mm_ds"] = list(map(lambda p: p[0] + "_" + p[1], zip(data.mm, data.ds)))
data.mm = list(map(lambda tup: tup[0] if tup[1] == "small" else tup[0] + "-large", zip(data.mm, data.bag_size)))
data = data[data.mm.isin(SMRs)]
data = data[data.key_range == 100000]
data = data.drop(["bag_size", "ds", "mm"], axis=1)

# take average of each runs
avg = data.groupby(['mm_ds', 'threads', 'non_coop', 'get_rate', 'key_range']).mean().reset_index()

avg[SMR_ONLY] = pd.Categorical(avg.mm_ds.map(str), COMBs)
avg.sort_values(by=SMR_ONLY, inplace=True)
for i, bench in [(0, WRITE), (1, HALF), (2, READ)]:
    avg_data[bench] = avg[avg.get_rate == i]

# 1. throughput graphs, 3 lines (SMR_ONLY) each.
draw_throughput(avg_data[WRITE], WRITE)
draw_throughput(avg_data[HALF], HALF)
draw_throughput(avg_data[READ], READ)

# 2. peak garbage graph
draw_peak_garb(avg_data[WRITE], WRITE)
draw_peak_garb(avg_data[HALF], HALF)
draw_peak_garb(avg_data[READ], READ)
