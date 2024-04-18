import pandas as pd
import warnings
import os, math
import matplotlib
import matplotlib.pyplot as plt
from bisect import bisect_right
from legends import *

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)

# avoid Type 3 fonts
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

# legend
SMR_ONLY = "SMR\n"

# raw column names
KEY_RANGE = "key_range"
THROUGHPUT = "throughput"
PEAK_MEM = "peak_mem"
AVG_GARB = "avg_garb"
PEAK_GARB = "peak_garb"
SMR_PART = [NR, EBR, HP, NBR, HP_BRCU, HP_RCU]

(label_size, xtick_size, ytick_size) = (24, 20, 18)
(marker_size_def, marker_size_big) = (22, 32)

mm_order = [
    HP_BRCU,
    HP_RCU,
    EBR,
    HP,
    HP_PP,
    PEBR,
    NBR,
    NBR_LARGE,
    VBR,
    NR,
]

krs = [(1 << e) for e in range(18, 30, 1)]

def draw(name, data, y_value, y_label=None, y_max=None, y_min=None, x_label=None, marker_size=None):
    print(name)
    plt.figure(figsize=(10, 7))

    for mm in sorted(list(set(data.mm)), key=lambda mm: len(mm_order) - mm_order.index(mm)):
        d = data[data.mm == mm].sort_values(by=[KEY_RANGE], axis=0)
        plt.plot(d[KEY_RANGE], d[y_value],
                 linewidth=3, markersize=marker_size if marker_size else marker_size_def, **line_shapes[mm], zorder=30)

    plt.xlabel(x_label if x_label else "Key range", fontsize=label_size)
    plt.ylabel(y_label, fontsize=label_size)
    plt.yticks(fontsize=ytick_size)
    plt.xscale("log")
    curr_krs = krs[:bisect_right(krs, data[KEY_RANGE].max())]
    plt.xticks(curr_krs, [f"$2^{{{math.floor(math.log2(k))}}}$" for k in curr_krs], fontsize=xtick_size)
    plt.grid(alpha=0.5)

    y_max = min(y_max, data[y_value].max()) if y_max else data[y_value].max()
    y_min = max(y_min, data[y_value].min()) if y_min else data[y_value].min()
    y_margin = (y_max - y_min) * 0.05
    plt.ylim(y_min-y_margin, y_max+y_margin)

    plt.savefig(name, bbox_inches='tight')


def draw_throughput(data):
    data = data.copy()
    y_label = 'Throughput ratio to NR'
    draw(f'{RESULTS_PATH}/long-running-throughput.pdf',
         data, THROUGHPUT, y_label)

def draw_peak_garb(data, full):
    data = data.copy()
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    y_label = 'Peak unreclaimed blocks (×10³)'
    if full:
        y_max = data.peak_garb.max()
        tail = "-full"
    else:
        y_max = data[data.mm == HP_RCU].peak_garb.max() * 1.05
        tail = "-trunc"
    draw(f'{RESULTS_PATH}/long-running-peak-garb{tail}.pdf',
         data, PEAK_GARB, y_label, y_max=y_max)

def draw_throughput_partial(data):
    data = data.copy()
    data = data[data.mm.isin(SMR_PART)]
    data = data[data.key_range < krs[-4]]
    y_label = 'Throughput ratio to NR'
    draw(f'{RESULTS_PATH}/long-running-throughput-partial.pdf',
         data, THROUGHPUT, y_label, x_label="Length of a read operation", marker_size=marker_size_big)

def draw_peak_garb_partial(data):
    data = data.copy()
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    y_label = 'Peak unreclaimed blocks (×10³)'
    y_max = data[data.mm == EBR][data.key_range < krs[-7]].peak_garb.max() * 1.4
    data = data[data.mm.isin(SMR_PART)]
    data = data[data.key_range < krs[-4]]
    draw(f'{RESULTS_PATH}/long-running-peak-garb-partial.pdf',
         data, PEAK_GARB, y_label, y_max, x_label="Length of a read operation", marker_size=marker_size_big)

# preprocess
data = pd.read_csv(f'{RESULTS_PATH}/long-running.csv')

data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))
data.peak_garb = data.peak_garb.map(lambda x: x / 1000)
data.avg_garb = data.avg_garb.map(lambda x: x / 1000)

# take average of each runs
avg = data.groupby(['mm', 'key_range']).mean().reset_index()

baseline = avg[avg.mm == NR]
for kr in krs:
    base = baseline[baseline.key_range == kr].throughput.iloc[0]
    for m in SMRs:
        thr = avg[(avg.mm == m) & (avg.key_range == kr)].throughput.iloc[0]
        avg.loc[(avg.mm == m) & (avg.key_range == kr), "throughput"] = thr / base

avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMRs)
draw_throughput(avg)
draw_peak_garb(avg, True)
draw_peak_garb(avg, False)
draw_throughput_partial(avg)
draw_peak_garb_partial(avg)
