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
THROUGHPUT_RATIO = "throughput_ratio"
PEAK_MEM = "peak_mem"
PEAK_MEM_RATIO = "peak_mem_ratio"
AVG_MEM = "avg_mem"

# legend
SMR_ONLY = "SMR\n"
SMR_I = "SMR, interf.\n"

DOUBLELINK = "double-link"

FORMAL_NAMES = {
    DOUBLELINK: "DoubleLink",
}

EBR = "ebr"
NR = "nr"
CDRC_EBR = "cdrc-ebr"
CDRC_EBR_FLUSH = "cdrc-ebr-flush"
CIRC_EBR = "circ-ebr"

SMR_ONLYs = [NR, EBR, CDRC_EBR, CDRC_EBR_FLUSH, CIRC_EBR]

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
(label_size, xtick_size, ytick_size, marker_size) = (22, 22, 18, 18)

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
    CDRC_EBR_FLUSH: {
        "marker": "s",
        "color": "#828282",
        "markeredgewidth": 0.75,
        "markerfacecolor": "#828282" + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dashed",
    }
}

mm_order = {
    NR: 1,
    EBR: 4,
    CDRC_EBR: 2,
    CIRC_EBR: 3,
    CDRC_EBR_FLUSH: 5
}

def draw(name, data, y_value, y_label, y_max=None, y_min=None, y_log=False):
    plt.figure(figsize=(width, height))

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

    if y_log:
        plt.yscale("log")
    else:
        y_max = min(y_max, data[y_value].max()) if y_max else data[y_value].max()
        y_min = max(y_min, data[y_value].min()) if y_min else data[y_value].min()
        y_margin = (y_max - y_min) * 0.05
        plt.ylim(y_min-y_margin, y_max+y_margin)

    plt.savefig(name, bbox_inches='tight')


def draw_throughput(data):
    y_label = 'Throughput (M op/s)'
    name = f'{RESULTS_PATH}/queue/double-link_throughput.pdf'
    draw(name, data, THROUGHPUT, y_label)
    return name

def draw_throughput_ratio(data):
    y_label = 'Throughput ratio to RCU'
    name = f'{RESULTS_PATH}/queue/double-link_throughput_ratio.pdf'
    draw(name, data, THROUGHPUT_RATIO, y_label)
    return name

def draw_peak_mem(data):
    y_label = 'Peak memory usage (GiB)'
    name = f'{RESULTS_PATH}/queue/double-link_peak_mem.pdf'
    y_max = data[data.mm == CDRC_EBR].peak_mem.max()
    draw(name, data, PEAK_MEM, y_label, y_max=y_max)
    return name

def draw_peak_mem_ratio(data):
    y_label = 'Peak memory usage ratio to RCU'
    name = f'{RESULTS_PATH}/queue/double-link_peak_mem_ratio.pdf'
    draw(name, data, PEAK_MEM_RATIO, y_label)
    return name

def draw_peak_mem_ratio_log(data):
    y_label = 'Peak memory usage ratio to RCU'
    name = f'{RESULTS_PATH}/queue/double-link_peak_mem_ratio_log.pdf'
    draw(name, data, PEAK_MEM_RATIO, y_label, y_log=True)
    return name

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_rows', None)

    # avoid Type 3 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42

    os.makedirs(f'{RESULTS_PATH}/queue', exist_ok=True)

    data = pd.read_csv(f'{RESULTS_PATH}/{DOUBLELINK}.csv')
    data = data.drop(['bag_size'], axis=1, errors='ignore')
    data = data[data.mm.isin(SMR_ONLYs)]
    data = data[(1 == data.threads) | (8 <= data.threads)]
    data.throughput = data.throughput.map(lambda x: x / 1_000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 30))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 30))
    avg = data.groupby(['mm', 'threads']).mean().reset_index()

    base = avg[avg.mm == EBR].throughput.values
    for m in SMR_ONLYs:
        thr = avg[avg.mm == m].throughput.values
        avg.loc[avg.mm == m, THROUGHPUT_RATIO] = thr / base
    
    base = avg[avg.mm == EBR].peak_mem.values
    for m in SMR_ONLYs:
        mem = avg[avg.mm == m].peak_mem.values
        avg.loc[avg.mm == m, PEAK_MEM_RATIO] = mem / base

    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
    avg['ds'] = 'double-link'
    draw_throughput(avg)
    draw_throughput_ratio(avg)
    draw_peak_mem(avg)
    draw_peak_mem_ratio(avg)
    draw_peak_mem_ratio_log(avg)
