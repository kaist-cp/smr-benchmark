# type: ignore
import pandas as pd
import warnings
import os
import matplotlib
import matplotlib.pyplot as plt

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")
long_running_is = list(range(10, 61, 10))
cpu_count = os.cpu_count()
ts = [cpu_count//2, cpu_count, cpu_count*2]

# raw column names
THREADS = "threads"
THROUGHPUT = "throughput"
PEAK_MEM = "peak_mem"
AVG_MEM = "avg_mem"
INTERVAL = "interval"

# legend
SMR_ONLY = "SMR\n"

EBR = "ebr"
NR = "nr"
HP = "hp"
CDRC_EBR = "cdrc-ebr"
CDRC_HP = "cdrc-hp"
CIRC_EBR = "circ-ebr"
CIRC_HP = "circ-hp"

SMR_ONLYs = [EBR, CDRC_EBR, CIRC_EBR]

color_triple = ["#E53629", "#2CD23E", "#4149C3"]
face_alpha = "DF"

line_shapes = {
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
}

mm_order = {
    EBR: 1,
    CDRC_EBR: 2,
    CIRC_EBR: 4,
}

def draw(name, data, y_value, y_label):
    plt.figure(figsize=(5, 4))

    for mm in sorted(list(set(data.mm)), key=lambda mm: mm_order[mm]):
        d = data[data.mm == mm].sort_values(by=[INTERVAL], axis=0)
        plt.plot(d[INTERVAL], d[y_value],
                 linewidth=3, markersize=15, **line_shapes[mm], zorder=30)

    plt.xlabel("Time interval to run (seconds)", fontsize=13)
    if y_label:
        plt.ylabel(y_label, fontsize=13)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.grid(alpha=0.5)
    plt.savefig(name, bbox_inches='tight')


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_rows', None)

    # avoid Type 3 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42

    os.makedirs(f'{RESULTS_PATH}/queue-long-running', exist_ok=True)

    # preprocess (map)
    data = pd.read_csv(f'{RESULTS_PATH}/' + "double-link-long-running.csv")
    data.throughput = data.throughput.map(lambda x: x / 1_000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 30))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 30))

    # take average of each runs
    avg = data.groupby(['mm', 'interval']).mean().reset_index()
    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)

    y_label = 'Avg memory usage (GiB)'
    name = f'{RESULTS_PATH}/queue-long-running/long-running-queue_avg_mem.pdf'
    draw(name, avg, AVG_MEM, y_label)
