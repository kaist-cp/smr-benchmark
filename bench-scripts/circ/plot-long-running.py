# type: ignore
import pandas as pd
from plotnine import *
import warnings
import os
import matplotlib

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

SKIPLIST = "skip-list"

FORMAL_NAMES = {
    SKIPLIST: "SkipList",
}

EBR = "ebr"
NR = "nr"
HP = "hp"
CDRC_EBR = "cdrc-ebr"
CDRC_HP = "cdrc-hp"
CIRC_EBR = "circ-ebr"
CIRC_HP = "circ-hp"

# DS with read-dominated bench & write-only bench
dss_write = [SKIPLIST]

SMR_ONLYs = [EBR, HP, CDRC_EBR, CIRC_EBR, CDRC_HP, CIRC_HP]

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

def range_to_str(kr: int):
    UNITS = ["K", "M", "B", "T"]
    for i in range(len(UNITS)):
        unit = pow(10, 3 * (i+1))
        div = kr // unit
        if div < 1000 or i == len(UNITS) - 1:
            return f"{div}{UNITS[i]}"


def draw(title, name, data, line_name, y_value, y_label=None, y_max=None, legend=False, y_log=False, y_min=None):
    p = ggplot(
            data,
            aes(x=INTERVAL, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Time interval to run (seconds)') + geom_point(size=7) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=long_running_is) + \
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
    
    p.save(name, width=7, height=5, units="in")

def draw_peak_mem(data, threads):
    data = data[data.threads == threads]
    y_label = 'Peak memory usage (GiB)'
    name = f'{RESULTS_PATH}/long-running/long-running-{threads}_peak_mem.pdf'
    draw(f"{threads} Threads", name, data, SMR_ONLY, PEAK_MEM, y_label)
    return name


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_rows', None)

    # avoid Type 3 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42

    os.makedirs(f'{RESULTS_PATH}/long-running', exist_ok=True)

    # preprocess (map)
    data = pd.read_csv(f'{RESULTS_PATH}/' + "skip-list-long-running.csv")
    data.throughput = data.throughput.map(lambda x: x / 1_000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 30))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 30))

    # take average of each runs
    avg = data.groupby(["threads", 'ds', 'mm', 'interval', 'non_coop', 'get_rate', 'key_range']).mean().reset_index()
    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)

    draw_peak_mem(avg.copy(deep=True), 32)
    draw_peak_mem(avg.copy(deep=True), 64)
    draw_peak_mem(avg.copy(deep=True), 128)
