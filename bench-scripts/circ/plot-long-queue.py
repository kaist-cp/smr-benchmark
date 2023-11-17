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

EBR = "ebr"
NR = "nr"
HP = "hp"
CDRC_EBR = "cdrc-ebr"
CDRC_HP = "cdrc-hp"
CIRC_EBR = "circ-ebr"
CIRC_HP = "circ-hp"

SMR_ONLYs = [EBR, CDRC_EBR, CIRC_EBR]

# https://matplotlib.org/stable/api/markers_api.html
line_shapes = {
    EBR: 'o',
    CDRC_EBR: "1",
    CIRC_EBR: "X",
}

# https://matplotlib.org/stable/gallery/color/named_colors.html
line_colors = {
    EBR: 'c',
    CDRC_EBR: "green",
    CIRC_EBR: "blue",
}

line_types = {
    EBR: 'dotted',
    CDRC_EBR: (5, (10, 3)),
    CIRC_EBR: (0, (3, 1)),
}


def draw(title, name, data, line_name, y_value, y_label):
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

    p += theme(
            axis_title_x=element_text(size=15),
            axis_text_x=element_text(size=14),
            axis_text_y=element_text(size=14))
    if y_label:
        p += ylab(y_label)
        p += theme(axis_title_y=element_text(size=14))
    else:
        p += theme(axis_title_y=element_blank())

    p += theme(legend_position='none')
    p.save(name, width=5, height=4, units="in")

def draw_peak_mem(data, threads):
    data = data[data.threads == threads]
    y_label = 'Avg memory usage (GiB)'
    name = f'{RESULTS_PATH}/long-running/long-running-queue_avg_mem.pdf'
    draw(None, name, data, SMR_ONLY, PEAK_MEM, y_label)
    return name


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
    draw("", name, avg, SMR_ONLY, AVG_MEM, y_label)
