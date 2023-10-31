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
THROUGHPUT_RATIO = "throughput-ratio"
PEAK_MEM = "peak_mem"
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
CIRC_EBR = "circ-ebr"

SMR_ONLYs = [NR, EBR, CDRC_EBR, CIRC_EBR]

cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = [1, 2, 3] + list(range(4, 33, 4))
elif cpu_count <= 64:
    ts = [1, 2, 4] + list(range(8, 129, 8))
else:
    ts = [1, 3, 5] + list(range(10, 151, 10))

# https://matplotlib.org/stable/api/markers_api.html
line_shapes = {
    NR: '.',
    EBR: 'o',
    CDRC_EBR: "1",
    CIRC_EBR: "X",
}

# https://matplotlib.org/stable/gallery/color/named_colors.html
line_colors = {
    NR: 'k',
    EBR: 'c',
    CDRC_EBR: "green",
    CIRC_EBR: "blue",
}

line_types = {
    NR: '-',
    EBR: 'dotted',
    CDRC_EBR: (5, (10, 3)),
    CIRC_EBR: (0, (3, 1)),
}

# line_name: SMR, SMR_I
def draw(name, data, line_name, y_value, width, height, y_max=None, y_label=None, legend=False):
    p = ggplot(
            data,
            aes(x=THREADS, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Threads') + geom_point(size=7) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=ts) + \
        theme(plot_title = element_text(size=36))

    p += theme(
            axis_title_x=element_text(size=15),
            axis_text_x=element_text(size=14),
            axis_text_y=element_text(size=14))
    if y_label:
        p += ylab(y_label)
        p += theme(axis_title_y=element_text(size=14))
    else:
        p += theme(axis_title_y=element_blank())

    if y_max:
        p += coord_cartesian(ylim=(0, y_max))
    if legend:
        # HACK: `\n` at the end of legend title
        p += theme(legend_title=element_text(size=18, linespacing=1.5))
        p += theme(legend_key_size=15)
        p += theme(legend_text=element_text(size=18))
        p += theme(legend_entry_spacing=15)
    else:
        p += theme(legend_position='none')
    
    if data.threads.max() >= cpu_count:
        p += annotate(geom="rect", xmin=cpu_count, xmax=float("inf"), ymin=-float("inf"), ymax=float("inf"), fill = "#FF00000A")

    p.save(name, width=width, height=height, units="in")

def draw_throughput(data, max_threads, width, height, legend):
    data = data[data.threads <= max_threads]
    y_label = 'Throughput (M op/s)'
    name = f'{RESULTS_PATH}/queue/double-link_xmax{max_threads}_throughput.pdf'
    draw(name, data, SMR_ONLY, THROUGHPUT, width, height, y_label=y_label, legend=legend)
    return name

def draw_throughput_ratio(data, max_threads, width, height, legend):
    data = data[data.threads <= max_threads]
    y_label = 'Throughput ratio to CIRC EBR'
    name = f'{RESULTS_PATH}/queue/double-link_xmax{max_threads}_throughput-ratio.pdf'
    draw(name, data, SMR_ONLY, THROUGHPUT_RATIO, width, height, y_label=y_label, legend=legend)
    return name

def draw_peak_mem(data, max_threads, width, height, legend):
    data = data[data.threads <= max_threads]
    y_label = 'Peak memory usage (GiB)'
    name = f'{RESULTS_PATH}/queue/double-link_xmax{max_threads}_peak_mem.pdf'
    y_max = data[data.mm == CDRC_EBR].peak_mem.max() * 1.05
    draw(name, data, SMR_ONLY, PEAK_MEM, width, height, y_max=y_max, y_label=y_label, legend=legend)
    return name

def draw_avg_mem(data, max_threads, width, height, legend):
    data = data[data.threads <= max_threads]
    y_label = 'Avg. memory usage (GiB)'
    name = f'{RESULTS_PATH}/queue/double-link_xmax{max_threads}_avg_mem.pdf'
    y_max = data[data.mm == CDRC_EBR].peak_mem.max() * 1.05
    draw(name, data, SMR_ONLY, AVG_MEM, width, height, y_max=y_max, y_label=y_label, legend=legend)
    return name

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    pd.set_option('display.max_rows', None)

    # avoid Type 3 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42

    os.makedirs(f'{RESULTS_PATH}/queue', exist_ok=True)

    data = pd.read_csv(f'{RESULTS_PATH}/{DOUBLELINK}.csv')
    data = data[data.mm.isin(SMR_ONLYs)]
    data.throughput = data.throughput.map(lambda x: x / 1_000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 30))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 30))
    avg = data.groupby(['mm', 'threads']).mean().reset_index()

    base = avg[avg.mm == CIRC_EBR].throughput.values
    for m in SMR_ONLYs:
        thr = avg[avg.mm == m].throughput.values
        avg.loc[avg.mm == m, THROUGHPUT_RATIO] = thr / base

    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
    avg['ds'] = 'double-link'
    draw_throughput(avg, 128, 10, 7, True)
    draw_throughput_ratio(avg, 128, 10, 7, True)
    draw_peak_mem(avg, 128, 10, 7, True)
    draw_avg_mem(avg, 128, 10, 7, True)
    draw_throughput(avg, 32, 7, 5, False)
    draw_throughput_ratio(avg, 32, 7, 5, False)
    draw_peak_mem(avg, 32, 7, 5, False)
    draw_avg_mem(avg, 32, 7, 5, False)
