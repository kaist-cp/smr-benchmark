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
KEY_RANGE = "key_range"
THROUGHPUT = "throughput"
PEAK_MEM = "peak_mem"
AVG_GARB = "avg_garb"
PEAK_GARB = "peak_garb"

# legend
SMR_ONLY = "SMR\n"
SMR_I = "SMR, interf.\n"

EBR = "EBR"
PEBR = "PEBR"
NR = "NR"
HP = "HP"
HP_PP = "HP_PP"
NBR = "NBR"
CDRC_EBR = "CDRC_EBR"

SMR_ONLYs = [NR, EBR, HP, HP_PP, PEBR, CDRC_EBR]

# https://matplotlib.org/stable/api/markers_api.html
line_shapes = {
    NR: '.',
    EBR: 'o',
    HP: 'v',
    HP_PP: 'D',
    PEBR: "x",
    NBR: "s",
    CDRC_EBR: "1",
}

line_colors = {
    NR: 'k',
    EBR: 'c',
    HP: 'hotpink',
    HP_PP: 'purple',
    PEBR: "y",
    NBR: "C0",
    CDRC_EBR: "green",
}

line_types = {
    NR: '-',
    EBR: 'dotted',
    HP: 'dashed',
    HP_PP: 'dashdot',
    PEBR: (5, (10, 3)),
    NBR: (0, (3, 1, 1, 1)),
    CDRC_EBR: (0, (3, 1, 1, 1, 1, 1)),
}

krs = [(2 ** e) for e in range(18, 27, 1)]

# line_name: SMR, SMR_I
def draw(title, name, data, line_name, y_value, y_label=None, y_max=None, legend=False):
    p = ggplot(
            data,
            aes(x=KEY_RANGE, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Key range') + geom_point(size=7) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=krs, trans='log2') + \
        labs(title = title) + theme(plot_title = element_text(size=36)) + \
        scale_y_continuous(breaks=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2])


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
    
    p.save(name, width=10, height=7, units="in")

def draw_throughput(data):
    data = data.copy()
    y_label = 'Throughput ratio to NR'
    legend = False
    y_max = data.throughput.max() * 1.05
    draw("", f'{RESULTS_PATH}/throughput.pdf',
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend)


# preprocess
data = pd.read_csv(f'{RESULTS_PATH}/long-running.csv')

data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))

# take average of each runs
avg = data.groupby(['mm', 'key_range']).mean().reset_index()

baseline = avg[avg.mm == NR]
for kr in krs:
    base = baseline[baseline.key_range == kr].throughput.iloc[0]
    for m in SMR_ONLYs:
        thr = avg[(avg.mm == m) & (avg.key_range == kr)].throughput.iloc[0]
        avg.loc[(avg.mm == m) & (avg.key_range == kr), "throughput"] = thr / base

avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
draw_throughput(avg)