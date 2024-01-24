# type: ignore
import pandas as pd
from plotnine import *
import warnings
import matplotlib
import os

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")

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

EBR = "ebr"
PEBR = "pebr"
NR = "nr"
HP = "hp"
HP_PP = "hp-pp"
NBR = "nbr"
NBR_LARGE = "nbr-large"
CDRC_EBR = "cdrc-ebr"
HP_SHARP = "hp-sharp"
HP_SHARP_0 = "hp-sharp-0"
CDRC_HP_SHARP = "cdrc-hp-sharp"
VBR = "vbr"

SMR_ONLYs = [EBR, PEBR, NR, HP, HP_PP, NBR, NBR_LARGE, HP_SHARP, HP_SHARP_0, VBR]
SMR_PART = [NR, EBR, HP, NBR, HP_SHARP, HP_SHARP_0]

# https://matplotlib.org/stable/api/markers_api.html
line_shapes = {
    NR: '.',
    EBR: 'o',
    HP: 'v',
    HP_PP: 'D',
    PEBR: "x",
    CDRC_EBR: "1",
    NBR: "p",
    NBR_LARGE: "H",
    HP_SHARP: "s",
    HP_SHARP_0: "P",
    CDRC_HP_SHARP: "P",
    VBR: "*",
}

line_colors = {
    NR: 'k',
    EBR: 'c',
    HP: 'hotpink',
    HP_PP: 'purple',
    PEBR: "y",
    CDRC_EBR: "green",
    NBR: "blue",
    NBR_LARGE: "indigo",
    HP_SHARP: "r",
    HP_SHARP_0: "green",
    CDRC_HP_SHARP: "brown",
    VBR: "orange",
}

line_types = {
    NR: '-',
    EBR: 'dotted',
    HP: 'dashed',
    HP_PP: 'dashdot',
    PEBR: (5, (10, 3)),
    CDRC_EBR: (0, (3, 1)),
    NBR: (5, (10, 3)),
    NBR_LARGE: (5, (10, 3)),
    HP_SHARP: (0, (3, 1, 1, 1)),
    HP_SHARP_0: (0, (3, 1, 1, 1)),
    CDRC_HP_SHARP: (0, (3, 1, 1, 1)),
    VBR: (0, (2, 1)),
}

krs = [(2 ** e) for e in range(18, 30, 1)]

def draw(title, name, data, line_name, y_value, y_label=None, y_max=None, legend=False, y_breaks=None, width=10, height=7, point_size=7, line_size=0.5, x_label="Key range"):
    p = ggplot(
            data,
            aes(x=KEY_RANGE, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line(size=line_size) + xlab(x_label) + geom_point(size=point_size) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=krs, trans='log2') + \
        labs(title = title) + theme(plot_title = element_text(size=36)) + \
        theme(
            axis_title_x=element_text(size=15),
            axis_text_x=element_text(size=14),
            axis_text_y=element_text(size=14))
    
    if y_breaks:
        p += scale_y_continuous(breaks=y_breaks)

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
    
    p.save(name, width=width, height=height, units="in")

def draw_throughput(data):
    data = data.copy()
    y_label = 'Throughput ratio to NR'
    legend = False
    y_max = data.throughput.max() * 1.05
    draw("", f'{RESULTS_PATH}/long-running-throughput.pdf',
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend,
         y_breaks=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2])

def draw_throughput_partial(data):
    data = data.copy()
    data = data[data.mm.isin(SMR_PART)]
    data = data[data.key_range < krs[-4]]
    y_label = 'Throughput ratio to NR'
    legend = False
    y_max = data.throughput.max() * 1.05
    draw("", f'{RESULTS_PATH}/long-running-throughput-partial.pdf',
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend,
         y_breaks=[0, 0.4, 0.8, 1, 1.2],
         width=7, height=5, point_size=9, line_size=1, x_label="Length of a read operation")

def draw_peak_garb(data, full):
    data = data.copy()
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    y_label = 'Peak unreclaimed memory blocks (×10³)'
    legend = False
    if full:
        y_max = data.peak_garb.max() * 1.05
        tail = "-full"
    else:
        y_max = data[data.mm == HP_SHARP_0].peak_garb.max() * 1.05
        tail = "-trunc"
    draw("", f'{RESULTS_PATH}/long-running-peak-garb{tail}.pdf',
         data, SMR_ONLY, PEAK_GARB, y_label, y_max, legend)

def draw_peak_garb_partial(data):
    data = data.copy()
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    y_label = 'Peak unreclaimed memory blocks (×10³)'
    legend = False
    y_max = data[data.mm == EBR][data.key_range < krs[-7]].peak_garb.max() * 1.4
    data = data[data.mm.isin(SMR_PART)]
    data = data[data.key_range < krs[-4]]
    draw("", f'{RESULTS_PATH}/long-running-peak-garb-partial.pdf',
         data, SMR_ONLY, PEAK_GARB, y_label, y_max, legend,
         width=7, height=5, point_size=9, line_size=1, x_label="Length of a read operation")
    
def draw_avg_garb(data, full):
    data = data.copy()
    data = data[data.mm != NR]
    data = data[data.mm != VBR]
    y_label = 'Avg. unreclaimed memory blocks (×10³)'
    legend = False
    if full:
        y_max = data.avg_garb.max() * 1.05
        tail = "-full"
    else:
        y_max = data[data.mm == HP_SHARP_0].avg_garb.max() * 1.05
        tail = "-trunc"
    draw("", f'{RESULTS_PATH}/long-running-avg-garb{tail}.pdf',
         data, SMR_ONLY, AVG_GARB, y_label, y_max, legend)

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
    for m in SMR_ONLYs:
        thr = avg[(avg.mm == m) & (avg.key_range == kr)].throughput.iloc[0]
        avg.loc[(avg.mm == m) & (avg.key_range == kr), "throughput"] = thr / base

avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
draw_throughput(avg)
draw_throughput_partial(avg)
draw_peak_garb(avg, True)
draw_peak_garb(avg, False)
draw_peak_garb_partial(avg)
draw_avg_garb(avg, True)
draw_avg_garb(avg, False)
