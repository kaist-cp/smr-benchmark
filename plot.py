# type: ignore
import pandas as pd
from plotnine import *
import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)

# raw column names
THREADS = "threads"
THROUGHPUT = "throughput"
PEAK_MEM = "peak_mem"
AVG_MEM = "avg_mem"

# legend
SMR_ONLY = "SMR\n"
SMR_I = "SMR, interf.\n"

HLIST = "HList"
HMLIST = "HMList"
HHSLIST = "HHSList"
HASHMAP = "HashMap"
NMTREE = "NMTree"
BONSAITREE = "BonsaiTree"

EBR = "EBR"
PEBR = "PEBR"
NR = "NR"

N1MS = ', 1ms'
N10MS = ', 10ms'
STALLED = ', stalled'

# DS with read-dominated bench & write-only bench
dss_all   = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, BONSAITREE]
dss_read  = [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE, BONSAITREE]
dss_write = [HLIST, HMLIST,          HASHMAP, NMTREE, BONSAITREE]

SMR_ONLYs = [NR, EBR, PEBR]
SMR_Is = [NR, EBR, EBR+N10MS, EBR+STALLED, PEBR, PEBR+N10MS, PEBR+STALLED]

ts = [1] + list(range(5, 76, 5))

n_map = {0: '', 1: N1MS, 2: N10MS, 3: STALLED}

line_shapes = {
    NR: '.',
    EBR: 'o',
    EBR + N10MS: 'o',
    EBR + STALLED: 'o',
    PEBR: 'D',
    PEBR + N10MS: 'D',
    PEBR + STALLED: 'D',
}

line_colors = {
    NR: 'k',
    EBR: 'c',
    EBR + N10MS: 'darkblue',
    EBR + STALLED: 'g',
    PEBR: 'hotpink',
    PEBR + N10MS: 'firebrick',
    PEBR + STALLED: 'orange',
}

line_types = {
    NR: '-',
    EBR: '--',
    EBR + N10MS: '-.',
    EBR + STALLED: ':',
    PEBR: '--',
    PEBR + N10MS: '-.',
    PEBR + STALLED: ':',
}


# line_name: SMR, SMR_I
def draw(ds, name, data, line_name, y_value, y_label=None, y_max=None, legend=False):
    p = ggplot(
            data,
            aes(x=THREADS, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Threads') + geom_point(size=3) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=ts) + \
        labs(title = ds) + theme(plot_title = element_text(size=28))
        # ggtitle(title[ds]) + guides(title_position='bottom')

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

    p.save(name, width=8, height=5.5, units="in")

def draw_throughput(data, ds, bench):
    data = data[ds].copy()
    data = data[data.non_coop == 0]
    y_label = 'Throughput (M op/s)' if ds in [HLIST, HASHMAP] else None
    legend = ds == (BONSAITREE if bench == "read" else HMLIST)
    y_max = data.throughput.max() * 1.05
    draw(ds, f'results/{ds}_{bench}_throughput.pdf',
         data, SMR_ONLY, THROUGHPUT, y_label, y_max, legend)


def draw_mem(data, ds, bench):
    data = data[ds].copy()
    y_label = 'Peak memory usage (MiB)' if ds in [HLIST, HASHMAP] else None
    y_max = None
    legend = ds == (BONSAITREE if bench == "read" else HMLIST)
    if ds == BONSAITREE:
        _d = data[~data[SMR_I].isin([NR, EBR + STALLED])]  # exclude NR and EBR stalled
        max_threads = _d.threads.max()
        y_max = _d[_d.threads == max_threads].peak_mem.max() * 0.80
    elif ds in [HLIST, HMLIST, HHSLIST, HASHMAP, NMTREE]:
        _d = data[~data[SMR_I].isin([NR, EBR + STALLED])]  # exclude NR and EBR stalled
        y_max = _d[_d.ds == ds].peak_mem.max() * 1.05
    else:
        y_max = data.peak_mem.max() * 1.05
    draw(ds, f'results/{ds}_{bench}_peak_mem.pdf',
         data, SMR_I, PEAK_MEM, y_label, y_max, legend)



raw_data = {}
# averaged data for read-dominated bench and write-only bench
write_data = {}
read_data = {}

# preprocess
for ds in dss_all:
    data = pd.read_csv('results/' + ds + '.csv')

    data.throughput = data.throughput.map(lambda x: x / 1000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))

    # ignore -c4 data
    data = data[data.ops_per_cs == 1]
    # ignore -n1 data
    data = data[data.non_coop != 1]

    raw_data[ds] = data.copy()

    # take average of each runs
    avg = data.groupby(['ds', 'mm', 'threads', 'non_coop', 'get_rate']).mean().reset_index()

    # sort by SMR_I
    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
    avg[SMR_I] = pd.Categorical(avg.mm.map(str) + avg.non_coop.map(n_map), SMR_Is)
    avg.sort_values(by=SMR_I, inplace=True)
    write_data[ds] = avg[avg.get_rate == 0]
    read_data[ds] = avg[avg.get_rate == 1]

# 1. throughput graphs, 3 lines (SMR_ONLY) each.
for ds in dss_write:
    draw_throughput(write_data, ds, 'write')
for ds in dss_read:
    draw_throughput(read_data, ds, 'read')

# 2. peak mem graph, 7 lines (SMR_I)
for ds in dss_write:
    draw_mem(write_data, ds, 'write')
for ds in dss_read:
    draw_mem(read_data, ds, 'read')
