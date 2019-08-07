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
# SMR_OPCS = "SMR & ops / CS"

LIST = "List"
HASHMAP = "HashMap"
NMTREE = "NMTree"
BONSAITREE = "BonsaiTree"

EBR = "EBR"
PEBR = "PEBR"
NR = "NR"

N1MS = ', 1ms'
N10MS = ', 10ms'
STALLED = ', stalled'

# OPCS_1 = ', 1 op / CS'
# OPCS_4 = ', 4 ops / CS'

dss = [LIST, HASHMAP, NMTREE, BONSAITREE]
SMR_ONLYs = [NR, EBR, PEBR]
SMR_Is = [NR, EBR, EBR+N10MS, EBR+STALLED, PEBR, PEBR+N10MS, PEBR+STALLED]

ns = [1, 2]
ts = [1] + list(range(5, 76, 5))

n_map = {0: '', 1: N1MS, 2: N10MS, 3: STALLED}
# c_map = {1: OPCS_1, 4: OPCS_4}

line_shapes = {
    NR: '.',
    EBR: 'o',
    # EBR + N1MS: 'o',
    EBR + N10MS: 'o',
    EBR + STALLED: 'o',
    # EBR + OPCS_1: 'o',
    # EBR + OPCS_4: 'o',
    PEBR: 'D',
    # PEBR + N1MS: 'D',
    PEBR + N10MS: 'D',
    PEBR + STALLED: 'D',
    # PEBR + OPCS_1: 'D',
    # PEBR + OPCS_4: 'D',
}

line_colors = {
    NR: 'k',
    EBR: 'c',
    # EBR + N1MS: 'navy',
    EBR + N10MS: 'darkblue',
    EBR + STALLED: 'g',
    # EBR + OPCS_1: 'darkblue',
    # EBR + OPCS_4: 'g',
    PEBR: 'hotpink',
    # PEBR + N1MS: 'hotpink',
    PEBR + N10MS: 'firebrick',
    PEBR + STALLED: 'orange',
    # PEBR + OPCS_1: 'firebrick',
    # PEBR + OPCS_4: 'orange',
}

line_types = {
    NR: '-',
    EBR: '--',
    # EBR + N1MS: '--',
    EBR + N10MS: '-.',
    EBR + STALLED: ':',
    # EBR + OPCS_1: '-.',
    # EBR + OPCS_4: ':',
    PEBR: '--',
    # PEBR + N1MS: ':',
    PEBR + N10MS: '-.',
    PEBR + STALLED: ':',
    # PEBR + OPCS_1: '-.',
    # PEBR + OPCS_4: ':',
}

# title = { LIST: 'List', HASHMAP: 'Hashmap', NMTREE: 'NMtree', BONSAITREE: 'Bonsai' }

valid_line_name = list(line_shapes.keys())


# line_name: SMR, SMR_OPCS, SMR_I
def draw(ds, name, data, line_name, y_value, y_label=None, y_max=None, legend=False):
    p = ggplot(
            data,
            aes(x=THREADS, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Threads') + geom_point(size=3) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=ts)
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
    width = 8
    if legend:
        width = 9
        # HACK: `\n` at the end of legend title
        p += theme(legend_title=element_text(size=18, linespacing=1.5))
        p += theme(legend_key_size=15)
        p += theme(legend_text=element_text(size=18))
        p += theme(legend_entry_spacing=15)
    else:
        p += theme(legend_position='none')

    p.save(name, width=width, height=5.5, units="in")


# preprocess
raw_data = {}
avg_data = {}
max_data = {}
for ds in dss:
    data = pd.read_csv('results/' + ds + '.csv')

    data.throughput = data.throughput.map(lambda x: x / 1000_000)
    data.peak_mem = data.peak_mem.map(lambda x: x / (2 ** 20))
    data.avg_mem = data.avg_mem.map(lambda x: x / (2 ** 20))

    # ignore -c4 data
    data = data[data.ops_per_cs == 1]
    # ignore -n1 data
    data = data[data.non_coop != 1]

    # NR, EBR c1/c4, PEBR c1/c4
    # data[SMR_OPCS] = data.mm.map(str) + data.ops_per_cs.map(c_map)
    # def rename_smr_opcs(old):
    #     if old == NR + OPCS_1:
    #         return NR
    #     return old
    # data[SMR_OPCS] = data[SMR_OPCS].map(rename_smr_opcs)
    # data = data[data[SMR_OPCS].isin(valid_line_name)]

    raw_data[ds] = data.copy()

    # sort by SMR_I
    avg = data.groupby(['ds', 'mm', 'threads', 'non_coop']).mean().reset_index()
    avg[SMR_ONLY] = pd.Categorical(avg.mm.map(str), SMR_ONLYs)
    avg[SMR_I] = pd.Categorical(avg.mm.map(str) + avg.non_coop.map(n_map), SMR_Is)
    avg.sort_values(by=SMR_I, inplace=True)
    avg_data[ds] = avg

    mx = data.groupby(['ds', 'mm', 'threads', 'non_coop']).mean().reset_index()
    mx[SMR_ONLY] = pd.Categorical(mx.mm.map(str), SMR_ONLYs)
    mx[SMR_I] = pd.Categorical(mx.mm.map(str) + mx.non_coop.map(n_map), SMR_Is)
    mx.sort_values(by=SMR_I, inplace=True)
    avg_data[ds] = mx


# 1. 4(DS) throughput graphs, 3 lines (SMR_ONLY) each.
for ds in dss:
    # data = raw_data[ds].copy()
    data = avg_data[ds].copy()
    data = data[data.non_coop == 0]
    legend = ds == BONSAITREE
    y_max = data.throughput.max() * 1.05
    draw(ds, f'results/{ds}_throughput.pdf',
         data,
         SMR_ONLY,
         THROUGHPUT,
         'Throughput (M op/s)' if ds == LIST else None,
         y_max,
         legend=legend)

# 3. 4(DS) peak mem graph, 7 lines (SMR_I)
for ds in dss:
    data = avg_data[ds].copy()

    # readable
    y_max = None
    if ds == BONSAITREE:
        _d = data[~data[SMR_I].isin([NR, EBR + STALLED])]  # exclue NR and EBR stalled
        max_threads = _d.threads.max()
        y_max = _d[_d.threads == max_threads].peak_mem.max() * 0.80
    elif ds in [HASHMAP, NMTREE]:
        _d = data[~data[SMR_I].isin([NR, EBR + STALLED])]  # exclue NR and EBR stalled
        y_max = _d[_d.ds == ds].peak_mem.max() * 1.05
    else:
        y_max = data.peak_mem.max() * 1.05
    draw(ds, f'results/{ds}_peak_mem.pdf',
         data,
         SMR_I,
         PEAK_MEM,
         'Peak memory usage (MiB)' if ds == LIST else None,
         y_max,
         legend=(ds==BONSAITREE))

# for ds in dss:
#     data = raw_data[ds].copy()
#     draw(f'{ds}_avg_mem.pdf', data, SMR_I, 'Average memory usage (MiB)', AVG_MEM)
