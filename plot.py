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
SMR_ONLY = "SMR"
SMR_I = "SMR & interference"
SMR_OPCS = "SMR & ops / CS"

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

OPCS_1 = ', 1 op / CS'
OPCS_4 = ', 4 ops / CS'

dss = [LIST, HASHMAP, NMTREE, BONSAITREE]
smrs = [NR, EBR, PEBR]
ns = [1, 2]
ts = [1] + list(range(5, 76, 5))

n_map = {0: '', 1: N1MS, 2: N10MS, 3: STALLED}
c_map = {1: OPCS_1, 4: OPCS_4}

line_shapes = {
    NR: '',
    EBR: '.',
    EBR + N1MS: 'o',
    EBR + N10MS: 'o',
    EBR + STALLED: 's',
    EBR + OPCS_1: 'o',
    EBR + OPCS_4: 's',
    PEBR: '^',
    PEBR + N1MS: 'D',
    PEBR + N10MS: 'D',
    PEBR + STALLED: '*',
    PEBR + OPCS_1: 'D',
    PEBR + OPCS_4: '*',
}

line_colors = {
    NR: 'k',
    EBR: 'b',
    EBR + N1MS: 'navy',
    EBR + N10MS: 'c',
    EBR + STALLED: 'g',
    EBR + OPCS_1: 'c',
    EBR + OPCS_4: 'g',
    PEBR: 'r',
    PEBR + N1MS: 'hotpink',
    PEBR + N10MS: 'm',
    PEBR + STALLED: '#FFB86F',
    PEBR + OPCS_1: 'm',
    PEBR + OPCS_4: '#FFB86F',
}

line_types = {
    NR: '-',
    EBR: '--',
    EBR + N1MS: '--',
    EBR + N10MS: '--',
    EBR + STALLED: '--',
    EBR + OPCS_1: '--',
    EBR + OPCS_4: '--',
    PEBR: ':',
    PEBR + N1MS: ':',
    PEBR + N10MS: ':',
    PEBR + STALLED: ':',
    PEBR + OPCS_1: ':',
    PEBR + OPCS_4: ':',
}

valid_line_name = list(line_shapes.keys())


# line_name: SMR, SMR_OPCS, SMR_I
def draw(name, data, line_name, y_label, y_value, y_max=None):
    p = ggplot(
            data,
            aes(x=THREADS, y=y_value,
                color=line_name, shape=line_name, linetype=line_name)) + \
        geom_line() + xlab('Threads') + ylab(y_label) + geom_point(size=4) + \
        scale_shape_manual(line_shapes, na_value='x') + \
        scale_color_manual(line_colors, na_value='y') + \
        scale_linetype_manual(line_types, na_value='-.') + \
        theme_bw() + scale_x_continuous(breaks=ts)
    if y_max:
        p += ylim(0, y_max)
    p.save(name, width=8, height=5.5, units="in")


# preprocess
raw_data = {}
for ds in dss:
    data = pd.read_csv('results/' + ds + '.csv')

    mega = lambda x: x / 1000_000
    data.throughput = data.throughput.map(mega)
    data.peak_mem = data.peak_mem.map(mega)
    data.avg_mem = data.avg_mem.map(mega)

    # NOTE: need to filter out rows with -n and -c options
    data[SMR_ONLY] = data.mm.map(str)

    data[SMR_I] = data.mm.map(str) + data.non_coop.map(n_map)
    data = data[data[SMR_I].isin(valid_line_name)]

    # NR, EBR c1/c4, PEBR c1/c4
    data[SMR_OPCS] = data.mm.map(str) + data.ops_per_cs.map(c_map)
    def rename_smr_opcs(old):
        if old == NR + OPCS_1:
            return NR
        return old
    data[SMR_OPCS] = data[SMR_OPCS].map(rename_smr_opcs)
    data = data[data[SMR_OPCS].isin(valid_line_name)]

    raw_data[ds] = data.copy()

# 1. 4(DS) throughput graphs, 3 lines (SMR) each. for HashMap, both c1/c4
for ds in dss:
    data = raw_data[ds].copy()
    data = data[data.non_coop == 0]
    if ds in [HASHMAP, NMTREE]:
        draw(f'results/{ds}_throughput.pdf', data, SMR_OPCS, 'Throughput (M op/s)', THROUGHPUT)
    else:
        # colum SMR_ONLY may 
        draw(f'results/{ds}_throughput.pdf', data, SMR_ONLY, 'Throughput (M op/s)', THROUGHPUT)

# 3. 4 (DS) peak mem graph, 7 lines (SMR & interference), c4 for HashMap/NMTree, c1 otherwise
for ds in dss:
    data = raw_data[ds].copy()
    if ds in [HASHMAP, NMTREE]:
        # use c4 except for NR
        data = data[(data.ops_per_cs == 4) | (data.mm == NR)]
    else:
        data = data[data.ops_per_cs == 1]


    # readable
    y_max = None
    if ds not in [LIST]:
        _d = data[~data[SMR_I].isin([NR, EBR+STALLED])] # exclue NR and EBR stalled
        y_max = _d[data.ds == ds].peak_mem.max() * 1.05
    draw(f'results/{ds}_peak_mem.pdf', data, SMR_I, 'Peak memory usage (MB)', PEAK_MEM, y_max)

# for ds in dss:
#     data = raw_data[ds].copy()
#     draw(f'{ds}_avg_mem.pdf', data, SMR_I, 'Average memory usage (MB)', AVG_MEM)
