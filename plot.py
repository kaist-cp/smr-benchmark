# type: ignore
import pandas as pd
from plotnine import *

pd.set_option('display.max_rows', None)

THREADS = "threads"
THROUGHPUT = "throughput"
SMR_AND_INTERFERENCE = "SMR & interference"
LIST = "List"
HASHMAP = "HashMap"
NMTREE = "NMTree"
BONSAITREE = "BonsaiTree"
EBR = "EBR"
PEBR = "PEBR"
NR = "NR"
N10MS = ', 10ms'
STALLED = ', stalled'

dss = [LIST, HASHMAP, NMTREE, BONSAITREE]
mms = [EBR, PEBR, NR]
gs = [0, 2]
ns = [1, 2]
ts = [1] + list(range(5, 76, 5))

g_map = {0: 'write-dominated', 2: 'read-dominated'}
n_map = {0: '', 1: N10MS, 2: STALLED}

# TODO: SMR_AND_INTERFERENCE |-> style
shapes = ['o', '^', 's', 'D', '.', 'x', '*']
colors = ['b', 'g', 'r', 'c', 'm', 'k', '#FFB86F']
linetypes = [
    '-',  # NR
    ':',  # EBR
    '--'  # PEBR
]

line_shapes = {
    NR: '',
    EBR: '.',
    EBR + N10MS: 'o',
    EBR + STALLED: 's',
    PEBR: '^',
    PEBR + N10MS: 'D',
    PEBR + STALLED: '*',
}

line_colors = {
    NR: 'k',
    EBR: 'b',
    EBR + N10MS: 'c',
    EBR + STALLED: 'g',
    PEBR: 'r',
    PEBR + N10MS: 'm',
    PEBR + STALLED: '#FFB86F',
}

line_types = {
    NR: '-',
    EBR: '--',
    EBR + N10MS: '--',
    EBR + STALLED: '--',
    PEBR: ':',
    PEBR + N10MS: ':',
    PEBR + STALLED: ':',
}

valid_smri = list(line_shapes.keys())


def draw(name, data):
    p = ggplot(
            data,
            aes(x=THREADS, y=THROUGHPUT,
                color=SMR_AND_INTERFERENCE, shape=SMR_AND_INTERFERENCE, linetype=SMR_AND_INTERFERENCE)) + \
        geom_line() + xlab('Threads') + ylab('Throughput (M op/s)') + geom_point(size=4) + \
        scale_shape_manual(line_shapes) + scale_color_manual(line_colors) + scale_linetype_manual(line_types) + \
        theme_bw()

    p.save(name, width=8, height=5.5, units="in")


raw_data = {}
for ds in dss:
    data = pd.read_csv(ds + '_results.csv')
    data.throughput = data.throughput.map(lambda x: x / 1000000)
    data[SMR_AND_INTERFERENCE] = data.mm.map(str) + data.non_coop.map(n_map)
    data = data[data[SMR_AND_INTERFERENCE].isin(valid_smri)]
    raw_data[ds] = data.copy()

# 1. 4(DS) throughput graphs, 3 lines (SMR) each. for HashMap, both c1/c4
# 3. 4 (DS) peak mem graph, 7 lines (SMR & interference)

for ds in dss:
    for g in gs:
        data = raw_data[ds].copy()
        data = data[data.get_rate == g]
        draw(f'{ds}_g{g}_throughput.pdf', data[data.ops_per_cs == 1])
        if ds == "HashMap":
            draw(f'HashMap_g{g}_c4_throughput.pdf', data[data.ops_per_cs == 4])
        # with pd.option_context('display.max_rows', None):
        #     print(data)
