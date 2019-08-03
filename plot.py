# type: ignore
import pandas as pd
from plotnine import *

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NR']
gs = [0, 2]
ns = [1, 2]
ts = [1] + list(range(5, 76, 5))

g_map = {0: 'write-dominated', 2: 'read-dominated'}
n_map = {0: '', 1: ' 10ms', 2: ' dead'}

shapes = ['o', '^', 's', 'D', '.', '+', '*']
colors = ['b', 'g', 'r', 'c', 'm', 'k', '#FFB86F']
linetypes = [':', '-.', '--', '-', ':', '-.', '--']

def draw(name, data):
    p = ggplot(data, aes(x='threads', y='throughput', color='MM & period', shape='MM & period', linetype='MM & period')) + \
        geom_line() + xlab('threads') + ylab('throughput (M op/s)') + geom_point(size=4) + \
        scale_shape_manual(shapes) + scale_color_manual(colors) + scale_linetype_manual(linetypes)
    p.save(name, width=8, height=5.5, units="in")

raw_data = {}
for ds in dss:
    # TODO: run expr several times
    data = pd.read_csv(ds + '_results.csv')
    data.throughput = data.throughput.map(lambda x: x / 1000000)
    # data['load type'] = data.get_rate.map(g_map)
    data['MM & period'] = data.mm.map(str) + data.non_coop.map(n_map)
    raw_data[ds] = data.copy()


for ds in dss:
    for g in gs:
        data = raw_data[ds].copy()
        data = data[data.get_rate == g]
        draw(f'{ds}_g{g}_throughput.pdf', data[data.ops_per_cs == 1])
        if ds == "HashMap":
            draw(f'HashMap_g{g}_c4_throughput.pdf', data[data.ops_per_cs == 4])
        # with pd.option_context('display.max_rows', None):
        #     print(data)

