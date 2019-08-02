#!/usr/bin/python3

import subprocess

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NR']
gs = ['', '-g', '-gg']
ns = ['', '-n', '-nn']
ts = [1] + list(range(5, 101, 5))

subprocess.run(['git', 'submodule', 'update', '--init'])

run_cmd = ['cargo', 'run', '--release', '--']


def opts(ds, mm, t, g='', n=''):
    return ['-d', ds, '-m', mm, '-t', t
            ] + ([] if g == '' else [g]) + ([] if n == '' else [n])


# througput: less contention
for ds in dss:
    for g in gs:
        for mm in mms:
            for t in map(str, ts):
                subprocess.run(run_cmd + opts(ds, mm, t, g=g))
                # print(' '.join(run_cmd + opts(ds, mm, t, g=g)))

# non-cooperative t (don't test NR)
for ds in dss:
    for n in ['-n', '-nn']:
        for mm in ['EBR', 'PEBR']:
            for t in map(str, ts):
                subprocess.run(run_cmd + opts(ds, mm, t, n=n))
                # print(' '.join(run_cmd + opts(ds, mm, t, n=n)))

# througput: high contention
# for ds in dss:
#     for mm in mms:
#         for t in map(str, ts):
#             subprocess.run(run_cmd + opts(ds, mm, t, g, n) + ['-r100', '-p50'])

# max mem usage
# export JEMALLOC_SYS_WITH_MALLOC_CONF=prof:true,prof_gdump:true
# --features profiling
# jeprof --text --show_bytes `which w` ...
