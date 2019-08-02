#!/usr/bin/python3

import subprocess

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NR']
threadss = [1] + list(range(5, 101, 5))

subprocess.run(['git', 'submodule', 'update', '--init'])

# througput: less contention
for ds in dss:
    for mm in mms:
        for threads in map(str, threadss):
            subprocess.run(['cargo', 'run', '--release', '--'] +
                           ['-d', ds, '-m', mm, '-t', threads])

# througput: high contention
for ds in dss:
    for mm in mms:
        for threads in map(str, threadss):
            subprocess.run(
                ['cargo', 'run', '--release', '--'] +
                ['-d', ds, '-m', mm, '-t', threads, '-r100', '-p50'])

# non-cooperative threads (don't test NR)
for ds in dss:
    for mm in ['EBR', 'PEBR']:
        for threads in map(str, threadss):
            subprocess.run(
                ['cargo', 'run', '--release', '--'] +
                ['-d', ds, '-m', mm, '-t', threads, '-n1,1024'])

# max mem usage
# export JEMALLOC_SYS_WITH_MALLOC_CONF=prof:true,prof_gdump:true
# --features profiling
# jeprof --text --show_bytes `which w` ...
