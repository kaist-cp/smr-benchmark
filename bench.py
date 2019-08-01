#!/usr/bin/python3

import subprocess

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NoMM']
threadss = [1] + list(range(5, 101, 5))

subprocess.run(['git', 'submodule', 'update', '--init'])

# througput
for ds in dss:
    for mm in mms:
        for threads in map(str, threadss):
            subprocess.run(['cargo', 'run', '--release', '--'] +
                           ['-d', ds, '-m', mm, '-t', threads])

# max mem usage
# export JEMALLOC_SYS_WITH_MALLOC_CONF=prof:true,prof_gdump:true
# --features profiling
