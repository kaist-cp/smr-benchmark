#!/usr/bin/python3

import subprocess

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NoMM']
threadss = [1] + list(range(5, 101, 5))

subprocess.run(['git', 'submodule', 'update', '--init'])

for ds in dss:
    for mm in mms:
        for threads in map(str, threadss):
            subprocess.run(
                ['cargo', 'run', '--release', '--', '-d', ds, '-m', mm, '-t', threads])
