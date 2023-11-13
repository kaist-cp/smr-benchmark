#!/usr/bin/env python

import subprocess
import os

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")
BIN_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "target", "release")

dss = ['h-list', 'hm-list', 'hhs-list', 'hash-map', 'nm-tree', 'skip-list']
mms_map = ['nr', 'ebr', 'hp', 'circ-ebr', 'circ-hp', 'cdrc-ebr', 'cdrc-hp']
mms_queue = ['nr', 'ebr', 'circ-ebr', 'cdrc-ebr', 'cdrc-ebr-flush']
i = 10
runs = 1
gs = [0, 1, 2]

cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts_map = list(map(str, [1] + list(range(4, 33, 4))))
    ts_queue = list(map(str, [1, 2, 3] + list(range(4, 33, 4))))
elif cpu_count <= 64:
    ts_map = list(map(str, [1] + list(range(8, 129, 8))))
    ts_queue = list(map(str, [1, 2, 4] + list(range(8, 129, 8))))
else:
    ts_map = list(map(str, [1] + list(range(8, 193, 8))))
    ts_queue = list(map(str, [1, 2, 4] + list(range(8, 193, 8))))

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release'])

def key_ranges(ds):
    if ds in ["h-list", "hm-list", "hhs-list"]:
        return ["1000", "10000"]
    else:
        # 100K and 100M
        return ["100000", "100000000"]

def invalid(mm, ds, g):
    is_invalid = False
    if ds == 'hhs-list':
        is_invalid |= g == 0  # HHSList is just HList with faster get()
    if mm == 'hp':
        is_invalid |= ds in ["h-list", "hhs-list", "nm-tree"]
    return is_invalid

def extract_interval(cmd):
    for i in range(len(cmd)):
        if cmd[i] == '-i' and i + 1 < len(cmd):
            return int(cmd[i + 1])
    return 10

cmds = []

for ds in dss:
    for mm in mms_map:
        for g in gs:
            if invalid(mm, ds, g):
                continue
            for t in ts_map:
                for kr in key_ranges(ds):
                    cmd = [os.path.join(BIN_PATH, mm), '-i', str(i), '-d', ds, '-g', str(g), '-t', t, '-r', str(kr), '-o', os.path.join(RESULTS_PATH, f'{ds}.csv')]
                    cmds.append(cmd)

for mm in mms_queue:
    for t in ts_queue:
        cmd = [os.path.join(BIN_PATH, "double_link"), '-m', mm, '-i', str(i), '-t', str(t), '-o', os.path.join(RESULTS_PATH, 'double-link.csv')]
        cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', sum(map(extract_interval, cmds)) // 60, ' min *', runs, 'times')

os.makedirs(RESULTS_PATH, exist_ok=True)
failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            subprocess.run(cmd, timeout=extract_interval(cmd) + 30)
        except subprocess.TimeoutExpired:
            print("timeout")
            failed.append(' '.join(cmd))
        except KeyboardInterrupt:
            if len(failed) > 0:
                print("====failed====")
                print("\n".join(failed))
            exit(0)
        except:
            failed.append(' '.join(cmd))

if len(failed) > 0:
    print("====failed====")
    print("\n".join(failed))
