#!/usr/bin/env python

import subprocess
import os

RESULTS_PATH = "bench-scripts/hp-sharp/results"

dss = ['h-list', 'hm-list', 'hhs-list', 'hash-map', 'nm-tree', 'skip-list']
mms = ['nr', 'ebr', 'pebr', 'hp', 'hp-pp', 'nbr', 'cdrc-ebr', 'hp-sharp']
i = 10
cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = list(map(str, [1] + list(range(4, 33, 4))))
elif cpu_count <= 64:
    ts = list(map(str, [1] + list(range(8, 129, 8))))
else:
    ts = list(map(str, [1] + list(range(10, 151, 10))))
runs = 1
gs = [0, 1, 2, 3]

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release'])

run_cmd = ['./target/release/smr-benchmark', '-i', str(i)]

def key_range(ds):
    if ds in ["h-list", "hm-list", "hhs-list"]:
        return "10000"
    else:
        return "100000"

def opts(ds, mm, g, t, kr):
    return ['-d', ds, '-m', mm, '-g', str(g), '-t', t, '-r', str(kr), '-o', f'{RESULTS_PATH}/{ds}.csv']

def invalid(mm, ds, g):
    is_invalid = False
    if ds == 'hhs-list':
        is_invalid |= g == 0  # HHSList is just HList with faster get()
    if mm == 'hp':
        is_invalid |= ds in ["h-list", "hhs-list", "nm-tree"]
    if mm == 'nbr':
        is_invalid |= ds in ["hm-list", "skip-list"]
    return is_invalid

cmds = []

for ds in dss:
    for mm in mms:
        for g in gs:
            if invalid(mm, ds, g):
                continue
            for t in ts:
                cmd = run_cmd + opts(ds, mm, g, t, key_range(ds))
                cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.1) // 60, ' min *', runs, 'times')

os.makedirs(RESULTS_PATH, exist_ok=True)
failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            subprocess.run(cmd, timeout=40)
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
