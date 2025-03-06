#!/usr/bin/env python

import subprocess
import os

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")
BIN_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "target", "release")

dss = ['h-list', 'hm-list', 'hhs-list', 'hash-map', 'nm-tree', 'skip-list', 'elim-ab-tree']
# "-large" suffix if it uses a large garbage bag.
mms = ['nr', 'ebr', 'pebr', 'hp', 'hp-pp', 'hp-brcu', 'vbr', 'circ-hp', 'cdrc-hp']
i = 10
cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = list(map(str, [1] + list(range(4, 33, 4))))
elif cpu_count <= 64:
    ts = list(map(str, [1] + list(range(8, 129, 8))))
else:
    ts = list(map(str, [1] + list(range(12, 193, 12))))
runs = 2
gs = [0, 1, 2]

subprocess.run(['cargo', 'build', '--release'])

def key_ranges(ds):
    if ds in ["h-list", "hm-list", "hhs-list"]:
        # 1K and 10K
        return ["1000", "10000"]
    else:
        # 100K and 100M
        return ["100000", "100000000"]

def is_suffix(orig, suf):
    return len(suf) <= len(orig) and mm[-len(suf):] == suf

def make_cmd(mm, i, ds, g, t, kr):
    bag = "small"
    if is_suffix(mm, "-large"):
        mm = mm[:len(mm)-len("-large")]
        bag = "large"

    return [os.path.join(BIN_PATH, mm),
            '-i', str(i),
            '-d', str(ds),
            '-g', str(g),
            '-t', str(t),
            '-r', str(kr),
            '-b', bag,
            '-o', os.path.join(RESULTS_PATH, f'{ds}.csv')]

def invalid(mm, ds, g):
    is_invalid = False
    if ds == 'hhs-list':
        is_invalid |= g == 0  # HHSList is just HList with faster get()
    if mm == 'nbr':
        is_invalid |= ds in ["hm-list", "skip-list"]
    if ds == 'elim-ab-tree':
        is_invalid |= mm in ["hp-pp"]
    return is_invalid

cmds = []

for ds in dss:
    for kr in key_ranges(ds):
        for mm in mms:
            for g in gs:
                if invalid(mm, ds, g):
                    continue
                for t in ts:
                    cmds.append(make_cmd(mm, i, ds, g, t, kr))

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.1) // 60, ' min *', runs, 'times\n')

for i, cmd in enumerate(cmds):
    try:
        print(f"\rdry-running commands... ({i+1}/{len(cmds)})", end="")
        subprocess.run(cmd + ['--dry-run'])
    except:
        print(f"A dry-run for the following command is failed:\n{' '.join(cmd)}")
        exit(1)
print("\nAll dry-runs passed!\n")

os.makedirs(RESULTS_PATH, exist_ok=True)
failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            subprocess.run(cmd, timeout=i+30)
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
