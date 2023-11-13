#!/usr/bin/env python

import subprocess
import os

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")
BIN_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "target", "release")

mms_map = ['ebr', 'hp', 'circ-ebr', 'circ-hp', 'cdrc-ebr', 'cdrc-hp', 'cdrc-ebr-flush']
long_running_is = list(range(10, 61, 10))
cpu_count = os.cpu_count()
ts = [cpu_count//2, cpu_count, cpu_count*2, cpu_count*4]
runs = 1

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release'])

def extract_interval(cmd):
    for i in range(len(cmd)):
        if cmd[i] == '-i' and i + 1 < len(cmd):
            return int(cmd[i + 1])
    return 10

cmds = []

for t in ts:
    for mm in mms_map:
        for i in long_running_is:
            cmd = [os.path.join(BIN_PATH, mm), '-t', str(t), '-i', str(i), '-d', "skip-list", '-g', '1', '-r', '100000', '-o', os.path.join(RESULTS_PATH, f'skip-list-long-running.csv')]
            cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', sum(map(extract_interval, cmds)) // 60, ' min *', runs, 'times')

os.makedirs(RESULTS_PATH, exist_ok=True)
failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            subprocess.run(cmd, timeout=extract_interval(cmd) + 60)
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
