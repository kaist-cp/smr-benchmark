import subprocess
import os
import sys

dss = ['HList', 'HMList', 'HHSList', 'HashMap', 'NMTree', 'EFRBTree', 'SkipList', 'BonsaiTree']
mms = ['EBR', 'NR', 'HP', 'HP_PP', 'PEBR', 'NBR', 'CDRC_EBR']
cs = [1]
i = 10
cpu_count = os.cpu_count()
if not cpu_count or cpu_count <= 24:
    ts = list(map(str, [1] + list(range(4, 33, 4))))
elif cpu_count <= 56:
    ts = list(map(str, [1] + list(range(5, 76, 5))))
elif cpu_count <= 96:
    ts = list(map(str, [1] + list(range(8, 121, 8))))
else:
    ts = list(map(str, [1] + list(range(10, 151, 10))))
gs = [0, 1, 2]
krs = [False, True] # Small, Large
runs = 2
if len(sys.argv) >= 2 and sys.argv[1] == 'simple':
    ts = list(map(str, [1, 20, 30]))
    gs = [0]
    runs = 1

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release'])

run_cmd = ['./target/release/pebr-benchmark', '-i', str(i), '-s1']

def key_range(ds, large):
    if ds in ["HList", "HMList", "HHSList"]:
        if large:
            return "10000"
        else:
            return "16"
    else:
        if large:
            return "100000"
        else:
            return "128"

def opts(ds, mm, g, c, t, kr_str):
    return ['-d', ds, '-m', mm, '-g', str(g), '-c', str(c), '-t', t, '-r', kr_str]

def invalid(mm, ds, c, g):
    is_invalid = False
    if mm == 'NR':
        is_invalid |= c != 1  # meaningless config
    if ds == 'HHSList':
        is_invalid |= g == 0  # HHSList is just HMList with faster get()
    if mm == 'HP':
        is_invalid |= ds in ["HList", "HHSList"]
    if mm == 'NBR':
        is_invalid |= ds in ["BonsaiTree"]
    return is_invalid


cmds = []

for kr in krs:
    for ds in dss:
        for mm in mms:
            for g in gs:
                for c in cs:
                    if invalid(mm, ds, c, g):
                        continue
                    for t in ts:
                        cmd = run_cmd + opts(ds, mm, g, c, t, key_range(ds, kr))
                        cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.3) // 60, ' min *', runs, 'times')

failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            subprocess.run(cmd, timeout=20)
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