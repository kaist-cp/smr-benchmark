import subprocess
import os.path

dss = ['HList', 'HMList', 'HHSList', 'HashMap', 'NMTree', 'BonsaiTree', 'MSQueue']
mms = ['EBR', 'PEBR', 'NR']
ns = [0, 2, 3]
ts = list(map(str, [1] + list(range(5, 76, 5))))
cs = [1]
gs = [0, 1]
i = 3
runs = 3

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release'])

run_cmd = ['./target/release/pebr-benchmark', '-i', str(i), '-s1']


def opts(ds, mm, g, n, c, t):
    r = 1000 if ds in ['HList', 'HMList', 'HHSList'] else 100000
    return ['-d', ds, '-r', str(r), '-m', mm, '-g', str(g), '-n', str(n), '-c', str(c), '-t', t]

def invalid(mm, ds, c, n, g):
    is_invalid = False
    if mm == 'NR':
        is_invalid |= n != 0 or c != 1  # meaningless config
    if ds == 'MSQueue':
        is_invalid |= g != 0  # push:pop=1:1 only
    if ds == 'HHSList':
        is_invalid |= g == 0  # HHSList is just HMList with faster get()
    return is_invalid


cmds = []

for ds in dss:
    for mm in mms:
        for g in gs:
            for n in ns:
                for c in cs:
                    if invalid(mm, ds, c, n, g):
                        continue
                    for t in ts:
                        cmd = run_cmd + opts(ds, mm, g, n, c, t)
                        cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.3) // 60, ' min *', runs, 'times')

for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        subprocess.run(cmd)
