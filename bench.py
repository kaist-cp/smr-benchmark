import subprocess
import os.path

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NR']
ns = [0, 1, 2, 3]
ts = list(map(str, [1] + list(range(5, 76, 5))))
cs = [1, 4]
i = 10

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release'])

run_cmd = ['./target/release/pebr-benchmark', '-i', str(i), '-s1']


def opts(ds, mm, t, c=1, n=0):
    return ['-d', ds, '-m', mm, '-t', t, '-n', str(n), '-c', str(c)]


cmds = []

for ds in dss:
    for mm in mms:
        for n in ns:
            for c in cs:
                # meaningless
                if mm == 'NR' and (n != 0 or c != 1):
                    continue
                for t in ts:
                    cmd = run_cmd + opts(ds, mm, t, c=c, n=n)
                    cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.3) // 60, ' min')

for i, cmd in enumerate(cmds):
    print("{}/{}: '{}'".format(i + 1, len(cmds), ' '.join(cmd)))
    subprocess.run(cmd)
