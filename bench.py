import subprocess

dss = ['List', 'HashMap', 'NMTree', 'BonsaiTree']
mms = ['EBR', 'PEBR', 'NR']
# gs = ['', '-gg']
ns = ['', '-n', '-nn']
ts = list(map(str, [1] + list(range(5, 76, 5))))
cs = ['1', '4']

subprocess.run(['git', 'submodule', 'update', '--init'])

# TODO: i1
run_cmd = ['cargo', 'run', '--release', '--', '-i1', '-s1']


def opts(ds, mm, t, c=1, g='', n=''):
    return ['-d', ds, '-m', mm, '-c', str(c), '-t', t] +\
            ([] if g == '' else [g]) +\
            ([] if n == '' else [n])


# througput
for ds in dss:
    for n in ns:
        for mm in mms:
            for t in ts:
                if ds == 'HashMap':
                    cmd = run_cmd + opts(ds, mm, t, c=4)
                    # print(cmd)
                    subprocess.run(cmd)
                cmd = run_cmd + opts(ds, mm, t, c=1, n=n)
                # print(cmd)
                subprocess.run(cmd)
