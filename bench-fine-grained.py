import subprocess
import os
import sys

mms = ['EBR', 'NR', 'HP', 'HP_PP', 'PEBR', 'CDRC_EBR']

krs = [(2 ** e) for e in range(18, 27, 1)]
cpu_count = os.cpu_count()
writers = cpu_count // 2
readers = cpu_count // 2
runs = 2
i = 10

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release', '--bin', 'fine_grained_bench'])

run_cmd = ['./target/release/fine_grained_bench', f'-i{i}', f'-w{writers}', f'-g{readers}']

cmds = []

for mm in mms:
    for kr in krs:
        cmd = run_cmd + [f'-m{mm}', f'-r{kr}']
        cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.3) // 60, ' min *', runs, 'times')

failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            subprocess.run(cmd, timeout=30)
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