import subprocess
import os

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")
BIN_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "target", "release", "long-running")

mms = ['nr', 'ebr', 'pebr', 'hp', 'hp-pp', 'nbr', 'nbr-large', 'hp-brcu', 'hp-rcu', 'vbr']

krs = [(2 ** e) for e in range(18, 30, 1)]
cpu_count = os.cpu_count()
writers = cpu_count // 2
readers = cpu_count // 2
runs = 4
i = 10

if os.path.exists('.git'):
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'])
subprocess.run(['cargo', 'build', '--release', '--bin', 'long-running'])

run_cmd = [BIN_PATH, f'-i{i}', f'-w{writers}', f'-g{readers}']

cmds = []

for mm in mms:
    for kr in krs:
        cmd = run_cmd + [f'-m{mm}', f'-r{kr}', f'-o{RESULTS_PATH}/long-running.csv']
        cmds.append(cmd)

print('number of configurations: ', len(cmds))
print('estimated time: ', (len(cmds) * i * 1.7) // 60, ' min *', runs, 'times')

os.makedirs(RESULTS_PATH, exist_ok=True)
failed = []
for run in range(runs):
    for i, cmd in enumerate(cmds):
        print("run {}/{}, bench {}/{}: '{}'".format(run + 1, runs, i + 1, len(cmds), ' '.join(cmd)))
        try:
            # NOTE(`timeout=120`): prefilling may take a while...
            subprocess.run(cmd, timeout=120, check=True)
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
