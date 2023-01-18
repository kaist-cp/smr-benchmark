import pandas as pd
from plotnine import *
import matplotlib.pyplot as plt

# Load NMTree with HP++
nmtree = pd.read_csv("./results/NMTree.csv")
nmtree = nmtree[nmtree.mm == "HP_PP"]

# Load HMList with HP
efrbtree = pd.read_csv("./results/EFRBTree.csv")
efrbtree = efrbtree[efrbtree.mm == "HP"]

def draw_throughput_by_thread(key_range: int):
    d1 = nmtree[nmtree.key_range == key_range]
    d1 = d1.groupby('threads')['throughput'].mean()
    d2 = efrbtree[efrbtree.key_range == key_range]
    d2 = d2.groupby('threads')['throughput'].mean()
    plt.figure()
    plt.title(f"Key Range {key_range}")
    plt.plot(d1.index, d1.values, marker="o", color="purple", label="HP++ NMTree")
    plt.plot(d2.index, d2.values, marker="D", color="red", label="HP EFRBTree")
    plt.xlabel("Threads")
    plt.ylabel("Throughput (op/s)")
    plt.legend()
    plt.grid()
    plt.savefig(f"./results/NMTree-EFRBTree-Throughput-{key_range}")

draw_throughput_by_thread(16)
draw_throughput_by_thread(10000)