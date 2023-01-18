import pandas as pd
from plotnine import *
import matplotlib.pyplot as plt

# Load NMTree with HP++
nmtree = pd.read_csv("./results/NMTree.csv")
nmtree = nmtree[nmtree.mm == "HP_PP"]

# Load HMList with HP
efrbtree = pd.read_csv("./results/EFRBTree.csv")
efrbtree = efrbtree[efrbtree.mm == "HP"]

get_rate_str = {
    0: "0%",
    1: "50%",
    2: "90%"
}

def draw_throughput_by_thread(key_range: int, get_rate: int):
    d1 = nmtree[(nmtree.key_range == key_range) & (nmtree.get_rate == get_rate) & (nmtree.threads <= 80)]
    d1 = d1.groupby('threads')['throughput'].mean()
    d2 = efrbtree[(efrbtree.key_range == key_range) & (efrbtree.get_rate == get_rate) & (efrbtree.threads <= 80)]
    d2 = d2.groupby('threads')['throughput'].mean()
    plt.figure()
    plt.suptitle(f"Key Range {key_range}", fontsize=20)
    plt.plot(d1.index, d1.values, marker='D', linestyle="--", color="purple", label="HP++ NMTree", markersize=13)
    plt.plot(d2.index, d2.values, marker="v", linestyle="--", color="hotpink", label="HP EFRBTree", markersize=13)
    plt.xlabel("Threads")
    plt.ylabel("Throughput (op/s)")
    plt.grid()
    plt.axvspan(64, 80, facecolor='gray', alpha=0.1, hatch='///')
    plt.savefig(f"./results/NMTree-EFRBTree-Throughput-{key_range}-{get_rate}.pdf", format="pdf")

for key_range in [128, 100000]:
    for get_rate in [0, 1, 2]:
        draw_throughput_by_thread(key_range, get_rate)