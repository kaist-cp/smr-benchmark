# (HP++ HHSList) vs (HP HMList): Throughput

import pandas as pd
from plotnine import *
import matplotlib.pyplot as plt

# Load HHSList with HP++
hhslist = pd.read_csv("./results/HHSList.csv")
hhslist = hhslist[hhslist.mm == "HP_PP"]

# Load HMList with HP
hmlist = pd.read_csv("./results/HMList.csv")
hmlist = hmlist[hmlist.mm == "HP"]

get_rate_str = {
    0: "0%",
    1: "50%",
    2: "90%"
}

def draw_throughput_by_thread(key_range: int, get_rate: int):
    d1 = hhslist[(hhslist.key_range == key_range) & (hhslist.get_rate == get_rate) & (hhslist.threads <= 80)]
    d1 = d1.groupby('threads')['throughput'].mean()
    d2 = hmlist[(hmlist.key_range == key_range) & (hmlist.get_rate == get_rate) & (hmlist.threads <= 80)]
    d2 = d2.groupby('threads')['throughput'].mean()
    plt.figure()
    plt.suptitle(f"Key Range {key_range}", fontsize=20)
    plt.plot(d1.index, d1.values, marker='D',  linestyle="--", color="purple", label="HP++ HHSList", markersize=13)
    plt.plot(d2.index, d2.values, marker="v", linestyle="--", color="hotpink", label="HP HMList", markersize=13)
    plt.xlabel("Threads")
    plt.ylabel("Throughput (M op/s)")
    plt.grid()
    plt.axvspan(64, 80, facecolor='gray', alpha=0.1, hatch='///')
    plt.savefig(f"./results/HHSList-HMList-Throughput-{key_range}-{get_rate}.pdf", format="pdf")

for key_range in [16, 10000]:
    draw_throughput_by_thread(key_range, 1)