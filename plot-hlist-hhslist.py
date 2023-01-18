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

def draw_throughput_by_thread(key_range: int):
    d1 = hhslist[hhslist.key_range == key_range]
    d1 = d1.groupby('threads')['throughput'].mean()
    d2 = hmlist[hmlist.key_range == key_range]
    d2 = d2.groupby('threads')['throughput'].mean()
    plt.figure()
    plt.title(f"Key Range {key_range}")
    plt.plot(d1.index, d1.values, marker="o", color="purple", label="HP++ HHSList")
    plt.plot(d2.index, d2.values, marker="D", color="red", label="HP HMList")
    plt.xlabel("Threads")
    plt.ylabel("Throughput (op/s)")
    plt.legend()
    plt.grid()
    plt.savefig(f"./results/HHSList-HMList-Throughput-{key_range}")

draw_throughput_by_thread(16)
draw_throughput_by_thread(10000)