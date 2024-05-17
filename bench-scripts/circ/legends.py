import os
import matplotlib.pyplot as plt

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")

color_triple = ["#E53629", "#2CD23E", "#4149C3"]
face_alpha = "DF"

EBR = "ebr"
NR = "nr"
HP = "hp"
CDRC_EBR = "cdrc-ebr"
CDRC_HP = "cdrc-hp"
CIRC_EBR = "circ-ebr"
CIRC_HP = "circ-hp"
CDRC_EBR_FLUSH = "cdrc-ebr-flush"

line_shapes = {
    NR: {
        "marker": ".",
        "color": "k",
        "linestyle": "-",
    },
    EBR: {
        "marker": "o",
        "color": color_triple[0],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[0] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "-",
    },
    CDRC_EBR: {
        "marker": "o",
        "color": color_triple[1],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[1] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dotted",
    },
    CIRC_EBR: {
        "marker": "o",
        "color": color_triple[2],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[2] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dashed",
    },
    HP: {
        "marker": "v",
        "color": color_triple[0],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[0] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "-",
    },
    CDRC_HP: {
        "marker": "v",
        "color": color_triple[1],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[1] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dotted",
    },
    CIRC_HP: {
        "marker": "v",
        "color": color_triple[2],
        "markeredgewidth": 0.75,
        "markerfacecolor": color_triple[2] + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dashed",
    },
    CDRC_EBR_FLUSH: {
        "marker": "s",
        "color": "#828282",
        "markeredgewidth": 0.75,
        "markerfacecolor": "#828282" + face_alpha,
        "markeredgecolor": "k",
        "linestyle": "dashed",
    }
}

SMRS = [EBR, NR, HP, CDRC_EBR, CDRC_HP, CIRC_EBR, CIRC_HP, CDRC_EBR_FLUSH]

os.makedirs(f'{RESULTS_PATH}/legends', exist_ok=True)
for smr in SMRS:
    fig, ax = plt.subplots(ncols=1)
    ax.plot([0] * 3, linewidth=3, markersize=18, **line_shapes[smr])
    ax.set_xlim(2/3, 4/3)
    ax.set_axis_off()
    fig.set_size_inches(1.25, 0.75)
    fig.tight_layout()
    fig.savefig(f"{RESULTS_PATH}/legends/{smr}.pdf", bbox_inches="tight")
