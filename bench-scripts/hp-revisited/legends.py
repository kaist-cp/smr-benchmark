import os
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from matplotlib.path import Path
from matplotlib.transforms import Affine2D

RESULTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "results")

EBR = "ebr"
PEBR = "pebr"
NR = "nr"
HP = "hp"
HP_PP = "hp-pp"
HP_BRCU = "hp-brcu"
HP_RCU = "hp-rcu"
VBR = "vbr"

SMRs = [NR, EBR, HP_PP, HP, PEBR, HP_BRCU, HP_RCU, VBR]

FACE_ALPHA = 0.85

# https://matplotlib.org/stable/gallery/lines_bars_and_markers/marker_reference.html
line_shapes = {
    NR: {
        "marker": ".",
        "color": "k",
        "linestyle": "-",
    },
    EBR: {
        "marker": "o",
        "color": "c",
        "linestyle": "-",
    },
    HP: {
        "marker": "v",
        "color": "hotpink",
        "linestyle": "dashed",
    },
    HP_PP: {
        "marker": "^",
        "color": "purple",
        "linestyle": "dashdot",
    },
    PEBR: {
        # Diamond("D") shape, but smaller.
        "marker": Path.unit_rectangle()
                      .transformed(Affine2D().translate(-0.5, -0.5)
                                             .rotate_deg(45)),
        "color": "y",
        "linestyle": (5, (10, 3)),
    },
    HP_BRCU: {
        "marker": "X",
        "color": "r",
        "linestyle": (5, (10, 3)),
    },
    HP_RCU: {
        "marker": "P",
        "color": "green",
        "linestyle": (5, (10, 3)),
    },
    VBR: {
        "marker": "d",
        "color": "orange",
        "linestyle": (0, (2, 1)),
    },
}

# Add some common or derivable properties.
line_shapes = dict(map(
    lambda kv: kv if kv[0] == NR else
        (kv[0], { **kv[1],
                 "markerfacecolor": (*colors.to_rgb(kv[1]["color"]), FACE_ALPHA),
                 "markeredgecolor": "k",
                 "markeredgewidth": 0.75 }),
    line_shapes.items()
))

if __name__ == "__main__":
    os.makedirs(f'{RESULTS_PATH}/legends', exist_ok=True)
    for smr in SMRs:
        fig, ax = plt.subplots(ncols=1)
        ax.plot([0] * 3, linewidth=3, markersize=18, **line_shapes[smr])
        ax.set_xlim(2/3, 4/3)
        ax.set_axis_off()
        fig.set_size_inches(1.25, 0.75)
        fig.tight_layout()
        fig.savefig(f"{RESULTS_PATH}/legends/{smr}.pdf", bbox_inches="tight")
