#!/usr/bin/env python3
"""Plot benchmarks/results.csv into docs/figures/ (a light and a dark PNG per figure).

    python scripts/plot_benchmarks.py

Each point is the median of the runs for that engine, log and core count; only
runs that finished and matched the ground truth are plotted.
"""

import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "figures"

# One colour per engine, the same in every figure (validated categorical slots 1-3).
THEMES = {
    "light": {"surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e", "grid": "#e4e3df",
              "baseline": "#2a78d6", "spark": "#eb6834", "spark-df": "#1baf7a"},
    "dark": {"surface": "#1a1a19", "text": "#f0efec", "muted": "#c3c2b7", "grid": "#33322f",
             "baseline": "#3987e5", "spark": "#d95926", "spark-df": "#199e70"},
}
LABELS = {"baseline": "networkx, 1 process", "spark": "Spark RDD", "spark-df": "Spark DataFrame"}


def load():
    groups, failed = defaultdict(list), {}
    with open(ROOT / "benchmarks" / "results.csv") as f:
        for row in csv.DictReader(f):
            key = (row["engine"], int(row["lines"]), int(row["cores"]))
            if row["status"] == "ok" and row["correct"] == "True":
                groups[key].append(float(row["wall_s"]))
            elif row["status"] != "ok":
                failed[key] = row["status"]
    return {k: statistics.median(v) for k, v in groups.items()}, failed


def style(ax, fig, t):
    fig.patch.set_facecolor(t["surface"])
    ax.set_facecolor(t["surface"])
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(t["grid"])
    ax.tick_params(colors=t["muted"], labelsize=9)
    ax.grid(True, which="major", color=t["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def human(n):
    return f"{n / 1e6:g}M" if n >= 1e6 else f"{n / 1e3:g}k"


def size_sweep(medians, failed, t, path):
    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=160)
    style(ax, fig, t)
    for engine in ("baseline", "spark", "spark-df"):
        cores = 1 if engine == "baseline" else 8
        pts = sorted((n, s) for (e, n, c), s in medians.items() if e == engine and c == cores)
        if not pts:
            continue
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=t[engine], linewidth=2, marker="o", markersize=5,
                markeredgecolor=t["surface"], markeredgewidth=1.5, label=LABELS[engine])
        ax.annotate(LABELS[engine] + ("" if engine == "baseline" else " (8 cores)"), (xs[-1], ys[-1]),
                    xytext=(6, 0), textcoords="offset points", va="center", fontsize=8.5, color=t["text"])
        for (e, n, c), status in failed.items():
            if e == engine and c == cores:
                ax.plot([n], [ys[-1]], marker="x", markersize=9, markeredgewidth=2, color=t[engine])
                ax.annotate(f"out of memory\n({status}, 9 GB cap)", (n, ys[-1]), xytext=(0, -14),
                            textcoords="offset points", ha="center", va="top", fontsize=8, color=t["muted"])
    ax.set_xscale("log")
    ax.set_yscale("log")
    xs = sorted({n for (_, n, _) in medians})
    ax.set_xticks(xs, [human(x) for x in xs])
    ax.minorticks_off()
    ax.set_xlabel("log lines (realistic generator, ~2% of processes deadlocked)", color=t["muted"], fontsize=9)
    ax.set_ylabel("end-to-end wall time, seconds (median of 3)", color=t["muted"], fontsize=9)
    ax.set_title("Runtime vs log size, one 8-core laptop", color=t["text"], fontsize=11, loc="left")
    ax.legend(frameon=False, fontsize=8.5, labelcolor=t["text"], loc="upper left")
    ax.set_xlim(right=xs[-1] * 3.5)
    fig.tight_layout()
    fig.savefig(path, facecolor=t["surface"])
    plt.close(fig)


def core_sweep(medians, lines, t, path):
    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=160)
    style(ax, fig, t)
    for engine in ("spark", "spark-df"):
        pts = sorted((c, s) for (e, n, c), s in medians.items() if e == engine and n == lines)
        if len(pts) < 2:
            continue
        xs, ys = zip(*pts)
        speedup = [ys[0] / y for y in ys]
        ax.plot(xs, speedup, color=t[engine], linewidth=2, marker="o", markersize=5,
                markeredgecolor=t["surface"], markeredgewidth=1.5, label=LABELS[engine])
        ax.annotate(f"{speedup[-1]:.1f}×", (xs[-1], speedup[-1]), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=8.5, color=t["text"])
    ax.plot([1, 8], [1, 8], color=t["muted"], linewidth=1, linestyle=(0, (4, 3)), label="ideal (linear)")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
    ax.set_yticks([1, 2, 4, 8], ["1×", "2×", "4×", "8×"])
    ax.minorticks_off()
    ax.set_xlabel("Spark local cores", color=t["muted"], fontsize=9)
    ax.set_ylabel("speedup over 1 core", color=t["muted"], fontsize=9)
    ax.set_title(f"Strong scaling on a {human(lines)}-line log", color=t["text"], fontsize=11, loc="left")
    ax.legend(frameon=False, fontsize=8.5, labelcolor=t["text"], loc="upper left")
    fig.tight_layout()
    fig.savefig(path, facecolor=t["surface"])
    plt.close(fig)


def main():
    medians, failed = load()
    OUT.mkdir(parents=True, exist_ok=True)
    core_lines = max((n for (e, n, c) in medians if e == "spark" and c == 1), default=None)
    for mode, t in THEMES.items():
        size_sweep(medians, failed, t, OUT / f"size_sweep_{mode}.png")
        if core_lines:
            core_sweep(medians, core_lines, t, OUT / f"core_sweep_{mode}.png")
    print(f"wrote {sorted(p.name for p in OUT.glob('*.png'))}")


if __name__ == "__main__":
    main()
