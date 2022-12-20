#!/usr/bin/env python3
import matplotlib as mpl
import matplotlib.pyplot as plt
import argparse
import pandas
from pathlib import Path
from itertools import cycle
from csvFields import ALLCSVFIELDS, AllCSVFieldsIndexedByKey
import textwrap as tw


def buildParser():
    parser = argparse.ArgumentParser(
        description="read expr csv file and render figures")
    parser.add_argument('--input', '-i', type=str, required=True,
                        help="The input csv file, the filename prefix (excluding the suffix .csv) will be used as the name of the experiment")
    parser.add_argument('--fields', '-F', type=lambda s: s.split(','), default="elapsed",
                        help=f"Comma separated list of fields in the csv that we want to plot (default: %(default)s) (all fields: {','.join([f.key for f in ALLCSVFIELDS])})")
    parser.add_argument('--drop-first', action="store_true",
                        help="Remove the first experiment (row) in the dataset, if you want to consider it as warmup.")
    parser.add_argument('--dir', '-d', type=str, default=".",
                        help="The directory to store rendered figures (default: '%(default)s)'")

    return parser.parse_args()


DEFAULT_FIELD_UNITS = {
    "elapsed": "sec",
}

FIGWIDTH = 9
FIGHEIGHT = 6


def plot(args):
    exprName = args.input.removesuffix(".csv")
    # prepare subplots
    fig, axs = plt.subplots(
        len(args.fields), 1,
        squeeze=False,
        figsize=(FIGWIDTH, FIGHEIGHT*len(args.fields)),
        sharex="row",
    )
    fig.subplots_adjust(hspace=0.4)
    csvData = pandas.read_csv(args.input)
    if args.drop_first:
        csvData.drop(0)
    colorCycler = cycle(mpl.colormaps['tab10'].colors)
    for oversub, oversubDF in csvData.groupby("oversub"):
        ncoresGroups = oversubDF.groupby("ncores")
        for ((ax,), field) in zip(axs, args.fields):
            # Fill in individual data points
            color = next(colorCycler)
            xaxis = []
            yaxis = []
            errorbars = []
            for ncores, ncoresDF in ncoresGroups:
                # ncoresDF should only contains dataframes of different trials with the same expr configurations
                fieldVals = ncoresDF[field]
                xaxis.append(ncores)
                yaxis.append(fieldVals.mean())
                errorbars.append(fieldVals.std())
            ax.plot(xaxis, yaxis, label=f"{oversub}x",
                    marker='.', markersize=6, color=color)
            for x, y, err in zip(xaxis, yaxis, errorbars):
                ax.errorbar(x, y, err, capsize=2, color=color)
            # draw subplot metadata
            ax.set_title(f"ncores <-> {field}")
            ax.set_xlabel("ncores")
            ax.set_ylabel(f"{field} ({DEFAULT_FIELD_UNITS.get(field, '')})")
            ax.legend()
            csvField = AllCSVFieldsIndexedByKey[field]
            ax.annotate(tw.fill(f"{csvField.key}: {csvField.description}", width=100), (0, 0), (0, -40), xycoords="axes fraction", textcoords="offset points", va="top", wrap=True)
            allNcores = list(ncoresGroups.groups.keys())
            ax.set_xticks(allNcores, labels=allNcores)
    outdir = Path(args.dir)
    outdir.mkdir(exist_ok=True)
    fig.suptitle(f"{exprName}", fontsize="xx-large")
    fig.savefig(outdir / f"{exprName}.plot.png", dpi=300, format="png", bbox_inches="tight")
    # plt.show()


if __name__ == "__main__":
    args = buildParser()
    plot(args)
