#!/usr/bin/env python3
import matplotlib as mpl
import matplotlib.pyplot as plt
import argparse
import pandas
from pathlib import Path
from csvFields import ALLCSVFIELDS, AllCSVFieldsIndexedByKey, CSVField, CSVNCORES, DeductiveOversub
import textwrap as tw
from typing import List, Any


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


FIGWIDTH = 9
FIGHEIGHT = 6

def plotSubfig(ax: mpl.axes.Axes, df: pandas.DataFrame, xfield: CSVField, yfield: CSVField, zfield: CSVField, colors: List[Any]):
    """
    zfield will be drawn as different series in the figure with a legend
    """
    xvals = set()
    for (zval, zsubgroup), color in zip(df.groupby(zfield.key), colors):
        xgroups = zsubgroup.groupby(xfield.key)
        xvals |= set(xgroups.groups.keys())
        xaxis = []
        yaxis = []
        errorbars = []
        for xval, xsubgroup in xgroups:
            # each ysubgroup contains dataframes of different trials with the same expr configurations
            validLoc = xsubgroup['note'].isnull()
            if not validLoc.any():
                # skip configurations that have no valid data points
                continue
            validYVals = xsubgroup[validLoc][yfield.key]
            xaxis.append(xval)
            yaxis.append(validYVals.mean())
            errorbars.append(validYVals.std())
        ax.plot(xaxis, yaxis, label=f"{zfield.key}: {zval}", marker='.', markersize=6, color=color)
        for x, y, err in zip(xaxis, yaxis, errorbars):
            ax.errorbar(x, y, err, capsize=2, color=color)
        # draw subplot metadata
        ax.set_title(f"{xfield.key}<->{yfield.key}")
        ax.set_xlabel(xfield.key + xfield.getUnitInParenthesisIfExists())
        ax.set_ylabel(yfield.key + yfield.getUnitInParenthesisIfExists())
        ax.legend()
        ax.annotate(tw.fill(f"{yfield.key}: {yfield.description}", width=100),
                    (0, 0), (0, -40), xycoords="axes fraction", textcoords="offset points", va="top", wrap=True)
    xvals = list(xvals)
    ax.set_xticks(xvals, labels=[str(x) for x in xvals])
    # revert the default behavior of subplots sharex hiding xticklabels
    ax.tick_params(labelbottom=True)

def plot(args):
    exprName = args.input.removesuffix(".csv")
    # prepare subplots
    # each row: ncores <-> field | oversub <-> field
    fig, axs = plt.subplots(
        len(args.fields), 2,
        squeeze=False,
        figsize=(FIGWIDTH * 2, FIGHEIGHT*len(args.fields)),
        sharey="row",
        sharex="col",
    )
    fig.subplots_adjust(hspace=0.4)
    csvData = pandas.read_csv(args.input)
    if args.drop_first:
        csvData.drop(0)
    colormap = mpl.colormaps['tab10'].colors
    for ((axCol0, axCol1), field) in zip(axs, args.fields):
        csvField = AllCSVFieldsIndexedByKey[field]
        plotSubfig(axCol0, csvData, CSVNCORES, csvField, DeductiveOversub, colormap)
        plotSubfig(axCol1, csvData, DeductiveOversub, csvField, CSVNCORES, colormap)
        # revert the default behavior of subplots sharey hiding yticklabels
        axCol1.tick_params(labelleft=True)
    outdir = Path(args.dir)
    outdir.mkdir(exist_ok=True)
    fig.suptitle(f"{exprName}", fontsize="xx-large")
    fig.savefig(outdir / f"{exprName}.plot.png",
                dpi=300, format="png", bbox_inches="tight")
    # plt.show()


if __name__ == "__main__":
    args = buildParser()
    for f in args.fields:
        if f not in AllCSVFieldsIndexedByKey:
            raise RuntimeError(f"field '{f}' is not a valid csv field")
    plot(args)
