import pdb
import numpy as np
import pandas
from collections import OrderedDict as OD
import matplotlib as mpl
import matplotlib.pyplot as plt
import argparse
import io
from typing import List, Any, Iterable, ClassVar
from pathlib import Path
from dataclasses import dataclass, field
from itertools import cycle, groupby
from functools import partial
import re


def buildParser():
    parser = argparse.ArgumentParser(
        description="read perf stat report and render figures")
    parser.add_argument('--package', '-p', type=str, required=True,
                        help="The package name of the corresponding trace files. Used as a prefix to match corresponding .txt report files. All report files should be in csv-like format")
    parser.add_argument('--id', type=str, default="comm-pid",
                        help="The csv field name that can be used as an identifier (default: %(default)s)")
    parser.add_argument('--ignore', action="append", default=[
                        "unit"], help="fields in the trace to ignore (default: %(default)s)")
    parser.add_argument('--dir', '-d', type=str, default=".",
                        help="The directory to store rendered figures (default: '%(default)s)'")
    parser.add_argument('--verbose', '-v', action="store_true",
                        help="Print verbose info regarding the trace file")
    return parser.parse_args()


INVALIDSET = {"not counted"}


def getNewColorPalette():
    return cycle(mpl.colormaps['tab10'].colors)


@dataclass(kw_only=True)
class SubfigureArtistBase(object):
    test: ClassVar = []
    title: str
    axes: mpl.axes.Axes = None
    colors: Iterable[Any] = field(default_factory=getNewColorPalette)

    def renderMetadata(self):
        self.axes.set_xlabel("timestamp (sec)")
        self.axes.set_ylabel("counters")
        self.axes.set_title(f"{self.title}")
        self.axes.legend()


@dataclass(kw_only=True)
class SubfigurePerEventArtist(SubfigureArtistBase):
    """
    per-event figure are rendered once per distinct <timestamp, eventType>
    it is supposed to work with all counters collected for a specific event given a timestamp
    """
    event: str
    event2Artist = {}

    def __post_init__(self):
        self.__class__.event2Artist[self.event] = self

    def renderData(self, series: pandas.Series, label):
        """
        series contain counters that have a MultiIndex('time', 'comm')
        series.name represents the name of that event counter, should match with current event of interest
        """
        assert (series.name == self.event)
        plotSeriesMeanStd(self.axes, series, self.colors, label)


@dataclass(kw_only=True)
class SubfigureDerivedMetric(SubfigureArtistBase):
    """
    Subfigure for derived metrics, which are calculated based on all event counters corresponding to a <time, comm> record
    """
    title: str = "derived-metric-base"

    def renderData(self, df: pandas.DataFrame, label):
        """
        The input DataFrame has a MultiIndex('time', 'comm') and each row contains all event counters
        """
        postprocessSeries = df.agg(self.counterSeriesAgg, axis=1)
        plotSeriesMeanStd(self.axes, postprocessSeries, self.colors, label)

    def counterSeriesAgg(self, couters: pandas.Series):
        """
        counters: a series of all event counters for a particular <time, comm>
        """
        return None


@dataclass(kw_only=True)
class SubfigureDerivedInstPerCS(SubfigureDerivedMetric):
    title: str = "inst-per-cs"

    def counterSeriesAgg(self, counters: pandas.Series):
        return counters['inst_retired.any'] / (counters['cs'] + 1)


# per-timestamp figure are rendered once per distinct timestamp
# it is supposed to work with all events in a timestamp
PERTSARTISTS = [SubfigureDerivedInstPerCS]
# per-event figure are rendered once per distinct <timestamp, eventType>
# it is supposed to work with all counters collected for a specific event given a timestamp
PEREVENTARTISTS = [
    partial(SubfigurePerEventArtist, title="cs", event="cs"),
    # partial(SubfigurePerEventArtist, title="instructions", event="instructions"),
    partial(SubfigurePerEventArtist, title="inst_retired",
            event="inst_retired.any"),
]
SUBPLOTCFG = PERTSARTISTS + PEREVENTARTISTS

FIGWIDTH = 9
FIGHEIGHT = 6


def plotSeriesMeanStd(ax: mpl.axes.Axes, series: pandas.Series, colors: Iterable[Any], label: str, plotStd: bool = False, plotMax: bool = False):
    """
    plot two line in `ax` based on the given data series
    xaxis is the timestamp
    yaxis are the mean and max (optional) of the input series (N/A values are always dropped)
    one line shows 
    """
    timestamps = []
    means = []
    stds = []
    maxs = []
    for tval, tsubgroup in series.dropna().groupby('time'):
        timestamps.append(tval)
        means.append(tsubgroup.mean())
        # NOTE: if tsubgroup.size == 1, this std will return np.nan
        stds.append(tsubgroup.std())
        maxs.append(tsubgroup.max())
    # plot mean
    meanColor = next(colors)
    ax.scatter(timestamps, means,
               label=f"mean({label})", marker='.', s=6, color=meanColor, alpha=0.5)
    if plotStd:
        for x, y, err in zip(timestamps, means, stds):
            if not np.isnan(err):
                ax.errorbar(x, y, err, capsize=2, color=meanColor)
    # plot max
    if plotMax:
        ax.scatter(timestamps, maxs,
                   label=f"max({label})", marker='.', s=6, color=next(colors), alpha=0.5)


def countsFilter(count: str):
    """mark counters that are known to mean "not valid"""
    if count in INVALIDSET:
        return pandas.NA
    else:
        return int(count.replace(',', ''))


class PerfTrace:
    """
    Abstracted representation of a trace file based on its filename
    Assumption: ^{package}.C{ncores}.O{oversub}.*
    """

    def __init__(self, tracepath: Path):
        self.path = tracepath
        traceRegex = r"(?P<package>[^.]+)\.C(?P<ncores>[0-9]+)\.O(?P<oversub>[0-9]+)\.*"
        m = re.match(traceRegex, tracepath.name)
        assert (m is not None), f"invalid trace path {tracepath}"
        for k, v in m.groupdict().items():
            self.__setattr__(k, v)
        self.identifier = f"{self.package}.C{self.ncores}.O{self.oversub}"

    def getSortKey(self):
        return (int(self.ncores), int(self.oversub))

    def __repr__(self) -> str:
        return self.path.__repr__()


def plot(args):
    allTraces = sorted([PerfTrace(p) for p in Path().glob(
        f"{args.package}*.txt")], key=PerfTrace.getSortKey)
    # Dict[ncores, List[PerfTrace]]
    groupByNcores = OD([(int(ncores), list(traces)) for ncores, traces in groupby(
        allTraces, key=lambda t: int(t.ncores))])
    # Each row is an artist, each col corresponds to one ncores configuration
    fig, ax_grid = plt.subplots(len(SUBPLOTCFG), len(groupByNcores), squeeze=False, figsize=(
        FIGWIDTH * len(groupByNcores), FIGHEIGHT * len(SUBPLOTCFG)), sharex='col')
    subplot_cols = {ncores: [] for ncores in groupByNcores.keys()}
    for subfigFactory, ax_row in zip(SUBPLOTCFG, ax_grid):
        rowSharedPalette = getNewColorPalette()
        for ncore_plots, ax in zip(subplot_cols.values(), ax_row):
            subf = subfigFactory()
            subf.colors = rowSharedPalette
            subf.axes = ax
            ncore_plots.append(subf)
    for ncores, tracelist in groupByNcores.items():
        for tracepath in tracelist:
            traceFile = open(tracepath.path, "r")
            # TODO: use `perf stat -x ',' report -i xxx.trace` can export as csv
            # First line of the trace is a commented header
            # like: "# time comm-pid counts unit events"
            firstLine = traceFile.readline().lstrip('# ')
            colNames = list(
                filter(lambda s: s not in args.ignore, firstLine.split()))
            # There is also quoting in the trace report.
            # Here let us replace <> with "" (recognizable by the pandas)
            preprocessTrans = str.maketrans("<>", '""')
            preprocessData = traceFile.read().translate(preprocessTrans)
            traceData = pandas.read_table(io.StringIO(
                preprocessData),
                delim_whitespace=True, comment='#', names=colNames, converters={'counts': countsFilter})
            # filter out invalid counters
            # FIXME: allowing NA cells for testing purpose
            # validTraceData = traceData.loc[~traceData['counts'].isna()]
            if args.verbose:
                print(
                    f"The trace contains {traceData.size} entries in total, {validTraceData.size} valid entries")
            # preprocess the trace data. Transform events as columns
            validTraceData = traceData.groupby(['time', 'comm-pid']).apply(
                lambda subdf:
                pandas.Series(subdf['counts'].values, index=subdf['events'])
            )
            validTraceData.index.rename(['time', 'comm'])
            for ncore_plot in subplot_cols[ncores]:
                if isinstance(ncore_plot, SubfigureDerivedMetric):
                    # per-timestamp process
                    ncore_plot.renderData(
                        df=validTraceData, label=tracepath.identifier)
                elif isinstance(ncore_plot, SubfigurePerEventArtist):
                    # per-event process
                    if ncore_plot.event in validTraceData:
                        ncore_plot.renderData(
                            series=validTraceData[ncore_plot.event], label=tracepath.identifier)
            traceFile.close()
    for ncore_plots in subplot_cols.values():
        for ncore_plot in ncore_plots:
            ncore_plot.renderMetadata()
    # output
    outdir = Path(args.dir)
    outdir.mkdir(exist_ok=True)
    fig.suptitle(f"{args.package}", fontsize="xx-large")
    fig.savefig(outdir / f"{args.package}.plot.png", dpi=300,
                format="png", bbox_inches="tight")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('error')
    args = buildParser()
    plot(args)
