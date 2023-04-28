#!/usr/bin/env python3
import argparse
from typing import List, Tuple, Callable, Optional
import csv
import itertools
from pathlib import Path
import os
import json
import subprocess
import shlex

from csvFields import RAWDATACSVFIELDS, ALLCSVFIELDS, DeductiveFields
from profiler import ALL_PROFILER, PROFILER_NAMEMAP
from parsecRun import ParsecRun
from threadedcg import ThreadedCG


def parseCherryPickedConf(conf: str) -> List[Tuple[int]]:
    """
    Parse configuration string that represents cherry-picked setup.
    e.g., (1 4),(2 4)
    term: ConfTuple means (1 4)
    """
    def parseConfTuple(s): return list(map(int, s.strip('()').split(' ')))
    return list(map(parseConfTuple, conf.split(',')))


def parseIntCommaList(conf: str) -> List[int]:
    return list(map(int, conf.split(',')))


def buildParser():
    """
    return: args
    """
    epilog = "Available Profilers and their description:\n"
    for p in ALL_PROFILER:
        epilog += f"#### {p.name}\n{p.getHelp()}\n"
    parser = argparse.ArgumentParser(
        description="Perform sweep experiment over thread oversubscription",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog = epilog)
    parser.add_argument('--packages', '-p', type=str, required=True,
                        help="A comma separated list of packages to run")
    parser.add_argument('--cores', '-C', type=parseIntCommaList, default=[],
                        help="A comma separated list of #cores to test")
    parser.add_argument('--oversub', '-S', type=parseIntCommaList, default=[],
                        help="A comma separated list of thread oversubscription ratio to test")
    parser.add_argument('--cherrypick', type=parseCherryPickedConf, default=[],
                        help="A comma separated list of specific (ncore oversub cgcfg) pairs of configurations. e.g., (1 4 0),(2 8 0)")
    parser.add_argument('--dryrun', action="store_true",
                        help="Dump the commands to run without running the benchmark")
    parser.add_argument('--numamem', '-m', type=int, default=0,
                        help="The numa node to allocate memory from. Passed to parsecmgmt (default: %(default)s)")
    parser.add_argument('--rundir', '-d', type=str, default="/tmp/parsec_sweep",
                        help="The root directory to run the benchmark. This is passed to `parsecmgmt` (default: %(default)s)")
    parser.add_argument('-t', '--time-csv', action="store_true",
                        help="Use /usr/bin/time to trace parsec executions and generate a csv.")
    parser.add_argument('--time-temp', type=str, default="/tmp/time.temp",
                        help="A temporary file that stores intermediate results reported by the /usr/bin/time (default: %(default)s)")
    parser.add_argument('--output', '-o', type=str, default="sweep.csv",
                        help="The output CSV file that stores the experiment results (default: %(default)s)")
    parser.add_argument('--ntrials', '-r', type=int, default=1,
                        help="The number of repeated runs to perform for the same configuration (default: %(default)s)")
    parser.add_argument('--keepdir', '-k',
                        action="store_true", help="Pass to parsecmgmt")
    parser.add_argument('--verbose', '-v', action="store_true",
                        help="Verbose, print command exec output")
    parser.add_argument('--openargs', '-O', type=str, default="a",
                        help="the open arguments for the output csv (default: %(default)s)")
    parser.add_argument('--profiler', choices=PROFILER_NAMEMAP.keys(), help="Pick a profiler (see later for a full list of available profiler and their description)")
    parser.add_argument('--profiler-args', type=lambda s: json.loads(s), default=dict(), help="Config the profiler with additional args. In json format.")
    parser.add_argument('--threadedcg-path', type=str, default="threaded.test.cg", help="The name of the cgroup name to test thread scheduling (default: %(default)s)")
    parser.add_argument('--threadedcg-core-num', type=parseIntCommaList, default=[0], help="Config the threaded cgroupv2 for scheduling experiments. 0 means to not use threadedcg and use all available cores. Positive number means how many cpu cores to be grouped together? (default: [0])")
    return parser.parse_args()

def launchTest(args, package: str, ncores: int, oversub: int, trialID: int, threadedCG: Optional[ThreadedCG]):
   # threadedCG: Optional[ThreadedCG] = None):
    """
    @param package the name of the parsec package you want to run
    @param ncores how many logical CPU cores should be allocated
    @param oversub how many thread oversubscription should be emulated
    @param trialID an identifier for the current run among other runs with the same configuration
    Assumptions:
    1. The args.time_temp will be available for processing after this function returns
    2. PERFCMD can be called without user interaction (e.g., no sudo prompt)
       sample sudoers: "${USER} ALL=(root:root) NOPASSWD:/usr/bin/perf, NOPASSWD:/usr/bin/chown"
    """
    nCoresPercg = threadedCG.noresPercg if threadedCG else ncores
    parsec = ParsecRun(args, package, ncores, oversub, trialID, nCoresPercg)
    if args.time_csv:
        parsec.setTimeAsPrefix()
    if not args.dryrun:
        parsec.runDetached()
        if threadedCG:
            pid = parsec.getPid()
            threadedCG.trackPID(pid)
            parsec.waitUntilTIDStabilized()
            threadedCG.randTIDCluster()
        if args.profiler:
            profiler = PROFILER_NAMEMAP[args.profiler](args, parsec)
            pid = parsec.getPid()
            profiler.run()
        parsec.waitUntilComplete()
        if args.profiler:
            profiler.callback()
    else:
        print(f"Dryrun, cmd: {parsec.cmd}")


def sweep(args, csvWriter, rowCallback: Callable[[], None]):
    """
    rowCallback is called every time a new row of experiment is appended to the csvWriter
    """
    packages = args.packages.split(',')
    allConfs = args.cherrypick
    if args.cores is not None and args.oversub is not None:
        allConfs += list(itertools.product(args.cores, args.oversub, args.threadedcg_core_num))
    for p in packages:
        for (ncores, oversub, nCoresPercg) in allConfs:
            for trialID in range(args.ntrials):
                # want to reuse threadedCG across runs
                if nCoresPercg >= ncores:
                    print(f"WARNING: skip invalid config (nCoresPercg >= ncores): {ncores} ncores, {oversub} oversub, {nCoresPercg} nCoresPercg")
                    continue
                elif nCoresPercg > 0:
                    threadedCG = ThreadedCG(args.threadedcg_path, nCoresPercg, ncores, args.numamem)
                elif nCoresPercg == 0:
                    threadedCG = None
                else:
                    raise RuntimeError(f"Invalid nCoresPercg {nCoresPercg}")
                try:
                    launchTest(args, p, ncores, oversub, trialID, threadedCG)
                except Exception as e:
                    print(f"WARNING: experiment {p} with C{ncores}.O{oversub} at trial.{trialID} failed with exception: {e}")
                    continue
                if not args.dryrun and args.time_csv:
                    with open(args.time_temp, "r") as f:
                        record_dict = {}
                        time_records = f.read().strip().split(',')
                        record_dict |= {f.key: v for f, v in zip(
                            RAWDATACSVFIELDS, time_records)}
                        for field in DeductiveFields:
                            field.callback(record_dict)
                        csvWriter.writerow(record_dict)
                        rowCallback()

def sanityCheckArgs(args):
    # Allow creating a new rundir
    Path(args.rundir).mkdir(exist_ok=True)
    # expect the rundir to be a tmpfs
    assert os.path.ismount(args.rundir), f"The rundir {args.rundir} is not a mount point. We expect that to be a tmpfs"
    # args.rundir is indeed a mountpoint, then let us check its mount parameters
    findmntRaw = subprocess.run(shlex.split(f"findmnt -J {args.rundir}"), capture_output=True, text=True)
    findmnt = json.loads(findmntRaw.stdout)
    rundir_mnt = findmnt["filesystems"][0]
    assert rundir_mnt["fstype"] == "tmpfs", f"The rundir {args.rundir} is expected to be a tmpfs mount"
    options = rundir_mnt["options"].split(',')
    for op in options:
        if '=' in op:
            k, v = op.split('=')
            if k == "mpol" and v != f"bind:{args.numamem}":
                raise RuntimeError(f"tmpfs with option {op} conflicts with the numamem {args.numamem}")

if __name__ == "__main__":
    args = buildParser()
    sanityCheckArgs(args)
    # TODO: only print CSV header when the csv file does not exist
    with open(args.output, args.openargs) as csvfile:
        csvWriter = csv.DictWriter(
            csvfile, fieldnames=[f.key for f in ALLCSVFIELDS])
        csvWriter.writeheader()
        sweep(args, csvWriter, lambda: csvfile.flush())
