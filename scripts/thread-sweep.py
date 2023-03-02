#!/usr/bin/env python3
import argparse
import shlex
from typing import List, Tuple, Callable, Dict, Any
import subprocess
import csv
import itertools
from pathlib import Path
from csvFields import RAWDATACSVFIELDS, ALLGNUTIMEFIELDS, ALLCSVFIELDS, DeductiveFields
import tempfile
import os
import time
import random
from datetime import datetime
import textwrap
import json


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


class Profiler(object):
    """
    Anything that is supposed to run in parallel (a separate process) as the application.
    E.g., perf
    args is the one from argparser, cmdline args
    """
    name: str = None

    def __init__(self, args):
        self.args = args
        self.profiler_args = self.getDefaultArgs()
        self.profiler_args.update(args.profiler_args)

    def getHelp(self) -> str:
        """Eventually passed to the argparse help"""
        raise NotImplementedError("Profiler Base Class")

    def getDefaultArgs(self) -> Dict[str, Any]:
        """The default configurations of this profiler"""
        raise NotImplementedError("Profiler Base Class")

    def blockingRun(self, package: str, ncores: int, oversub: int, pid: int) -> None:
        """Invoke this profiler with user-provided args. Only return when the profiler process exits."""
        raise NotImplementedError("Profiler Base Class")


class PerfStatProfiler(Profiler):
    name: str = "perfstat"

    def __init__(self, args):
        super().__init__(args)

    @classmethod
    def getHelp(cls) -> str:
        return textwrap.dedent('''\
            Run perf stat to collect simple counters.
            Args:
            - sample-ratio: Determine how should perf-stat sample threads. Ending with `%%` means a ratio of the total number of threads. Otherwise means to sample a fix number of threads.")
            Default:\
        ''') + \
            json.dumps(cls.getDefaultArgs(), indent=2)

    @classmethod
    def getDefaultArgs(cls) -> Dict[str, Any]:
        return {
            'sample-ratio': '10%'
        }

    def blockingRun(self, package: str, ncores: int, oversub: int, pid: int) -> None:
        tids = getTIDofPID(pid)
        if self.profiler_args['sample-ratio'].endswith('%'):
            nTIDSamples = int(
                int(self.profiler_args['sample-ratio'][:-1]) / 100 * len(tids))
        else:
            nTIDSamples = int(args.perftid_sample_ratio)
        sampledTIDs = random.sample(tids, nTIDSamples) if len(
            tids) > nTIDSamples else tids
        sampledTIDs_str = ','.join([str(x) for x in sampledTIDs])
        perfdataPath = f"{package}.C{ncores}.O{oversub}.{datetime.now().isoformat(timespec='seconds').replace(':','_')}.perf.data"
        print(f"run perf on tids {sampledTIDs_str}")
        subprocess.run(PERFCMD + shlex.split(
            f"stat record -e cs,instructions,inst_retired.any -I100 --quiet --per-thread -o {perfdataPath} -t {sampledTIDs_str}"))
        subprocess.run(shlex.split(
            f"sudo /usr/bin/chown {os.getuid()}:{os.getgid()} {perfdataPath}"))

class PerfSchedProfiler(Profiler):
    name: str = "perfsched"

    def __init__(self, args):
        super().__init__(args)

    @classmethod
    def getHelp(cls) -> str:
        return textwrap.dedent('''\
            Run perf record to collect sched related events.
            Args:
            - events: list of string. same as the `-e` option
            Default:\
            ''') + \
                json.dumps(cls.getDefaultArgs(), indent=2)

    @classmethod
    def getDefaultArgs(cls) -> Dict[str, Any]:
        return {
            'events': ['sched:sched_switch']
        }

    def blockingRun(self, package: str, ncores: int, oversub: int, pid: int) -> None:
        eventOpts = ' '.join([f"-e {event}" for event in self.profiler_args['events']])
        perfdataPath = f"{package}.C{ncores}.O{oversub}.{datetime.now().isoformat(timespec='seconds').replace(':','_')}.perf.data"
        subprocess.run(PERFCMD + shlex.split(
            f"record {eventOpts} -p {pid} -o {perfdataPath}"
        ))
        subprocess.run(shlex.split(
            f"sudo /usr/bin/chown {os.getuid()}:{os.getgid()} {perfdataPath}"))


ALL_PROFILER = [PerfStatProfiler, PerfSchedProfiler]
PROFILER_NAMEMAP = {p.name: p for p in ALL_PROFILER}


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
                        help="A comma separated list of specific (ncore oversub) pairs of configurations. e.g., (1 4),(2 8)")
    parser.add_argument('--dryrun', action="store_true",
                        help="Dump the commands to run without running the benchmark")
    parser.add_argument('--numamem', '-m', type=int, default=0,
                        help="The numa node to allocate memory from. Passed to parsecmgmt (default: %(default)s)")
    parser.add_argument('--rundir', '-d', type=str, default="/dev/shm/parsec_sweep",
                        help="The root directory to run the benchmark. This is passed to `parsecmgmt` (default: %(default)s)")
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
    #parser.add_argument('--perfstat', action="store_true",
    #                    help="Enable perf-stat")
    #parser.add_argument('--perftid-sample-ratio', type=str, default="10%",
    #                    help="Determine how should perf-stat sample threads. Ending with `%%` means a ratio of the total number of threads. Otherwise means to sample a fix number of threads (default: %(default)s)")
    return parser.parse_args()


PERFCMD = ['sudo', '/usr/bin/perf']


def getTIDofPID(pid: int) -> List[int]:
    """
    return a list of thread ID for a given process ID
    """
    ps = subprocess.run(shlex.split(
        f"ps -L -o tid --no-headers {pid}"), capture_output=True, text=True)
    return [int(tid) for tid in ps.stdout.splitlines()]


def launchTest(args, package: str, ncores: int, oversub: int, trialID: int):
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
    pidfile = tempfile.NamedTemporaryFile(mode="w+")
    cmd = [
        "parsecmgmt", "-a", "run", "-p", package, "-i", "native",
        "-n", f"{oversub}x", "-C", f"{ncores}",
        "-d", args.rundir, "--numamem", f"{args.numamem}",
        "--pid", pidfile.name,
    ]
    if args.profiler is None:
        # timer related prefix will break the pidpath functionality (the timing measurement process will override the actual app process)
        non_timer_prefix = [f'{ncores}',
                            f'{ncores * oversub}', f'{trialID}']
        timer_fmt_str = ','.join(
            non_timer_prefix + [f'%{f.timeFMT}' for f in ALLGNUTIMEFIELDS])
        timer_cmd = f"/usr/bin/time -o {args.time_temp} -f {timer_fmt_str}"
        cmd += ["-s", timer_cmd]
    if args.keepdir:
        cmd += ["-k", "--unpack"]
    print(f"Executing {shlex.join(cmd)}")
    stdout = None if args.verbose else subprocess.DEVNULL
    if not args.dryrun:
        parsecmgmt = subprocess.Popen(cmd, stdout=stdout)
        if args.profiler:
            profiler = PROFILER_NAMEMAP[args.profiler](args)
            time.sleep(2)  # wait for the pidpath file to be ready
            pidtext = pidfile.read()
            pid = int(pidtext)
            try:
                os.kill(pid, 0)
            except PermissionError:
                print("Failed to find the parasec process {pid}")
            profiler.blockingRun(package, ncores, oversub, pid)
        """
        if args.perfstat:
            # FIXME: integrate the following when perf-stat with specific cpus
            # assert args.numamem == 0, "Picking the right CPUID to track for other numa nodes has not been implemented"
            time.sleep(2)  # wait for the pidpath file to be ready
            pidtext = pidfile.read()
            pid = int(pidtext)
            try:
                os.kill(pid, 0)
            except PermissionError:
                print("Failed to find the parasec process {pid}")
            tids = getTIDofPID(pid)
            if args.perftid_sample_ratio.endswith('%'):
                nTIDSamples = int(
                    int(args.perftid_sample_ratio[:-1]) / 100 * len(tids))
            else:
                nTIDSamples = int(args.perftid_sample_ratio)
            sampledTIDs = random.sample(tids, nTIDSamples) if len(
                tids) > nTIDSamples else tids
            sampledTIDs_str = ','.join([str(x) for x in sampledTIDs])
            perfdataPath = f"{package}.C{ncores}.O{oversub}.{datetime.now().isoformat(timespec='seconds').replace(':','_')}.perf.data"
            print(f"run perf on tids {sampledTIDs_str}")
            subprocess.run(PERFCMD + shlex.split(
                f"stat record -e cs,instructions,inst_retired.any -I100 --quiet --per-thread -o {perfdataPath} -t {sampledTIDs_str}"))
            subprocess.run(shlex.split(
                f"sudo /usr/bin/chown {os.getuid()}:{os.getgid()} {perfdataPath}"))
        """
        parsecmgmt.wait()
    pidfile.close()


def sweep(args, csvWriter, rowCallback: Callable[[], None]):
    """
    rowCallback is called every time a new row of experiment is appended to the csvWriter
    """
    packages = args.packages.split(',')
    allConfs = args.cherrypick
    if args.cores is not None and args.oversub is not None:
        allConfs += list(itertools.product(args.cores, args.oversub))
    for p in packages:
        for (ncores, oversub) in allConfs:
            for trialID in range(args.ntrials):
                launchTest(args, p, ncores, oversub, trialID)
                if not args.dryrun and args.profiler is None:
                    with open(args.time_temp, "r") as f:
                        record_dict = {}
                        time_records = f.read().strip().split(',')
                        record_dict |= {f.key: v for f, v in zip(
                            RAWDATACSVFIELDS, time_records)}
                        for field in DeductiveFields:
                            field.callback(record_dict)
                        csvWriter.writerow(record_dict)
                        rowCallback()


if __name__ == "__main__":
    args = buildParser()
    Path(args.rundir).mkdir(exist_ok=True)
    with open(args.output, args.openargs) as csvfile:
        csvWriter = csv.DictWriter(
            csvfile, fieldnames=[f.key for f in ALLCSVFIELDS])
        csvWriter.writeheader()
        sweep(args, csvWriter, lambda: csvfile.flush())
