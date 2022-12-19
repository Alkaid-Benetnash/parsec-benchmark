#!/usr/bin/env python3
import argparse
import shlex
from dataclasses import dataclass
from typing import List, Tuple, Callable
import subprocess
import csv
import itertools
from pathlib import Path
from csv_postprocess import DeductiveFields


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
    parser = argparse.ArgumentParser(
        description="Perform sweep experiment over thread oversubscription")
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
    return parser.parse_args()


@dataclass
class CSVField:
    """
    Class for tracking what each field in the CSV file is
    key is a descriptive string
    """
    key: str
    description: str


@dataclass
class GNUTimeField(CSVField):
    """
    Class for tracking resources reported by /usr/bin/time that are used in this script
    timeFMT is the character that are used in the format string
    """
    timeFMT: str


ALLGNUTIMEFIELDS = [
    GNUTimeField(key="elapsed", timeFMT="e",
                 description="Elapsed real (wall clock) time used by the process, in seconds."),
    GNUTimeField(key="usertime", timeFMT="U",
                 description="Total number of CPU-seconds that the process used directly (in user mode), in seconds."),
    GNUTimeField(key="systime", timeFMT="S",
                 description="Total number of CPU-seconds used by the system on behalf of the process (in kernel mode), in seconds."),
    GNUTimeField(key="cpupercent", timeFMT="P",
                 description="Percentage of the CPU that this job got.  This is just user + system times divided by the total running time.  It also prints a percentage sign."),
    GNUTimeField(key="volswitch", timeFMT="w",
                 description="Number of times that the program was context-switched voluntarily, for instance while waiting for an I/O operation to complete."),
    GNUTimeField(key="invswitch", timeFMT="c",
                 description="Number of times the process was context-switched involuntarily (because the time slice expired)."),
    GNUTimeField(key="minpgfaults", timeFMT="R",
                 description="Number of minor, or recoverable, page faults.  These are pages that are not valid (so they fault) but which have not yet been claimed by other virtual pages.  Thus the data in the page is still valid but the system tables must be updated."),
]

RAWDATACSVFIELDS = [
    CSVField(key="ncores",
             description="The number of logical CPU cores allocated to this workload."),
    CSVField(key="nthreads",
             description="The number of threads requested for this workload."),
    CSVField(key="ntrial",
             description="The identifier to distinguish repeated runs of one configuration."),
    *ALLGNUTIMEFIELDS,
]
ALLCSVFIELDS = [
    *RAWDATACSVFIELDS,
    *[CSVField(key=f.key, description=f.description) for f in DeductiveFields]
]


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
                non_timer_prefix = [f'{ncores}',
                                    f'{ncores * oversub}', f'{trialID}']
                timer_fmt_str = ','.join(
                    non_timer_prefix + [f'%{f.timeFMT}' for f in ALLGNUTIMEFIELDS])
                timer_cmd = f"/usr/bin/time -o {args.time_temp} -f {timer_fmt_str}"
                cmd = [
                    "parsecmgmt", "-a", "run", "-p", p, "-i", "native",
                    "-n", f"{oversub}x", "-C", f"{ncores}",
                    "-d", args.rundir, "--numamem", f"{args.numamem}", "-s", timer_cmd,
                ]
                if args.keepdir:
                    cmd += ["-k", "--unpack"]
                print(f"Executing {shlex.join(cmd)}")
                stdout = None if args.verbose else subprocess.DEVNULL
                if not args.dryrun:
                    subprocess.run(cmd, stdout=stdout)
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
