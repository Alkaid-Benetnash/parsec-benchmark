from typing import Dict, Any, List
import json
import subprocess
from datetime import datetime
import textwrap
import shlex
import random
from pathlib import Path
from utils import getTIDofPID, sudochown, sudokill
from parsecRun import ParsecRun

PERFCMD = ['sudo', '/usr/bin/perf']


class Profiler(object):
    """
    Anything that is supposed to run in parallel (a separate process) as the application.
    E.g., perf
    args is the one from argparser, cmdline args
    """
    name: str = None

    def __init__(self, args, parsec: ParsecRun):
        self.args = args
        self.profiler_args = self.getDefaultArgs()
        self.profiler_args.update(args.profiler_args)
        self.parsec = parsec

    def getHelp(self) -> str:
        """Eventually passed to the argparse help"""
        raise NotImplementedError("Profiler Base Class")

    def getDefaultArgs(self) -> Dict[str, Any]:
        """The default configurations of this profiler"""
        raise NotImplementedError("Profiler Base Class")

    def run(self, package: str, ncores: int, oversub: int, pid: int) -> None:
        """Invoke this profiler with user-provided args. Only return when the profiler process exits."""
        raise NotImplementedError("Profiler Base Class")

    def callback(self) -> None:
        """
        A callback function that will be invoked after the main parsec command finishes
        """
        return None


class PerfStatProfiler(Profiler):
    name: str = "perfstat"

    def __init__(self, args, parsec: ParsecRun):
        super().__init__(args, parsec)

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

    def run(self) -> None:
        tids = getTIDofPID(self.parsec.getPid())
        if self.profiler_args['sample-ratio'].endswith('%'):
            nTIDSamples = int(
                int(self.profiler_args['sample-ratio'][:-1]) / 100 * len(tids))
        else:
            nTIDSamples = int(self.args.perftid_sample_ratio)
        sampledTIDs = random.sample(tids, nTIDSamples) if len(
            tids) > nTIDSamples else tids
        sampledTIDs_str = ','.join([str(x) for x in sampledTIDs])
        perfdataPath = f"{self.parsec.getIdentifier()}.perf.data"
        print(f"run perf on tids {sampledTIDs_str}")
        subprocess.run(PERFCMD + shlex.split(
            f"stat record -e cs,instructions,inst_retired.any -I100 --quiet --per-thread -o {perfdataPath} -t {sampledTIDs_str}"))
        sudochown(perfdataPath)


class PerfSchedProfiler(Profiler):
    name: str = "perfsched"

    def __init__(self, args, parsec: ParsecRun):
        super().__init__(args, parsec)

    @classmethod
    def getHelp(cls) -> str:
        return textwrap.dedent('''\
            Run perf record to collect sched related events.
            Args:
            - events: list of string. same as the `-e` option. Will fallback to `sched record` if this is left empty
            Default:\
            ''') + \
            json.dumps(cls.getDefaultArgs(), indent=2)

    @classmethod
    def getDefaultArgs(cls) -> Dict[str, Any]:
        return {
            'events': ['sched:sched_switch']
        }

    def run(self) -> None:
        # NOTE that perf will NOT follow newly created threads once launched
        # So this perfsched needs to wait for TID to become stable
        self.parsec.waitUntilTIDStabilized()
        perfdataPath = f"{self.parsec.getIdentifier()}.perf.data"
        if len(self.profiler_args['events']) > 0:
            eventOpts = ' '.join(
                [f"-e {event}" for event in self.profiler_args['events']])
            cmdargs = f"record {eventOpts} -p {self.parsec.getPid()} -o {perfdataPath}"
        else:
            cmdargs = f"sched record -p {self.parsec.getPid()} -o {perfdataPath}"
        subprocess.run(PERFCMD + shlex.split(cmdargs))
        sudochown(perfdataPath)


class PerfDebuggingProfiler(Profiler):
    name: str = "dbg"

    def __init__(self, args, parsec: ParsecRun):
        super().__init__(args, parsec)

    @classmethod
    def getHelp(cls) -> str:
        return textwrap.dedent('''\
            For debugging or manual process only. This profiler will dump useful information to the stdout.
            Args: None
            ''')

    @classmethod
    def getDefaultArgs(cls) -> Dict[str, Any]:
        return {}

    def run(self) -> None:
        self.starttime = datetime.now()
        print(f"DBG: pid is {self.parsec.getPid()}")
        self.parsec.waitUntilTIDStabilized()
        print(
            f"DBG: pid {self.parsec.getPid()} is now considered to have stable TID")

    def callback(self):
        print(
            f"DBG: pid completed. elapsed time {(datetime.now() - self.starttime).total_seconds()}")
        return None


class PerfBCCRunqlatProfiler(Profiler):
    name: str = "runqlat-bcc"
    BIN: Path = Path('/usr/sbin/runqlat')

    def __init__(self, args, parsec: ParsecRun):
        super().__init__(args, parsec)
        assert (self.BIN.exists()), f"Invalid bcc {str(self.BIN)}"

    @classmethod
    def getHelp(cls) -> str:
        return textwrap.dedent('''\
            Call BPF program runqlat (sudo required) to profile.
            This profiler will be detached and rely on the callback to stop.
            Args: None
            ''')
    @classmethod
    def getDefaultArgs(cls) -> Dict[str, Any]:
        return {}

    def run(self) -> None:
        outputPath = f"{self.parsec.getIdentifier()}.runqlat.txt"
        self.outputFile = open(outputPath, "w")
        self.process = subprocess.Popen(shlex.split(f"sudo {str(self.BIN)} -p {self.parsec.getPid()}"), stdout=self.outputFile, text=True)

    def callback(self) -> None:
        sudokill(self.process.pid, "SIGINT")
        self.process.wait()
        self.outputFile.close()


ALL_PROFILER = [PerfStatProfiler, PerfSchedProfiler, PerfBCCRunqlatProfiler, PerfDebuggingProfiler]
PROFILER_NAMEMAP = {p.name: p for p in ALL_PROFILER}
