"""
This file is introduced to encapsulate and manage the lifetime of a single parsec run
"""
import tempfile
from csvFields import ALLGNUTIMEFIELDS
from typing import Optional
import shlex
import subprocess
import os
from datetime import datetime, timedelta
import time
from utils import getTIDofPID, getNowTSEscaped


class ParsecRun(object):
    def __init__(self, args, package: str, ncores: int, oversub: int, trialID: int, nCoresPercg: int):
        self.args = args
        self.package = package
        self.ncores = ncores
        self.oversub = oversub
        self.trialID = trialID
        self.nCoresPercg = nCoresPercg
        self.numamem = self.args.numamem
        self.pidfile = tempfile.NamedTemporaryFile(mode="w+")
        self.pid: Optional[int] = None
        self.parsecmgmt: Optional[subprocess.Popen] = None
        self.cmd = [
            "parsecmgmt", "-a", "run", "-p", self.package, "-i", "native",
            "-n", f"{self.oversub}x", "-C", f"{self.ncores}",
            "-d", self.args.rundir, "--numamem", f"{self.numamem}",
            "--pid", self.pidfile.name,
        ]
        if self.args.keepdir:
            self.cmd += ["-k", "--unpack"]
        # track whether the number of threads have become stable
        self.tidStabilized = False

    def setTimeAsPrefix(self):
        non_timer_prefix = [f'{self.ncores}',
                            f'{self.ncores * self.oversub}', f'{self.nCoresPercg}', f'{self.trialID}']
        timer_fmt_str = ','.join(
            non_timer_prefix + [f'%{f.timeFMT}' for f in ALLGNUTIMEFIELDS])
        timer_cmd = f"/usr/bin/time -o {self.args.time_temp} -f {timer_fmt_str}"
        self.cmd += ["-s", timer_cmd]

    def runDetached(self):
        print(f"Executing {shlex.join(self.cmd)}")
        stdout = None if self.args.verbose else subprocess.DEVNULL
        self.parsecmgmt = subprocess.Popen(self.cmd, stdout=stdout)

    def waitUntilComplete(self):
        self.parsecmgmt.wait()
        self.pidfile.close()

    def getPid(self) -> int:
        """
        Wait for the parsecmgmt script to populate the pidfile and return the pid inside it
        Will cache the result.
        @return the pid of the parsec application.
        """
        if self.pid is not None:
            return self.pid
        waitPIDFileReadTimeout = timedelta(seconds=10) + datetime.now()
        pid = None
        while datetime.now() < waitPIDFileReadTimeout:
            try:
                pidtext = self.pidfile.read()
                pid = int(pidtext)
            except:
                time.sleep(0.5)  # wait for the pidpath file to be ready
                continue
            break
        # check whether the pid is valid
        if pid is None:
            raise NotImplementedError("pidfile time out")
        else:
            try:
                os.kill(pid, 0)
            except Exception as e:
                print(f"Failed to find the parasec process {pid}: {e}")
                raise e
        self.pid = pid
        return self.pid
    
    def waitUntilTIDStabilized(self, pollIntervalSec: float = 1.0, stableThreshold: int = 3):
        """
        Some profilers are supposed to be attached when the total number of threads stay stable
        Stable is defined as the total number of threads stay the same for {nThStayStableThreshold}*{pollInterval} time
        """
        if self.tidStabilized:
            return
        pid = self.getPid()
        nThStayStable = 0
        nTh = len(getTIDofPID(pid))
        while nThStayStable < stableThreshold or nTh < self.ncores * self.oversub:
            nThNew = len(getTIDofPID(pid))
            if nTh == nThNew:
                nThStayStable += 1
            else:
                nThStayStable = 0
                nTh = nThNew
                print(f"wait for {nTh} threads in {self.package} to become stable")
                time.sleep(pollIntervalSec)
        print(f"Consider {nTh} threads in {self.package} to be stable")
        self.tidStabilized = True

    def getIdentifier(self, timestamped=True) -> str:
        """
        Generate a timestamped (optional) identifier that encodes the name, config, etc. of the current parsec run.
        """
        plain_identifier = f"{self.package}.C{self.ncores}.O{self.oversub}.CG{self.nCoresPercg}"
        if timestamped:
            return f"{plain_identifier}.{getNowTSEscaped()}"
        else:
            return plain_identifier