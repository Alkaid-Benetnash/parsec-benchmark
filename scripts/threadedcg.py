"""
This file is introduced to manage customize thread scheduling related cgroup management
"""
from pathlib import Path
from utils import sudomkdir, sudochown, sudotee, getCoreList, getCoreListStr
import os
from typing import Callable, Optional
import io
import random
import numpy as np


class ThreadedCG(object):
    """
    Rules about setting up the cgroup for customized thread scheduling:
    1. The threaded cgroup is put righ under the root cgroup
    2. The threaded cgroup is delegated to the current (non-root) user to manage, via `chmod`
    """

    def __init__(self, cgname: str, ncoresPercg: int, ncores: int, numanode: int, subcgPrefix: str = "vnuma"):
        self.cgroupSubRoot = Path('/sys/fs/cgroup') / cgname
        assert ncores % ncoresPercg == 0, "Only support the same number of cores among threaded subcgroups"
        self.numcgroups = ncores // ncoresPercg
        assert self.numcgroups < 100, "Only reserve two characters for the threaded subcgroup sequence number"
        print(
            f"Going to setup cgroup under {self.cgroupSubRoot} with {self.numcgroups} subgroups, each having {ncoresPercg} cores")
        # make sure cgroupSubRoot exists
        if not self.cgroupSubRoot.exists():
            sudomkdir(self.cgroupSubRoot)
        # make sure the cgroupSubRoot is delegate to the current non-root user
        cgstat = self.cgroupSubRoot.stat()
        if cgstat.st_uid != os.getuid() or cgstat.st_gid != os.getgid():
            sudochown(self.cgroupSubRoot, recursive=True)
        # make sure the cpuset feature is enabled in cgroup
        if not self.ensureCGContent(self.cgroupSubRoot / "cgroup.controllers", lambda s: 'cpuset' in s.split()):
            raise NotImplementedError(
                f"Unexpected, cpuset is not enabled in {self.cgroupSubRoot}")
        self.ensureCGExactContent(
            self.cgroupSubRoot / "cgroup.subtree_control", "+cpuset")
        self.ensureCGExactContent(
            self.cgroupSubRoot / "cgroup.type", "threaded")
        cgroupSubRootCoreList = getCoreList(ncores, numanode)
        cgroupSubRootCoreListStr = getCoreListStr(ncores, numanode)
        self.ensureCGExactContent(
            self.cgroupSubRoot / "cpuset.cpus", cgroupSubRootCoreListStr)
        # handle the sub-cgroups. each of which represents a smaller core-group that enforces a certain level of locality
        self.subcgNames = [
            f"{subcgPrefix}.{str(i).zfill(2)}" for i in range(self.numcgroups)]
        for cgId, subcgName in enumerate(self.subcgNames):
            subcgPath = self.cgroupSubRoot / subcgName
            if not subcgPath.exists():
                subcgPath.mkdir()
            self.ensureCGExactContent(subcgPath/"cgroup.type", "threaded")
            subcgFirstCoreID = cgId * ncoresPercg
            subcgCoreList = cgroupSubRootCoreList[subcgFirstCoreID: subcgFirstCoreID + ncoresPercg]
            self.ensureCGExactContent(
                subcgPath/"cpuset.cpus", ','.join([str(x) for x in subcgCoreList]))
        # remove old subcg ({subcgPrefix}.i) that are not used in the latest cofiguration
        for subcgPath in self.cgroupSubRoot.glob(f"{subcgPrefix}.*"):
            if subcgPath.name not in self.subcgNames:
                subcgPath.rmdir()

    def trackPID(self, pid: int):
        sudotee(self.cgroupSubRoot / "cgroup.procs", str(pid))

    def randTIDCluster(self, seed: Optional[int] = None):
        with open(self.cgroupSubRoot / "cgroup.threads", "r") as f:
            threads = np.array([int(x) for x in f.read().splitlines()])
            if seed is None:
                seed = random.randint(0, 2**32)
            print(f"Redistributing {threads.size} threads among {self.numcgroups} threaded cgroups, with seed {seed}")
            rng = np.random.default_rng(seed)
            rng.shuffle(threads)
            threads_split = np.array_split(threads, self.numcgroups)
            for subcgName, subthreads in zip(self.subcgNames, threads_split):
                subcgPath = self.cgroupSubRoot / subcgName
                # use unbuffered binary write to operate on cgroup procs/threads interfaces
                with open(subcgPath / "cgroup.threads", "r+b", buffering=0) as subf:
                    for thread in subthreads:
                        subf.write(str(thread).encode())

    @classmethod
    def ensureCGContent(cls, path: str | Path, checkCallBack: Callable[[str], bool], enforcedContent: Optional[str] = None) -> bool:
        """
        @param[in] checkCallBack: callback function that decide whether the existing content in `path` needs to be updated. The callback should return true if the current content is already expected and return false otherwise.
        @param[in] enforcedContent: if the checkCallBack determines that update is necessary, this string will be written to `path`. None if don't want to update
        @return: True if the content in `path` is already enforced (either already pass the ckeck, or has been updated). False if `path` does not pass the check callback even after this function returns.
        """
        openmode = "r" if enforcedContent is None else "r+"
        isEnforced = True
        with open(path, openmode) as f:
            if not checkCallBack(f.read()):
                if enforcedContent is not None:
                    f.seek(0, io.SEEK_SET)
                    f.write(enforcedContent)
                    isEnforced = True
                else:
                    isEnforced = False
            else:
                isEnforced = True
        return isEnforced

    @classmethod
    def ensureCGExactContent(cls, path: str | Path, enforcedContent: str) -> bool:
        cls.ensureCGContent(path, lambda s: s ==
                            enforcedContent, enforcedContent)