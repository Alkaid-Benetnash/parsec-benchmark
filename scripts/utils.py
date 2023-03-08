from typing import List, Tuple
import subprocess
import shlex
from functools import cache
from pathlib import Path
import os


def getTIDofPID(pid: int) -> List[int]:
    """
    return a list of thread ID for a given process ID
    """
    ps = subprocess.run(shlex.split(
        f"ps -L -o tid --no-headers {pid}"), capture_output=True, text=True)
    return [int(tid) for tid in ps.stdout.splitlines()]


@cache
def getCoreList(ncores: int, numanode: int = 0) -> Tuple[int]:
    """
    Generate a consecutive list of cpu cores with the requested core count on the requested numa node
    Throws runtime error if cannot find enough satisfying cores.
    @return (0,1,2,3,4, ...)
    """
    #print(f"Calculating ncores {ncores} and numanode {numanode}")
    cpuList = []
    foundCPUs = 0
    lscpu = subprocess.run(shlex.split("lscpu -p=node,CPU"),
                           capture_output=True, text=True)
    for line in lscpu.stdout.splitlines():
        if line[0] == "#":
            continue
        node, cpu = (int(x) for x in line.split(','))
        if node != numanode:
            continue
        cpuList.append(cpu)
        foundCPUs += 1
        if foundCPUs == ncores:
            return tuple(cpuList)
    raise RuntimeError(
        f"Fail to find {ncores} CPU on Node {numanode}. Only found {foundCPUs} cores.")


@cache
def getCoreListStr(ncores: int, numanode: int = 0) -> str:
    cpuList = getCoreList(ncores, numanode)
    return ','.join([str(c) for c in cpuList])


@cache
def getCoreListCompressed(ncores: int, numanode: int = 0) -> Tuple[Tuple[int, int]]:
    """
    @return: [(0,4), (11,15)] to represent cpu choice of "0-4,11-15"
    """
    cpuList = getCoreList(ncores, numanode)
    compressedList = [(cpuList[0], cpuList[0])]
    for cpu in cpuList[1:]:
        if compressedList[-1][-1] + 1 == cpu:
            compressedList[-1] = (compressedList[-1][0], cpu)
        else:
            compressedList.append((cpu, cpu))
    return tuple(compressedList)


@cache
def getCoreListCompressedStr(ncores: int, numanode: int = 0):
    """
    Similar to getCoreList, but return the command line usable string
    "0-4,11-15"
    """
    cpuList = getCoreListCompressed(ncores, numanode)
    return ','.join([f"{cpurange[0]}-{cpurange[1]}" for cpurange in cpuList])


def sudomkdir(path: str | Path, parent: bool = True):
    subprocess.run(shlex.split(
        f"sudo /usr/bin/mkdir {'-p' if parent else ''} {path}"))


def sudormdir(path: str | Path):
    subprocess.run(shlex.split(f"sudo /usr/bin/rmdir {path}"))


def sudochown(path: str | Path, recursive: bool = False, uid: int = os.getuid(), gid: int = os.getgid()):
    subprocess.run(shlex.split(
        f"sudo /usr/bin/chown {'-R' if recursive else ''} {uid}:{gid} {path}"))


def sudotee(path: str | Path, input: str, output=subprocess.DEVNULL):
    tee = subprocess.Popen(shlex.split(
        f"sudo /usr/bin/tee {path}"), stdin=subprocess.PIPE, stdout=output, stderr=subprocess.PIPE, text=True)
    _, errs = tee.communicate(input=input)
    if len(errs) > 0:
        print(f"sudotee, stderr: {errs}")
