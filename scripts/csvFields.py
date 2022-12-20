from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Callable


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


@dataclass
class DeductiveField(CSVField):
    callback: Callable[[Dict[str, str]], None]


class DeductiveNote(DeductiveField):
    key = "note"
    description = "Additonal comments regarding a trial. Introduced to report abnormal program exit status messages"

    @classmethod
    def callback(cls, row):
        firstCol = row["ncores"]
        if firstCol.startswith("Command"):
            composedFields = firstCol.splitlines()
            row["ncores"] = composedFields[-1]
            row["note"] = composedFields[:-1]


class DeductiveOversub(DeductiveField):
    key = "oversub"
    description = "Compute a simple oversubscription ratio"

    @classmethod
    def callback(cls, row):
        row[cls.key] = int(row["nthreads"]) // int(row["ncores"])


class DeductiveCurTimeStamp(DeductiveField):
    key = "timestamp"
    description = "Tag the current record with the current time"

    @classmethod
    def callback(cls, row):
        if row[cls.key] is None:
            row[cls.key] = datetime.now().isoformat(timespec='seconds')


"""
Note that the order of these newfields matters and one may depends on another's pass
"""
DeductiveFields = [
    DeductiveNote,
    DeductiveOversub,
    DeductiveCurTimeStamp,
]

ALLCSVFIELDS = [
    *RAWDATACSVFIELDS,
    *DeductiveFields
]

AllCSVFieldsIndexedByKey = {f.key: f for f in ALLCSVFIELDS}
