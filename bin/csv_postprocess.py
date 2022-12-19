import csv
import argparse
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Callable


@dataclass
class NewDeductiveField:
    key: str
    callback: Callable[[Dict[str, str]], None]
    description: str


class DeductiveNote(NewDeductiveField):
    key = "note"
    description = "Additonal comments regarding a trial. Introduced to report abnormal program exit status messages"

    @classmethod
    def callback(cls, row):
        firstCol = row["ncores"]
        if firstCol.startswith("Command"):
            composedFields = firstCol.splitlines()
            row["ncores"] = composedFields[-1]
            row["note"] = composedFields[:-1]


class DeductiveOversub(NewDeductiveField):
    key = "oversub"
    description = "Compute a simple oversubscription ratio"

    @classmethod
    def callback(cls, row):
        row[cls.key] = int(row["nthreads"]) // int(row["ncores"])


class DeductiveCurTimeStamp(NewDeductiveField):
    key = "timestamp"
    description = "Tag the current record with the current time"

    @classmethod
    def callback(cls, row):
        if cls.key not in row:
            row[cls.key] = datetime.now().isoformat(timespec='seconds')


"""
Note that the order of these newfields matters and one may depends on another's pass
"""
DeductiveFields = [
    DeductiveNote,
    DeductiveOversub,
    DeductiveCurTimeStamp,
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert csv such that abnormal exit messages are put in a special 'note' field")
    parser.add_argument('--input', '-i', type=str, help="input csv")
    parser.add_argument('--output', '-o', type=str, help="output csv")
    args = parser.parse_args()

    incsvf = open(args.input, "r")
    inreader = csv.DictReader(incsvf)

    newfields = inreader.fieldnames
    for field in DeductiveFields:
        if field not in newfields:
            newfields.append(field.key)
    outcsvf = open(args.output, "w")
    outwriter = csv.DictWriter(outcsvf, newfields)
    outwriter.writeheader()

    for row in inreader:
        for field in DeductiveFields:
            field.callback(row)
        outwriter.writerow(row)
