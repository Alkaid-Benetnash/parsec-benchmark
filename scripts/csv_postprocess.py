#!/usr/bin/env python3
import csv
import argparse
from csvFields import DeductiveFields


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
