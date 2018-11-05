#! /usr/bin/python3

import csv
import datetime
import re
import sys

def main():
    td_deparse = re.compile(r"^(?:(?P<days>\d+)\sdays?,\s)?"
                            r"(?P<hours>\d{1,2}):"
                            r"(?P<minutes>\d{2}):"
                            r"(?P<seconds>\d{2}(?:\.\d+)?)$")

    rd = csv.reader(sys.stdin, dialect='unix')
    wr = csv.writer(sys.stdout, dialect='unix', quoting=csv.QUOTE_MINIMAL)

    for line, row in enumerate(rd):
        if row[0] == 'url':
            wr.writerow(row)
            continue

        m = td_deparse.match(row[1])
        if not m:
            sys.stderr.write("line {}: failed to parse {!r}\n"
                             .format(line+1, row[1]))
            continue

        try:
            g = m.groupdict(default="0")
            g["days"]    = int(g["days"])
            g["hours"]   = int(g["hours"])
            g["minutes"] = int(g["minutes"])
            g["seconds"] = float(g["seconds"])

            dsec = datetime.timedelta(**g).total_seconds()

        except Exception as e:
            sys.stderr.write("line {}: failed to parse {!r} ({}: {})\n"
                             .format(line+1, row[1], type(e).__name__, e))
            continue

        nrow = [row[0], dsec]
        nrow.extend(row[2:])
        wr.writerow(nrow)

main()
