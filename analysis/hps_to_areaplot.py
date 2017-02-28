#! /usr/bin/python3

import collections
import csv
import datetime
import math
import re
import sys

def load():
    td_deparse = re.compile(r"^(?:(?P<days>\d+)\sdays?,\s)?"
                            r"(?P<hours>\d{1,2}):"
                            r"(?P<minutes>\d{2}):"
                            r"(?P<seconds>\d{2}(?:\.\d+)?)$")

    rd = csv.reader(sys.stdin, dialect='unix')

    rows = []
    max_ttc = 0

    for line, (url, time_to_change, what_changed) in enumerate(rd):
        if url == 'url':
            continue

        try:
            time_to_change = float(time_to_change)
        except Exception:
            m = td_deparse.match(time_to_change)
            if not m:
                sys.stderr.write("line {}: failed to parse {!r}\n"
                                 .format(line+1, time_to_change))
                continue

            try:
                g = m.groupdict(default="0")
                g["days"]    = int(g["days"])
                g["hours"]   = int(g["hours"])
                g["minutes"] = int(g["minutes"])
                g["seconds"] = float(g["seconds"])

                time_to_change = datetime.timedelta(**g).total_seconds()

            except Exception as e:
                sys.stderr.write("line {}: failed to parse {!r} ({}: {})\n"
                                 .format(line+1, time_to_change, type(e).__name__, e))
                continue

        max_ttc = max(time_to_change, max_ttc)
        if what_changed == 'ok':
            what_changed = 'domain unparked'
        rows.append((time_to_change, what_changed))

    return max_ttc, rows

def transform(max_ttc, rows):
    ceil = math.ceil
    bins = [collections.defaultdict(lambda: 0) for _ in range(ceil(max_ttc / 86400) + 1)]

    total = 0
    for ttc, what in rows:
        total += 1
        bins[int(ceil(ttc/86400))][what] += 1

    assert len(bins[0]) == 0

    cumul = collections.defaultdict(lambda: 0)
    cumul['original'] = total

    for b in bins:
        for w, n in b.items():
            cumul[w] += n
            cumul['original'] -= n
        b.update(cumul)

    return bins

def emit(bins):
    wr = csv.writer(sys.stdout, dialect='unix', quoting=csv.QUOTE_MINIMAL)
    wr.writerow(("day","what","n"))
    for d, b in enumerate(bins):
        for w, n in sorted(b.items()):
            if n:
                wr.writerow((d, w, n))

def main():
    emit(transform(*load()))

main()
