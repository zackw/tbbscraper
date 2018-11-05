#! /usr/bin/python

import collections
import csv
import sys

reader = csv.reader(sys.stdin)

urls = collections.defaultdict(set)

for row in reader:
    if row[4] == "blocked":
        urls[row[0]].add(row[8].lower().strip())

writer = csv.writer(sys.stdout)
for url, tags in sorted(urls.items()):
    tagset = "|".join(sorted(t for t in tags if t))
    writer.writerow((url, tagset))
