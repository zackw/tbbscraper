#! /usr/bin/python3

import psycopg2
import domainparking
import zlib
import time
import itertools
import sys

# This is not in itertools, for no good reason.
def chunked(iterable, n):
    it = iter(iterable)
    while True:
       chunk = list(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

start = None
def progress(total, nproc, nparked):
    stop = time.monotonic()
    global start
    if start is None:
        start = stop

    m, s = divmod(stop - start, 60)
    h, m = divmod(m, 60)

    sys.stderr.write("{}:{:>02}:{:>05.2f}: {}/{}/{}\n".format(int(h), int(m), s, nparked, nproc, total))

def main():
    db = psycopg2.connect(dbname="censorship_study")v
    cur = db.cursor()
    classifier = domainparking.ParkingClassifier()
    cur.execute("select id from capture_html_content where is_parked is null")
    todo = sorted(r[0] for r in cur)

    total   = len(todo)
    nproc   = 0
    nparked = 0
    progress(total, nproc, nparked)
    for chunk in chunked(todo, 1000):
        with db:
            cur.execute("select id, content from capture_html_content where id = any(%s)",
                        (chunk,))
            for id, content in cur.fetchall():
                if content: html = zlib.decompress(content).decode('utf-8')
                else: html = ''
                cls = classifier.isParked(html)
                if cls.is_parked: nparked += 1
                cur.execute("update capture_html_content set is_parked = %s, parking_rules_matched = %s"
                            " where id = %s",
                            (cls.is_parked, cls.rules_matched, id))
                nproc += 1
        progress(total, nproc, nparked)

main()
