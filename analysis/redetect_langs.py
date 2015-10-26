#! /usr/bin/python3

import sys
import os
import psycopg2
import time

import collections
import itertools
from concurrent import futures
import json
import zlib
import hashlib

import cld2

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

# This chunk of the work doesn't touch the database at all, and so can
# be farmed out to worker threads.  Decompression and cld2.detect both
# drop the GIL enough that threads should be OK.

def do_redetect(args):
    id, text = args
    try:
        text = zlib.decompress(text)
    except:
        text = ''
    langs = cld2.detect(text)

    return (id, json.dumps([{"l":l.code, "s":l.score} for l in langs.scores]))

def redetect_pages(db, cur, pool):

    # This is not in itertools, for no good reason.
    def chunked(iterable, n):
        it = iter(iterable)
        while True:
           chunk = tuple(itertools.islice(it, n))
           if not chunk:
               return
           yield chunk

    # There is no good way to hold a cursor open on a read query while
    # simultaneously making commits to one of the tables involved.  We
    # work around this by maintaining a local list of rows to process.
    # We don't just pull out all of the page contents in advance
    # because that would blow out the RAM.

    sys.stdout.write("Determining job size...\n")
    sys.stdout.flush()

    cur.execute("SELECT id FROM analysis.capture_pruned_content"
                " WHERE lang_scores IS NULL")
    pages = cur.fetchall()
    total_pages = len(pages)
    if not total_pages:
        return

    processed = 0
    start = time.monotonic()
    sys.stdout.write("Processing 0/{}...\n".format(total_pages))
    for chunk in chunked(pages, 1000):
        with db:
            cur.execute(" SELECT id, content FROM analysis.capture_pruned_content"
                        "  WHERE id = ANY(%s)",
                        ([c[0] for c in chunk],))

            for result in pool.map(do_redetect, cur):
                cur.execute("UPDATE analysis.capture_pruned_content"
                            "   SET lang_scores = %s"
                            " WHERE id = %s",
                            (result[1], result[0]))
                processed += 1

        stop = time.monotonic()
        elapsed = stop - start
        remain  = (total_pages - processed)*(elapsed/processed)
        sys.stdout.write("Processed {}/{} in {} remaining {}\n"
                         .format(processed, total_pages,
                                 fmt_interval(elapsed),
                                 fmt_interval(remain)))

def main():
    db = psycopg2.connect("dbname="+sys.argv[1])
    cur = db.cursor()
    cur.execute("SET search_path TO public")
    with futures.ThreadPoolExecutor(max_workers=6) as pool:
        redetect_pages(db, cur, pool)

main()
