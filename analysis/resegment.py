#! /usr/bin/python3

import sys
import os
import psycopg2
import time
import collections
import itertools
import multiprocessing
import json

import cld2
import word_seg

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

def do_resegment(args):
    docid, text_pruned = args
    lang = cld2.detect(text_pruned, want_chunks=True)
    segmented = [ { "l": c[0].code,
                    "t": list(word_seg.segment(c[0].code, c[1])) }
                  for c in lang.chunks ]
    return (docid, json.dumps(segmented))

def resegment_pages(db, cur, pool):
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

    cur.execute("    SELECT p.id"
                "      FROM extracted_plaintext p"
                " LEFT JOIN extracted_pt_resegment q ON p.id = q.id"
                "     WHERE p.segmented IS NOT NULL"
                "       AND q.id IS NULL")
    pages = cur.fetchall()
    total_pages = len(pages)
    if not total_pages:
        return

    processed = 0
    start = time.monotonic()
    sys.stdout.write("Processing 0/{}...\n".format(total_pages))
    for chunk in chunked(pages, 1000):
        with db:
            cur.execute(" SELECT id, plaintext"
                        "   FROM extracted_plaintext p"
                        "  WHERE id = ANY(%s)",
                        ([c[0] for c in chunk],))

            for result in pool.imap_unordered(do_resegment, cur.fetchall()):
                cur.execute("INSERT INTO extracted_pt_resegment "
                            "VALUES (%s,%s::jsonb)",
                            result)
                processed += 1

        stop = time.monotonic()
        elapsed = stop - start
        remain  = (total_pages - processed)*(elapsed/processed)
        sys.stdout.write("Processed {}/{} in {} remaining {}\n"
                         .format(processed, total_pages,
                                 fmt_interval(elapsed),
                                 fmt_interval(remain)))

def main():
    with multiprocessing.Pool() as pool:
        db = psycopg2.connect("dbname="+sys.argv[1])
        cur = db.cursor()
        cur.execute("SET search_path TO analysis, public")
        cur.execute("SET standard_conforming_strings TO on")
        resegment_pages(db, cur, pool)

main()

