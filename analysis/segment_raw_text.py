#! /usr/bin/python3

import collections
import itertools
import json
import multiprocessing
import os
import sys
import time

import cld2
import psycopg2
import word_seg

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

start = None
def progress(message):
    stop = time.monotonic()
    global start
    if start is None:
        start = stop

    sys.stdout.write("{}: {}\n"
                     .format(fmt_interval(stop - start), message))
    sys.stdout.flush()

# This is not in itertools, for no good reason.
def chunked(iterable, n):
    it = iter(iterable)
    while True:
       chunk = list(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

# psycopg2 offers no way to push an UTF-8 byte string into a TEXT field,
# even though UTF-8 encoding is exactly how it pushes a unicode string.
# With standard_conforming_strings on, the only character that needs
# to be escaped (by doubling it) in a valid UTF-8 string literal is '.
# We also filter out NUL bytes, which Postgresql does not support in TEXT,
# substituting (the UTF-8 encoding of) U+FFFD.
def quote_utf8_as_text(s):
    return (b"'" +
            s.replace(b"'", b"''").replace(b"\x00", b"\xef\xbf\xbd") +
            b"'")

def do_segmentation(args):
    id, text = args
    lang = cld2.detect(text, want_chunks=True)
    segmented = [ { "l": c[0].code,
                    "t": list(word_seg.segment(c[0].code, c[1])) }
                  for c in lang.chunks ]
    return id, quote_utf8_as_text(json.dumps(segmented).encode("utf-8"))

def main(pool, dbname):
    db = psycopg2.connect(dbname=dbname)
    cur = db.cursor()

    progress("computing job size...")

    cur.execute("""
        SELECT id FROM analysis.extracted_plaintext
         WHERE segmented IS NULL AND length(plaintext) < 83886080
    """)
    ids = [r[0] for r in cur]
    jsize = len(ids)

    progress("computing job size... {}".format(jsize))

    n = 0
    for chunk in chunked(ids, 1000):
        cur.execute("""
            SELECT id, plaintext FROM analysis.extracted_plaintext WHERE id = ANY(%s)
        """, (chunk,))

        for id, segmented in pool.imap_unordered(do_segmentation, cur.fetchall()):
            try:
                cur.execute(b"UPDATE analysis.extracted_plaintext" +
                            b"   SET segmented = " + segmented +
                            b"::jsonb WHERE id = " + str(id).encode("utf-8"))
            except psycopg2.InternalError:
                progress("*** id {} segmented form too large @ {} bytes"
                         .format(id, len(segmented)))
            n += 1

        if n % 1000 == 0:
            progress("{}/{}".format(n, jsize))
            db.commit()

    progress("{}/{}".format(n, jsize))
    db.commit()

with multiprocessing.Pool(12) as pool:
    main(pool, sys.argv[1])
