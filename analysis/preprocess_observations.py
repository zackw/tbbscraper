#! /usr/bin/python3

import sys
import os
import psycopg2
import time

import collections
import itertools
import multiprocessing
import json
import zlib
import hashlib

import cld2
import html_extractor

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

# This chunk of the work doesn't touch the database at all, and so
# can be farmed out to worker processes.  We must use processes and
# not threads because of the GIL, and unfortunately that means we
# have to pass all the data back and forth in bare tuples.

def do_content_extraction(args):
    origin, page, baseurl = args
    try:
        page = zlib.decompress(page)
    except:
        page = ''
    pagelen = len(page)
    extr = html_extractor.ExtractedContent(baseurl, page)
    langs = cld2.detect(extr.text_pruned)

    pcontent = zlib.compress(extr.text_pruned.encode("utf-8"))
    phash = hashlib.sha256(pcontent).digest()
    headings = zlib.compress(json.dumps(extr.headings).encode("utf-8"))
    links = zlib.compress(json.dumps(extr.links).encode("utf-8"))
    resources = zlib.compress(json.dumps(extr.resources).encode("utf-8"))
    domstats = zlib.compress(json.dumps(extr.dom_stats.to_json()).encode("utf-8"))

    return (origin, pagelen, phash, langs[0].code, langs[0].percent,
            pcontent, links, resources, headings, domstats)

def preprocess_pages(db, cur, pool):

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

    # We need a base URL for each page. In the cases where more than
    # one URL maps to the same page, we just pick one and hope it
    # doesn't matter.  It is enormously more efficient to do this at
    # the same time as we pull out the list of pages.

    sys.stdout.write("Determining job size...\n")
    sys.stdout.flush()

    cur.execute("SELECT h.id, s.url"
                "  FROM collection.capture_html_content h"
                "  LEFT JOIN analysis.capture_pruned_content p ON h.id = p.origin"
                " INNER JOIN (SELECT DISTINCT ON (html_content) html_content, redir_url"
                "               FROM collection.captured_pages"
                "              WHERE access_time >= TIMESTAMP '2015-09-01'"
                "            ) c ON c.html_content = h.id"
                "  INNER JOIN collection.url_strings s ON c.redir_url = s.id"
                " WHERE p.origin IS NULL")
    pages = cur.fetchall()
    total_pages = len(pages)
    if not total_pages:
        return

    processed = 0
    start = time.monotonic()
    sys.stdout.write("Processing 0/{}...\n".format(total_pages))
    for chunk in chunked(pages, 1000):
        with db:
            cur.execute(" SELECT id, content FROM collection.capture_html_content"
                        "  WHERE id = ANY(%s)",
                        ([c[0] for c in chunk],))
            # (id, content) join (id, url) -> (id, content, url)
            # memoryviews cannot go through pickle/unpickle
            content = { r[0] : bytes(r[1]) for r in cur }
            block = [(c[0], content[c[0]], c[1]) for c in chunk]

            for result in pool.imap_unordered(do_content_extraction, block):
                cur.execute("INSERT INTO analysis.capture_pruned_content"
                            "(origin, content_len, hash, lang_code, lang_conf,"
                            " content, links, resources, headings, dom_stats)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            result)

        stop = time.monotonic()
        processed += len(block)
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
    with multiprocessing.Pool() as pool:
        preprocess_pages(db, cur, pool)

main()
