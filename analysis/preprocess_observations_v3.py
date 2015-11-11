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
import word_seg

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

# psycopg2 offers no way to push an UTF-8 byte string into a TEXT field,
# even though UTF-8 encoding is exactly how it pushes a unicode string.
# With standard_conforming_strings on, the only character that needs
# to be escaped (by doubling it) in a valid UTF-8 string literal is '.
def quote_utf8_as_text(s):
    return b"'" + s.replace(b"'", b"''") + b"'"

# We have to manually construct several variations on this construct.
# In principle, it could be done in one query, but it's a mess and
# probably not more efficient, especially as it involves transmitting
# large blobs to the database whether it needs them or not
def intern_blob(cur, table, column, hash, blob, is_jsonb):
    hash = cur.mogrify("%s", (hash,))
    cur.execute(b"SELECT id FROM " + table + b" WHERE hash = " + hash)
    rv = cur.fetchall()
    if rv:
        return rv[0][0]

    blob = quote_utf8_as_text(blob)
    if is_jsonb:
        blob += b"::jsonb"

    cur.execute(b"INSERT INTO " + table + b"(hash, " + column + b")"
                b" VALUES (" + hash + b"," + blob + b") RETURNING id")
    return cur.fetchone()[0]

def intern_pruned_segmented(cur, hash, pruned, segmented):
    hash = cur.mogrify("%s", (hash,))
    cur.execute(b"SELECT id FROM extracted_plaintext WHERE hash = " + hash)
    rv = cur.fetchall()
    if rv:
        return rv[0][0]

    pruned    = quote_utf8_as_text(pruned)
    segmented = quote_utf8_as_text(segmented) + b"::jsonb"
    cur.execute(b"INSERT INTO extracted_plaintext (hash, plaintext, segmented)"
                b" VALUES (" + hash + b"," + pruned + b"," + segmented + b")"
                b" RETURNING id")
    return cur.fetchone()[0]


# This chunk of the work doesn't touch the database at all, and so
# can be farmed out to worker processes.  We must use processes and
# not threads because of the GIL, and unfortunately that means we
# have to pass all the data back and forth in bare tuples.

def do_content_extraction(args):
    docid, page, baseurl = args
    try:
        page = zlib.decompress(page)
    except:
        page = ''
    extr = html_extractor.ExtractedContent(baseurl, page)
    lang = cld2.detect(extr.text_pruned, want_chunks=True)

    segmented = [ { "l": c[0].code,
                    "t": list(word_seg.segment(c[0].code, c[1])) }
                  for c in lang.chunks ]

    pagelen = len(page)
    content = extr.text_content.encode("utf-8")
    chash   = hashlib.sha256(content).digest()
    pruned  = extr.text_pruned.encode("utf-8")
    phash   = hashlib.sha256(pruned).digest()
    segmtd  = json.dumps(segmented).encode("utf-8")
    heads   = json.dumps(extr.headings).encode("utf-8")
    hhash   = hashlib.sha256(heads).digest()
    links   = json.dumps(extr.links).encode("utf-8")
    lhash   = hashlib.sha256(links).digest()
    rsrcs   = json.dumps(extr.resources).encode("utf-8")
    rhash   = hashlib.sha256(rsrcs).digest()
    domst   = json.dumps(extr.dom_stats.to_json()).encode("utf-8")
    dhash   = hashlib.sha256(domst).digest()

    return (docid, pagelen,
            chash, content,
            phash, pruned, segmtd,
            hhash, heads,
            lhash, links,
            rhash, rsrcs,
            dhash, domst)

def insert_result(cur, result):
    (docid, pagelen,
     chash, content,
     phash, pruned, segmtd,
     hhash, heads,
     lhash, links,
     rhash, rsrcs,
     dhash, domst) = result

    cid = intern_blob(cur, b"extracted_plaintext", b"plaintext",
                      chash, content, False)
    pid = intern_pruned_segmented(cur, phash, pruned, segmtd)
    hid = intern_blob(cur, b"extracted_headings", b"headings",
                      hhash, heads, True)
    lid = intern_blob(cur, b"extracted_urls", b"urls",
                      lhash, links, True)
    rid = intern_blob(cur, b"extracted_urls", b"urls",
                      rhash, rsrcs, True)
    did = intern_blob(cur, b"extracted_dom_stats", b"dom_stats",
                      dhash, domst, True)

    cur.execute("INSERT INTO extracted_content_ov"
                " (content_len, raw_text, pruned_text, links, resources,"
                "  headings, dom_stats)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                " RETURNING id",
                (pagelen, cid, pid, lid, rid, hid, did))
    eid = cur.fetchone()[0]
    cur.execute("UPDATE collection.capture_html_content"
                "   SET extracted = %s"
                " WHERE id = %s",
                (eid, docid))

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
                "  JOIN (SELECT DISTINCT ON (html_content)"
                "              html_content, redir_url"
                "         FROM collection.captured_pages"
                "        WHERE access_time >= TIMESTAMP '2015-09-01'"
                "     ) c ON c.html_content = h.id"
                "  JOIN collection.url_strings s ON c.redir_url = s.id"
                " WHERE h.extracted IS NULL")
    pages = cur.fetchall()
    total_pages = len(pages)
    if not total_pages:
        return

    processed = 0
    start = time.monotonic()
    sys.stdout.write("Processing 0/{}...\n".format(total_pages))
    for chunk in chunked(pages, 1000):
        with db:
            cur.execute(" SELECT id, content"
                        "  FROM collection.capture_html_content"
                        "  WHERE id = ANY(%s)",
                        ([c[0] for c in chunk],))
            # (id, content) join (id, url) -> (id, content, url)
            # memoryviews cannot go through pickle/unpickle
            content = { r[0] : bytes(r[1]) for r in cur }
            block = [(c[0], content[c[0]], c[1]) for c in chunk]

            for result in pool.imap_unordered(do_content_extraction, block):
                insert_result(cur, result)

        stop = time.monotonic()
        processed += len(block)
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
        preprocess_pages(db, cur, pool)

main()
