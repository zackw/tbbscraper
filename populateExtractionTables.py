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

def populate_language_codes(db):
    cur = db.cursor()
    try:
        cur.execute("BEGIN")
        cur.executemany("INSERT INTO ts_analysis.language_codes (code, name)"
                        " VALUES(%s,%s)",
                        [(k, v) for k,v in cld2.get_all_languages().items()])
        db.commit()
    except:
        db.rollback()
        raise

def merge_url_strings(db, schema):
    cur = db.cursor()
    sys.stdout.write("Populating url_strings for run {} (1/2)...\n"
                     .format(schema))
    try:
        cur.execute("BEGIN")
        cur.execute("UPDATE ts_analysis.url_strings v"
                    "   SET r{n}id = u.id"
                    "  FROM ts_run_{n}.url_strings u"
                    " WHERE v.url = u.url".format(n=schema))

        sys.stdout.write("Populating url_strings for run {} (2/2)...\n"
                         .format(schema))
        cur.execute("INSERT INTO ts_analysis.url_strings (url, r{n}id) "
                    "SELECT url, id FROM ts_run_{n}.url_strings"
                    " WHERE url NOT IN"
                    "  (SELECT url FROM ts_analysis.url_strings)"
                    .format(n=schema))
        db.commit()
    except:
        db.rollback()
        raise

def merge_capture_detail(db, schema):
    cur = db.cursor()
    sys.stdout.write("Populating capture_detail for run {}...\n"
                     .format(schema))
    try:
        cur.execute("BEGIN")
        cur.execute("INSERT INTO ts_analysis.capture_detail (detail) "
                    "SELECT detail FROM ts_run_{n}.capture_detail"
                    " WHERE detail NOT IN"
                    "  (SELECT detail FROM ts_analysis.capture_detail)"
                    .format(n=schema))
        db.commit()
    except:
        db.rollback()
        raise

def crossmap_url_strings(db, schema):
    sys.stdout.write("Run {}: preparing URL crossmap...\n".format(schema))
    cur = db.cursor()
    cur.execute("SELECT r{n}id, id FROM ts_analysis.url_strings"
                " WHERE r{n}id IS NOT NULL".format(n=schema))
    rv = { row[0] : row[1] for row in cur }
    rv[None] = None
    return rv

def crossmap_capture_detail(db, schema):
    sys.stdout.write("Run {}: preparing detail crossmap...\n".format(schema))
    cur = db.cursor()
    cur.execute("SELECT d.id, dd.id"
                "  FROM ts_run_{n}.capture_detail d,"
                "       ts_analysis.capture_detail dd"
                " WHERE d.detail = dd.detail".format(n=schema))
    rv = { row[0] : row[1] for row in cur }
    rv[None] = None
    return rv

# This chunk of the work doesn't touch the database at all, and so
# can be farmed out to worker processes.  We must use processes and
# not threads because of the GIL, and unfortunately that means we
# have to pass all the data back and forth in bare tuples.

def do_content_extraction(args):
    page, url, locale, sources, access_time, result, detail, ourl, rurl = args
    page = zlib.decompress(page)
    pagelen = len(page)
    pagehash = hashlib.sha256(page).digest()
    extr = html_extractor.ExtractedContent(url, page)
    langs = cld2.detect(extr.text_pruned)
    return (zlib.compress(extr.text_pruned.encode("utf-8")),
            zlib.compress(extr.text_content.encode("utf-8")),
            zlib.compress(json.dumps(extr.headings).encode("utf-8")),
            zlib.compress(json.dumps(extr.links).encode("utf-8")),
            zlib.compress(json.dumps(extr.resources).encode("utf-8")),
            zlib.compress(json.dumps(extr.dom_stats.to_json())
                          .encode("utf-8")),
            langs[0].code,
            langs[0].percent,
            locale,
            sources,
            access_time,
            result,
            detail,
            ourl,
            rurl,
            pagelen,
            pagehash)

def add_page_text(wcur, text, has_boilerplate, lang_code, lang_conf):
    wcur.execute("SELECT id FROM ts_analysis.page_text"
                 " WHERE md5(contents) = %s AND contents = %s",
                 (hashlib.md5(text).hexdigest(), text,))
    r = wcur.fetchall()
    if r:
        return r[0][0]

    wcur.execute("INSERT INTO ts_analysis.page_text"
                 " (has_boilerplate, lang_code, lang_conf, contents)"
                 " VALUES (%s, %s, %s, %s)"
                 " RETURNING id",
                 (has_boilerplate, lang_code, lang_conf, text))
    r = wcur.fetchone()
    return r[0]

def preprocess_observations(db, schema, pool):

    # This generator's sole function is to convert the memoryview
    # returned for 'captured_pages.html_content' into a proper byte
    # string, because byte strings can go through a pickle/unpickle
    # and memoryviews can't.
    def squash_memoryviews(block):
        for row in block:
            if len(row) != 9:
                raise RuntimeError("{}: {}".format(len(row), repr(row)))
            yield ((bytes(row[0])
                    if row[0] is not None
                    # this is zlib.compress('')
                    else b'x\x9c\x03\x00\x00\x00\x00\x01'),
                   row[1], row[2], row[3], row[4],
                   row[5], row[6], row[7], row[8])

    # This is not in itertools, for no good reason.
    def chunked(iterable, n):
        it = iter(iterable)
        while True:
           chunk = tuple(itertools.islice(it, n))
           if not chunk:
               return
           yield chunk

    urlmap    = crossmap_url_strings(db, schema)
    detailmap = crossmap_capture_detail(db, schema)

    # There is no good way to hold a cursor open on a read query while
    # simultaneously making commits to one of the tables involved.  We
    # work around this by maintaining a local list of rows to process.

    sys.stdout.write("Run {}: determining job size...\n".format(schema))

    cur = db.cursor()
    cur.execute("SELECT a.locale, a.url FROM ts_run_{n}.captured_pages a"
                " LEFT JOIN (SELECT o.locale, uu.r{n}id AS url"
                "              FROM ts_analysis.page_observations o,"
                "                   ts_analysis.url_strings uu"
                "             WHERE o.run = {n} AND o.url = uu.id) b"
                " ON a.locale = b.locale AND a.url = b.url"
                " WHERE b.locale IS NULL"
                .format(n=schema))

    pages = cur.fetchall()
    if not pages:
        return

    total_pages = len(pages)
    processed = 0
    start = time.time()
    sys.stdout.write("Run {n}: processing 0/{total}...\n"
                     .format(n=schema, total=total_pages))
    for chunk in chunked(pages, 1000):
        try:
            cur.execute("BEGIN")
            cur.execute("     SELECT c.html_content, u.url,"
                        "            c.locale, s.sources, c.access_time,"
                        "            c.result, c.detail, c.url, c.redir_url"
                        "       FROM ts_run_{n}.captured_pages c"
                        " INNER JOIN ts_run_{n}.url_strings u"
                        "            ON COALESCE(c.redir_url, c.url) = u.id"
                        " INNER JOIN ts_run_{n}.sources_by_url s"
                        "            ON c.url = s.url"
                        "      WHERE (c.locale, c.url) IN"
                        .format(n=schema).encode("ascii")
                        + b"("
                        + b",".join(cur.mogrify("(%s,%s)", row)
                                    for row in chunk)
                        + b")")

            block = cur.fetchall()
            for result in pool.imap_unordered(do_content_extraction,
                                              squash_memoryviews(block)):
                text_pruned, text_content, headings, links, resources, \
                    dom_stats, lang_code, lang_conf, locale, sources, \
                    access_time, result, detail, ourl, rurl, \
                    pagelen, pagehash = result

                detail = detailmap[detail]
                ourl   = urlmap[ourl]
                rurl   = urlmap[rurl]

                pruned_id = add_page_text(cur, text_pruned, False,
                                          lang_code, lang_conf)
                content_id = add_page_text(cur, text_content, True,
                                           lang_code, lang_conf)

                cur.execute("INSERT INTO ts_analysis.page_observations "
                            "VALUES ("
                            "%s,%s,%s,%s,%s,%s,%s,%s,"
                            "%s,%s,%s,%s,%s,%s,%s,%s)",
                            (pruned_id,
                             ourl,
                             locale,
                             schema,
                             sources,
                             content_id,
                             links,
                             resources,
                             headings,
                             dom_stats,
                             access_time,
                             result,
                             detail,
                             rurl,
                             pagelen,
                             pagehash))
            #endfor

            db.commit()
        except:
            db.rollback()
            raise

        stop = time.time()
        processed += len(block)
        elapsed = stop - start
        remain  = (total_pages - processed)*(elapsed/processed)
        sys.stdout.write("Run {}: processed {}/{} in {} remaining {}\n"
                         .format(schema, processed, total_pages,
                                 fmt_interval(elapsed),
                                 fmt_interval(remain)))

def preprocess_all_observations(db, schemas):
    with multiprocessing.Pool() as pool:
        for schema in schemas:
            preprocess_observations(db, schema, pool)

def main():
    db = psycopg2.connect("dbname="+sys.argv[1])
    cur = db.cursor()
    cur.execute("SET search_path TO ''")
    cur.execute("SELECT DISTINCT table_schema FROM information_schema.tables "
                " WHERE table_schema LIKE 'ts_run_%'")
    schemas = set(int(row[0][len("ts_run_"):]) for row in cur)
    schemas.remove(0)

    #for schema in schemas:
    #    merge_url_strings(db, schema)
    #    merge_capture_detail(db, schema)
    #
    #populate_language_codes(db)
    preprocess_all_observations(db, sorted(schemas))

main()
