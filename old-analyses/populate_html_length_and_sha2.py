#! /usr/bin/python3

import hashlib
import itertools
import os
import psycopg2
import sys
import time
import zlib

# This is not in itertools, for no good reason.
def chunked(iterable, n):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

def process_one_run(db, run):

    # There is no good way to hold a cursor open on a read query while
    # simultaneously making commits to one of the tables involved.  We
    # work around this by maintaining a local list of rows to process.

    sys.stdout.write("Run {}: determining job size...\n".format(run))

    cur = db.cursor()
    cur.execute("SELECT a.locale, a.url FROM ts_run_{n}.captured_pages a"
                "  JOIN (SELECT o.locale, uu.r{n}id AS url, o.html_length"
                "          FROM ts_analysis.page_observations o,"
                "               ts_analysis.url_strings uu"
                "         WHERE o.run = {n} AND o.url = uu.id) b"
                " ON a.locale = b.locale AND a.url = b.url"
                " WHERE b.html_length IS NULL"
                .format(n=run))

    pages = cur.fetchall()
    if not pages:
        return

    total_pages = len(pages)
    processed = 0
    start = time.time()
    sys.stdout.write("Run {n}: processing 0/{total}...\n"
                     .format(n=run, total=total_pages))
    for chunk in chunked(pages, 5000):
        try:
            cur.execute("BEGIN")
            cur.execute("SELECT c.locale, u.id, c.html_content"
                        "  FROM ts_run_{n}.captured_pages c"
                        "  JOIN ts_analysis.url_strings u ON c.url = u.r{n}id"
                        " WHERE (c.locale, c.url) IN"
                        .format(n=run).encode("ascii")
                        + b"("
                        + b",".join(cur.mogrify("(%s,%s)", row)
                                    for row in chunk)
                        + b")")

            block = cur.fetchall()
            for locale, url, html_content in block:
                if not html_content:
                    html_content = b''
                else:
                    html_content = zlib.decompress(html_content)

                html_len  = len(html_content)
                html_sha2 = hashlib.sha256(html_content).digest()

                cur.execute("UPDATE ts_analysis.page_observations"
                            "   SET html_length = %s, html_sha2 = %s"
                            " WHERE run = %s AND locale = %s AND url = %s",
                            (html_len, html_sha2, run, locale, url))

            db.commit()
        except:
            db.rollback()
            raise

        stop = time.time()
        processed += len(block)
        elapsed = stop - start
        remain  = (total_pages - processed)*(elapsed/processed)
        sys.stdout.write("Run {}: processed {}/{} in {} remaining {}\n"
                         .format(run, processed, total_pages,
                                 fmt_interval(elapsed),
                                 fmt_interval(remain)))


def main():
    db = psycopg2.connect("dbname="+sys.argv[1])
    cur = db.cursor()
    cur.execute("SET search_path TO ''")
    cur.execute("SELECT CAST(n AS INTEGER) FROM ("
                "SELECT SUBSTRING(column_name FROM 'r([0-9]+)id') AS n"
                "  FROM information_schema.columns"
                " WHERE table_schema = 'ts_analysis'"
                "   AND table_name = 'url_strings') _"
                " WHERE n <> ''")

    runs = sorted(row[0] for row in cur)
    cur.close()
    for run in runs:
        process_one_run(db, run)

main()
