#! /usr/bin/python

# Overview of collected data.  Emits a huge CSV file which can be
# visualized in several different ways.  Note: doesn't use pagedb.py
# because, for efficiency's sake, it wants to do a join that pagedb
# currently doesn't know how to do.

import psycopg2
import psycopg2.extras
import sys
import csv

def main():
    db = psycopg2.connect("dbname="+sys.argv[1])
    rd = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    wr = csv.DictWriter(sys.stdout,
                        ("url.id", "sources", "country", "access.time",
                         "result", "html.id", "document.id", "language"),
                        # DictCursor doesn't behave like a dictionary when
                        # iterated, causing extrasaction="raise" to malfunction.
                        extrasaction="ignore")
    wr.writeheader()

    rd.execute("SET search_path TO ts_analysis;")
    rd.execute("SELECT o.url                        AS \"url.id\","
               "       o.sources                    AS sources,"
               "       ld.name                      AS country,"
               "       o.access_time                AS \"access.time\","
               "       o.result                     AS result,"
               "       encode(o.html_sha2,'base64') AS \"html.id\","
               "       o.document                   AS \"document.id\","
               "       lc.name                      AS language"
               "  FROM page_observations o, page_text t,"
               "       locale_data ld, language_codes lc"
               " WHERE o.document = t.id"
               "   AND o.locale = ld.cc2"
               "   AND t.lang_code = lc.code"
               "   AND o.locale NOT IN ('cn', 'jp_kobe')"
               "   AND (o.locale != 'us' OR o.run = 1)")
    wr.writerows(rd)

main()
