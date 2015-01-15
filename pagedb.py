#! /usr/bin/python3

# Extract captured pages from the database.

import os
import psycopg2
import zlib
import json
import random
from html_extractor import ExtractedContent

class CapturedPage:
    """A page as captured from a particular locale.  Corresponds to one
       row of the .captured_pages table.  Not tied to the database.
    """

    def __init__(self,
                 run, urlid, orig_url, sources,
                 cc2, country, access_time,
                 result, detail, redir_url, capture_log, html_content):

        self.page_id     = (run, urlid, cc2)
        self.url         = orig_url
        self.sources     = sources
        self.cc2         = cc2
        self.country     = country
        self.access_time = access_time
        self.result      = result
        self.detail      = detail
        self.redir_url   = redir_url

        # For memory efficiency, the compressed data is only
        # uncompressed upon request.  (screenshot, if available, is
        # internally compressed - but is directly usable that way,
        # being a PNG.)
        self._capture_log = capture_log
        self._capture_log_unpacked = False

        self._html_content = html_content
        self._html_content_unpacked = False

        # Derived values.
        self._extracted = None

    def _do_extraction(self):
        if self._extracted is None:
            # redir_url may be None if, for instance, we got redirected to
            # an itmss: URL.
            url = self.redir_url
            if url is None:
                url = self.url
            self._extracted = ExtractedContent(url, self.html_content)

    @property
    def capture_log(self):
        if not self._capture_log_unpacked:
            self._capture_log_unpacked = True
            if self._capture_log is None:
                self._capture_log = ''
            else:
                self._capture_log = json.loads(
                    zlib.decompress(self._capture_log).decode("utf-8"))
        return self._capture_log

    @property
    def html_content(self):
        if not self._html_content_unpacked:
            self._html_content_unpacked = True
            if self._html_content is None:
                self._html_content = ''
            else:
                self._html_content = (
                    zlib.decompress(self._html_content).decode("utf-8"))
        return self._html_content

    @property
    def text_content(self):
        self._do_extraction()
        return self._extracted.text_content

    @property
    def resources(self):
        self._do_extraction()
        return self._extracted.resources

    @property
    def links(self):
        self._do_extraction()
        return self._extracted.links

    @property
    def dom_stats(self):
        self._do_extraction()
        return self._extracted.dom_stats

    def dump(self, fp, *,
             capture_log=False,
             html_content=False,
             dom_stats=False,
             text_content=False,
             resources=False,
             links=False):
        val = {
            "0_id": self.page_id,
            "0_url": self.url,
            "1_country": self.country,
            "1_cc2": self.cc2,
            "2_access_time": self.access_time.isoformat(' '),
            "3_result": self.result,
            "4_detail": self.detail,
            "5_redir": None,
            "6_log": None,
            "7_html": None,
            "7_text": None,
            "7_rsrcs": None,
            "7_links": None,
            "7_dom_stats": None
        }
        if self.redir_url != self.url:
            val["5_redir"] = self.redir_url

        if capture_log:  val["6_log"]   = self.capture_log
        if html_content: val["7_html"]  = self.html_content
        if text_content: val["7_text"]  = self.text_content
        if resources:    val["7_rsrcs"] = self.resources
        if links:        val["7_links"] = self.links
        if dom_stats:    val["7_dom_stats"] = self.dom_stats.to_json()

        fp.write(json.dumps(val, sort_keys=True).encode("utf-8"))
        fp.write(b'\n')

class PageDB:
    """Wraps a database handle and knows how to extract pages or other
       interesting material (add queries as they become useful!)"""

    def __init__(self, connstr):
        self._locales = None

        if "=" not in connstr:
            connstr = "dbname="+connstr
        self.db = psycopg2.connect(connstr)
        cur = self.db.cursor()
        cur.execute("SET search_path TO ts_analysis")

    @property
    def locales(self):
        """Retrieve a list of all available locales.  This involves a
           moderately expensive query, which has now been memoized on
           the server, but we memoize it again here just to be sure.
        """
        if self._locales is None:
            cur = self.db.cursor()
            cur.execute("SELECT DISTINCT locale FROM captured_locales")
            self._locales = sorted([
                row[0] for row in cur
            ])
        return self._locales

    def get_pages(self, *,
                  ordered=False,
                  where_clause="",
                  limit=None):
        """Retrieve pages from the database matching the where_clause.
           This is a generator, which produces one CapturedPage object
           per row.
        """

        query = ("SELECT run, urlid, orig_url, sources,"
                 "       cc2, country, access_time,"
                 "       result, detail, redir_url, capture_log, html_content"
                 "  FROM captured_pages")

        if where_clause:
            query += "  WHERE {}".format(where_clause)

        if limit is not None:
            query += "  LIMIT {}".format(limit)

        # This must be a named cursor, otherwise psycopg2 helpfully fetches
        # ALL THE ROWS AT ONCE, and they don't fit in RAM and it crashes.
        cur = self.db.cursor("pagedb_qtmp_{}_{}".format(os.getpid(), id(self)))
        cur.itersize = 10000
        cur.execute(query)
        for row in cur:
            yield CapturedPage(*row)

    def get_random_pages(self, count, seed, where_clause="", **kwargs):
        rng = random.Random(seed)

        cur = self.db.cursor()
        cur.execute("select min(id), max(id) from source_url_crossmap");
        lo_url, hi_url = cur.fetchone()

        sample = rng.sample(range(lo_url, hi_url + 1), count)
        cur.execute("SELECT r1, r2 FROM source_url_crossmap WHERE "
                    "id IN (" + ",".join(str(n) for n in sample) + ")")

        selection = ("(run, urlid) IN (VALUES " +
                     ",".join("(1,{}),(2,{})".format(r[0], r[1])
                              for r in cur) + ")")

        if where_clause:
            where_clause = "({}) AND ({})".format(where_clause, selection)
        else:
            where_clause = selection

        return self.get_pages(where_clause=where_clause, **kwargs)

if __name__ == '__main__':
    def main():
        import argparse
        import sys
        import subprocess

        ap = argparse.ArgumentParser(
            description="Dump out captured HTML pages."
        )
        ap.add_argument("database", help="Database to connect to")
        ap.add_argument("where", help="WHERE clause for query", nargs='*')
        ap.add_argument("--limit", help="maximum number of results",
                        default=None, type=int)
        ap.add_argument("--html", help="also dump the captured HTML",
                        action="store_true")
        ap.add_argument("--links", help="also dump extracted hyperlinks",
                        action="store_true")
        ap.add_argument("--resources", help="also dump extracted resource URLs",
                        action="store_true")
        ap.add_argument("--text", help="also dump extracted text",
                        action="store_true")
        ap.add_argument("--dom-stats",
                        help="also dump statistics about the DOM structure",
                        action="store_true")
        ap.add_argument("--capture-log", help="also dump the capture log",
                        action="store_true")
        ap.add_argument("--random", help="select pages at random", type=int, metavar="seed",
                        default=None)

        args = ap.parse_args()
        args.where = " ".join(args.where)

        db = PageDB(args.database)

        if args.random is not None:
            if args.limit is None:
                ap.error("--random must be used with --limit")
            pages = db.get_random_pages(args.limit,
                                        args.random,
                                        where_clause=args.where)
        else:
            pages = db.get_pages(where_clause = args.where,
                                 limit        = args.limit)

        prettifier = subprocess.Popen(["cat"], #["underscore", "pretty"],
                                      stdin=subprocess.PIPE)
        prettifier.stdin.write(b'[')
        for page in pages:
            page.dump(prettifier.stdin,
                      html_content = args.html,
                      text_content = args.text,
                      dom_stats    = args.dom_stats,
                      links        = args.links,
                      resources    = args.resources,
                      capture_log  = args.capture_log)
            prettifier.stdin.write(b',')
            prettifier.stdin.flush()
        prettifier.stdin.write(b']')
        prettifier.stdin.close()

    main()
