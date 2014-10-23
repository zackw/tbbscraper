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

    def __init__(self, locale, url_id, url, access_time, result, detail,
                 redir_url, capture_log, html_content, screenshot, want_links=True):

        self.page_id     = (locale, url_id)
        self.locale      = locale
        self.url         = url
        self.access_time = access_time
        self.result      = result
        self.detail      = detail
        self.redir_url   = redir_url
        self.screenshot  = screenshot
        self.want_links  = want_links

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
            self._extracted = ExtractedContent(self.redir_url,
                                               self.html_content,
                                               self.want_links)

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
            "1_locale": self.locale,
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
        if "=" not in connstr:
            connstr = "dbname="+connstr
        self.db = psycopg2.connect(connstr)
        self._locales = None

    @property
    def locales(self):
        """Retrieve a list of all available locales.  This involves a
           moderately expensive query so it's memoized.
        """
        if self._locales is None:
            with self.db, self.db.cursor() as cur:
                cur.execute("SELECT DISTINCT locale FROM captured_pages")
                self._locales = sorted([
                    row[0] for row in cur
                ])
        return self._locales

    def get_pages(self, *,
                  ordered=False,
                  where_clause="",
                  limit=None,
                  want_links=True):
        """Retrieve pages from the database matching the where_clause.
           This is a generator, which produces one CapturedPage object
           per row.
        """

        query = ("SELECT c.locale, c.url, u.url, c.access_time, c.result, d.detail,"
                 "       r.url, c.capture_log, c.html_content, c.screenshot"
                 "       FROM captured_pages c"
                 "       JOIN url_strings u    ON c.url = u.id"
                 "  LEFT JOIN url_strings r    ON c.redir_url = r.id"
                 "  LEFT JOIN capture_detail d ON c.detail = d.id")

        if where_clause:
            query += "  WHERE {}".format(where_clause)

        if ordered:
            # Note: these ordering options do not require the database server to
            # sort the entire result set before returning it.  If you add another
            # option, use EXPLAIN SELECT in the psql command-line tool and make
            # sure there are no "Sort" steps.
            if ordered == "locale":
                query += "  ORDER BY c.locale"
            elif ordered == "url":
                query += "  ORDER BY u.url"
            elif ordered == True or ordered == "both":
                query += "  ORDER BY c.locale, c.url"
            else:
                raise ValueError("invalid argument: ordered={!r}".format(ordered))

        if limit is not None:
            query += "  LIMIT {}".format(limit)

        # This must be a named cursor, otherwise psycopg2 helpfully fetches
        # ALL THE ROWS AT ONCE, and they don't fit in RAM and it crashes.
        with self.db, \
             self.db.cursor("pagedb_qtmp_{}".format(os.getpid())) as cur:
            cur.itersize = 100
            cur.execute(query)
            for row in cur:
                yield CapturedPage(*row, want_links=want_links)

    def get_random_pages(self, count, seed, **kwargs):
        rng = random.Random(seed)

        with self.db, self.db.cursor() as cur:
            cur.execute("select min(url), max(url) from captured_pages");
            lo_url, hi_url = cur.fetchone()

        sample = rng.sample(range(lo_url, hi_url + 1), count)
        where = "c.url IN (" + ",".join(str(n) for n in sample) + ")"

        return self.get_pages(where_clause=where, **kwargs)

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
        ap.add_argument("--ordered", help="sort returned results",
                        choices=('url', 'locale', 'both'))
        ap.add_argument("--random", help="select pages at random", type=int, metavar="seed",
                        default=None)

        args = ap.parse_args()
        args.where = " ".join(args.where)

        db = PageDB(args.database)

        if args.random is not None:
            if args.limit is None:
                ap.error("--random must be used with --limit")
            pages = db.get_random_pages(args.random, args.limit, ordered=args.ordered)
        else:
            pages = db.get_pages(where_clause = args.where,
                                 limit        = args.limit,
                                 ordered      = args.ordered)

        prettifier = subprocess.Popen(["underscore", "pretty"],
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
