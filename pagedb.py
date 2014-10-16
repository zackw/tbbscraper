#! /usr/bin/python3

# Extract captured pages from the database.

import psycopg2
import zlib
import json

class CapturedPage:
    """A page as captured from a particular locale.  Corresponds to one
       row of the .captured_pages table.  Not tied to the database.
    """

    def __init__(self, locale, url, access_time, result, detail,
                 redir_url, capture_log, html_content, screenshot):

        self.locale      = locale
        self.url         = url
        self.access_time = access_time
        self.result      = result
        self.detail      = detail
        self.redir_url   = redir_url
        self.screenshot  = screenshot

        # For memory efficiency, the compressed data is only
        # uncompressed upon request.  (screenshot, if available, is
        # internally compressed - but is directly usable that way,
        # being a PNG.)
        self._capture_log = capture_log
        self._capture_log_unpacked = False

        self._html_content = html_content
        self._html_content_unpacked = False

    @property
    def capture_log(self):
        if not self._capture_log_unpacked:
            self._capture_log = json.loads(
                zlib.decompress(self._capture_log).decode("utf-8"))
            self._capture_log_unpacked = True
        return self._capture_log

    @property
    def html_content(self):
        if not self._html_content_unpacked:
            self._html_content = (
                zlib.decompress(self._html_content).decode("utf-8"))
            self._html_content_unpacked = True
        return self._html_content


    def dump(self, fp, html_content=False, capture_log=False):
        val = {
            "0_url": self.url,
            "1_locale": self.locale,
            "2_access_time": self.access_time.isoformat(' '),
            "3_result": self.result,
            "4_detail": self.detail,
            "5_redir": None,
            "6_html": None,
            "7_log": None
        }
        if self.redir_url != self.url:
            val["5_redir"] = self.redir_url
        if html_content:
            val["6_html"] = self.html_content
        if capture_log:
            val["7_log"] = self.capture_log
        fp.write(json.dumps(val, sort_keys=True).encode("utf-8"))
        fp.write(b'\n')

class PageDB:
    """Wraps a database handle and knows how to extract pages or other
       interesting material (add queries as they become useful!)"""

    def __init__(self, connstr):
        self.db = psycopg2.connect(connstr)

    def get_pages(self, *,
                  ordered=False,
                  where_clause="",
                  limit=None):
        """Retrieve pages from the database matching the where_clause.
           This is a generator, which produces one CapturedPage object
           per row.
        """

        query = ("SELECT c.locale, u.url, c.access_time, c.result, d.detail,"
                 "       r.url, c.capture_log, c.html_content, c.screenshot"
                 "       FROM captured_pages c"
                 "  LEFT JOIN url_strings u    ON c.url = u.id"
                 "  LEFT JOIN url_strings r    ON c.redir_url = r.id"
                 "  LEFT JOIN capture_detail d ON c.detail = d.id")

        if where_clause:
            query += "  WHERE {}".format(where_clause)

        if ordered:
            query += "  ORDER BY u.url, c.locale"

        if limit is not None:
            query += "  LIMIT {}".format(limit)

        with self.db, self.db.cursor() as cur:
            cur.execute(query)
            for row in cur:
                yield CapturedPage(*row)

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
                        default=None)
        ap.add_argument("--html", help="also dump the captured HTML",
                        action="store_true")
        ap.add_argument("--capture-log", help="also dump the capture log",
                        action="store_true")
        ap.add_argument("--ordered", help="sort pages by URL and then locale",
                        action="store_true")

        args = ap.parse_args()
        args.where = " ".join(args.where)

        if "=" not in args.database:
            args.database = "dbname="+args.database

        db = PageDB(args.database)
        prettifier = subprocess.Popen(["underscore", "pretty"],
                                      stdin=subprocess.PIPE)

        prettifier.stdin.write(b'[')

        for page in db.get_pages(where_clause = args.where,
                                 limit        = args.limit,
                                 ordered      = args.ordered):
            page.dump(prettifier.stdin,
                      html_content = args.html,
                      capture_log = args.capture_log)
            prettifier.stdin.write(b',')
            prettifier.stdin.flush()

        prettifier.stdin.write(b']')
        prettifier.stdin.close()

    main()
