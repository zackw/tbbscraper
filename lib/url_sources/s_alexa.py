# Copyright © 2013, 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Download the current Alexa top-1-million-sites list and add it to the
   URL database."""

def setup_argp(ap):
    ap.add_argument("--src", "-s", metavar="URL",
                help="Source URL for the sites list. "
                     "Assumed to name a zipfile.",
                default="http://s3.amazonaws.com/alexa-static/top-1m.csv.zip")
    ap.add_argument("--src-name", "-n", metavar="NAME",
                help="Name of the file to extract from the zipfile.",
                default="top-1m.csv")
    ap.add_argument("--cache", "-c", metavar="DIR",
                help="Directory in which to cache downloaded site lists.",
                default="alexa")

def run(args):
    Monitor(AlexaExtractor(args),
            banner="Extracting URLs from Alexa top 1,000,000")

import contextlib
import gzip
import io
import os
import os.path
import re
import requests
import sys
import time
import urllib.parse
import zipfile

from shared import url_database
from shared.monitor import Monitor

def to_https(spliturl):
    return urllib.parse.SplitResult("https",
                                    spliturl.netloc,
                                    spliturl.path,
                                    spliturl.query,
                                    spliturl.fragment)

def to_siteroot(spliturl):
    return urllib.parse.SplitResult(spliturl.scheme,
                                    spliturl.netloc,
                                    "/",
                                    spliturl.query,
                                    spliturl.fragment)

def add_www(spliturl):
    if "@" in spliturl.netloc:
        (auth, rest) = spliturl.netloc.split("@", 1)
        netloc = auth + "@www." + rest
    else:
        netloc = "www." + spliturl.netloc

    return urllib.parse.SplitResult(spliturl.scheme,
                                    netloc,
                                    spliturl.path,
                                    spliturl.query,
                                    spliturl.fragment)

no_www_re = re.compile(r"^(?:\d+\.\d+\.\d+\.\d+$|\[[\dA-Fa-f:]+\]$|www\.)")

class AlexaExtractor:
    def __init__(self, args):
        # Do not open the database until we are on the correct thread.
        self.args = args
        self.summary = None

    def __call__(self, mon, thr):
        datestamp = time.strftime("%Y-%m-%d", time.gmtime())
        db        = url_database.ensure_database(self.args)
        sitelist  = self.download_sitelist(mon, datestamp)
        self.process_sitelist(mon, db, sitelist, datestamp)
        self.update_canon_queue(mon, db)

    def download_sitelist(self, mon, datestamp):
        # We hardwire the knowledge that Alexa only updates this once a
        # day.  Don't re-download it if we already have it.
        basename, ext = os.path.splitext(self.args.src_name)

        cached_csv = os.path.join(self.args.cache,
                                  basename + "-" + datestamp + ext + ".gz")
        ci_csv = os.path.join(self.args.cache,
                              "imported-" + datestamp + ext + ".gz")
        if os.path.isfile(cached_csv) or os.path.isfile(ci_csv):
            return cached_csv, ci_csv

        mon.report_status("Downloading list...")
        with contextlib.closing(io.BytesIO()) as mbuf:
            with contextlib.closing(requests.get(self.args.src,
                                                 stream=True)) as src:
                total = src.headers['content-length']
                npad = len(total)
                sofar = 0
                for block in src.iter_content(8192):
                    mbuf.write(block)
                    sofar += len(block)
                    mon.report_status("Downloading list: {1:>{0}}/{2} bytes..."
                                      .format(npad, sofar, total))
                    mon.maybe_pause_or_stop()

            mbuf.seek(0)
            mon.report_status("Recompressing {}...".format(cached_csv))
            zipf = zipfile.ZipFile(mbuf, "r")
            # canonicalize line endings, as long as we're recompressing
            inf = zipf.open(self.args.src_name, "rU")
            with gzip.GzipFile(filename=cached_csv+".tmp", mode="wb") as ouf:
                n = 1
                for line in inf:
                    ouf.write(line)
                    mon.report_status("Recompressing {}... {}"
                                      .format(cached_csv, n))
                    n += 1

        os.rename(cached_csv+".tmp", cached_csv)
        return cached_csv, ci_csv

    @staticmethod
    def add_urls_from_site(cur, site, rank, datestamp, batch, already_seen):
        # Subroutine of process_sitelist.
        #
        # Alexa's "site" list has two different kinds of
        # addresses on it: with and without a URL path.
        # Also, most but not all of the sites are second-level
        # domains: any third-level piece (such as "www.") has
        # been stripped.  In no case is there a scheme; in
        # particular we have no idea whether the site prefers
        # http: or https:.  So we expand each entry to four:
        #
        #   http://       site
        #   https://      site
        #   http://  www. site
        #   https:// www. site
        #
        # If there was a path, we include all of the above
        # both with and without the path.  This scheme won't
        # do us any good if the actual content people are
        # loading is neither at the name in the list nor at
        # www. the name in the list; for instance,
        # akamaihd.net is site #68, but neither akamaihd.net
        # nor www.akamaihd.net has any A records, because,
        # being a CDN, all of the actual content is on servers
        # named SOMETHINGELSE.akamaihd.net, and you're not
        # expected to notice that the domain even exists.
        # But there's nothing we can do about that.
        #
        # It does not make sense to prepend 'www.' if 'site' already
        # starts with 'www.' or if it is an IP address.

        parsed = url_database.canon_url_syntax("http://" + site,
                                               want_splitresult=True)

        assert parsed.path != ""
        if parsed.path != "/":
            root = to_siteroot(parsed)
            need_path = True
        else:
            root = parsed
            need_path = False

        urls = [ root.geturl(),
                 to_https(root).geturl() ]

        host = root.hostname
        if no_www_re.match(host):
            need_www = False
        else:
            need_www = True
            with_www = add_www(root)
            urls.extend([ with_www.geturl(),
                          to_https(with_www).geturl() ])


        if need_path:
            urls.extend([ parsed.geturl(),
                          to_https(parsed).geturl() ])

            if need_www:
                with_www = add_www(parsed)
                urls.extend([ with_www.geturl(),
                              to_https(with_www).geturl() ])

        for url in urls:
            (uid, url) = url_database.add_url_string(cur, url)
            if url in already_seen:
                continue
            batch.append( (uid, rank, datestamp) )
            already_seen.add(url)

    def flush_batch(self, db, cur, batch):
        batch_str = b",".join(cur.mogrify("(%s,%s,%s)", row) for row in batch)
        cur.execute(b"INSERT INTO urls_alexa (url, rank, retrieval_date) "
                    b"VALUES " + batch_str)
        db.commit()

    def process_sitelist(self, mon, db, sitelist_names, datestamp):
        # sometimes the same site is on the list in several different guises
        already_seen = set()

        todo_name, done_name = sitelist_names
        if not os.path.isfile(todo_name):
            assert os.path.isfile(done_name)
            return

        cur = db.cursor()
        with gzip.GzipFile(todo_name, "r") as sitelist:
            nurls = 0
            batch = []
            for line in sitelist:
                rank, _, site = line.decode("ascii").partition(",")
                site = site.rstrip()
                self.add_urls_from_site(cur, site, rank, datestamp,
                                        batch, already_seen)
                n = len(batch)
                if n > 10000:
                    self.flush_batch(db, cur, batch)
                    nurls += n
                    mon.report_status("Loaded {:>8} URLs from {:>7} sites | {}"
                                      .format(nurls, rank, site[:35]))
                    mon.maybe_pause_or_stop()
                    batch = []

        db.commit()
        os.rename(todo_name, done_name)

    def update_canon_queue(self, mon, db):
        cur = db.cursor()

        mon.report_status("Flushing duplicates...")
        # The urls_alexa table doesn't have any uniquifier.
        # Flush any duplicate rows that may have occurred.
        cur.execute(
            'DELETE FROM urls_alexa WHERE ctid IN (SELECT ctid FROM ('
            '  SELECT ctid, row_number() OVER ('
            '    PARTITION BY url,rank,retrieval_date'
            '    ORDER BY ctid) AS rnum FROM urls_alexa) t'
            '  WHERE t.rnum > 1)')
        db.commit()

        mon.report_status("Adding URLs to be canonicalized...")
        cur.execute("INSERT INTO canon_urls (url) "
                    "  SELECT DISTINCT url FROM urls_alexa"
                    "  EXCEPT SELECT url FROM canon_urls")
        db.commit()
