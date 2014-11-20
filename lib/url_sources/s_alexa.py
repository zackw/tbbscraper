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
    ap.add_argument("--top-n", "-t", metavar="N",
                help="How many sites (from the top down) to add.",
                type=int, default=0)
    ap.add_argument("--http-only", "-H",
                    help="Only add http:// URLs.",
                    action="store_true")
    ap.add_argument("--www-only", "-w",
                    help="Only add URLs with 'www.' prefixed to the hostname.",
                    action="store_true")


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

    def add_urls_from_site(self, cur, site, rank, datestamp, batch,
                           already_seen):
        for (uid, url) in url_database.add_site(cur, site,
                                                self.args.http_only,
                                                self.args.www_only):
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
                rank = int(rank)
                site = site.rstrip()
                self.add_urls_from_site(cur, site, rank, datestamp,
                                        batch, already_seen)
                n = len(batch)
                if n >= 10000 or rank >= self.args.top_n:
                    self.flush_batch(db, cur, batch)
                    nurls += n
                    mon.report_status("Loaded {:>8} URLs from {:>7} sites | {}"
                                      .format(nurls, rank, site[:35]))
                    mon.maybe_pause_or_stop()
                    batch = []
                if rank >= self.args.top_n:
                    break

        db.commit()
        os.rename(todo_name, done_name)
