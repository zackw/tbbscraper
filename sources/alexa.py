#! /usr/bin/python

# Alexa-top-1-million source for URLs.

import argparse
import contextlib
import cStringIO
import gzip
import os
import os.path
import re
import sqlite3
import sys
import time
import urllib2
import urlparse
import zipfile

from . import urldb

def parse_args():
    ap = argparse.ArgumentParser(description="Download the current Alexa "
                                 "top-1-million-sites list and add it to the "
                                 "URL database.")
    ap.add_argument("--database", "-d", metavar="DB",
                help="The database to update.",
                default="urls.db")
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

    return ap.parse_args()

def download_sitelist(args, datestamp):
    # We hardwire the knowledge that Alexa only updates this once a day.
    # Don't re-download it if we already have it.
    basename, ext = os.path.splitext(args.src_name)

    cached_csv = os.path.join(args.cache,
                              basename + "-" + datestamp + ext + ".gz")
    if os.path.isfile(cached_csv):
        return cached_csv

    # urllib2's filelike is not with-compatible; neither is it seekable.
    with contextlib.closing(cStringIO.StringIO()) as mbuf:
        with contextlib.closing(urllib2.urlopen(args.src)) as src:
            mbuf.write(src.read())

        mbuf.seek(0)
        zipf = zipfile.ZipFile(mbuf, "r")
        # canonicalize line endings, as long as we're recompressing
        inf = zipf.open(args.src_name, "rU")
        with gzip.GzipFile(filename=cached_csv+".tmp", mode="wb") as ouf:
            ouf.writelines(inf)

    os.rename(cached_csv+".tmp", cached_csv)
    return cached_csv

def add_urls_from_site(cur, site, ordinal, oid, already_seen):
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
    # Because the database schema requires the ordinal+oid to be unique,
    # we shift the ordinal left three bits to make room for a prefix index
    # and an indication of whether or not there was a path component.

    ordinal = int(ordinal) * 8

    if "/" in site:
        pathbit = 4
    else:
        pathbit = 0
        site = site + "/"

    for i, prefix in enumerate(("http://", "https://",
                                "http://www.", "https://www.")):
        url = urlparse.urlparse(prefix + site).geturl()
        if url not in already_seen:
            cur.execute("INSERT INTO urls VALUES(?, ?, ?)",
                        (oid, ordinal + pathbit + i, url))
            already_seen.add(url)

def process_sitelist(db, sitelist_name, datestamp):

    # sometimes the same site is on the list in several different guises
    already_seen = set()

    # one giant transaction should be fine for this job
    with db:
        cur = db.cursor()

        # First, create our entry in the origins table.  We don't need
        # a metadata table; all of the available meta-information is
        # captured by the source name (with its embedded date stamp)
        # and the ordinal within Alexa's list, which is what we use
        # for the origin_id.
        cur.execute("INSERT INTO origins VALUES(NULL, ?)",
                    ("alexa_" + datestamp,))
        oid = cur.lastrowid

        db.commit()

        with gzip.GzipFile(sitelist_name, "r") as sitelist:
            for i, line in enumerate(sitelist):
                ordinal, _, site = line.partition(",")
                site = site.rstrip()
                add_urls_from_site(cur, site, ordinal, oid, already_seen)
                x = site.find("/")
                if x != -1:
                    add_urls_from_site(cur, site[:x], ordinal, oid,
                                       already_seen)
                if i % 1000 == 0:
                    db.commit()
                    sys.stderr.write("\r{}".format(i))

        sys.stderr.write("\n")



def main():
    args      = parse_args()
    datestamp = time.strftime("%Y%m%d", time.gmtime())
    db        = urldb.ensure_database(args)
    sitelist  = download_sitelist(args, datestamp)
    process_sitelist(db, sitelist, datestamp)

if __name__ == '__main__': main()
