#! /usr/bin/python3

import collections
import datetime
import hashlib
import os
import psycopg2
import re
import sqlite3
import sys
import time
import urllib.parse
import zlib

class savepoint:
    def __init__(self, cur, name):
        self._cur  = cur
        self._name = name

    def __enter__(self):
        self._cur.execute('SAVEPOINT "' + self._name + '"')

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self._cur.execute('RELEASE SAVEPOINT "' + self._name + '"')
        else:
            self._cur.execute('ROLLBACK TO SAVEPOINT "' + self._name + '"')
        return False

def _urlsplit_forced_encoding(url):
    try:
        return urllib.parse.urlsplit(url)
    except UnicodeDecodeError:
        return urllib.parse.urlsplit(url.decode("utf-8", "surrogateescape"))

_enap_re = re.compile(br'[\x00-\x20\x7F-\xFF]|'
                      br'%(?!(?:[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}))')
def _encode_nonascii_and_percents(segment):
    segment = segment.encode("utf-8", "surrogateescape")
    return _enap_re.sub(
        lambda m: "%{:02X}".format(ord(m.group(0))).encode("ascii"),
        segment).decode("ascii")

def canon_url_syntax(url, *, want_splitresult=None):
    """Syntactically canonicalize a URL.  This makes the following
       transformations:
         - scheme and hostname are lowercased
         - hostname is punycoded if necessary
         - vacuous user, password, and port fields are stripped
         - ports redundant to the scheme are also stripped
         - path becomes '/' if empty
         - characters outside the printable ASCII range in path,
           query, fragment, user, and password are %-encoded, as are
           improperly used % signs

       You can provide either a string or a SplitResult, and you get
       back what you put in.  You can set the optional argument
       want_splitresult to True or False to force a particular
       type of output.

       This version of this function tolerates a number of syntactic
       problems that other versions reject, e.g. "http:/host.dom.ain"
       and "itmss://whatever".
    """

    if isinstance(url, urllib.parse.SplitResult):
        if want_splitresult is None: want_splitresult = True
        exploded = url

    else:
        if want_splitresult is None: want_splitresult = False

        exploded = _urlsplit_forced_encoding(url)
        if not exploded.hostname:
            # Canonicalize the number of slashes after the scheme and retry.
            corrected = re.sub(r'(?i)^([a-z]+):/+', r'\1://', url)
            exploded = _urlsplit_forced_encoding(corrected)

    scheme = exploded.scheme
    host   = exploded.hostname or ""
    user   = _encode_nonascii_and_percents(exploded.username or "")
    passwd = _encode_nonascii_and_percents(exploded.password or "")
    port   = exploded.port
    path   = _encode_nonascii_and_percents(exploded.path or "/")
    query  = _encode_nonascii_and_percents(exploded.query)
    frag   = _encode_nonascii_and_percents(exploded.fragment)

    # It is easier to do this unconditionally than to try to detect
    # non-ASCII characters.  The split-join is to avoid barfing on
    # syntactically invalid hostnames (e.g. "foo..com").
    # encode('idna') is idempotent.
    host = ".".join((label.encode("idna").decode("ascii") if label else "")
                    for label in host.split("."))

    if port is None:
        port = ""
    elif ((port == 80  and scheme == "http") or
          (port == 443 and scheme == "https")):
        port = ""
    else:
        port = ":{}".format(port)

    # We don't have to worry about ':' or '@' in the user and password
    # strings, because urllib.parse does not do %-decoding on them.
    if user == "" and passwd == "":
        auth = ""
    elif passwd == "":
        auth = "{}@".format(user)
    else:
        auth = "{}:{}@".format(user, passwd)
    netloc = auth + host + port

    result = urllib.parse.SplitResult(scheme, netloc, path, query, frag)
    if want_splitresult:
        return result
    else:
        return result.geturl()

def add_url_string(cur, url):
    """Add an URL to the url_strings table for DB, if it is not already there.
       Returns a pair (id, url) where ID is the table identifier, and URL
       is the URL as returned by canon_url_syntax()."""

    url = canon_url_syntax(url)

    # Wrap the operation below in a savepoint, so that if it aborts
    # (for instance, if the URL is too long) any outer transaction is
    # not ruined.
    with savepoint(cur, "url_string_insertion"):
        # Theoretically this could be done in one query with WITH and
        # INSERT ... RETURNING, but it is convoluted enough that I don't
        # believe it will be faster.  Alas.
        cur.execute("SELECT id FROM url_strings WHERE url = %s", (url,))
        row = cur.fetchone()
        if row is not None:
            id = row[0]
        else:
            cur.execute("INSERT INTO url_strings(id, url) VALUES(DEFAULT, %s) "
                        "RETURNING id", (url,))
            id = cur.fetchone()[0]
        return (id, url)

def add_capture_html_content(cur, content):
    # Wrap the operation below in a savepoint, so that if it aborts any
    # outer transaction is not ruined.
    with savepoint(cur, "capture_html_content_insertion"):
        # This definitely should not be done in one query, because we can
        # avoid pushing the actual data over the connection if it's a dupe.

        h = hashlib.sha256(content).digest()
        cur.execute("SELECT id FROM capture_html_content WHERE hash = %s",
                    (h,))
        row = cur.fetchone()
        if row is not None:
            return row[0]
        else:
            cur.execute("INSERT INTO capture_html_content(id, hash, content)"
                        "  VALUES(DEFAULT, %s, %s)"
                        "  RETURNING id", (h, content))
            return cur.fetchone()[0]

def record_cc_page(cur, url, date, html):
    if html == '':
        html = b''
    else:
        html = zlib.compress(html.encode("utf-8"))
    (uid, _) = add_url_string(cur, url)
    cid      = add_capture_html_content(cur, html)
    cur.execute("INSERT INTO common_crawl_pages (id, url, date, html_content)"
                "     VALUES (DEFAULT, %s, %s, %s)",
                (uid, date, cid))

class Cruncher:
    def __init__(self, dbname, crawldata):
        self.dbname    = dbname
        self.crawldata = crawldata
        self.fdb       = sqlite3.connect(crawldata)
        self.fdb.text_factory = bytes
        self.tdb       = psycopg2.connect(dbname=dbname)
        self.start     = time.monotonic()

    def run(self):
        fcur = self.fdb.cursor()
        tcur = self.tdb.cursor()
        self.progress("counting records to import...")
        fcur.execute("SELECT COUNT(*) FROM samples_html_content")
        nrec = fcur.fetchone()[0]
        self.progress("total {} records".format(nrec))

        tcur.execute("SET search_path TO collection, public")

        nproc = 0
        fcur.execute("SELECT url, date, html_content"
                     "  FROM samples_html_content")
        for url, date, html in fcur:
            url = url.decode("utf-8")
            date = date.decode("ascii")
            try:
                html = html.decode("utf-8")
            except UnicodeDecodeError:
                html = html.decode("iso-8859-1")
            record_cc_page(tcur, url, date, html)
            nproc += 1
            if nproc % 1000 == 0:
                self.progress("processed {}/{}".format(nproc, nrec))
                self.tdb.commit()

        self.progress("processed {}/{}.".format(nproc, nrec))
        self.tdb.commit()

    def progress(self, message):
        now = time.monotonic()
        delta = datetime.timedelta(seconds = now - self.start)
        sys.stderr.write("[{}] {}\n".format(delta, message))

def main():
    dbname = sys.argv[1]
    crawldata = sys.argv[2]
    Cruncher(dbname, crawldata).run()

main()
