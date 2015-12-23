#! /usr/bin/python3

import asyncio
import bisect
import collections
import concurrent.futures
import csv
import datetime
import glob
import hashlib
import itertools
import json
import math
import multiprocessing
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unicodedata
import urllib.parse
import zlib

import aiohttp
import aiopg
from werkzeug.http import parse_options_header

#
# Utilities
#

start = None
def elapsed():
    global start
    now = time.monotonic()
    if start is None:
        start = now
    elapsed = now - start
    esec  = int(math.floor(elapsed))
    efrac = elapsed - esec

    d, hms = divmod(esec, 86400)
    h, ms = divmod(hms, 3600)
    m, s = divmod(ms, 60)

    s += efrac

    return "{d}:{h:02}:{m:02}:{s:06.3f}: ".format(d=d,h=h,m=m,s=s)

stdout_is_tty = None
def status(message, done=False):
    global stdout_is_tty
    if stdout_is_tty is None:
        stdout_is_tty = sys.stdout.isatty()

    if stdout_is_tty:
        sys.stdout.write("\r\x1b[K" + elapsed() + message)
        if done:
            sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        sys.stdout.write(elapsed() + message + "\n")
        sys.stdout.flush()

# Sampling ranges.
# The oldest sample is taken one year and one week before LO,
# and the newest, one week before HI.  We work backward in time.

ONE_YEAR = datetime.timedelta(days=365.2425)
ONE_WEEK = datetime.timedelta(days=7)

def fuzzy_year_range_lo(lo):
    return lo - (ONE_YEAR + ONE_WEEK)
def fuzzy_year_range_hi(hi):
    return hi - ONE_WEEK

def fuzzy_year_range_backward(lo, hi):
    assert lo < hi
    lo = fuzzy_year_range_lo(lo)
    hi = fuzzy_year_range_hi(hi)

    year = ONE_YEAR
    while hi >= lo:
        yield hi
        hi -= year

# This can't be done with collections.defaultdict, but __missing__ is
# a feature of base dict.
class default_identity_dict(dict):
    def __missing__(self, key): return key

# from itertools recipes
def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)

def sync_wait(coro, loop):
    """Synchronously wait for CORO to return its result.  This spins the
       event loop; be careful about where you use it."""
    task = loop.create_task(coro)
    loop.run_until_complete(task)
    return task.result()

class rate_limiter:
    """Unblocks a calling coroutine RATE times per second:

       rate = rate_limiter(10)
       while work_to_do:
           yield from rate
           ...
    """

    def __init__(self, rate, *, loop=None):
        self.rate     = rate
        self.interval = 1.0/rate
        self.last     = 0
        self.loop     = loop or asyncio.get_event_loop()

    def __iter__(self):
        @asyncio.coroutine
        def delay(to_wait):
            nonlocal self
            nonlocal now

            if to_wait >= 0:
                yield from asyncio.sleep(to_wait, loop=self.loop)
                now = self.loop.time()
            self.last = now

        now = self.loop.time()
        elapsed = now - self.last
        return delay(self.interval - elapsed)

    # it happens to be convenient to use these as context managers,
    # even though they are stateless
    def __enter__(self): return self
    def __exit__(self, *dontcare): return False

class work_buffer:
    """Buffer up work until there is enough of it, or till a timeout
       expires (default 30 seconds with no new work added), then
       process it all at once.

       The worker procedure, WORKER, is called with one positional
       argument, which is a list of pairs (item, future) where ITEM is
       a value given to .put(), and FUTURE is the future waiting for
       that item.  It is responsible for satisfying all the futures.
       Additional keyword arguments can be given to the worker
       by passing keyword arguments to the constructor.
    """

    def __init__(self, worker, jobsize, *,
                 label="?", flush_timeout=5, loop=None, **wargs):
        self.worker   = worker
        self.jobsize  = jobsize
        self.wargs    = wargs
        self.loop     = loop or asyncio.get_event_loop()
        self.batch    = []
        self.ftimer   = None
        self.ftimeout = flush_timeout
        self.running  = set()
        self.label    = label

    def __del__(self):
        # This is a backstop; users of this class should ensure that
        # each buffer has been drained long before this point.
        if self.batch or self.running:
            self.loop.run_until_complete(self.drain())

    def put(self, item):
        """Add ITEM to the current batch; return a Future which will receive
           the result of processing ITEM.  If ITEM fills the current
           batch, the batch will be queued for processing, asynchronously
           (this function will _not_ wait for completion).
        """
        fut = asyncio.Future(loop=self.loop)
        self.batch.append((item, fut))

        if len(self.batch) >= self.jobsize:
            self.flush()
        else:
            if self.ftimer is not None:
                self.ftimer.cancel()
            self.ftimer = self.loop.call_later(self.ftimeout, self.flush)

        return fut

    def flush(self):
        """Queue the current batch for processing and start a new one.
           Does not wait for completion.
        """

        if self.ftimer is not None:
            self.ftimer.cancel()
            self.ftimer = None

        batch = self.batch
        self.batch = []
        if batch:
            fut = self.loop.create_task(self._run_batch(batch))
            fut.add_done_callback(self.running.discard)
            self.running.add(fut)

    @asyncio.coroutine
    def drain(self):
        """Queue the current batch for processing, then wait until all
           outstanding work has been completed."""
        self.flush()
        running = self.running
        self.running = set()
        if running:
            yield from asyncio.wait(running, loop=self.loop)

    @asyncio.coroutine
    def _run_batch(self, batch):
        try:
            yield from self.worker(batch, **self.wargs)
        except Exception as e:
            for _, fut in batch:
                if not fut.done():
                    fut.set_exception(e)

def find_le(a, x):
    """Find the rightmost value of A which is less than or equal to X."""
    i = bisect.bisect_right(a, x)
    if i: return a[i-1]
    return None

#
# Database utilities.
#

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
    """

    if isinstance(url, urllib.parse.SplitResult):
        if want_splitresult is None: want_splitresult = True
        exploded = url

    else:
        if want_splitresult is None: want_splitresult = False

        exploded = _urlsplit_forced_encoding(url)
        if not exploded.hostname:
            # Remove extra slashes after the scheme and retry.
            corrected = re.sub(r'(?i)^([a-z]+):///+', r'\1://', url)
            exploded = _urlsplit_forced_encoding(corrected)

    if not exploded.hostname:
        raise ValueError("url with no host - " + repr(url))

    scheme = exploded.scheme
    if scheme != "http" and scheme != "https":
        raise ValueError("url with non-http(s) scheme - " + repr(url))

    host   = exploded.hostname
    user   = _encode_nonascii_and_percents(exploded.username or "")
    passwd = _encode_nonascii_and_percents(exploded.password or "")
    port   = exploded.port
    path   = _encode_nonascii_and_percents(exploded.path)
    query  = _encode_nonascii_and_percents(exploded.query)
    frag   = _encode_nonascii_and_percents(exploded.fragment)

    if path == "":
        path = "/"

    # We do this even if there are no non-ASCII characters, because it
    # has the side-effect of throwing a UnicodeError if the hostname
    # is syntactically invalid (e.g. "foo..com").
    host = host.encode("idna").decode("ascii")

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

@asyncio.coroutine
def add_url_string(db, url):
    """Add an URL to the url_strings table, if it is not already there.
       Returns a pair (id, url) where ID is the table identifier, and
       URL is the URL as returned by canon_url_syntax().
    """

    url = canon_url_syntax(url)

    # Accept either a database connection or a cursor.
    if hasattr(db, 'cursor'):
        cur = yield from db.cursor()
    elif hasattr(db, 'execute'):
        cur = db
    else:
        raise TypeError("'db' argument must be a connection or cursor, not "
                        + type(db))

    # Theoretically this could be done in one query with WITH and
    # INSERT ... RETURNING, but it is convoluted enough that I don't
    # believe it will be faster.  Alas.
    yield from cur.execute(
        "SELECT id FROM url_strings WHERE url = %s", (url,))
    row = yield from cur.fetchone()
    if row is not None:
        id = row[0]
    else:
        yield from cur.execute(
            "INSERT INTO url_strings(id, url) VALUES(DEFAULT, %s) "
            "RETURNING id", (url,))
        id = (yield from cur.fetchone())[0]
    return (id, url)

http_statuses_by_code = {
    200: "ok",

    301: "redirection loop",
    302: "redirection loop",
    303: "redirection loop",
    307: "redirection loop",
    308: "redirection loop",

    400: "bad request (400)",
    401: "authentication required (401)",
    403: "forbidden (403)",
    404: "page not found (404/410)",
    410: "page not found (404/410)",

    500: "server error (500)",
    503: "service unavailable (503)",

    502: "proxy error (502/504/52x)", # not our proxy, but a CDN's.
    504: "proxy error (502/504/52x)",
    520: "proxy error (502/504/52x)",
    521: "proxy error (502/504/52x)",
    522: "proxy error (502/504/52x)",
    523: "proxy error (502/504/52x)",
    524: "proxy error (502/504/52x)",
    525: "proxy error (502/504/52x)",
    526: "proxy error (502/504/52x)",
    527: "proxy error (502/504/52x)",
    528: "proxy error (502/504/52x)",
    529: "proxy error (502/504/52x)",
}

@asyncio.coroutine
def add_http_status(db, status, reason):
    # Accept either a database connection or a cursor.
    if hasattr(db, 'cursor'):
        cur = yield from db.cursor()
    elif hasattr(db, 'execute'):
        cur = db
    else:
        raise TypeError("'db' argument must be a connection or cursor, not "
                        + type(db))

    coarse = http_statuses_by_code.get(status, "other HTTP response")
    fine = "{} {}".format(status, reason)

    yield from cur.execute(
        "SELECT id FROM capture_coarse_result WHERE result = %s",
        (coarse,))
    row = yield from cur.fetchone()
    if row is not None:
        cid = row[0]
    else:
        yield from cur.execute(
            "INSERT INTO capture_coarse_result(id, result)"
            "  VALUES(DEFAULT, %s)"
            "  RETURNING id", (result,))
        cid = (yield from cur.fetchone())[0]

    yield from cur.execute(
        "SELECT id, result FROM capture_fine_result"
        " WHERE detail = %s", (fine,))
    row = yield from cur.fetchone()
    if row is not None:
        fid = row[0]
        if row[1] != cid:
            raise RuntimeError("{!r}: coarse result {!r} inconsistent "
                               "with prior coarse result (id={!r})"
                               .format(fine, result, cid))
    else:
        yield from cur.execute(
            "INSERT INTO capture_fine_result(id, result, detail)"
            "  VALUES(DEFAULT, %s, %s)"
            "  RETURNING id", (cid, fine))
        fid = (yield from cur.fetchone())[0]

    return fid


# psycopg2 offers no way to push an UTF-8 byte string into a TEXT field,
# even though UTF-8 encoding is exactly how it pushes a unicode string.
# With standard_conforming_strings on, the only character that needs
# to be escaped (by doubling it) in a valid UTF-8 string literal is '.
def quote_utf8_as_text(s):
    return b"'" + s.replace(b"'", b"''") + b"'"

# We have to manually construct several variations on this construct.
# In principle, it could be done in one query, but it's a mess and
# probably not more efficient, especially as it involves transmitting
# large blobs to the database whether it needs them or not.
@asyncio.coroutine
def intern_blob(cur, table, column, hash, blob, is_jsonb):
    hash = yield from cur.mogrify("%s", (hash,))
    yield from cur.execute(
        b"SELECT id FROM " + table + b" WHERE hash = " + hash)
    rv = yield from cur.fetchall()
    if rv:
        return rv[0][0]

    blob = quote_utf8_as_text(blob)
    if is_jsonb:
        blob += b"::jsonb"

    yield from cur.execute(
        b"INSERT INTO " + table + b"(hash, " + column + b")"
        b" VALUES (" + hash + b"," + blob + b") RETURNING id")
    return (yield from cur.fetchone())[0]

@asyncio.coroutine
def intern_html_content(cur, hash, blob):
    yield from cur.execute(
        "SELECT id, extracted"
        "  FROM collection.capture_html_content WHERE hash = %s",
        (hash,))
    rv = yield from cur.fetchall()
    if rv:
        return rv[0][0], rv[0][1]

    yield from cur.execute(
        "INSERT INTO collection.capture_html_content (hash, content)"
        "VALUES (%s, %s) RETURNING id", (hash, blob))
    return (yield from cur.fetchone())[0], None

@asyncio.coroutine
def intern_pruned_segmented(cur, hash, pruned, segmented):
    hash = yield from cur.mogrify("%s", (hash,))
    yield from cur.execute(
        b"SELECT id FROM extracted_plaintext WHERE hash = " + hash)
    rv = yield from cur.fetchall()
    if rv:
        return rv[0][0]

    pruned    = quote_utf8_as_text(pruned)
    segmented = quote_utf8_as_text(segmented) + b"::jsonb"
    yield from cur.execute(
        b"INSERT INTO extracted_plaintext (hash, plaintext, segmented)"
        b" VALUES (" + hash + b"," + pruned + b"," + segmented + b")"
        b" RETURNING id")
    return (yield from cur.fetchone())[0]

class Database:
    def __init__(self, dbname, loop=None, **cargs):
        self.loop   = loop or asyncio.get_event_loop()
        # aiopg offers asynchrony, but _not_ concurrency; only one query
        # can be executing per connection.
        self.dblock = asyncio.Lock(loop=loop)
        self.dbname = dbname
        self.cargs  = cargs
        self.db     = None
        self.cur    = None

    def __enter__(self):
        self.db = sync_wait(aiopg.connect(dbname=self.dbname, loop=self.loop,
                                          **self.cargs),
                            loop=self.loop)
        self.cur = sync_wait(self.db.cursor(), loop=self.loop)
        return self

    def __exit__(self, *dontcare):
        self.db.close()
        return False

    # Canned queries
    @asyncio.coroutine
    def get_translations(self):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute("SELECT lang, word, engl FROM translations")

            translations = collections.defaultdict(dict)
            words = 0
            while True:
                block = yield from cur.fetchmany(10000)
                if not block: break
                for lang, word, engl in block:
                    translations[lang][word] = engl
                    words += 1

                status("loading translations... {} languages {} words"
                       .format(len(translations), words))

            status("loading translations... {} languages {} words"
                   .format(len(translations), words), done=True)
            return translations

    @asyncio.coroutine
    def record_translations(self, lang, translations):
        with (yield from self.dblock):
            cur = self.cur

            # Note: assumes the table and column names are
            # ASCII and need no quoting.  It's unfortunate
            # psycopg2 provides no high-level way to do this.
            query = "INSERT INTO {} ({}) VALUES".format(
                table, ",".join(columns)).encode("ascii")
            template = ("(" +
                        ",".join("%s" for _ in range(len(columns))) +
                        ")")

            values = b",".join(
                (yield from cur.mogrify(template, (lang, word, engl)))
                for word, engl in translations)

            yield from cur.execute(query + values)

    @asyncio.coroutine
    def load_date_range_for_url(self, urlid):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute("""
                SELECT MIN(COALESCE(
                           SUBSTRING(u.meta->>'timestamp' FOR 10)::DATE,
                           (u.meta->>'date')::DATE,
                           t.last_updated))::TIMESTAMP AS lodate,
                       MAX(cp.access_time) AS hidate
                  FROM collection.urls u,
                       collection.url_sources t,
                       collection.captured_pages cp
                 WHERE u.src = t.id AND u.url = cp.url AND u.url = %s
            """, (urlid,))
            return (yield from cur.fetchone())

    @asyncio.coroutine
    def load_page_availability(self, archive, urlid):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "SELECT snapshots FROM collection.historical_page_availability"
                " WHERE archive = %s AND url = %s",
                (archive, urlid))

            row = yield from cur.fetchone()
            if row and row[0]: return row[0]
            return []

    @asyncio.coroutine
    def record_page_availability(self, archive, urlid, snapshots):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "INSERT INTO collection.historical_page_availability"
                " VALUES (%s, %s, %s)", (archive, urlid, snapshots))

    @asyncio.coroutine
    def load_page_topics(self, archive, urlid):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "SELECT archive_time, topic_tag"
                "  FROM historical_pages"
                " WHERE url = %s AND archive = %s"
                " ORDER BY archive_time",
                (urlid, archive))
            return {
                atime: topic_tag
                for atime, topic_tag in (
                    yield from cur.fetchall())
                if topic_tag is not None
            }

    @asyncio.coroutine
    def load_page_texts(self, trans, archive, urlid):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "SELECT h.archive_time, ep.segmented"
                "  FROM collection.historical_pages h,"
                "       collection.capture_html_content ch,"
                "       analysis.extracted_content_ov eo,"
                "       analysis.extracted_plaintext ep"
                " WHERE h.url = %s AND h.archive = %s"
                "   AND h.html_content = ch.id"
                "   AND ch.extracted   = eo.id"
                "   AND eo.pruned_text = ep.id",
                (urlid, archive))

            # fetchall() is used despite the potential size of the result
            # so we aren't poking the database and gtrans at the same time
            texts = yield from cur.fetchall()

        # release the database lock at this point, because translate_segmented
        # may call back into the database
        translated = {}
        for atime, seg in texts:
            translated[atime] = yield from trans.translate_segmented(seg)

        return translated

    @asyncio.coroutine
    def load_contemp_capture(self, trans, urlid, access_time):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute("""
                SELECT ep.segmented
                  FROM collection.captured_pages cp,
                       collection.capture_html_content ch,
                       analysis.extracted_content_ov eo,
                       analysis.extracted_plaintext ep
                 WHERE cp.url = %s AND cp.access_time = %s
                   AND cp.html_content = ch.id
                   AND ch.extracted    = eo.id
                   AND eo.pruned_text  = ep.id
                 LIMIT 1
            """, (urlid, access_time))
            segmented = (yield from cur.fetchone())[0]

        # release the database lock at this point, because translate_segmented
        # may call back into the database
        return (yield from trans.translate_segmented(segmented))

    @asyncio.coroutine
    def record_historical_page(self, archive, date, ec):
        with (yield from self.dblock):
            cur = self.cur
            docid, eid = yield from intern_html_content(
                cur, ec.ohash, ec.original)

            if not eid:
                cid = yield from intern_blob(
                    cur, b"analysis.extracted_plaintext", b"plaintext",
                    ec.chash, ec.content, False)
                pid = yield from intern_pruned_segmented(
                    cur, ec.phash, ec.pruned, ec.segmtd)
                hid = yield from intern_blob(
                    cur, b"analysis.extracted_headings", b"headings",
                    ec.hhash, ec.heads, True)
                lid = yield from intern_blob(
                    cur, b"analysis.extracted_urls", b"urls",
                    ec.lhash, ec.links, True)
                rid = yield from intern_blob(
                    cur, b"analysis.extracted_urls", b"urls",
                    ec.rhash, ec.rsrcs, True)
                did = yield from intern_blob(
                    cur, b"analysis.extracted_dom_stats", b"dom_stats",
                    ec.dhash, ec.domst, True)

                yield from cur.execute(
                    "INSERT INTO analysis.extracted_content_ov"
                    " (content_len, raw_text, pruned_text, links, resources,"
                    "  headings, dom_stats)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " RETURNING id",
                    (ec.olen, cid, pid, lid, rid, hid, did))
                eid = (yield from cur.fetchone())[0]

                yield from cur.execute(
                    "UPDATE collection.capture_html_content"
                    "   SET extracted = %s,"
                    "       is_parked = %s,"
                    "       parking_rules_matched = %s"
                    " WHERE id = %s",
                    (eid, ec.parked, ec.prules, docid))

            uid, _ = yield from add_url_string(cur, ec.url)
            if ec.redir_url == ec.url:
                ruid = uid
            else:
                ruid, _ = yield from add_url_string(cur, ec.redir_url)

            sid = yield from add_http_status(cur, ec.status, ec.reason)

            yield from cur.execute(
                "INSERT INTO collection.historical_pages"
                " (url, archive, archive_time, result, redir_url,"
                "  html_content, is_parked)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (uid, archive, date, sid, ruid, docid, ec.parked))

    @asyncio.coroutine
    def record_historical_page_topic(self, archive, date, urlid, topic):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "UPDATE collection.historical_pages"
                "   SET topic_tag = %s"
                " WHERE archive = %s AND archive_time = %s AND url = %s",
                (topic, archive, date, urlid))

    @asyncio.coroutine
    def get_unprocessed_pages(self, session):
        status("counting completely unprocessed pages...")
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "    SELECT DISTINCT u.url, s.url"
                "      FROM collection.urls u"
                "      JOIN collection.url_strings s ON u.url = s.id"
                " LEFT JOIN collection.historical_page_availability h"
                "        ON h.archive = %s AND u.url = h.url"
                "     WHERE h.url IS NULL",
                (session.archive,))
            rv = []
            while True:
                block = yield from cur.fetchmany()
                status("counting completely unprocessed pages... {}"
                       .format(len(rv)), done = (not block))
                if not block:
                    break
                rv.extend(Document(session, urlid, url)
                          for urlid, url in block)
            return rv

    @asyncio.coroutine
    def get_incomplete_pages(self, session):
        with (yield from self.dblock):
            cur = self.cur

            status("counting partially processed pages...")
            yield from cur.execute("""
                SELECT _.urlid, s.url, _.lodate, _.hidate, _.snapshots
                 FROM (
                  SELECT u.url AS urlid,
                         MIN(COALESCE(
                             SUBSTRING(u.meta->>'timestamp' FOR 10)::DATE,
                             (u.meta->>'date')::DATE,
                             t.last_updated))::TIMESTAMP AS lodate,
                         MAX(cp.access_time) AS hidate,
                         h.snapshots
                   FROM collection.urls u,
                        collection.url_sources t,
                        collection.captured_pages cp,
                        collection.historical_page_availability h
                  WHERE u.src = t.id AND u.url = cp.url AND u.url = h.url
                        AND h.archive = %s
                  GROUP BY u.url, h.snapshots
                  ) _,
                  collection.url_strings s
                 WHERE _.urlid = s.id;
                """,
                (session.archive,))

            all_documents = []
            while True:
                block = yield from cur.fetchmany()
                status("counting partially unprocessed pages... {}"
                       .format(len(all_documents)),
                       done=(not block))
                if not block:
                    break
                for urlid, url, lodate, hidate, snapshots in block:
                    snapshots.sort()
                    all_documents.append(Document(session, urlid, url,
                                                  snapshots, lodate, hidate))

            n = 0
            incomplete_documents = []
            for doc in all_documents:
                not_captured = set(doc.snapshots)

                yield from cur.execute(
                    "SELECT archive_time, topic_tag"
                    "  FROM historical_pages"
                    " WHERE url = %s AND archive = %s"
                    " ORDER BY archive_time",
                    (doc.urlid, session.archive))

                captured_dates = []
                for cdate, topic_tag in (yield from cur.fetchall()):
                    if topic_tag is not None:
                        captured_dates.append(cdate)
                        doc.topics[cdate] = topic_tag
                        not_captured.discard(cdate)

                if not_captured:
                    lo = fuzzy_year_range_lo(doc.lodate)
                    hi = fuzzy_year_range_hi(doc.hidate)
                    for cdate in sorted(not_captured):
                        # We do not need to retrieve snapshots outside the range
                        # we care about.
                        if not (lo <= cdate <= hi):
                            not_captured.remove(cdate)
                            continue

                        # We do not need to retrieve a snapshot if the
                        # topic_tag from the snapshots we do have on
                        # either side of it are the same.
                        p = bisect.bisect_left(captured_dates, cdate)
                        if p > 0 and p < len(captured_dates) and \
                           (doc.topics[captured_dates[p-1]] ==
                            doc.topics[captured_dates[p]]):
                            not_captured.remove(cdate)

                if not_captured:
                    incomplete_documents.append(doc)

                n += 1
                if n % 1000 == 0:
                    status("weeding partially unprocessed pages... {}/{}"
                           .format(n, len(incomplete_documents)))

            status("weeding partially unprocessed pages... {}/{}"
                   .format(n, len(incomplete_documents)), done=True)
            return incomplete_documents

#
# Interacting with the Wayback Machine
#

# This chunk of the work is CPU-bound and farmed out to worker
# processes.  We must use processes and not threads because of the
# GIL, and unfortunately that means we have to pass all the data back
# and forth in bare tuples.

EC = collections.namedtuple("EC",
                            ("url", "redir_url", "status", "reason",
                             "ohash", "olen", "original",
                             "chash", "content",
                             "phash", "pruned", "segmtd",
                             "hhash", "heads",
                             "lhash", "links",
                             "rhash", "rsrcs",
                             "dhash", "domst",
                             "parked", "prules"))

# Expensive initialization needed only in worker processes.
extract_page_context = None
def extract_page_context_init():
    import cld2
    import domainparking
    import html_extractor
    import word_seg

    global extract_page_context
    extract_page_context = (
        cld2, html_extractor, word_seg, domainparking.ParkingClassifier())

def extract_page(url, redir_url, status, reason, ctype, data):
    """Worker-process procedure: extract content from a page retrieved
       from the Internet Archive.
    """
    global extract_page_context
    if extract_page_context is None:
        extract_page_context_init()

    cld2, html_extractor, word_seg, parking_cfr = extract_page_context

    ctype, options = parse_options_header(ctype)
    charset = options.get("charset", "")

    extr = html_extractor.ExtractedContent(redir_url, data, ctype, charset)
    lang = cld2.detect(extr.text_pruned, want_chunks=True)
    segmented = [ { "l": c[0].code,
                    "t": list(word_seg.segment(c[0].code, c[1])) }
                  for c in lang.chunks ]

    original = zlib.compress(extr.original)
    olen     = len(extr.original)
    ohash    = hashlib.sha256(original).digest()
    content  = extr.text_content.encode("utf-8")
    chash    = hashlib.sha256(content).digest()
    pruned   = extr.text_pruned.encode("utf-8")
    phash    = hashlib.sha256(pruned).digest()
    segmtd   = json.dumps(segmented).encode("utf-8")
    heads    = json.dumps(extr.headings).encode("utf-8")
    hhash    = hashlib.sha256(heads).digest()
    links    = json.dumps(extr.links).encode("utf-8")
    lhash    = hashlib.sha256(links).digest()
    rsrcs    = json.dumps(extr.resources).encode("utf-8")
    rhash    = hashlib.sha256(rsrcs).digest()
    domst    = json.dumps(extr.dom_stats.to_json()).encode("utf-8")
    dhash    = hashlib.sha256(domst).digest()

    parked, prules = parking_cfr.isParked(extr.original.decode("utf-8"))

    return EC(url, redir_url, status, reason,
              ohash, olen, original,
              chash, content,
              phash, pruned, segmtd,
              hhash, heads,
              lhash, links,
              rhash, rsrcs,
              dhash, domst,
              parked, prules)

class WaybackMachine:
    def __init__(self, executor, http_client, rate, loop=None):
        self.executor    = executor
        self.http_client = http_client
        self.rate        = rate
        self.loop        = loop or asyncio.get_event_loop()
        self.errlog      = open("wayback-machine-errors.log", "at")
        self.n_errors    = 0
        self.n_requests  = 0
        self.session     = None

    def __enter__(self):
        return self

    def __exit__(self, *dontcare):
        self.errlog.close()

    @asyncio.coroutine
    def get_unique_snapshots_of_url(self, url):
        """Retrieve a list of all available snapshots of URL."""
        backoff = 1
        while True:
            yield from self.rate
            resp = None
            try:
                self.n_requests += 1
                resp = yield from self.http_client.get(
                    "https://web.archive.org/cdx/search/cdx",
                    params = { "url": url,
                               "collapse": "digest",
                               "fl": "original,timestamp,statuscode" })
                if resp.status == 200:
                    text = yield from resp.text()
                    yield from resp.release()
                    break

                if resp.status == 403:
                    # We get this when the Machine has snapshots but can't
                    # show them to us because of robots.txt.
                    self.errlog.write("GET /cdx/search/cdx?{} = {} {}\n"
                                      .format(url, resp.status, resp.reason))
                    yield from resp.release()
                    return []

                if resp.status != 503:
                    self.errlog.write("GET /cdx/search/cdx?{} = {} {}\n"
                                      .format(url, resp.status, resp.reason))
                    self.errlog.flush()

            except Exception:
                traceback.print_exc(file=self.errlog)
                self.errlog.flush()

            if resp is not None:
                try:
                    yield from resp.release()
                except Exception:
                    resp.close()

            self.n_errors += 1
            self.session.progress()
            yield from asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

        self.session.progress()
        snapshots = []
        for line in text.split("\n"):
            if not line: continue
            try:
                url_r, timestamp, statuscode = line.split()
                # This API *does not* guarantee that you get back out
                # exactly the URL you put in.  In particular, there
                # appears to be no way to make it pay attention to the scheme.
                if url_r == url and statuscode in ("200","301","302",
                                                   "303","307","308"):
                    snapshots.append(datetime.datetime.strptime(
                        timestamp, "%Y%m%d%H%M%S"))
            except:
                self.errlog.write("CDX parse error for {}:\n\t{}\n"
                                  .format(url, line))
                self.errlog.flush()

        return snapshots

    @asyncio.coroutine
    def get_page_http_request(self, query):
        backoff = 1
        while True:
            resp = None
            try:
                yield from self.rate
                self.n_requests += 1
                resp = yield from \
                    self.http_client.get(query, allow_redirects=False)
                if 300 <= resp.status <= 399:
                    location = resp.headers.get('location', '')
                    ctype = None
                    data = None
                else:
                    location = None
                    ctype = resp.headers.get("content-type", "")
                    # Helpfully, the Wayback Machine returns the
                    # page in its _original_ character encoding.
                    # aiohttp does not implement HTML5 encoding
                    # detection, so read the data in binary mode
                    # to avoid problems.
                    try:
                        data = yield from resp.read()
                    except (zlib.error,
                            aiohttp.errors.ContentEncodingError,
                            aiohttp.errors.ServerDisconnectedError):
                        # The Wayback Machine faithfully records and
                        # plays back malformed HTTP responses!  Treat
                        # this as an empty document, and close the
                        # connection to avoid further problems.
                        data = b""
                        resp.close()

                try:
                    # This can barf on a malformed HTTP response
                    # even if read() has already succeeded.  Do not
                    # discard the data in this case.
                    yield from resp.release()
                except (zlib.error,
                        aiohttp.errors.ContentEncodingError,
                        aiohttp.errors.ServerDisconnectedError):
                    resp.close()

                self.session.progress()
                return (resp.status, resp.reason, location, ctype, data)

            except Exception as e:
                self.errlog.write("In query: {}\n".format(query))
                traceback.print_exc(file=self.errlog)
                self.errlog.write("\n")
                self.errlog.flush()

                if resp is not None:
                    try:
                        yield from resp.release()
                    except Exception:
                        resp.close()

                self.n_errors += 1
                self.session.progress()
                yield from asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    @asyncio.coroutine
    def get_page_at_time(self, url, snap):
        """Retrieve URL as of SNAP, which must be an entry in the list
           returned by get_unique_snapshots_of_url (above).  The
           return value is an ExtractedContent object.
        """

        # The undocumented "id_" token is how you get the Wayback
        # Machine to spit out the page *without* its usual
        # modifications (rewriting links and adding a toolbar).  The
        # URL must *not* be quoted.
        snap = snap.strftime("%Y%m%d%H%M%S")
        query = "https://web.archive.org/web/{}id_/{}".format(snap, url)
        # We have to do manual redirection handling.
        redir_url = url
        redirections = 0
        while redirections < 20:
            (status, reason, loc, ctype, data) = \
                yield from self.get_page_http_request(query)

            if data is not None: break

            redirections += 1

            # Redirections can happen either because the original
            # page gave the Wayback Machine a redirection, or
            # because the snapshot date is off by a few (this only
            # happens after the first case happens).  In the
            # former case we will be redirected _to the site
            # itself_, and we need to update redir_url; in the
            # latter case the redirection stays inside the WBM,
            # and we _don't_ want to update redir_url.
            if loc.startswith('/web/'):
                query = "https://web.archive.org" + loc
            elif loc.startswith('https://web.archive.org/'):
                query = loc
            elif (loc.startswith('http://') or
                  loc.startswith('https://')):
                redir_url = canon_url_syntax(loc)
                query = ("https://web.archive.org/web/{}id_/{}"
                         .format(snap, redir_url))
            else:
                redir_url = canon_url_syntax(
                    urllib.parse.urljoin(redir_url, loc))
                query = ("https://web.archive.org/web/{}id_/{}"
                         .format(snap, redir_url))

        # We can get here with data=None if there is a redirection loop.
        if data is None:
            data = b''

        # html_extractor _does_ implement HTML5 encoding detection.  This
        # stage is CPU-bound and pushed to a worker process.
        return (yield from self.loop.run_in_executor(
            self.executor, extract_page,
            url, redir_url, status, reason, ctype, data))

#
# Translation of unknown words.
# aiohttp-ified version of
# http://thomassileo.com/blog/2012/03/26/using-google-translation-api-v2-with-python/
#

with open(os.path.join(os.environ["HOME"], ".google-api-key"), "rt") as f:
    GOOGLE_API_KEY = f.read().strip()

# Map CLD2's names to Google Translate's names.  Currently there's only one
# point of disagreement.
CLD2_TO_GOOGLE = default_identity_dict({
    "zh-Hant" : "zh-TW"
})
GOOGLE_TO_CLD2 = default_identity_dict({
    "zh-TW" : "zh-Hant"
})

# Maximum number of characters per POST request.  The documentation is
# a little vague about exactly how you structure this, but I *think*
# it means to say that if you use POST then you don't have to count
# the other parameters and repeated &q= constructs toward the limit.
CHARS_PER_POST = 5000

# There is also a completely undocumented limit of 128 q= segments per
# translation request.
WORDS_PER_POST = 128

TRANSLATE_URL = \
    "https://www.googleapis.com/language/translate/v2"
GET_LANGUAGES_URL = \
    "https://www.googleapis.com/language/translate/v2/languages"

class GoogleTranslate:
    def __init__(self, db, http_client, rate, loop=None):
        self.db           = db
        self.http_client  = http_client
        self.rate         = rate
        self.loop         = loop or asyncio.get_event_loop()
        self.errlog       = open("google-translate-errors.log", "at")
        self.n_errors     = 0
        self.n_requests   = 0
        self.langs        = None
        self.translations = None
        self.prepare_lock = asyncio.Lock(loop=self.loop)
        self.tbufs        = {}
        self.session      = None

    def __enter__(self):
        return self

    def __exit__(self, *dontcare):
        self.loop.run_until_complete(
            self.loop.create_task(
                self.drain_translations()))
        self.errlog.close()
        return False

    @asyncio.coroutine
    def prepare(self):
        # Many coroutines may call this simultaneously.  Only load
        # translatable languages and old translations once.
        with (yield from self.prepare_lock):
            if self.langs is not None: return

            # Load the translations _first_, because the moment we
            # assign to .langs, translate_segmented will think we're
            # done.
            self.translations = (yield from self.db.get_translations())

            yield from self.rate
            self.n_requests += 1
            resp = yield from self.http_client.get(
                GET_LANGUAGES_URL,
                params = { "key" : GOOGLE_API_KEY })
            blob = yield from resp.json()
            yield from resp.release()
            # Don't bother translating English into English.
            self.langs = \
                frozenset(GOOGLE_TO_CLD2[x["language"]]
                          for x in blob["data"]["languages"]
                          if x["language"] != "en")


    @asyncio.coroutine
    def get_translations_internal(self, lang, words):
        backoff = 5
        while True:
            resp = None
            try:
                blob = None
                yield from self.rate
                self.n_requests += 1
                resp = yield from self.http_client.post(
                    TRANSLATE_URL,
                    data = {
                        "key":    GOOGLE_API_KEY,
                        "source": CLD2_TO_GOOGLE[lang],
                        "target": "en",
                        "q":      words,
                    },
                    headers = {
                        "Content-Type":
                            "application/x-www-form-urlencoded;charset=utf-8",
                        "X-HTTP-Method-Override": "GET",
                    })
                if resp.status != 200:
                    # 503 and 403 seem to be used interchangeably by
                    # google translate with the meaning "slow down a little"
                    if resp.status != 503 and resp.status != 403:
                        self.errlog.write(
                            "POST /language/translate/v2 = {} {}\n"
                            .format(resp.status, resp.reason))
                        self.errlog.write("  source: {}\n"
                                          "  target: en\n"
                                          "  q:      {!r}\n\n"
                                          .format(CLD2_TO_GOOGLE[lang], words))
                        self.errlog.flush()
                        text = yield from resp.text()
                        self.errlog.write(text)
                        self.errlog.write("\n\n")

                    self.errlog.flush()
                    try:
                        yield from resp.release()
                    except Exception:
                        resp.close()

                    self.n_errors += 1
                    self.session.progress()
                    yield from asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                blob = yield from resp.json()
                yield from resp.release()
                self.session.progress()
                return list(zip(words,
                                (unicodedata.normalize(
                                    "NFKC", x["translatedText"]).casefold()
                                 for x in blob["data"]["translations"])))

            except Exception:
                traceback.print_exc(file=self.errlog)
                try:
                    self.errlog.write("\n")
                    if blob is not None:
                        json.dump(blob, self.errlog)
                    elif resp is not None:
                        text = yield from resp.text()
                        self.errlog.write(text)
                except Exception:
                    self.errlog.write("\nWhile dumping response:\n")
                    traceback.print_exc(file=self.errlog)

                if resp is not None:
                    try:
                        yield from resp.release()
                    except Exception:
                        resp.close()

                self.n_errors += 1
                self.session.progress()
                yield from asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

    @asyncio.coroutine
    def get_translations_worker(self, batch, *, lang):
        # The same word might have been requested more than once
        # (from different documents); we must make sure to satisfy all
        # futures waiting for each word.
        # Re-check whether each word has been translated already, because
        # we could've received yet more requests for a word while we were
        # waiting for its translation to come back.

        sleepers = collections.defaultdict(list)
        for word, fut in batch:
            engl = self.translations[lang].get(word)
            if engl is not None:
                fut.set_result(engl)
            else:
                sleepers[word].append(fut)
        to_translate = sorted(sleepers.keys())

        translations = yield from self.get_translations_internal(
            lang, to_translate)

        for word, engl in translations:
            self.translations[lang][word] = engl
            for fut in sleepers[word]:
                fut.set_result(engl)

        yield from self.db.record_translations(lang, translations)

    @asyncio.coroutine
    def translate_segmented(self, segmented):
        if self.langs is None:
            yield from self.prepare()

        words_seen = {}
        translation = []
        sleepers = []
        languages = set()

        for chunk in segmented:
            lang = chunk['l']
            languages.add(lang)
            words = chunk['t']
            tdict = self.translations[lang]
            for word in words:
                key = (lang, word)
                if key in words_seen:
                    trans = words_seen[key]
                    translation.append(trans)

                elif word in tdict:
                    # this one has already been translated
                    trans = " ".join(tdict[word].split())
                    words_seen[key] = trans
                    translation.append(trans)

                elif lang not in self.langs:
                    # untranslatable, return as is
                    trans = " ".join(
                        unicodedata.normalize("NFKC", word).casefold().split())
                    tdict[word] = trans
                    words_seen[key] = trans
                    translation.append(trans)

                else:
                    if lang not in self.tbufs:
                        self.tbufs[lang] = work_buffer(
                            self.get_translations_worker,
                            WORDS_PER_POST,
                            label="gtrans-"+lang,
                            loop=self.loop,
                            lang=lang)
                    fut = self.tbufs[lang].put(word)
                    words_seen[key] = fut
                    sleepers.append(fut)
                    translation.append(fut)

        if sleepers:
            yield from asyncio.wait(sleepers, loop=self.loop)
            for i in range(len(translation)):
                f = translation[i]
                if not isinstance(f, str):
                    translation[i] = " ".join(f.result().split())

        # The format expected by the topic analyzer is a space-separated
        # set of language codes, followed by a comma, followed by a
        # space-separated list of words to end of line.
        # The " ".join(xyz.split()) stuff above ensures that no other
        # forms of whitespace sneak in.
        return (" ".join(sorted(languages)) + "," +
                " ".join(translation))

    def flush_translations(self):
        for wb in self.tbufs.values(): wb.flush()

    @asyncio.coroutine
    def drain_translations(self):
        sleepers = [wb.drain() for wb in self.tbufs.values()]
        if sleepers:
            yield from asyncio.wait(sleepers, loop=self.loop)

#
# The topic-analysis subprocess
#

class TopicAnalyzer:
    def __init__(self, analyzer, loop=None):
        self.analyzer   = analyzer
        self.loop       = loop or asyncio.get_event_loop()
        self.wbuffer    = work_buffer(self._process_topic_batch, 100,
                                      label="topic", 
                                      loop=self.loop)
        self.exit_evt   = asyncio.Event(loop=self.loop)
        self.ready_evt  = asyncio.Event(loop=self.loop)
        self.proc_t     = None
        self.proc_p     = None
        self.n_pending  = 0
        self.n_requests = 0
        self.session    = None

    def __enter__(self):
        self.loop.run_until_complete(
            self.loop.create_task(
                self.start()))
        return self

    def __exit__(self, *args):
        self.loop.run_until_complete(
            self.loop.create_task(
                self.wbuffer.drain()))
        self.loop.run_until_complete(
            self.loop.create_task(
                self.stop()))
        return False

    @asyncio.coroutine
    def start(self):
        self.proc_t, self.proc_p = yield from self.loop.subprocess_exec(
            lambda: TopicAnalyzer.TAProtocol(self.exit_evt, self.ready_evt),
            self.analyzer,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=2)
        yield from self.ready_evt.wait()

    @asyncio.coroutine
    def stop(self):
        if self.proc_p is not None:
            self.proc_p.stop()
            yield from self.exit_evt.wait()

            self.ready_evt.clear()
            self.exit_evt.clear()
            self.proc_t = None
            self.proc_p = None

    @asyncio.coroutine
    def is_same_topic(self, a, b):
        if isinstance(a, asyncio.Future):
            a = yield from a
        if isinstance(b, asyncio.Future):
            b = yield from b

        try:
            assert isinstance(a, str)
            assert isinstance(b, str)
            assert a != ''
            assert b != ''
            assert a == ',' or (a[0] != ',' and a[-1] != ',')
            assert b == ',' or (b[0] != ',' and b[-1] != ',')
        except AssertionError:
            self.session.errlog.write(
                "bad arguments to is_same_topic:\na={!r}\nb={!r}\n\n"
                .format(a, b))
            self.session.errlog.flush()
            self.session.n_errors += 1
            self.session.progress()
            return False

        self.n_requests += 1

        # An empty document appears here as ','.  It should be
        # considered the "same topic" as another empty document,
        # but not the same topic as any nonempty document.
        if a == ',':
            return b == ','
        if b == ',':
            return False

        self.n_pending += 1
        self.session.progress()
        return (yield from self.wbuffer.put((a, b)))

    def flush(self):
        self.wbuffer.flush()

    @asyncio.coroutine
    def _process_topic_batch(self, batch):
        yield from self.ready_evt.wait()

        with tempfile.NamedTemporaryFile(
                mode="w+t", encoding="utf-8",
                suffix=".txt", prefix="topics.") as ifp:
            iname = ifp.name
            rname = iname + ".result"

            for (a, b), _ in batch:
                ifp.write(a)
                ifp.write("\n")
                ifp.write(b)
                ifp.write("\n")

            ifp.flush()
            try:
                fut = asyncio.Future(loop=self.loop)
                self.proc_p.post_batch(iname, fut)
                yield from fut

                try:
                    with open(rname, "rt") as rfp:
                        for (_, fut), val in zip(batch, rfp):
                            self.n_pending -= 1
                            fut.set_result(int(val) != 0)

                except OSError:
                    # Sometimes we go look for the result file and it isn't
                    # there.  If this happens, retry.
                    yield from self._process_topic_batch(batch)

            finally:
                try:
                    os.remove(rname)
                except FileNotFoundError:
                    pass

        self.session.progress()

    class TAProtocol(asyncio.SubprocessProtocol):
        def __init__(self, exit_evt, ready_evt):
            self.exit_evt  = exit_evt
            self.ready_evt = ready_evt
            self.transport = None
            self.stdin     = None
            self.pending   = collections.deque()
            self.stopping  = False

        # Called by TopicAnalyzer
        def post_batch(self, iname, fut):
            assert not self.stopping
            assert self.transport is not None
            self.pending.appendleft(fut)
            self.stdin.write(iname.encode("utf-8") + b"\n")

        def stop(self):
            assert not self.stopping
            assert self.transport is not None
            self.stdin.write(b"quit\n")
            self.stdin.write_eof()
            self.stopping = True

        # Called by transport
        def connection_made(self, transport):
            self.transport = transport
            self.stdin     = transport.get_pipe_transport(0)
            self.ready_evt.set()

        def pipe_data_received(self, fd, data):
            assert fd == 1
            for c in data:
                assert c == 0x0A # '\n'
                self.pending.pop().set_result(None)

        def pipe_connection_lost(self, fd, exc):
            assert self.stopping
            assert exc is None
            assert fd in (0, 1)
            if fd == 1:
                assert len(self.pending) == 0

        def process_exited(self):
            self.exit_evt.set()
            assert self.stopping
            assert self.transport.get_returncode() == 0
            self.transport.close()

#
# Core per-document data structure
#

class Document:
    def __init__(self, session, urlid, url,
                 snapshots=None, lodate=None, hidate=None):
        self.session   = session
        self.urlid     = urlid
        self.url       = url
        self.snapshots = snapshots # dates of available snapshots
        self.lodate    = lodate    # date when flagged by the source
        self.hidate    = hidate    # date when retrieved by our crawler
        self.topics    = {}        # map snapshot_date : topic
        self.texts     = {}        # map snapshot_date : text
        self.ntopics   = None

    def topic_symbol(self):
        K = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        C = "123456"
        s = []
        for d in self.snapshots:
            t = self.topics.get(d, -1)
            if t == -1: t = "."
            elif t < len(K):
                t = K[t]
            elif t < len(K)*len(C):
                q,r = divmod(t, len(K))
                t = "\x1b[3" + C[q-1] + "m" + K[r] + "\x1b[0m"
            else: t = "^"
            s.append(t)
        return "".join(s)

    @asyncio.coroutine
    def load_history(self):
        if not self.lodate:
            self.lodate, self.hidate = yield from \
                self.session.db.load_date_range_for_url(self.urlid)

        if not self.snapshots:
            self.snapshots = yield from \
                self.session.db.load_page_availability(
                    self.session.archive, self.urlid)

        self.topics.update(
            (yield from self.session.db.load_page_topics(
                self.session.archive, self.urlid)))

        self.texts.update(
            (yield from self.session.db.load_page_texts(
                self.session.gtrans, self.session.archive, self.urlid)))

        hi_text = yield from \
            self.session.db.load_contemp_capture(
                self.session.gtrans, self.urlid, self.hidate)
        # the page captured at hidate has topic tag 0 by definition
        self.topics[self.hidate] = 0
        self.texts[self.hidate] = hi_text

        self.ntopics = max(self.topics.values()) + 1

    @asyncio.coroutine
    def retrieve_history(self):
        yield from self.load_history()

        if not self.snapshots:
            self.snapshots = yield from \
                self.session.wayback.get_unique_snapshots_of_url(self.url)
            yield from self.session.db.record_page_availability(
                self.session.archive, self.urlid, self.snapshots)

            self.session.note_have_snapshots()

        self.snapshots.append(self.hidate)
        self.snapshots.sort()

        # Phase 1: retrieve snapshots at one-year intervals, starting
        # just before hidate and going back in time until a little before
        # lodate (see fuzzy_year_range_backward).

        prev_date = self.hidate
        for date in fuzzy_year_range_backward(self.lodate, self.hidate):
            date = find_le(self.snapshots, date)
            if date is not None and date != prev_date:
                self.check_topic(date, prev_date)
                prev_date = date

        sleepers = [v for v in self.topics.values()
                    if isinstance(v, asyncio.Future)]

        if sleepers:
            yield from asyncio.wait(sleepers)

        # Phase 2: determine the ranges of time over which the page
        # had the same topic, and fine-tune the boundaries by
        # retrieving more snapshots.
        dates = sorted(self.topics.keys(), reverse=True)
        changes = []
        for cur, prev in pairwise(dates):
            tcur  = self.topics[cur]
            tprev = self.topics[prev]
            if isinstance(tcur, asyncio.Future):
                tcur = yield from tcur
                assert not isinstance(tcur, asyncio.Future)
            if isinstance(tprev, asyncio.Future):
                tprev = yield from tprev
                assert not isinstance(tprev, asyncio.Future)

            if tcur != tprev:
                changes.append((cur, prev))

        while changes:
            cur, prev = changes.pop()
            # The topic changed somewhere between CUR and PREV.
            # Narrow down the date by bisection.
            lo = bisect.bisect_right(self.snapshots, prev)
            hi = bisect.bisect_left(self.snapshots, cur)
            if lo < hi:
                date = self.snapshots[lo + (hi - lo)//2]
                assert prev < date < cur
                fut = self.check_topic(date, prev, cur)
                if isinstance(fut, asyncio.Future): yield from fut

                if self.topics[date] != self.topics[prev]:
                    changes.append((date, prev))
                if self.topics[date] != self.topics[cur]:
                    changes.append((cur, date))

        status("{}:  {}".format(self.url, self.topic_symbol()), done=True)

    def check_topic(self, target, *others):
        if target in self.topics:
            return self.topics[target]
        task = self.session.loop.create_task(
            self.check_topic_internal(target, *others))
        self.topics[target] = task
        return task

    @asyncio.coroutine
    def check_topic_internal(self, *dates):
        loop = self.session.loop
        sleepers = []
        for d in dates:
            txt = self.retrieve_snapshot(d)
            if isinstance(txt, asyncio.Future):
                sleepers.append(txt)
        if sleepers:
            yield from asyncio.wait(sleepers)

        target = dates[0]
        comparisons = []
        for i in range(1, len(dates)):
            comparisons.append(self.session.topic_analyzer.is_same_topic(
                self.texts[target], self.texts[dates[i]]))
        results = yield from asyncio.gather(*comparisons)

        for i in range(1, len(dates)):
            if results[i-1]:
                if isinstance(self.topics[dates[i]], asyncio.Future):
                    yield from self.topics[dates[i]]
                    assert not isinstance(self.topics[dates[i]], asyncio.Future)

                self.topics[target] = self.topics[dates[i]]
                break
        else:
            self.topics[target] = self.ntopics
            self.ntopics += 1

        yield from self.session.db.record_historical_page_topic(
            self.session.archive, target, self.urlid, self.topics[target])

        return self.topics[target]

    def retrieve_snapshot(self, date):
        if date in self.texts:
            return self.texts[date]
        task = self.session.loop.create_task(
            self.retrieve_snapshot_internal(date))
        self.texts[date] = task
        return task

    @asyncio.coroutine
    def retrieve_snapshot_internal(self, date):
        try:
            S = self.session
            ec = yield from S.wayback.get_page_at_time(self.url, date)
            yield from S.db.record_historical_page(S.archive, date, ec)
            self.texts[date] = yield from S.gtrans.translate_segmented(
                json.loads(ec.segmtd.decode("utf-8")))
            return self.texts[date]
        except Exception:
            traceback.print_exc(file=self.session.errlog)
            self.session.errlog.write("\n")
            self.session.errlog.flush()
            self.session.n_errors += 1
            self.session.progress()
            self.texts[date] = ','
            return ','


#
# Master control
#

class HistoryRetrievalSession:
    """Container for all the things that are set up in main().
       This is mostly to avoid passing six arguments around all the time.
    """
    def __init__(self, archive, db,
                 wayback, gtrans, topic_analyzer, loop):
        self.loop           = loop
        self.topic_analyzer = topic_analyzer
        self.gtrans         = gtrans
        self.wayback        = wayback
        self.db             = db
        self.archive        = archive

        self.topic_analyzer.session = self
        self.gtrans.session = self
        self.wayback.session = self

    def __enter__(self):
        self.errlog         = open("history-retrieval-errors.log", "at")
        return self

    def __exit__(self, *dontcare):
        self.errlog.close()

    @asyncio.coroutine
    def get_documents_to_process(self):
        status("loading documents to process...")
        docs_unprocessed = yield from self.db.get_unprocessed_pages(self)
        status("loading documents to process: {} unprocessed..."
               .format(len(docs_unprocessed)))

        docs_incomplete = yield from self.db.get_incomplete_pages(self)
        status("loading documents to process: {} unprocessed, {} incomplete"
               .format(len(docs_unprocessed), len(docs_incomplete)),
               done=True)

        return docs_unprocessed, docs_incomplete

    def progress(self, message="", done=False):
        if message and message != ".":
            message = "; " + message

        status("{} unprocessed, {} incomplete, {} complete, {} errors; "
               "wb {}e/{}r tr {}e/{}r ta {}p/{}r{}"
               .format(self.n_unprocessed, self.n_incomplete,
                       self.n_complete, self.n_errors,
                       self.wayback.n_errors, self.wayback.n_requests,
                       self.gtrans.n_errors, self.gtrans.n_requests,
                       self.topic_analyzer.n_pending,
                       self.topic_analyzer.n_requests,
                       message),
               done)

    def note_have_snapshots(self):
        self.n_unprocessed -= 1
        self.n_incomplete += 1
        self.progress()

    @asyncio.coroutine
    def get_page_histories(self):

        docs_unprocessed, docs_incomplete = \
            yield from self.get_documents_to_process()

        self.n_unprocessed = len(docs_unprocessed)
        self.n_incomplete  = len(docs_incomplete)
        self.n_complete    = 0
        self.n_errors      = 0

        tasks = []
        for doc in docs_unprocessed:
            tasks.append(self.loop.create_task(doc.retrieve_history()))
        for doc in docs_incomplete:
            tasks.append(self.loop.create_task(doc.retrieve_history()))

        for fut in asyncio.as_completed(tasks, loop=self.loop):
            try:
                yield from fut
                self.n_incomplete -= 1
                self.n_complete += 1

            except Exception:
                traceback.print_exc(file=self.errlog)
                self.errlog.write("\n")
                self.errlog.flush()
                self.n_errors += 1

            self.progress()

        self.progress(".", done=True)

@asyncio.coroutine
def inner_main(session):
    try:
        yield from session.get_page_histories()
    except:
        traceback.print_exc()

def main(loop, argv):
    _, dbname, analyzer = argv

    # child watcher must be initialized before anything creates threads
    # everything that might spin the event loop on teardown must be a context
    # manager so it'll be torn down before the loop itself is (__del__ might
    # not run early enough, even for locals)
    with asyncio.get_child_watcher() as watcher,                          \
         aiohttp.ClientSession(
             connector=aiohttp.TCPConnector(
                 loop=loop,
                 conn_timeout=5, limit=3, use_dns_cache=True
             ),
             headers={
                 'User-Agent': 'tbbscraper/get_page_histories; zackw@cmu.edu'
             }) as http_client,                                           \
         TopicAnalyzer(analyzer, loop=loop) as topic_analyzer,            \
         concurrent.futures.ProcessPoolExecutor() as executor,            \
         rate_limiter(10, loop=loop) as wb_rate,                          \
         rate_limiter(4096, loop=loop) as gt_rate,                        \
         Database(dbname, loop, timeout=3600) as db,                      \
         WaybackMachine(executor, http_client, wb_rate, loop) as wayback, \
         GoogleTranslate(db, http_client, gt_rate, loop) as gtrans,       \
         HistoryRetrievalSession(
             "wayback", db, wayback,
             gtrans, topic_analyzer, loop) as session:

        loop.run_until_complete(loop.create_task(inner_main(session)))

def outer_main():
    try:
        # work around sloppy file descriptor hygiene in the guts of asyncio
        multiprocessing.set_start_method('spawn')

        loop = asyncio.get_event_loop()
        #loop.set_debug(True)
        #import logging
        #logging.basicConfig(level=logging.DEBUG)

        main(loop, sys.argv)

    finally:
        loop.close()

if __name__ == '__main__':
    outer_main()
