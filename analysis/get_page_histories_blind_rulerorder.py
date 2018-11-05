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
import random
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

# This is not in itertools, for no good reason.
def chunked(iterable, n):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

# The "ruler order" of a list is the order a binary search would visit
# each of its members.  Think of the heights of the tick marks on a
# ruler calibrated in inches.  Note that the output is reversed, because
# caller wants to take things off the end with pop().
def ruler_order(lst):
    if not lst: return lst
    k = len(lst) // 2
    left = lst[:k]
    right = lst[(k+1):]
    mid = [lst[k]]
    return ruler_order(lst[k+1:]) + ruler_order(lst[:k]) + [lst[k]]

def sync_wait(coro, loop):
    """Synchronously wait for CORO to return its result.  This spins the
       event loop; be careful about where you use it."""
    task = loop.create_task(coro)
    loop.run_until_complete(task)
    return task.result()

# This appears in the official documentation for asyncio, but is not
# actually defined by the library!  But it _is_ defined by the
# externally maintained backport to 3.3 (and that's where this code
# comes from).  No, I don't understand either.

try:
    aio_timeout = asyncio.timeout

except AttributeError:
    def aio_timeout(timeout, *, loop=None):
        """A factory which produce a context manager with timeout.
        Useful in cases when you want to apply timeout logic around block
        of code or in cases when asyncio.wait_for is not suitable.
        For example:
        >>> with asyncio.timeout(0.001):
        ...     yield from coro()
        timeout: timeout value in seconds
        loop: asyncio compatible event loop
        """
        if loop is None:
            loop = asyncio.get_event_loop()
        return _Timeout(timeout, loop=loop)

    class _Timeout:
        def __init__(self, timeout, *, loop):
            self._timeout = timeout
            self._loop = loop
            self._task = None
            self._cancelled = False
            self._cancel_handler = None

        def __enter__(self):
            self._task = asyncio.Task.current_task(loop=self._loop)
            if self._task is None:
                raise RuntimeError('Timeout context manager should be used '
                                   'inside a task')
            self._cancel_handler = self._loop.call_later(
                self._timeout, self._cancel_task)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is asyncio.futures.CancelledError and self._cancelled:
                self._cancel_handler = None
                self._task = None
                raise asyncio.futures.TimeoutError
            self._cancel_handler.cancel()
            self._cancel_handler = None
            self._task = None

        def _cancel_task(self):
            self._cancelled = self._task.cancel()

class Meter:
    """Unblocks a calling coroutine RATE times per second, allowing no
       more than CONCURRENCY callers to proceed simultaneously.  Each
       caller is issued a session object minted by the SESSION_FACTORY,
       and may hold onto it for no more than TIMEOUT seconds before
       being cancelled.  Session objects will be reused, round-robin,
       for up to SESSION_TIMEOUT seconds from the moment of their
       creation, after which they will be closed (close() method
       called) and new ones created.  The SESSION_FACTORY is expected to
       be a callable taking no arguments.

       Usage example:

       meter = Meter(rate=10, concurrency=3, timeout=60,
                     session_factory=make_session, session_timeout=15*60)
       while work_to_do:
           with (yield from rate) as s:
               ...do something with s...

    """
    def __init__(self, *, rate, concurrency, timeout,
                 session_factory, session_timeout,
                 loop=None):
        self.rate        = rate
        self.interval    = 1.0/rate
        self.concurrency = concurrency
        self.timeout     = timeout
        self.stimeout    = session_timeout
        self.sfactory    = session_factory
        self.loop        = loop or asyncio.get_event_loop()
        self.sessions    = collections.deque()
        self.waiters     = asyncio.BoundedSemaphore(loop=self.loop,
                                                    value=self.concurrency)
        self.last_tick   = self.loop.time()

    class session_wrapper:
        def __init__(self, meter):
            self._in_use  = False
            self._closed  = False
            self._closing = False
            self._meter   = meter
            self._loop    = meter.loop
            self._sess    = meter.sfactory()
            self._timer   = aio_timeout(meter.timeout, loop=self._loop)
            self._stimer  = self._loop.call_later(meter.stimeout, self.close)

        def __enter__(self):
            self._in_use = True
            self._timer.__enter__()
            return self._sess

        def __exit__(self, *args):
            self._in_use = False
            if self._closing:
                self._closed = True
                self._sess.close()
                self._meter._release_session(None)
            else:
                self._meter._release_session(self)

            # Note: this may raise its own exception, so it must be
            # done after all cleanup operations.
            return self._timer.__exit__(*args)

        def close(self):
            if self._closed: return

            self._closing = True
            if self._stimer:
                self._stimer.cancel()
                self._stimer = None

            # If this gets called while in use, do not close the
            # session out from under the user.
            if not self._in_use:
                self._closed = True
                self._sess.close()
                self._meter._discard_session(self)

        __del__ = close

    def _discard_session(self, sess):
        try:
            self.sessions.remove(self)
        except ValueError:
            pass

    def _release_session(self, sess):
        if sess is not None:
            self.sessions.append(sess)
        self.waiters.release()

    @asyncio.coroutine
    def __iter__(self):
        yield from self.waiters.acquire()

        now = self.loop.time()
        delay = self.interval - (now - self.last_tick)
        if delay > 0:
            yield from asyncio.sleep(delay, loop=self.loop)
            now = self.loop.time()
        self.last_tick = now

        if self.sessions:
            return self.sessions.popleft()

        return self.session_wrapper(self)

class work_buffer:
    """Buffer up work until there is enough of it, or till a timeout
       expires (default 5 seconds), then process it all at once.

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
            if self.ftimer is None:
                self.ftimer = self.loop.call_later(self.ftimeout, self.flush)

        return fut

    def flush(self):
        """Queue the current batch for processing and start a new one.
           Does not wait for completion.
        """
        batch = self.batch
        self.batch = []

        if self.ftimer is not None:
            self.ftimer.cancel()
            self.ftimer = None

        # Adjust the flush timeout downward if there are only a few
        # things in the batch; toward the end of a job, we shouldn't
        # be wasting a lot of time waiting for more to come in.  There
        # is a hard floor of 100ms.
        if len(batch) < self.jobsize/2:
            self.ftimeout = max(self.ftimeout/2, 0.1)

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

ONE_YEAR    = datetime.timedelta(days=365.2425)
THIRTY_DAYS = datetime.timedelta(days=30)

def select_snapshots(avail, lo, hi):
    """AVAIL is a list of datetime objects, and LO and HI are likewise
       datetime objects.  (Before anything else happens, AVAIL is
       sorted in place, and LO and HI are swapped if LO > HI.)

       Choose and return a subset of the datetimes in AVAIL, as
       follows:

          * the most recent datetime older than LO, or, if there is no such
            datetime, the oldest available datetime

          * a sequence of datetimes more recent than, or equal to, LO,
            but older than HI, separated by at least 30 days

          * the most recent datetime older than HI
    """

    if not avail: return []

    avail.sort()
    if lo > hi: lo, hi = hi, lo
    rv = []

    start = bisect.bisect_right(avail, lo)
    if start:
        start -= 1
    rv.append(avail[start])

    for i in range(start+1, len(avail)):
        if avail[i] >= hi:
            # Always take the most recent datetime older than HI, even if
            # that violates the thirty-day rule.
            if rv[-1] < avail[i-1]:
                rv.append(avail[i-1])
            return rv

        if avail[i] - rv[-1] >= THIRTY_DAYS:
            rv.append(avail[i])

    # If we get here, it means the WBM doesn't have anything _newer_
    # than 'hi', so take the last thing it does have.
    if rv[-1] < avail[-1]:
        rv.append(avail[-1])
    return rv

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
    451: "unavailable for legal reasons (451)",

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
# We also filter out NUL bytes, which Postgresql does not support in TEXT,
# substituting (the UTF-8 encoding of) U+FFFD.
def quote_utf8_as_text(s):
    return (b"'" +
            s.replace(b"'", b"''").replace(b"\x00", b"\xef\xbf\xbd") +
            b"'")

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
        # Must also eliminate \u0000 to satisfy PostgreSQL's lack of
        # support for NUL in TEXT; JSONB text strings appear to
        # inherit the limitations of TEXT.
        blob = blob.replace(br'\u0000', br'\uFFFD')
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
def intern_text_segmented(cur, hash, text, segmented):
    hash = yield from cur.mogrify("%s", (hash,))
    yield from cur.execute(
        b"SELECT id FROM extracted_plaintext WHERE hash = " + hash)
    rv = yield from cur.fetchall()
    if rv:
        return rv[0][0]

    text = quote_utf8_as_text(text)
    if segmented is None:
        segmented = b"NULL"
    else:
        segmented = quote_utf8_as_text(segmented) + b"::jsonb"
    yield from cur.execute(
        b"INSERT INTO extracted_plaintext (hash, plaintext, segmented)"
        b" VALUES (" + hash + b"," + text + b"," + segmented + b")"
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

    @asyncio.coroutine
    def load_date_range_for_url(self, urlid):
        with (yield from self.dblock):
            cur = self.cur

            yield from cur.execute("""
                SELECT earliest_date, latest_date
                  FROM collection.historical_page_availability
                 WHERE url = %s
            """, (urlid,))
            rv = yield from cur.fetchall()
            if len(rv) == 1 and rv[0][0] is not None and rv[0][1] is not None:
                return rv[0]

            yield from cur.execute("""
                SELECT MIN(COALESCE(
                           SUBSTRING(u.meta->>'timestamp' FOR 10)::DATE,
                           (u.meta->>'date')::DATE,
                           t.last_updated))::TIMESTAMP AS lodate
                  FROM collection.urls u,
                       collection.url_sources t
                 WHERE u.src = t.id AND u.url = %s
            """, (urlid,))
            (lodate,) = (yield from cur.fetchone())

            yield from cur.execute("""
                SELECT MAX(date) FROM (
                    SELECT access_time AS date FROM collection.captured_pages
                     WHERE url = %s
                     UNION ALL
                    SELECT date FROM collection.common_crawl_pages
                     WHERE url = %s
                ) _
            """, (urlid, urlid))
            (hidate,) = (yield from cur.fetchone())

            if lodate is None or hidate is None:
                raise RuntimeError("missing dates for url {}: lo={} hi={}"
                                   .format(urlid, lodate, hidate))

            yield from cur.execute("""
                UPDATE collection.historical_page_availability
                   SET earliest_date = %s, latest_date = %s
                 WHERE url = %s
            """, (lodate, hidate, urlid))

            return lodate, hidate

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
            return None

    @asyncio.coroutine
    def record_page_availability(self, archive, urlid, snapshots):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "INSERT INTO collection.historical_page_availability"
                " (archive, url, snapshots)"
                " VALUES (%s, %s, %s)",
                (archive, urlid, snapshots))

    @asyncio.coroutine
    def note_page_processed(self, archive, urlid):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute("""
                UPDATE collection.historical_page_availability
                   SET processed = true
                 WHERE archive = %s AND url = %s
            """, (archive, urlid))

    @asyncio.coroutine
    def load_page_processed_count(self, archive):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute("""
                SELECT COUNT(*) FROM collection.historical_page_availability
                 WHERE archive = %s AND processed = true
            """, (archive,))
            return (yield from cur.fetchone())[0]

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
    def load_page_texts(self, archive, urlid):
        with (yield from self.dblock):
            cur = self.cur
            yield from cur.execute(
                "SELECT h.archive_time"
                "  FROM collection.historical_pages h"
                " WHERE h.url = %s AND h.archive = %s",
                (urlid, archive))

            return [row[0] for row in cur]

    @asyncio.coroutine
    def record_historical_page(self, archive, date, ec):
        with (yield from self.dblock):
            cur = self.cur
            docid, eid = yield from intern_html_content(
                cur, ec.ohash, ec.original)

            if not eid:
                cid = yield from intern_text_segmented(
                    cur, ec.chash, ec.content, ec.csegmtd)
                pid = yield from intern_text_segmented(
                    cur, ec.phash, ec.pruned, ec.psegmtd)
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
                SELECT h.url, s.url,
                       h.earliest_date, h.latest_date, h.snapshots
                  FROM collection.historical_page_availability h,
                       collection.url_strings s
                 WHERE h.url = s.id AND h.archive = %s
                   AND h.processed = false;
                """,
                (session.archive,))

            all_documents = []
            while True:
                block = yield from cur.fetchmany()
                status("counting partially processed pages... {}"
                       .format(len(all_documents)),
                       done=(not block))
                if not block:
                    break
                for urlid, url, lodate, hidate, snapshots in block:
                    snapshots.sort()
                    all_documents.append(Document(session, urlid, url,
                                                  snapshots, lodate, hidate))
            return all_documents

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
                             "chash", "content", "csegmtd",
                             "phash", "pruned", "psegmtd",
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
        cld2,
        html_extractor,
        domainparking.ParkingClassifier(),
        word_seg
    )

def hash_and_maybe_segment(cld2, word_seg, text):
    encoded = text.encode("utf-8")
    ehash   = hashlib.sha256(encoded).digest()

    # Very large documents will expand to so much JSON that
    # we will hit the 1GB size limit on a TOASTed table cell
    # (see http://www.postgresql.org/docs/9.4/static/storage-toast.html).
    # 80MB after UTF-8 coding seems to be a safe threshold.
    if len(encoded) < 80 * 1024 * 1024:
        lang = cld2.detect(text, want_chunks=True)
        segmented = [ { "l": c[0].code,
                        "t": list(word_seg.segment(c[0].code, c[1])) }
                      for c in lang.chunks ]
    else:
        segmented = None

    return encoded, ehash, json.dumps(segmented).encode("utf-8")

def extract_page(url, redir_url, status, reason, ctype, data):
    """Worker-process procedure: extract content from a page retrieved
       from the Internet Archive.
    """
    global extract_page_context
    if extract_page_context is None:
        extract_page_context_init()

    cld2, html_extractor, parking_cfr, word_seg = extract_page_context

    if not ctype: ctype = ""
    ctype, options = parse_options_header(ctype)
    charset = options.get("charset", "")

    extr = html_extractor.ExtractedContent(redir_url, data, ctype, charset)

    content, chash, csegmtd = hash_and_maybe_segment(cld2, word_seg, extr.text_content)
    pruned,  phash, psegmtd = hash_and_maybe_segment(cld2, word_seg, extr.text_pruned)

    original = zlib.compress(extr.original)
    olen     = len(extr.original)
    ohash    = hashlib.sha256(original).digest()
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
              chash, content, csegmtd,
              phash, pruned, psegmtd,
              hhash, heads,
              lhash, links,
              rhash, rsrcs,
              dhash, domst,
              parked, prules)

class MeteredHTTPClient:
    def __init__(self, *, loop=None,
                 query_timeout, conn_timeout, sess_timeout,
                 rate, concurrency, headers):
        self.loop          = loop or asyncio.get_event_loop()
        self.query_timeout = query_timeout
        self.conn_timeout  = conn_timeout
        self.sess_timeout  = sess_timeout
        self.rate          = rate
        self.concurrency   = concurrency
        self.headers       = headers
        self.http_meter    = Meter(
            loop            = self.loop,
            rate            = self.rate,
            timeout         = self.query_timeout,
            concurrency     = self.concurrency,
            session_factory = self.http_session_factory,
            session_timeout = self.sess_timeout)

    def http_session_factory(self):
        # aiohttp has serious bugs if you allow it any concurrent
        # connections (mixing up which data is supposed to be
        # transmitted on which channel)
        return aiohttp.ClientSession(
            headers   = self.headers,
            connector = aiohttp.TCPConnector(
                loop          = self.loop,
                conn_timeout  = self.conn_timeout,
                limit         = 1,
                use_dns_cache = True))

class WaybackMachine(MeteredHTTPClient):
    def __init__(self, executor, **kwargs):
        MeteredHTTPClient.__init__(self, **kwargs)
        self.executor    = executor
        self.errlog      = open("wayback-machine-errors.log", "at")
        self.n_errors    = 0
        self.n_requests  = 0
        self.n_pending   = 0
        self.session     = None
        self.serializer  = asyncio.Lock(loop=self.loop)

    def __enter__(self):
        return self

    def __exit__(self, *dontcare):
        self.errlog.close()

    @asyncio.coroutine
    def get_unique_snapshots_http_request(self, url):
        with (yield from self.http_meter) as client:

            self.n_requests += 1
            self.n_pending += 1
            self.session.progress()

            # The Wayback Machine replays Set-Cookie headers, and
            # since all requests are going to the same origin, they
            # accumulate until we hit the request size limit.
            # It doesn't ever _need_ us to send cookies, AFAICT.
            client.cookies.clear()

            resp = yield from client.get(
                "https://web.archive.org/cdx/search/cdx",
                params = { "url": url,
                           "collapse": "digest",
                           "fl": "original,timestamp,statuscode" })

            try:
                if resp.status == 200:
                    text = yield from resp.text()
                    yield from resp.release()
                    return text

                if resp.status == 403:
                    # We get this when the Machine has snapshots but can't
                    # show them to us because of robots.txt.
                    self.errlog.write("GET /cdx/search/cdx?{} = {} {}\n"
                                      .format(url, resp.status, resp.reason))
                    yield from resp.release()
                    return ""

                yield from resp.release()
                raise aiohttp.errors.HttpProcessingError(
                    code=resp.status,
                    message=resp.reason,
                    headers=resp.headers)

            except:
                resp.close()
                raise

    @asyncio.coroutine
    def get_unique_snapshots_of_url(self, url):
        """Retrieve a list of all available snapshots of URL."""
        backoff = 1
        while True:
            try:
                text = yield from self.get_unique_snapshots_http_request(url)
                self.n_pending -= 1
                break

            except Exception:
                self.errlog.write("GET /cdx/search/cdx?{}:\n".format(url))
                traceback.print_exc(file=self.errlog)
                self.errlog.flush()

            self.n_pending -= 1
            self.n_errors += 1
            self.session.progress()
            yield from asyncio.sleep(backoff)
            backoff = min(backoff * 2, 3600)

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

    def error_from_wayback_machine(self, status, data):
        # These codes may or may not involve a synthetic page,
        # but are always treated as reflecting the true status
        # of the page.
        if status in (200, 401, 403, 404, 410, 451):
            return False

        # To distinguish between errors generated by the Wayback Machine
        # itself, and errors it saw when it crawled the site (and is
        # faithfully replaying) we need to look at the response body.
        # The Wayback Machine's errors normally always contain the full
        # URL of the request, including the 'web.archive.org' part,
        # whereas replayed errors will never have this.  (An exception is
        # their "we are completely down for maintenance" message.)
        # Moreover, the Wayback Machine's errors are consistently ASCII-only.
        try:
            decoded = data.decode("ascii")
        except:
            return False

        return ('//web.archive.org/' in decoded or
                '//archive.org/' in decoded or
                '>Internet Archive: Scheduled Maintenance<' in decoded)

    def maybe_log_http_exception(self, e, query):
        #if not isinstance(e, (asyncio.TimeoutError,
        #                      aiohttp.errors.ClientTimeoutError,
        #                      aiohttp.errors.ClientResponseError)):
        self.errlog.write("In query: {}\n".format(query))
        traceback.print_exc(file=self.errlog)
        self.errlog.write("\n")
        self.errlog.flush()

    @asyncio.coroutine
    def get_page_do_http_request(self, query):
        with (yield from self.http_meter) as client:

            self.n_requests += 1
            self.n_pending += 1
            self.session.progress()

            # The Wayback Machine replays Set-Cookie headers, and
            # since all requests are going to the same origin, they
            # accumulate until we hit the request size limit.
            # It doesn't ever _need_ us to send cookies, AFAICT.
            client.cookies.clear()

            resp = yield from client.get(query, allow_redirects=False)
            try:
                status   = resp.status
                reason   = resp.reason
                location = resp.headers.get("location", "")
                ctype    = resp.headers.get("content-type", "")
                # Helpfully, the Wayback Machine returns the page in
                # its _original_ character encoding, and aiohttp doesn't
                # always know what that is; read the data in binary mode.
                data = b""
                data = yield from resp.read()
                yield from resp.release()

            # The Wayback Machine faithfully records and plays back
            # malformed HTTP responses, which will trigger one of these
            # exceptions.  This can happen in either resp.read() or
            # resp.release(); in the former case we will treat it as
            # an empty document. Other exceptions should be propagated.
            except (zlib.error,
                    aiohttp.errors.ContentEncodingError,
                    aiohttp.errors.ServerDisconnectedError):
                resp.close()

            except:
                resp.close()
                raise

        if 300 <= resp.status <= 399:
            ctype = None
            data = None
        else:
            location = None
            if self.error_from_wayback_machine(resp.status, data):
                raise aiohttp.errors.HttpProcessingError(
                    code=resp.status,
                    message=resp.reason,
                    headers=resp.headers)

        return (status, reason, location, ctype, data)

    @asyncio.coroutine
    def get_page_http_request(self, query):
        backoff = 1
        failures = 0
        while True:
            try:
                return (yield from self.get_page_do_http_request(query))

            except Exception as e:
                failures += 1
                self.n_errors += 1
                self.maybe_log_http_exception(e, query)
                if isinstance(e, aiohttp.errors.HttpProcessingError) and \
                   400 <= e.code < 499:
                    # Do not retry 4xx-series errors, even if they came
                    # from the WBM.  If we hit this case it indicates
                    # some sort of bug in this program.
                    raise

                if failures == 10:
                    # If we have received ten failures in a row, we're probably
                    # not going to get this one ever.
                    raise

            finally:
                self.n_pending -= 1
                self.session.progress()

            yield from asyncio.sleep(backoff)
            backoff = min(backoff * 2, 3600)

    def redirect_url(self, snap, redir_url, loc):
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
        else:
            if (loc.startswith('http://') or
                loc.startswith('https://')):
                redir_url = canon_url_syntax(loc)
            else:
                redir_url = canon_url_syntax(
                    urllib.parse.urljoin(redir_url, loc))

            query = ("https://web.archive.org/web/{}id_/{}"
                     .format(snap, redir_url))

        return (query, redir_url)

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
            try:
                (query, redir_url) = self.redirect_url(snap, redir_url, loc)

            except (ValueError, UnicodeError):
                # 'loc' is invalid; treat as a redirection loop.
                break

        # We can get here with data=None if there is a redirection loop.
        if data is None:
            data = b''

        # html_extractor _does_ implement HTML5 encoding detection.  This
        # stage is CPU-bound and pushed to a worker process.
        return (yield from self.loop.run_in_executor(
            self.executor, extract_page,
            url, redir_url, status, reason, ctype, data))

#
# Core per-document data structure
#

class Document:
    def __init__(self, session, urlid, url,
                 snapshots=None, lodate=None, hidate=None):
        self.session     = session
        self.urlid       = urlid
        self.url         = url
        self.snapshots   = snapshots # dates of available snapshots
        self.lodate      = lodate    # date when flagged by the source
        self.hidate      = hidate    # date when retrieved by our crawler
        self.texts       = set()     # dates of texts that we have
        self.to_retrieve = None

    def topic_symbol(self):
        s = []
        for d in self.snapshots:
            s.append("+" if d in self.texts else "-")
        return "".join(s)

    @asyncio.coroutine
    def load_history(self):
        if self.snapshots is None:
            self.snapshots = yield from \
                self.session.db.load_page_availability(
                    self.session.archive, self.urlid)

        if self.snapshots is None:
            self.snapshots = yield from \
                self.session.wayback.get_unique_snapshots_of_url(self.url)
            yield from self.session.db.record_page_availability(
                self.session.archive, self.urlid, self.snapshots)

        if not self.lodate:
            self.lodate, self.hidate = yield from \
                self.session.db.load_date_range_for_url(self.urlid)

        self.snapshots.append(self.hidate)
        self.snapshots.sort()

        self.texts.update(
            (yield from self.session.db.load_page_texts(
                self.session.archive, self.urlid)))

        # Retrieve snapshots at thirty-day intervals, starting just
        # before hidate and going back in time until a full year
        # before lodate.  Do so in ruler order (see above)
        self.to_retrieve = ruler_order([
            snap for snap in select_snapshots(self.snapshots,
                                              self.lodate - ONE_YEAR,
                                              self.hidate)
            if snap not in self.texts])

    def is_complete(self):
        return not self.to_retrieve

    @asyncio.coroutine
    def retrieve_next(self):
        if not self.to_retrieve: return

        snap = self.to_retrieve.pop()
        try:
            S = self.session
            ec = yield from S.wayback.get_page_at_time(self.url, snap)
            yield from S.db.record_historical_page(S.archive, snap, ec)

        except Exception:
            self.session.errlog.write("While retrieving snapshot {} for {}:\n"
                                      .format(snap, self.url))
            traceback.print_exc(file=self.session.errlog)
            self.session.errlog.write("\n")
            self.session.errlog.flush()
            self.session.n_errors += 1
            self.session.progress()

        self.texts.add(snap)

        if not self.to_retrieve:
            status("{}:  {}".format(self.url, self.topic_symbol()), done=True)
            yield from self.session.db.note_page_processed(
                self.session.archive, self.urlid)


#
# Master control
#

class HistoryRetrievalSession:
    """Container for all the things that are set up in main().
       This is mostly to avoid passing six arguments around all the time.
    """
    def __init__(self, archive, db, wayback, loop):
        self.loop            = loop
        self.wayback         = wayback
        self.db              = db
        self.archive         = archive

        self.wayback.session = self

        self.cycle           = 0
        self.n_unprocessed   = 0
        self.n_errors        = 0
        self.n_complete      = 0

    def __enter__(self):
        self.errlog          = open("history-retrieval-errors.log", "at")
        return self

    def __exit__(self, *dontcare):
        self.errlog.close()

    @asyncio.coroutine
    def get_documents_to_process(self):
        status("loading...")

        docs_unprocessed = yield from self.db.get_unprocessed_pages(self)
        status("loading: {} unprocessed...".format(len(docs_unprocessed)))

        docs_incomplete = yield from self.db.get_incomplete_pages(self)
        status("loading: {} unprocessed, {} partial..."
               .format(len(docs_unprocessed), len(docs_incomplete)))

        self.n_complete = \
            yield from self.db.load_page_processed_count(self.archive)

        status("loading: {} unprocessed, {} partial, {} complete."
               .format(len(docs_unprocessed), len(docs_incomplete),
                       self.n_complete),
               done=True)

        return docs_unprocessed, docs_incomplete

    def progress(self, message="", done=False):
        if message and message != ".":
            message = "; " + message

        status("cycle {}: {} to do, {} complete, {} errors; "
               "wb {}p/{}e/{}r{}"
               .format(self.cycle,
                       self.n_unprocessed,
                       self.n_complete,
                       self.n_errors,
                       self.wayback.n_pending,
                       self.wayback.n_errors,
                       self.wayback.n_requests,
                       message),
               done)

    @asyncio.coroutine
    def monitor_cycle(self, tasks):
        docs = []

        # This is not entirely unlike the guts of asyncio.as_completed,
        # which doesn't give us back the original futures, which is bad,
        # because we need to map back from the future to the document.
        todo = len(tasks)
        q = asyncio.Queue(loop=self.loop)
        def on_completion(f):
            nonlocal q
            q.put_nowait(f)

        for f in tasks:
            f.add_done_callback(on_completion)

        while todo > 0:
            self.progress()
            f = yield from q.get()
            todo -= 1
            self.n_unprocessed -= 1

            try:
                yield from f
                if f.document.is_complete():
                    self.n_complete += 1
                else:
                    docs.append(f.document)

            except Exception:
                traceback.print_exc(file=self.errlog)
                self.errlog.write("\n")
                self.errlog.flush()
                self.n_errors += 1

        self.progress("complete.", done=True)
        return docs


    @asyncio.coroutine
    def get_page_histories(self):
        docs_unprocessed, docs_incomplete = \
            yield from self.get_documents_to_process()

        # cycle 0: load what's already in the database
        self.n_unprocessed = len(docs_unprocessed) + len(docs_incomplete)

        tasks = []
        for doc in itertools.chain(docs_unprocessed, docs_incomplete):
            tsk = self.loop.create_task(doc.load_history())
            setattr(tsk, 'document', doc)
            tasks.append(tsk)

        docs = yield from self.monitor_cycle(tasks)

        while docs:
            self.cycle        += 1
            self.n_unprocessed = len(docs)
            random.shuffle(docs)

            tasks = []
            for doc in docs:
                tsk = self.loop.create_task(doc.retrieve_next())
                setattr(tsk, 'document', doc)
                tasks.append(tsk)

            docs = yield from self.monitor_cycle(tasks)

        self.progress(".", done=True)


@asyncio.coroutine
def inner_main(session):
    try:
        yield from session.get_page_histories()
    except:
        traceback.print_exc()

def main(loop, argv):
    _, dbname = argv

    # child watcher must be initialized before anything creates threads
    # everything that might spin the event loop on teardown must be a context
    # manager so it'll be torn down before the loop itself is (__del__ might
    # not run early enough, even for locals)

    headers = {
        'User-Agent': 'tbbscraper/get_page_histories; zackw@cmu.edu'
    }
    with asyncio.get_child_watcher() as watcher, \
         concurrent.futures.ProcessPoolExecutor() as executor, \
         Database(dbname, loop, timeout = 3600 * 24) as db,               \
         WaybackMachine(
             executor=executor, loop=loop,
             query_timeout=900, conn_timeout=5, sess_timeout=1800,
             rate=30, concurrency=5, headers=headers
         ) as wayback, \
         HistoryRetrievalSession(
             "wayback", db, wayback, loop) as session:

        loop.run_until_complete(loop.create_task(inner_main(session)))

def outer_main():
    try:
        # work around sloppy file descriptor hygiene in the guts of asyncio
        multiprocessing.set_start_method('spawn')

        loop = asyncio.get_event_loop()
        #loop.set_debug(True)
        import logging
        #logging.basicConfig(level=logging.DEBUG)
        logging.basicConfig(level=logging.WARNING,
                            filename='history-retrieval-logger.log')

        main(loop, sys.argv)

    finally:
        loop.close()

if __name__ == '__main__':
    outer_main()
