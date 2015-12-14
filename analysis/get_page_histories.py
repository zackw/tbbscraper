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
import queue
import tempfile
import threading
import traceback
import unicodedata

import aiohttp
import aiopg
from werkzeug.http import parse_options_header
#import psycopg2

import html_extractor
import word_seg
import cld2

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

ONE_YEAR = datetime.timedelta(days=365.2425)
ONE_WEEK = datetime.timedelta(days=7)

def fuzzy_year_range_backward(lo, hi):
    # The oldest sample is taken one year and one week before LO,
    # and the newest, one week before HI.  The range is generated
    # going backward in time.

    assert lo < hi
    year = ONE_YEAR
    week = ONE_WEEK

    lo -= year
    lo -= week
    hi -= week

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
    a, b = tee(iterable)
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
                 flush_timeout=30, loop=None, **wargs):
        self.worker   = worker
        self.jobsize  = jobsize
        self.wargs    = wargs
        self.loop     = loop or asyncio.get_event_loop()
        self.batch    = []
        self.ftimer   = None
        self.ftimeout = flush_timeout
        self.running  = set()

    def __del__(self):
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
        yield from asyncio.wait(running, loop=self.loop)

    @asyncio.coroutine
    def _run_batch(self, batch):
        try:
            yield from self.worker(batch, **wargs)
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
        self.db     = sync_wait(aiopg.connect(dbname=dbname, **cargs), loop)
        self.cur    = sync_wait(db.cursor(), loop)

    @asyncio.coroutine
    def begin():
        yield from self.cur.execute("BEGIN")
        yield from self.cur.fetchall()

    @asyncio.coroutine
    def commit():
        yield from self.cur.execute("COMMIT")
        yield from self.cur.fetchall()

    @asyncio.coroutine
    def rollback():
        yield from self.cur.execute("ROLLBACK")
        yield from self.cur.fetchall()

    @asyncio.coroutine
    def insertv(self, table, columns, values):
        # Note: assumes the table and column names are
        # ASCII and need no quoting.  It's unfortunate
        # psycopg2 provides no high-level way to do this.
        query = "INSERT INTO {} ({}) VALUES".format(
            table, ",".join(columns)).encode("ascii")

        template = ("(" +
                    ",".join("%s" for _ in range(len(columns))) +
                    ")")
        values = b",".join((yield from self.cur.mogrify(template, v))
                           for v in values)

        yield from self.cur.execute(query + values)
        yield from self.cur.fetchall()

    # Canned queries
    @asyncio.coroutine
    def get_translations(self):
        yield from self.cur.execute(
            "SELECT lang, word, engl FROM translations")

        translations = collections.defaultdict(dict)
        words = 0
        while True:
            block = yield from self.cur.fetchmany(10000)
            if not block: break
            for lang, word, engl in block:
                translations[lang][word] = engl
                words += 1

            status("loading translations... {} words {} languages"
                   .format(len(translations), words))

        status("loading translations... {} words {} languages"
               .format(len(translations), words), done=True)
        return translations

    @asyncio.coroutine
    def record_translations(self, lang, translations):
        values = [(lang, word, engl) for word, engl in translations]
        yield from self.insertv("translations",
                                ("lang", "word", "engl"),
                                values)

    @asyncio.coroutine
    def load_page_availability(self, archive, url):
        yield from self.cur.execute(
            "SELECT snapshots FROM collection.historical_page_availability"
            " WHERE archive = %s AND url = %s",
            (archive, url))

        return (yield from self.cur.fetchone())[0]

    @asyncio.coroutine
    def save_page_availability(self, archive, url, snapshots):
        yield from self.cur.execute(
            "INSERT INTO collection.historical_page_availability"
            " VALUES (%s, %s, %s)", (archive, url, snapshots))
        yield from self.cur.fetchall()

    @asyncio.coroutine
    def record_historical_page(self, archive, date, url, redir_url,
                               result, ec, topic_tag):
        cur = self.cur
        try:
            yield from cur.execute("SAVEPOINT record_historical_page")

            docid, is_new = yield from intern_blob(
                cur, b"collection.capture_html_content", b"content",
                ec.ohash, ec.original, False)
            if is_new:
                cid, _ = yield from intern_blob(
                    cur, b"analysis.extracted_plaintext", b"plaintext",
                    ec.chash, ec.content, False)
                pid = yield from intern_pruned_segmented(
                    cur, ec.phash, ec.pruned, ec.segmtd)
                hid, _ = yield from intern_blob(
                    cur, b"analysis.extracted_headings", b"headings",
                    ec.hhash, ec.heads, True)
                lid, _ = yield from intern_blob(
                    cur, b"analysis.extracted_urls", b"urls",
                    ec.lhash, ec.links, True)
                rid, _ = yield from intern_blob(
                    cur, b"analysis.extracted_urls", b"urls",
                    ec.rhash, ec.rsrcs, True)
                did, _ = yield from intern_blob(
                    cur, b"analysis.extracted_dom_stats", b"dom_stats",
                    ec.dhash, ec.domst, True)

                yield from cur.execute(
                    "INSERT INTO analysis.extracted_content_ov"
                    " (content_len, raw_text, pruned_text, links, resources,"
                    "  headings, dom_stats)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " RETURNING id",
                    (pagelen, cid, pid, lid, rid, hid, did))
                eid = (yield from cur.fetchone())[0]

                yield from cur.execute(
                    "UPDATE collection.capture_html_content"
                    "   SET extracted = %s AND is_parked = %s"
                    "   AND parking_rules_matched = %s"
                    " WHERE id = %s",
                    (eid, ec.parked, ec.prules, docid))

            yield from cur.execute(
                "INSERT INTO collection.historical_pages"
                " (url, archive, archive_time, result, redir_url,"
                "  html_content, topic_tag, is_parked)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (url, archive, date, result, redir_url, eid, topic_tag,
                 ec.parked))

            yield from cur.execute("RELEASE SAVEPOINT record_historical_page")

        except:
            yield from cur.execute(
                "ROLLBACK TO SAVEPOINT record_historical_page")

    @asyncio.coroutine
    def load_page_history(self, document):
        ...

    @asyncio.coroutine
    def save_page_history(self, document):
        ...

    @asyncio.coroutine
    def get_unprocessed_pages(self, archive):
        status("counting completely unprocessed pages...")
        yield from cur.execute(
            "    SELECT DISTINCT u.url"
            "      FROM urls u"
            " LEFT JOIN historical_page_availability h"
            "        ON h.archive = %s AND u.url = h.url"
            "     WHERE h.url IS NULL",
            (archive,))
        rv = []
        while True:
            block = yield from cur.fetchmany()
            status("counting completely unprocessed pages... {}"
                   .format(len(rv)), done = (not block))
            if not block:
                break
            rv.extend(row[0] for row in block)
        return rv

    @asyncio.coroutine
    def get_incomplete_pages(self, archive):
        status("counting partially processed pages...")
        yield from cur.execute(
            "SELECT url, snapshots FROM historical_page_availability"
            " WHERE archive = %s", (archive,))
        have_availability = {}
        while True:
            block = yield from cur.fetchmany()
            status("counting partially unprocessed pages... {}"
                   .format(len(have_availability)),
                   done=(not block))
            if not block:
                break
            for row in block:
                row[1].sort()
                have_availability[row[0]] = row[1]

        need_more_snapshots = set(have_availability.keys())
        n = 0
        for url, avail_snapshots in have_availability.items():
            not_captured = set(avail_snapshots)

            yield from cur.execute(
                "SELECT archive_time, topic_tag, is_parked"
                "  FROM historical_pages"
                " WHERE url = %s AND archive = %s"
                " ORDER BY archive_time",
                (url, archive))

            captured = {}
            captured_dates = []
            for cdate, topic_tag, is_parked in (yield from cur.fetchall()):
                captured_dates.append(cdate)
                captured[sdate] = (topic_tag, is_parked)
                not_captured.discard(cdate)

            if len(not_captured) > 0:
                for cdate in sorted(not_captured):
                    # We do not need to retrieve a snapshot if the topic_tag
                    # and is_parked state from the snapshots we do have on
                    # either side of it are the same.
                    p = bisect.bisect_left(captured_dates, cdate)
                    if p > 0 and (captured[captured_dates[p-1]] ==
                                  captured[captured_dates[p]]):
                        not_captured.remove(cdate)

            if len(not_captured) == 0:
                need_more_snapshots.remove(url)

            n += 1

            if n % 1000 == 0:
                status("weeding partially unprocessed pages... {}/{}"
                       .format(n, len(need_more_snapshots)))

        status("weeding partially unprocessed pages... {}/{}"
               .format(n, len(need_more_snapshots)), done=True)
        return need_more_snapshots




#
# Interacting with the Wayback Machine
#

# This chunk of the work is CPU-bound and farmed out to worker
# processes.  We must use processes and not threads because of the
# GIL, and unfortunately that means we have to pass all the data back
# and forth in bare tuples.

EC = collections.namedtuple("EC",
                            ("ohash", "olen", "original",
                             "chash", "content",
                             "phash", "pruned", "segmtd",
                             "hhash", "heads",
                             "lhash", "links",
                             "rhash", "rsrcs",
                             "dhash", "domst",
                             "parked", "prules"))

def extract_page(url, ctype, data):
    """Worker-process procedure: extract content from URL, CTYPE, and DATA.
    """
    ctype, options = parse_options_header(ctype)
    charset = options.get("charset", "")

    extr = html_extractor.ExtractedContent(url, data, ctype, charset)
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

    parked, prules = domainparking.test(extr.original)

    return EC(ohash, olen, original,
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

    @asyncio.coroutine
    def get_unique_snapshots_of_url(self, url):
        """Retrieve a list of all available snapshots of URL."""
        yield from self.rate
        resp = yield from self.http_client.get(
            "https://web.archive.org/cdx/search/cdx",
            params = { "url": url,
                       "collapse": "digest",
                       "fl": "original,timestamp,statuscode" })

        text = yield from resp.text()

        snapshots = []
        for line in text.split("\n"):
            if not line: continue
            url_r, timestamp, statuscode = line.split()
            # This API *does not* guarantee that you get back out exactly the
            # URL you put in.  In particular, there appears to be no way to
            # make it pay attention to the scheme.
            if url_r == url and statuscode == "200":
                snapshots.append(datetime.datetime.strptime(
                    timestamp, "%Y%m%d%H%M%S"))
        snapshots.sort()
        return snapshots

    @asyncio.coroutine
    def get_page_at_time(self, url, snap):
        """Retrieve URL as of SNAP, which must be an entry in the list
           returned by get_unique_snapshots_of_url (above).  The
           return value is an ExtractedContent object.
        """
        yield from self.rate()

        # The undocumented "id_" token is how you get the Wayback
        # Machine to spit out the page *without* its usual
        # modifications (rewriting links and adding a toolbar).  The
        # URL must *not* be quoted.
        query = ("https://web.archive.org/web/{}id_/{}"
                 .format(snap.strftime("%Y%m%d%H%M%S"), url))
        resp = yield from self.http_client.get(query)
        if resp.status != 200:
            raise RuntimeError("Wayback Machine returned status " +
                               str(resp.status))

        ctype = resp.headers.get("content-type", "")

        # Helpfully, the Wayback Machine returns the page in its
        # _original_ character encoding.  aiohttp does not implement HTML5
        # encoding detection, so read the data in binary mode to avoid problems.
        data = yield from resp.read()

        # html_extractor _does_ implement HTML5 encoding detection.  This
        # stage is CPU-bound and pushed to a worker process.
        return (yield from self.loop.run_in_executor(
            self.executor, extract_page, url, ctype, data))

#
# Translation of unknown words.
# aiohttp-ified version of
# http://thomassileo.com/blog/2012/03/26/using-google-translation-api-v2-with-python/
#

with open(os.path.join(os.environ["HOME"], ".google-api-key"), "rt") as f:
    GOOGLE_API_KEY = f.read().strip()

# Map CLD2's names for a few things to Google Translate's names.
REMAP_LCODE = default_identity_dict({
    "zh-Hant" : "zh-TW"
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
        self.langs        = None
        self.translations = None
        self.tbufs        = {}

    def __del__(self):
        self.loop.run_until_complete(
            self.loop.create_task(
                self.drain_translations()))

    @asyncio.coroutine
    def get_translatable_languages(self):
        if self.langs is None:
            yield from self.rate()
            resp = yield from self.http_client.get(
                GET_LANGUAGES_URL,
                params = { "key" : GOOGLE_API_KEY })
            blob = yield from resp.json()
            # Don't bother translating English into English.
            self.langs = \
                frozenset(x["language"] for x in blob["data"]["languages"]
                          if x["language"] != "en")
        return self.langs

    @asyncio.coroutine
    def load_old_translations(self):
        self.translations = (yield from self.db.get_translations())

    @asyncio.coroutine
    def get_translations_internal(self, lang, words):
        while True:
            try:
                blob = None
                yield from self.rate()
                resp = yield from self.http_client.post(
                    TRANSLATE_URL,
                    data = {
                        "key":    GOOGLE_API_KEY,
                        "source": REMAP_LCODE[lang],
                        "target": "en",
                        "q":      words,
                    },
                    headers = {
                        "Content-Type":
                            "application/x-www-form-urlencoded;charset=utf-8",
                        "X-HTTP-Method-Override": "GET",
                    })
                if resp.status != 200:
                    self.errlog.write("POST /language/translate/v2 = {} {}\n"
                                       .format(resp.status, resp.reason))
                    self.errlog.flush()
                    self.errlog.write(yield from resp.text())
                    self.errlog.write("\n\n")
                    self.errlog.flush()
                    yield from asyncio.sleep(15)
                    continue

                blob = yield from resp.json()
                return list(zip(words,
                                unicodedata.normalize(
                                    "NFKC", x["translatedText"]).casefold()
                                for x in blob["data"]["translations"]))

            except Exception:
                traceback.print_exc(file=self.errlog)
                try:
                    self.errlog.write("\n")
                    if blob is not None:
                        json.dump(blob, self.errlog)
                    else:
                        self.errlog.write(yield from resp.text())
                except Exception:
                    self.errlog.write("\nWhile dumping response:\n")
                    traceback.print_exc(file=self.errlog)

                yield from asyncio.sleep(15)
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
            yield from self.get_translatable_languages()
        if self.translations is None:
            yield from self.load_old_translations()

        sleepers = {}
        translation = []
        for lang, words in segmented:
            tdict = self.translations[lang]
            for word in words:
                key = (lang, word)
                if key in sleepers:
                    translation.append(sleepers[key])

                elif word in tdict:
                    # this one has already been translated
                    fut = asyncio.Future(loop=self.loop)
                    fut.set_result(tdict[word])
                    translation.append(fut)
                    sleepers[key] = fut

                elif lang not in self.langs:
                    # untranslatable, return as is
                    trans = unicodedata.normalize("NFKC", word).casefold()
                    tdict[word] = trans
                    fut = asyncio.Future(loop=self.loop)
                    fut.set_result(trans)
                    translation.append(fut)
                    sleepers[key] = fut

                else:
                    if lang not in self.tbufs:
                        self.tbufs[lang] = work_buffer(
                            self.get_translations_worker,
                            WORDS_PER_POST,
                            loop=self.loop,
                            lang=lang)
                    fut = self.tbufs[lang].put(word)
                    sleepers[key] = fut
                    translation.append(fut)

        yield from asyncio.wait(sleepers.values(), loop=self.loop)

        return [ fut.result() for fut in translation ]

    def flush_translations(self):
        for wb in self.tbufs.values(): wb.flush()

    @asyncio.coroutine
    def drain_translations(self):
        yield from asyncio.wait(
            (wb.drain() for wb in self.tbufs.values()),
            loop=self.loop)

#
# The topic-analysis subprocess
#

class TopicAnalyzer:
    def __init__(self, analyzer, loop=None):
        self.analyzer  = analyzer
        self.loop      = loop or asyncio.get_event_loop()
        self.wbuffer   = work_buffer(self._process_topic_batch, 1000, loop=loop)
        self.exit_evt  = asyncio.Event(loop=loop)
        self.ready_evt = asyncio.Event(loop=loop)
        self.proc_t    = None
        self.proc_p    = None

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
        return (yield from self.wbuffer.put((a, b)))

    def flush(self):
        self.wbuffer.flush()

    @asyncio.coroutine
    def _process_topic_batch(self, batch):
        yield from self.ready_evt.wait()

        with tempfile.NamedTemporaryFile(
                mode="w+t", encoding="utf-8",
                suffix=".txt", prefix="topics.") as ifp:
            iname = fp.name
            rname = iname + ".result"

            for (a, b), _ in batch:
                ifp.write("{}\n{}\n".format(a, b))

            ifp.flush()
            try:
                fut = asyncio.Future(loop=self.loop)
                self.proc_p.post_batch(iname, fut)
                yield from fut

                with open(rname, "rt") as rfp:
                    for (_, fut), val in zip(batch, rfp):
                        fut.set_result(int(val) != 0)

            finally:
                try:
                    os.remove(rname)
                except FileNotFoundError:
                    pass

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
            self.stdin.write(iname)

        def stop(self):
            assert not self.stopping
            assert self.transport is not None
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
                assert c == "\n"
                self.pending.pop().set_result(None)

        def pipe_connection_lost(self, fd, exc):
            assert self.stopping
            assert exc is None
            assert fd == 1
            assert len(self.pending) == 0

        def process_exited(self):
            self.exit_evt.set()
            assert self.stopping
            assert self.transport.get_returncode() == 0

#
# Core per-document data structure
#

class Document:
    def __init__(self, session, urlid, snap_f=None):
        self.session   = session
        self.urlid     = urlid
        self.url       = None
        self.snapshots = None # dates of available Wayback Machine snapshots
        self.rdate     = None # date when retrieved by our crawler
        self.fdate     = None # date when flagged by the source
        self.topics    = {}   # map snapshot_date : (topic_number, is_parked)
        self.texts     = {}   # map snapshot_date : text
        self.ntopics   = None
        self.snap_f    = None

    @asyncio.coroutine
    def load_history(self):
        yield from self.session.db.load_page_history(self)

        if self.topics:
            self.ntopics = max(self.topics.values()) + 1
        else:
            self.topics[self.rdate] = 0
            self.ntopics = 1

    @asyncio.coroutine
    def save_history(self):
        yield from self.session.db.save_page_history(self)

    @asyncio.coroutine
    def retrieve_history(self):
        yield from self.load_history()
        try:
            yield from self.retrieve_history_internal()
            return "done"
        finally:
            yield from self.save_history()

    @asyncio.coroutine
    def retrieve_history_internal(self):
        if not self.snapshots:
            self.snapshots = \
                self.session.wayback.get_unique_snapshots_of_url(self.url)
            self.snapshots.append(self.rdate)
            self.snapshots.sort()

        if self.snap_f:
            self.snap_f.set_result("snap")

        # Phase 1: retrieve snapshots at one-year intervals, starting
        # just before rdate and going back in time until a little before
        # fdate (see fuzzy_year_range_backward).

        prev_date = self.rdate
        for date in fuzzy_year_range_backward(self.fdate, self.rdate):
            date = find_le(self.snapshots, date)
            if date is not None and date != prev_date:
                self.check_topic(date, prev_date)
                prev_date = date

        yield from asyncio.wait(v for v in self.topics.values()
                                if isinstance(v, asyncio.Future))

        # Phase 2: determine the ranges of time over which the page
        # had the same topic, and fine-tune the boundaries by
        # retrieving more snapshots.
        dates = sorted(self.topics.keys(), reverse=True)
        changes = []
        for cur, prev in pairwise(dates):
            if topics[cur] != topics[prev]:
                changes.append((cur, prev))

        while changes:
            cur, prev = changes.pop()
            # The topic changed somewhere between CUR and PREV.
            # Narrow down the date by bisection.
            lo = bisect.bisect_right(self.snapshots, prev)
            hi = bisect.bisect_left(self.snapshots, cur)
            if lo < hi:
                date = snapshots[lo + (hi - lo)//2]
                assert prev < date < cur
                yield from self.check_topic(date, prev, cur)

                if topics[date] != topics[prev]:
                    changes.append((date, prev))
                if topics[date] != topics[cur]:
                    changes.append((cur, date))

    @asyncio.coroutine
    def retrieve_snapshot(self, date):
        S = self.session
        if date not in self.texts:
            ec = yield from S.wayback.get_page_at_time(self.url, date)
            yield from S.db.record_historical_page(self.url, date, ec)
            self.texts[date] = \
                yield from S.gtrans.translate_segmented(
                    json.loads(ec.segmtd.decode("utf-8")))

    @asyncio.coroutine
    def check_topic(self, target, *others):
        task = self.loop.create_task(
            self.check_topic_internal(target, *others))
        topics[target] = task
        yield from task

    @asyncio.coroutine
    def check_topic_internal(self, *dates):
        sleepers = []
        for d in dates:
            if d not in self.texts:
                sleepers.append(self.retrieve_snapshot(date))
            f = self.topics.get(d)
            if isinstance(f, asyncio.Future):
                sleepers.append(f)
        yield from asyncio.wait(sleepers)

        target = dates[0]
        comparisons = []
        for i in range(1, len(dates)):
            comparisons.append(self.session.topic_analyzer.is_same_topic(
                self.texts[target], self.texts[dates[i]]))
        results = yield from asyncio.gather(comparisons)

        for i in range(1, len(dates)):
            if results[i-1]:
                self.topics[target] = self.topics[dates[i]]
                return

        self.topics[target] = self.ntopics
        self.ntopics += 1

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
        self.errlog         = open("history-retrieval-errors.log", "at")

    @asyncio.coroutine
    def get_documents_to_process(self):
        status("loading documents to process...")
        docs_unprocessed = yield from self.db.get_unprocessed_pages(
            self.archive)
        status("loading documents to process: {} unprocessed..."
               .format(len(docs_unprocessed)))

        docs_incomplete = yield from self.db.get_incomplete_pages(
            self.archive)
        status("loading documents to process: {} unprocessed, {} incomplete"
               .format(len(docs_unprocessed), len(docs_incomplete)),
               done=True)

        return docs_unprocessed, docs_incomplete

    @asyncio.coroutine
    def get_page_histories(self):
        docs_unprocessed, docs_incomplete = \
            yield from self.get_documents_to_process()

        n_unprocessed = len(docs_unprocessed)
        n_incomplete  = len(docs_incomplete)
        n_complete    = 0
        n_errors      = 0

        # For monitoring purposes, completely unprocessed documents
        # signal a future when their snapshot availability has been
        # recorded.
        tasks = []
        for doc in docs_unprocessed:
            tasks.append(doc.have_availability)
            tasks.append(self.loop.create_task(doc.retrieve_history()))

        for doc in docs_incomplete:
            tasks.append(self.loop.create_task(doc.retrieve_history()))

        tick = 0
        for fut in asyncio.as_completed(tasks, loop=self.loop):
            try:
                what = yield from fut
                if what == "snap":
                    n_unprocessed -= 1
                    n_incomplete += 1
                else:
                    n_incomplete -= 1
                    n_complete += 1

            except Exception:
                n_errors + 1
                traceback.print_exc(file=self.errlog)

            status("{} unprocessed, {} incomplete, {} complete, {} errors"
                   .format(n_unprocessed, n_incomplete, n_complete, n_errors))
            tick += 1
            if tick == 5000:
                yield from self.db.commit()
                tick = 0

        status("{} unprocessed, {} incomplete, {} complete, {} errors."
               .format(n_unprocessed, n_incomplete, n_complete, n_errors),
               done=True)

def main():
    _, dbname, analyzer = sys.argv
    loop = asyncio.get_event_loop()
    # child watcher must be initialized before anything creates threads
    asyncio.get_child_watcher()

    wb_rate = rate_limiter(10, loop=loop)
    gt_rate = rate_limiter(4096, loop=loop)

    with concurrent.futures.ProcessPoolExecutor() as executor,   \
         async_pgconn(dbname, loop=loop) as db,                  \
         aiohttp.ClientSession(loop=loop) as http_client,        \
         TopicAnalyzer(analyzer, loop=loop) as topic_analyzer:

        wayback = WaybackMachine(executor, http_client, wb_rate, loop)
        gtrans  = GoogleTranslate(db, http_client, gt_rate, loop)

        session = HistoryRetrievalSession("wayback",
            db, wayback, gtrans, topic_analyzer, loop)

        loop.run_until_complete(loop.create_task(session.get_page_histories()))

if __name__ == '__main__':
    main()
