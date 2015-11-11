#! /usr/bin/python3

import asyncio
import bisect
import collections
import csv
import datetime
import glob
import itertools
import json
import queue
import tempfile
import threading
import traceback

import aiohttp
from werkzeug.http import parse_options_header
import psycopg2

import html_extractor
import word_seg
import cld2

#
# Utilities
#

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

class rate_limiter:
    """Unblocks a calling coroutine RATE times per second.
       Must be used as a context manager:

           with rate_limiter(10, loop=loop) as rate:
               while work_to_do:
                   yield from rate()
                   ...
    """

    def __init__(self, rate, *, loop=None):
        self.rate = rate
        self.loop = loop or asyncio.get_event_loop()
        # maxsize=1 prevents bursts of queries.
        self.q    = asyncio.Queue(maxsize=1, loop=loop)
        self.e    = asyncio.Event(loop=loop)
        self.t    = None

    @asyncio.coroutine
    def __call__(self):
        yield from self.q.get()

    def __enter__(self):
        @asyncio.coroutine
        def rate_limit_task(rate, queue, done):
            delay = 1/rate
            while not done.is_set():
                yield from asyncio.sleep(delay)
                yield from queue.put(None)

        self.t = self.loop.create_task(rate_limit_task(self.rate,
                                                       self.q,
                                                       self.e))
        return self

    def __exit__(self, *args):
        self.e.set()
        self.loop.run_until_complete(self.t)
        return False

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
        batch = self.batch
        self.batch = []
        self.loop.create_task(self._run_batch(batch))
        if self.ftimer is not None:
            self.ftimer.cancel()
            self.ftimer = None

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

def extract_page(url, ctype, data):
    """Worker-thread procedure: produce an ExtractedContent object from
       URL, CTYPE, and DATA.  Note that we intentionally do not care
       whether CTYPE is actually text/html.
    """
    charset = parse_options_header(ctype)[1].get("charset", "")
    return html_extractor.ExtractedContent(url, data, charset)

#
# Poor man's async database handle.
# All operations are shipped off to a dedicated thread and serialized.
# Context management is responsible for starting/stopping the thread.
#

class async_pgconn:

    # Opcodes for the request queue:
    _SHUTDOWN = 0
    _BEGIN    = 1
    _COMMIT   = 2
    _ROLLBACK = 3
    _QUERY    = 4

    def __init__(self, dbname, loop=None, **cargs):
        self.dbname = dbname
        self.cargs  = cargs
        self.thr    = None
        self.inq    = None
        self.loop   = loop or asyncio.get_event_loop()

    def __enter__(self):
        inq      = queue.Queue()
        thr      = threading.Thread(target=self._database_thread)
        thr.name = "db-" + self.dbname + "-" + thr.name.partition("-")[2]
        self.inq = inq
        self.thr = thr
        self.thr.start()
        return self

    def __exit__(self, ty, vl, tb):
        if self.inq is not None:
            inq = self.inq
            thr = self.thr
            self.inq = None
            self.thr = None

            inq.put((None, self._SHUTDOWN, ty is None))
            inq.join()
            thr.join()

    @asyncio.coroutine
    def begin(self):
        f = asyncio.Future()
        self.inq.put((f, self._BEGIN, None))
        return (yield from f)

    @asyncio.coroutine
    def commit(self):
        f = asyncio.Future()
        self.inq.put((f, self._COMMIT, None))
        return (yield from f)

    @asyncio.coroutine
    def rollback(self):
        f = asyncio.Future()
        self.inq.put((f, self._ROLLBACK, None))
        return (yield from f)

    @asyncio.coroutine
    def query(self, query, args=()):
        f = asyncio.Future()
        self.inq.put((f, self._QUERY, (query, args)))
        return (yield from f)

    def _database_thread(self):
        db    = psycopg2.connect(database=self.dbname, **cargs)
        cur   = db.cursor()
        alive = True
        inq   = self.inq
        loop  = self.loop
        while alive:
            try:
                rv = None
                fut, op, args  = inq.get()
                if op == self._SHUTDOWN:
                    if args:
                        db.commit()
                    else:
                        db.rollback()
                    db.close()
                    alive = False

                elif op == self._BEGIN:
                    cur.execute("BEGIN")

                elif op == self._COMMIT:
                    db.commit()

                elif op == self._ROLLBACK:
                    db.rollback()

                elif op == self._QUERY:
                    query, params = args
                    cur.execute(query, params)
                    rv = cur.fetchall()

                else:
                    rv = RuntimeError("invalid db opcode %d" % op)

            except Exception as e:
                rv = e

            self.inq.task_done()
            if fut is not None:
                if isinstance(rv, Exception):
                    loop.call_soon_threadsafe(fut.set_exception, rv)
                else:
                    loop.call_soon_threadsafe(fut.set_result, rv)

        # At this point it should be impossible for any more requests to
        # be added to the queue.  Discard any that are (cancelling the future).
        try:
            while True:
                fut, op, args = inq.get(block=False)
                if fut:
                    loop.call_soon_threadsafe(fut.cancel)
                inq.task_done()
        except queue.Empty:
            pass

    # Canned queries
    @asyncio.coroutine
    def get_translated_page(self, ec):
        ...

    @asyncio.coroutine
    def get_untranslated_words(self, ec):
        ...

    @asyncio.coroutine
    def record_historical_page(self, docid, date, ec):
        ...

    @asyncio.coroutine
    def load_page_history(self, document):
        ...

    @asyncio.coroutine
    def save_page_history(self, document):
        ...

#
# Interacting with the Wayback Machine
#

class WaybackMachine:
    def __init__(self, session, rate, loop=None):
        self.session = session
        self.rate    = rate
        self.loop    = loop or asyncio.get_event_loop()

    @asyncio.coroutine
    def get_unique_snapshots_of_url(self, url):
        """Retrieve a list of all available snapshots of URL."""
        yield from self.rate()
        resp = yield from self.session.get(
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
        resp = yield from self.session.get(query)
        if resp.status != 200:
            raise RuntimeError("Wayback Machine returned status " +
                               str(resp.status))

        ctype = resp.headers.get("content-type", "")

        # Helpfully, the Wayback Machine returns the page in its
        # _original_ character encoding.  aiohttp does not implement HTML5
        # encoding detection, so read the data in binary mode to avoid problems.
        data = yield from resp.read()

        # html_extractor _does_ implement HTML5 encoding detection.  This
        # stage is mostly implemented in C and can safely be pushed to a
        # worker thread.
        return (yield from self.loop.run_in_executor(None, extract_page,
                                                      url, ctype, data))

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
    def __init__(self, session, rate, loop=None):
        self.session = session
        self.rate    = rate
        self.loop    = loop or asyncio.get_event_loop()
        self.errlog  = open("google-translate-errors.log", "at")
        self.langs   = None
        self.tbufs   = {}

    @asyncio.coroutine
    def get_translatable_languages(self):
        if self.langs is None:
            yield from self.rate()
            resp = yield from self.session.get(
                GET_LANGUAGES_URL,
                params = { "key" : GOOGLE_API_KEY })
            blob = yield from resp.json()
            # Don't bother translating English into English.
            self.langs = \
                frozenset(x["language"] for x in blob["data"]["languages"]
                          if x["language"] != "en")
        return self.langs

    @asyncio.coroutine
    def get_translations_internal(self, lang, words):
        while True:
            try:
                blob = None
                yield from self.rate()
                resp = yield from self.session.post(
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
                                x["translatedText"].casefold()
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
        sleepers = collections.defaultdict(list)
        for word, fut in batch:
            sleepers[word].append(fut)
        to_translate = sorted(sleepers.keys())

        translations = yield from self.get_translations_internal(lang, words)

        for word, engl in translations:
            for fut in sleepers[word]:
                fut.set_result(engl)

    @asyncio.coroutine
    def get_translations(self, wordlist):
        if self.langs is None:
            yield from self.get_translatable_languages()

        sleepers = {}
        for lang, word in wordlist:
            key = (lang,word)
            assert key not in sleepers

            if lang in self.langs:
                if lang not in self.tbufs:
                    self.tbufs[lang] = work_buffer(
                        self.get_translations_worker,
                        WORDS_PER_POST,
                        loop=self.loop,
                        lang=lang)
                sleepers[key] = self.tbufs[lang].put(word)

            else:
                # Just return untranslatable words as is.
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(word.casefold())
                sleepers[key] = fut

        yield from asyncio.wait(sleepers.values(), loop=self.loop)

        return [ (lang, word, sleepers[(lang,word)].result())
                 for lang, word in wordlist ]

    def flush_translations(self):
        for wb in self.tbufs.values(): wb.flush()

    # Convenience: translate all of the text of one page.
    @asyncio.coroutine
    def translate_segmented(self, ec):
        ...

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
    def __init__(self, session, docid):
        self.session   = session
        self.docid     = docid
        self.url       = None
        self.snapshots = None # dates of available Wayback Machine snapshots
        self.rdate     = None # date when retrieved by our crawler
        self.fdate     = None # date when flagged by the source
        self.topics    = {}   # map snapshot_date : topic_number
        self.texts     = {}   # map snapshot_date : text
        self.ntopics   = None

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
        finally:
            yield from self.save_history()

    @asyncio.coroutine
    def retrieve_history_internal(self):
        if not self.snapshots:
            self.snapshots = \
                self.session.wayback.get_unique_snapshots_of_url(self.url)
            self.snapshots.append(self.rdate)
            self.snapshots.sort()

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
            yield from S.db.record_historical_page(self.docid, date, ec)
            self.texts[date] = \
                yield from S.gtrans.translate_segmented(ec)

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
    def __init__(self, runs, db, wayback, gtrans, topic_analyzer, loop):
        self.loop  = loop
        self.topic_analyzer = topic_analyzer
        self.gtrans = gtrans
        self.wayback = wayback
        self.db = db
        self.runs = runs

    @asyncio.coroutine
    def get_documents_to_process(self):
        ...

    @asyncio.coroutine
    def get_page_histories(self):
        docids = yield from self.get_documents_to_process()

        yield from asyncio.wait(
            loop.create_task(Document(self, docid).retrieve_history())
            for docid in docids)

def main():
    _, dbname, analyzer, runs = sys.argv
    runs = runs.split(",")
    loop = asyncio.get_event_loop()
    # child watcher must be initialized before anything creates threads
    asyncio.get_child_watcher()

    with async_pgconn(dbname, loop=loop) as db,                  \
         aiohttp.ClientSession(loop=loop) as http_client,        \
         rate_limiter(10, loop=loop) as wb_rate,                 \
         rate_limiter(4096, loop=loop) as gt_rate,               \
         TopicAnalyzer(analyzer, loop=loop) as topic_analyzer:

        wayback = WaybackMachine(http_client, wb_rate)
        gtrans  = GoogleTranslate(http_client, gt_rate)

        session = HistoryRetrievalSession(
            runs, db, wayback, gtrans, topic_analyzer, loop)

        loop.run_until_complete(loop.create_task(session.get_page_histories()))

main()
