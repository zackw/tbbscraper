# Copyright © 2014–2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Capture the HTML content and a screenshot of each URL in a list,
using Firefox (with OpenWPM automation), from many locations
simultaneously --- implementation."""

import asyncio
import json
import os
import os.path
import random
import subprocess
import sys
import time
import zlib

from shared.util import canon_url_syntax, categorize_result_ff
from shared.aioproxies import ProxySet
from shared.openwpm_browsers import BrowserManager

class CaptureResult:
    """The result of one capture job."""
    def __init__(self, url):
        self.status       = ""
        self.detail       = ""
        self.log          = {}
        self.canon_url    = ""
        self.content      = ""
        self.elapsed      = 0.

        # Make sure the URL is not so mangled that the browser is just
        # going to give up and report nothing at all.
        try:
            self.original_url = canon_url_syntax(url, want_splitresult = False)

        except ValueError as e:
            self.original_url = url
            self.status = 'invalid URL'
            self.detail = str(e)

        except UnicodeError as e:
            while e.__cause__ is not None: e = e.__cause__
            self.original_url = url
            self.status = 'invalid URL'
            self.detail = 'invalid hostname: ' + str(e)

    def is_failure(self):
        # Pages that don't exist anymore, etc. are not counted as
        # failures; only cases where we got nothing useful back.
        return self.status == 'crawler failure' or not self.content

    def set_result(self, final_url, status, detail, content, capture_log):
        self.detail = detail
        self.status = categorize_result_ff(detail)
        self.canon_url = canon_url_syntax(final_url, want_splitresult = False)
        self.content = content
        self.log = capture_log

    def write_result(self, fname):
        """Write the results file for this URL, to FNAME.  Results files are
           binary, but the first chunk is mostly human-readable.

           The initial eight bytes of a result file are a magic number:

               7F 63 61 70 20 30 31 0A
               ^? c  a  p  SP 0  1  LF

           The two bytes between the SP and the LF are a decimal
           version number.

           In versions 00 and 01, exactly six LF-terminated lines of
           UTF-8-encoded text follow the magic number.  The first and
           fifth of these lines will not be empty, but the other three
           may be empty.  They are:

               the original URL
               the final URL after following redirections
               the summarized page-load status
               the detailed page-load status
               elapsed time for collection (floating-point number; seconds)
               two space-separated lengths (nonnegative integers)

           Following this are two sequences of zlib-compressed data:
           the contents of the captured page, and the capture log, in
           that order.  The numbers on the fifth line are the lengths
           (in bytes) of these sequences.  The captured page is HTML
           and the capture log is a JSON blob; both are UTF-8 under
           the compression.

           The only differences between version 00 and version 01 are:
           In version 00 the capture log is a custom thingy defined by
           pj-trace-redir.js, and in version 01 it is a HAR archive.
           In version 00, if either the HTML or the capture log was
           completely empty it was written out as zero bytes rather
           than the zlib compression of zero bytes (which is eight
           bytes long), but in version 01 this special case has been
           removed.

           Note that in both versions, the page contents are a
           serialization of the DOM at snapshot time, *not* the
           original HTML received on the wire.

           If any parent directory of FNAME does not exist it will be
           created.  It is an error if FNAME itself already exists.

        """
        fname = os.path.abspath(fname)
        os.makedirs(os.path.dirname(fname), exist_ok=True)

        with open(fname, "xb") as fp:
            compressed_content = zlib.compress(self.content.encode("utf-8"), 9)
            compressed_log = zlib.compress(self.log.encode("utf-8"), 9)

            fp.write("\u007Fcap 01\n"
                     "{ourl}\n"
                     "{curl}\n"
                     "{stat}\n"
                     "{dtyl}\n"
                     "{elap:.6f}\n"
                     "{clen} {llen}\n"
                     .format(ourl=self.original_url,
                             curl=self.canon_url,
                             stat=self.status,
                             dtyl=self.detail,
                             elap=self.elapsed,
                             clen=len(compressed_content),
                             llen=len(compressed_log))
                     .encode("utf-8"))

            fp.write(compressed_content)
            fp.write(compressed_log)

@asyncio.coroutine
def do_capture(url, browser, loop):
    result = CaptureResult(url)
    if result.status:
        return result

    start = time.monotonic()
    result.set_result(*(yield from browser.visit_url(url)))
    result.elapsed = time.monotonic() - start

    return result

class claim_one:
    """Context manager which "claims" an entry from a list (as-if via
       pop()).  If the with-context throws an exception, the entry
       will be restored to the list.
    """
    def __init__(self, lst):
        self._lst = lst
        self._itm = None

    def __enter__(self):
        try:
            self._itm = self._lst.pop()
            return self._itm
        except IndexError:
            return None

    def __exit__(self, ty, vl, tb):
        if ty is not None and self._itm is not None:
            self._lst.append(self._itm)

@asyncio.coroutine
def output_queue_drainer(queue):
    """Drain the queue of completed result.write_result jobs.
       This exists solely because of asyncio's lack of any "run this
       task to completion, but I don't care what its result is" mechanism.
    """
    while True:
        task = yield from queue.get()
        if task is None:
            assert queue.empty()
            break
        yield from asyncio.wait_for(task, None)

class CaptureWorker:

    """Control the process of crunching through all the URLs for a given
       locale."""
    def __init__(self, output_dir, locale, urls,
                 loop, max_workers, output_queue, quiet):
        self.output_dir   = output_dir
        self.locale       = locale
        self.urls         = urls
        self.loop         = loop
        self.max_workers  = max_workers
        self.output_queue = output_queue
        self.quiet        = quiet

    def output_fname(self, serial):
        return "{}/{:02d}/{:03d}/{:03d}.{}".format(
            self.output_dir,
            serial // 1000000, (serial % 1000000) // 1000, serial % 1000,
            self.locale)

    def progress(self, label, url, message):
        if self.quiet: return
        if message == "...":
            sys.stderr.write(label + url + "...\n")
        else:
            sys.stderr.write(label + url + ": " + message + "\n")

    @asyncio.coroutine
    def run_worker(self, bmgr, proxy, i):
        """MAX_WORKERS instances of this coroutine are spawned by run()."""
        label = "{} {}: ".format(proxy.label(), i)

        with (yield from bmgr.start_browser(proxy)) as browser:
            while True:
                with claim_one(self.urls) as task:

                    if task is None: break
                    (serial, url) = task

                    self.progress(label, url, "...")
                    try:
                        result = yield from do_capture(url, proxy, self.loop)
                        self.progress(label, url, result.status)
                    except:
                        self.progress(label, url, "fail")
                        raise

                # The result is written out in an executor because neither
                # file I/O nor zlib are asynchronous, and we don't want
                # this to hold up the event loop. (Both do drop the GIL.)
                # The output_queue_drainer waits for the future, and we go on.
                yield from self.output_queue.put(
                    self.loop.run_in_executor(None,
                        result.write_result,
                        self.output_fname(serial)))

    @asyncio.coroutine
    def run(self, bmgr, proxy):
        # There is no point in running more workers than we have URLs
        # (left) to process.
        nworkers = min(self.max_workers, len(self.urls))

        # Unlike wait_for(), wait() does _not_ cancel everything it's
        # waiting for when it is itself cancelled.  Since that's what
        # we want, we have to do it by hand.
        try:
            workers = [self.loop.create_task(self.run_worker(bmgr, proxy, i))
                       for i in range(nworkers)]
            done, pending = yield from asyncio.wait(workers, loop=self.loop)
        except:
            for w in workers: w.cancel()
            yield from asyncio.wait(workers, loop=self.loop)
            raise

        # Detect and propagate any failures
        assert len(pending) == 0
        for w in done:
            w.result()

        # If we get here, we should be completely done with this locale.
        assert len(self.urls) == 0
        proxy.close()


class CaptureDispatcher:
    def __init__(self, args):
        self.args         = args
        self.loop         = asyncio.get_event_loop()
        self.workers      = {}
        self.active       = {}
        self.output_queue = asyncio.Queue()
        self.drainer      = self.loop.create_task(
            output_queue_drainer(self.output_queue))
        self.proxies      = ProxySet(args,
                                     nstag="cap",
                                     loop=self.loop,
                                     proxy_sort_key=self.proxy_sort_key)

        run = 1
        while True:
            try:
                path = os.path.join(self.args.output_dir, str(run))
                os.makedirs(path)
                self.output_dir = path
                break
            except FileExistsError:
                run += 1
                continue

        with open(self.args.urls) as f:
            urls = [l for l in (ll.strip() for ll in f)
                    if l and l[0] != '#']

        random.shuffle(urls)
        urls = list(enumerate(urls))

        self.workers = {
            loc: CaptureWorker(self.output_dir, loc, urls[:],
                               self.loop, self.args.workers_per_loc,
                               self.output_queue, self.args.quiet)

            for loc in self.proxies.locations.keys()
        }

    def proxy_sort_key(self, loc, method):
        # Consider locales with more work to do first.
        # Consider locales whose proxy is 'direct' first.
        # Consider locales named 'us' first.
        # As a final tie breaker use alphabetical order of locale name.
        return (-len(self.workers[loc].urls),
                method != 'direct',
                loc != 'us',
                loc)

    @asyncio.coroutine
    def run(self):
        with BrowserManager(...) as bmgr:
            self.bmgr = bmgr
            yield from self.proxies.run(self)
            if self.active:
                yield from asyncio.wait(self.active, loop=self.loop)
            yield from self.output_queue.put(None)
            yield from asyncio.wait_for(self.drainer, None)

    @asyncio.coroutine
    def proxy_online(self, proxy):
        self.active[proxy.loc] = self.loop.create_task(
            self.workers[proxy.loc].run(self.bmgr, proxy))

    @asyncio.coroutine
    def proxy_offline(self, proxy):
        job = self.active.get(proxy.loc)
        if job is not None:
            del self.active[proxy.loc]
            job.cancel()
            # swallow the cancellation exception
            try: yield from asyncio.wait_for(job, None)
            except: pass
