# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Capture the HTML content and a screenshot of each URL in a list,
from many locations simultaneously.  Locations are defined by the
config file passed as an argument, which is line- oriented, each line
having the general form

  locale method arguments ...

'locale' is an arbitrary word (consisting entirely of lowercase ASCII
letters) which names the location.

'method' selects a general method for capturing pages from this
location.  Subsequent 'arguments' are method-specific.  There are
currently two supported methods:

  direct: The controller machine will issue HTTP requests directly.
          No arguments.

  ovpn:   HTTP requests will be proxied via openvpn.
          One or more arguments are passed to the 'openvpn-netns'
          helper program (see scripts/openvpn-netns.c).  The initial
          argument is treated as a glob pattern which should expand to
          one or more OpenVPN config files; if there's more than one,
          they are placed in a random order and then used round-robin
          (i.e. if connection with one config file fails or drops, the
          next one is tried).

The second non-optional argument is the list of URLs to process, one per
line.

The third non-optional argument is the directory in which to store
results.  Each result will be written to its own file in this
directory; the directory hierarchy has the structure

  ${OUTPUT_DIR}/${RUN}/AB/CDE/FGH.${LOCALE}

where RUN starts at zero and is incremented by one each time the program is
invoked, and AB,CDE,FGH is a 8-digit decimal number assigned to each URL.
This number is not meaningful; you must look in each file to learn which
URL goes with which result.

The output files are binary; see CaptureResult.write_result for the format.
"""

def setup_argp(ap):
    ap.add_argument("locations",
                    action="store",
                    help="List of location specifications.")
    ap.add_argument("urls",
                    action="store",
                    help="List of URLs to process.")
    ap.add_argument("output_dir",
                    action="store",
                    help="Directory in which to store output.")
    ap.add_argument("-w", "--workers-per-location",
                    action="store", dest="workers_per_loc", type=int, default=8,
                    help="Maximum number of concurrent workers per location.")
    ap.add_argument("-W", "--total-workers",
                    action="store", dest="total_workers", type=int, default=40,
                    help="Total number of concurrent workers to use.")
    ap.add_argument("-p", "--max-simultaneous-proxies",
                    action="store", type=int, default=10,
                    help="Maximum number of proxies to use simultaneously.")

def run(args):
    if os.environ.get("PYTHONASYNCIODEBUG"):
        import logging
        logging.basicConfig(level=logging.DEBUG)
        import warnings
        warnings.simplefilter('default')
    loop = asyncio.get_event_loop()
    asyncio.get_child_watcher()
    loop.run_until_complete(CaptureDispatcher(args).run())
    loop.close()

import asyncio
import json
import os
import os.path
import random
import subprocess
import sys
import time
import zlib

# Note: the functions imported from url_database do not actually make
# use of the database.
from shared.url_database import canon_url_syntax, categorize_result_nr
from shared.aioproxies import ProxySet
from shared.strsignal import strsignal

pj_trace_redir = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../../scripts/pj-trace-redir.js"))

class CaptureResult:
    """The result of one capture job."""
    def __init__(self, url):
        self.status       = ""
        self.detail       = ""
        self.log          = {}
        self.canon_url    = ""
        self.content      = ""
        self.elapsed      = 0.

        # Make sure the URL is not so mangled that phantomjs is just going
        # to give up and report nothing at all.
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

    def set_result(self, exitcode, stdout, stderr, elapsed):
        # We parse stdout regardless of exit status, because sometimes
        # phantomjs prints a complete crawl result and _then_ crashes.
        valid_result = self.parse_stdout(stdout)

        # We only expect to get stuff on stderr with exit code 1.
        stderr = stderr.decode('utf-8').strip().splitlines()
        if exitcode == 1 and not valid_result:
            self.parse_stderr(stderr)
        else:
            if stderr:
                self.log["stderr"] = stderr

            if not self.status:
                self.status = "crawler failure"
                if exitcode > 1:
                    self.detail = "unexpected exit code {}".format(exitcode)
                elif exitcode >= 0:
                    self.detail = "exit {} with invalid output".format(exitcode)
                else:
                    self.detail = strsignal(-exitcode)

        self.status = categorize_result_nr(self.status, self.detail)
        self.elapsed = elapsed

    def parse_stdout(self, stdout):
        if not stdout:
            # This may get overridden later, by analysis of stderr.
            self.status = "crawler failure"
            self.detail = "no output from tracer"
            return False

        # The output, taken as a whole, should be one complete JSON object.
        try:
            stdout = stdout.decode('utf-8')
            results = json.loads(stdout)
            self.canon_url     = results["canon"]
            self.status        = results["status"]
            self.detail        = results.get("detail")
            if not self.detail:
                if self.status == "timeout":
                    self.detail = "timeout"
                else:
                    self.detail = self.status
                    self.status = "crawler failure"

            self.log['events'] = results.get("log",    [])
            self.log['chain']  = results.get("chain",  [])
            self.log['redirs'] = results.get("redirs", None)

            if 'content' in results:
                self.content = results['content']
            return True

        except:
            # There is some sort of bug causing junk to be emitted along
            # with the expected output.  We used to try to clean up after
            # this but that caused its own problems.  Just fail.
            if not isinstance(stdout, str):
                stdout = stdout.decode('utf-8', 'backslashreplace')
            self.log["stdout"] = stdout
            self.status = "crawler failure"
            self.detail = "garbage output from tracer"
            return False

    def parse_stderr(self, stderr):
        status = ""
        detail = ""
        anomalous_stderr = []

        for err in stderr:
            if err.startswith("isolate: phantomjs: "):
                # This is 'isolate' reporting the status of the child
                # process.  Certain fatal signals have predictable causes.

                rc = err[len("isolate: phantomjs: "):]
                if rc in ("Alarm clock", "CPU time limit exceeded"):
                    status = "timeout"
                    detail = rc

                else:
                    status = "crawler failure"
                    if rc in ("Segmentation fault", "Killed"):
                        # This is most likely to be caused by hitting the
                        # memory resource limit; webkit doesn't cope well.
                        detail = "out of memory"

                    elif rc == "Aborted":
                        # This happens after "bad_alloc", usually.
                        if not detail:
                            detail = rc
                    else:
                        detail = rc

            elif "bad_alloc" in err:
                # PJS's somewhat clumsy way of reporting memory
                # allocation failure.
                status = "crawler failure"
                detail = "out of memory"

            else:
                anomalous_stderr.append(err)

        if not status:
            status = "crawler failure"
            detail = "unexplained unsuccessful exit"

        self.status = status
        self.detail = detail

        if anomalous_stderr:
            self.log["stderr"] = anomalous_stderr

    def write_result(self, fname):
        """Write the results file for this URL, to FNAME.  Results files are
           binary, but the first chunk is mostly human-readable.

           The initial eight bytes of a result file are a magic number:

               7F 63 61 70 20 30 30 0A
               ^? c  a  p  SP 0  0  LF

           The zeroes constitute a two-digit version field.

           In version 00, exactly six LF-terminated lines of
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
           the compression.  (If either is *completely* empty, then it
           will be written out as zero bytes, rather than as the
           compression of zero bytes.)

           If any parent directory of FNAME does not exist it will be
           created.  It is an error if FNAME itself already exists.

        """
        fname = os.path.abspath(fname)
        os.makedirs(os.path.dirname(fname), exist_ok=True)

        with open(fname, "xb") as fp:
            if self.content:
                compressed_content = zlib.compress(
                    self.content.encode("utf-8"))
            else:
                compressed_content = b""

            if self.log:
                compressed_log = zlib.compress(
                    json.dumps(self.log).encode("utf-8"))
            else:
                compressed_log = b""

            fp.write("\u007Fcap 00\n"
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
def do_capture(url, proxy, loop):
    result = CaptureResult(url)
    if result.status:
        return result

    start = time.monotonic()
    proc = yield from asyncio.create_subprocess_exec(
        *proxy.adjust_command([
            "isolate",
            "ISOL_RL_MEM=unlimited",
            "ISOL_RL_STACK=8388608",
            "PHANTOMJS_DISABLE_CRASH_DUMPS=1",
            "MALLOC_CHECK_=0",
            "phantomjs",
            "--local-url-access=no",
            "--load-images=false",
            pj_trace_redir,
            "--capture",
            result.original_url
        ]),
        stdin  = subprocess.DEVNULL,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        loop   = loop)

    stdout, stderr = yield from proc.communicate()
    elapsed = time.monotonic() - start
    result.set_result(proc.returncode, stdout, stderr, elapsed)
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
                 loop, max_workers, global_bound, output_queue):
        self.output_dir   = output_dir
        self.locale       = locale
        self.urls         = urls
        self.loop         = loop
        self.max_workers  = max_workers
        self.global_bound = global_bound
        self.output_queue = output_queue

    def output_fname(self, serial):
        return "{}/{:02d}/{:03d}/{:03d}.{}".format(
            self.output_dir,
            serial // 1000000, (serial % 1000000) // 1000, serial % 1000,
            self.locale)

    @asyncio.coroutine
    def run_worker(self, proxy, i):
        """MAX_WORKERS instances of this coroutine are spawned by run()."""
        label = "{} {}: ".format(proxy.label(), i)
        while True:
            with (yield from self.global_bound), \
                 claim_one(self.urls) as task:

                if task is None: break
                (serial, url) = task

                sys.stderr.write(label + url + "...\n")
                try:
                    result = yield from do_capture(url, proxy, self.loop)
                    sys.stderr.write(label + url + ": " + result.status + "\n")
                except:
                    sys.stderr.write(label + url + ": fail\n")
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
    def run(self, proxy):
        # There is no point in running more workers than we have URLs
        # (left) to process.
        nworkers = min(self.max_workers, len(self.urls))

        # Unlike wait_for(), wait() does _not_ cancel everything it's
        # waiting for when it is itself cancelled.  Since that's what
        # we want, we have to do it by hand.
        try:
            workers = [self.loop.create_task(self.run_worker(proxy, i))
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
        self.global_bound = asyncio.Semaphore(args.total_workers)
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
                               self.global_bound, self.output_queue)

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
        yield from self.proxies.run(self)
        if self.active:
            yield from asyncio.wait(self.active, loop=self.loop)
        yield from self.output_queue.put(None)
        yield from asyncio.wait_for(self.drainer, None)

    @asyncio.coroutine
    def proxy_online(self, proxy):
        self.active[proxy.loc] = \
            self.loop.create_task(self.workers[proxy.loc].run(proxy))

    @asyncio.coroutine
    def proxy_offline(self, proxy):
        job = self.active.get(proxy.loc)
        if job is not None:
            del self.active[proxy.loc]
            job.cancel()
            # swallow the cancellation exception
            try: yield from asyncio.wait_for(job, None)
            except: pass
