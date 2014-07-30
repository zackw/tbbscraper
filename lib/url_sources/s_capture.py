# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Capture the HTML content and a screenshot of each URL in an existing
database.  This reads from the 'canon_urls' table (so 'canonize' needs to
be run first, or concurrently) and writes to the 'content_captures' table.

Captures can be farmed out to many different worker machines; if so, one
capture occurs for each unique TLD in the worker list.  Robust to workers
crashing or just plain not being around.  It must be possible to ssh into
each worker with no password, and once there, to use 'sudo' to execute
arbitrary commands as root with no password."""

def setup_argp(ap):
    ap.add_argument("-w", "--worker-list",
                    action="store", dest="worker_list",
                    help="List of worker machines: one DNS name per line.")
    ap.add_argument("-l", "--login",
                    action="store", dest="login", default=os.environ["LOGNAME"],
                    help="Login name to pass to ssh(1) connecting to workers.")
    ap.add_argument("-b", "--batch-size",
                    action="store", dest="batch_size", type=int, default=20,
                    help="Number of URLs to feed to each worker at once.")
    ap.add_argument("-p", "--min-proxy-port",
                    action="store", dest="min_proxy_port", type=int,
                    default=9100,
                    help="Low end of range of TCP ports to use for "
                    "local proxy listeners.")

def run(args):
    # must do this before creating threads
    locale.getpreferredencoding(True)

    Monitor(CaptureDispatcher(args),
            banner="Capturing content and screenshots of web pages")

import base64
import collections
import contextlib
import fcntl
import io
import itertools
import json
import locale
import os
import os.path
import queue
import random
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import traceback
import zlib

from psycopg2 import IntegrityError
from shared import url_database
from shared.monitor import Monitor
from shared.strsignal import strsignal

# Utilities

def queue_iter(q):
    """Generator which yields messages pulled from a queue.Queue in
       sequence, until empty.  Can block before yielding any items,
       but not after at least one item has been yielded.
    """
    yield q.get()
    try:
        while True:
            yield q.get(block=False)
    except queue.Empty:
        pass

def nonblocking_readlines(f):
    """Generator which yields lines from F (a file object, used only for
       its fileno()) without blocking.  If there is no data, you get an
       endless stream of empty strings until there is data again (caller
       is expected to sleep for a while).
       The lines emitted by this function do *not* have a trailing newline.
    """

    fd = f.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    enc = locale.getpreferredencoding(False)

    buf = bytearray()
    while True:
        try:
            block = os.read(fd, 8192)
        except BlockingIOError:
            yield ""
            continue

        if not block:
            if buf:
                yield buf.decode(enc)
                buf.clear()
            break

        buf.extend(block)

        while True:
            r = buf.find(b'\r')
            n = buf.find(b'\n')
            if r == -1 and n == -1: break

            if r == -1 or r > n:
                yield buf[:n].decode(enc)
                buf = buf[(n+1):]
            elif n == -1 or n > r:
                yield buf[:r].decode(enc)
                if n == r+1:
                    buf = buf[(r+2):]
                else:
                    buf = buf[(r+1):]

# Lifted from more_itertools:
_marker = object()
def chunked(iterable, n):
    """Break an iterable into lists of a given length::

    >>> list(chunked([1, 2, 3, 4, 5, 6, 7], 3))
    [[1, 2, 3], [4, 5, 6], [7]]

    If the length of ``iterable`` is not evenly divisible by ``n``, the last
    returned list will be shorter.

    This is useful for splitting up a computation on a large number of keys
    into batches, to be pickled and sent off to worker processes. One example
    is operations on rows in MySQL, which does not implement server-side
    cursors properly and would otherwise load the entire dataset into RAM on
    the client.

    """
    # Doesn't seem to run into any number-of-args limits.
    for group in itertools.zip_longest(*[iter(iterable)] * n,
                                       fillvalue=_marker):
        group = list(group)
        if group[-1] is _marker:
            # If this is the last group, shuck off the padding:
            del group[group.index(_marker):]
        yield group

# PhantomJS's internal PNG writer does not do a very good job of emitting
# compact PNGs, so we recompress them once we get them back, using
# 'optipng' (http://optipng.sourceforge.net/).  In testing, saves ~20% per
# image.  The -zc, -zs, -f options to the command below select a more useful
# range of its compression search space than the default.
def recompress_image(img):
    # this is the base64 encoding of the first six bytes of the PNG signature
    if img.startswith("iVBORw0KG"):
        img = base64.b64decode(img, validate=True)

    # this is the full 8-byte PNG signature
    if not img.startswith(b"\x89PNG\x0d\x0a\x1a\x0a"):
        raise ValueError("not a PNG image")

    with tempfile.NamedTemporaryFile(suffix=".png") as oldf:
        oldf.write(img)
        oldf.flush()

        # infuriatingly, optipng cannot be told to write *into* a file
        # that already exists; it will always do the rename-out-of-the-way
        # thing.  Thus there is an unfixable race condition here.
        newname = oldf.name.replace(".png", "_n.png")
        try:
            output = subprocess.check_output(
                [ "optipng", "-q", "-zc9", "-zs0,1,3", "-f0-5",
                  "-out", newname, oldf.name ],
                stdin=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
            if output:
                raise CaptureBatchError(0, "optipng", output=output)

            with open(newname, "rb") as newf:
                return newf.read()

        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(newname)

# End of utilities

class SshProxy:
    """Helper thread which runs and supervises an ssh-based local SOCKS
       proxy that tunnels traffic to another machine.

       Events are posted to the queue provided to the constructor,
       whenever the proxy becomes available or unavailable.
    """

    # Event status codes.  Events are 1-tuples of the status code.
    PROXY_OFFLINE = 0
    PROXY_ONLINE  = 1

    def __init__(self, port, host, login, queue):
        self._local_port   = port
        self._remote_host  = host
        self._remote_login = login
        self._queue        = queue
        self._proc         = None
        self._done         = False

    def report_status(self, msg):
        self._status = msg
        self._mon.report_status("ssh: {}: {}".format(self._remote_host, msg))

    def stop(self):
        if self._proc:
            self._proc.terminate()
            # Don't wait at this point, the pipe still needs draining.
        self._done = True

    def __call__(self, mon, thr):
        self._mon = mon
        backoff = 0
        while True:
            self.report_status("connecting...")
            self._proc = subprocess.Popen(
                ["ssh", "-2akNTxv", "-e", "none",
                 "-o", "BatchMode=yes", "-o", "ServerAliveInterval=30",
                 "-D", "localhost:" + str(self._local_port),
                 "-l", self._remote_login,
                 self._remote_host],
                stdin  = subprocess.DEVNULL,
                stdout = subprocess.DEVNULL,
                stderr = subprocess.PIPE)

            forced_disconnect = False

            def disconnect_hook():
                self._proc.terminate()
                self._proc.stderr.close()
                self._proc.wait()
                self._proc = None
                self._queue.put((self.PROXY_OFFLINE,))
                forced_disconnect = True

            for line in nonblocking_readlines(self._proc.stderr):
                if line == "":
                    self._mon.idle(1, disconnect_hook)
                    if forced_disconnect:
                        break
                    continue

                line = line.strip()
                #self.report_status(line)

                if (not line.startswith("debug1:") and
                    not line.startswith("Transferred") and
                    not line.startswith("OpenSSH_") and
                    not line.endswith("closed by remote host.")):
                    self.report_status(line)

                if line == "debug1: Entering interactive session.":
                    self.report_status("online.")
                    backoff = 0
                    self._queue.put((self.PROXY_ONLINE,))

                self._mon.maybe_pause_or_stop(disconnect_hook)
                if forced_disconnect:
                    break

            # If self._done is true at this point we should just exit as
            # quickly as possible.
            if self._done:
                self.report_status("shut down.")
                self._proc.wait()
                self._proc = None
                break

            # If forced_disconnect is true at this point it means we
            # killed the proxy because the monitor told us to suspend,
            # and the suspension is now over.  So we should restart the
            # proxy immediately.
            if forced_disconnect:
                continue

            # EOF on stderr means ssh has exited.
            self._queue.put((self.PROXY_OFFLINE,))

            last_status = self._status
            if last_status == "online.":
                last_status = "disconnected."

            rc = self._proc.wait()
            self._proc = None
            if rc < 0:
                last_status += " (ssh: {})".format(strsignal(-rc))
            elif rc > 0:
                last_status += " (ssh: exit {})".format(rc)

            # exponential backoff, starting at 15 minutes and going up to
            # eight hours
            idletime = 2**backoff * 15 * 60
            if idletime < 3600:
                human_idletime = str(idletime / 60) + " minutes"
            elif idletime == 3600:
                human_idletime = "1 hour"
            else:
                human_idletime = "{:.2g} hours".format(idletime/3600.).strip()

            self.report_status("{} (retry in {})"
                               .format(last_status, human_idletime))
            if backoff < 6:
                backoff += 1

            self._mon.idle(idletime)
            if self._done:
                self.report_status("shut down.")
                break


class CaptureBatchError(subprocess.SubprocessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output
        self.stderr = stderr
    def __str__(self):
        if self.returncode:
            text = ("Command '{}' returned non-zero exit status {}"
                    .format(self.cmd, self.returncode))
            if self.output is not None or self.stderr is not None:
                text += " and unexpected output"
        else:
            text = ("Command '{}' exited with unexpected output"
                    .format(self.cmd))
        if self.output is not None:
            text += "\nstdout:\n"
            text += textwrap.indent(self.output, "| ", lambda line: True)
        if self.stderr is not None:
            text += "\nstderr:\n"
            text += textwrap.indent(self.stderr, "| ", lambda line: True)
        return text

# note: very similar to code in s_canonize.py (should be collapsed together)

pj_trace_redir = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../../scripts/pj-trace-redir.js"))

_stdout_junk_re = re.compile(
    r"^(?:"
    r"|[A-Z][a-z]+Error: .*"
    r"|[A-Z_]+?_ERR: .*"
    r"|Cannot init XMLHttpRequest object!"
    r"|Error requesting /.*"
    r"|Current location: https?://.*"
    r"|  (?:https?://.*?|undefined)?:[0-9]+(?: in \S+)?"
    r")$")

class CaptureTask:
    """Representation of one capture job."""
    def __init__(self, url, proxyport):
        self.proc         = None
        self.original_url = url
        self.canon_url    = None
        self.status       = None
        self.detail       = None
        self.anomaly      = {}
        self.content      = None
        self.render       = None

        # Attempt a DNS lookup for the URL's hostname right now.  This
        # preloads the DNS cache, reduces overhead in the surprisingly
        # common case where the hostname is not found (2.85%), and most
        # importantly, catches the rare URL that is *so* mangled that
        # phantomjs just gives up and reports nothing at all.
        try:
            url = url_database.canon_url_syntax(url, want_splitresult = True)
            dummy = socket.getaddrinfo(url.hostname, 80, proto=socket.SOL_TCP)
            self.original_url = url.geturl()

        except ValueError as e:
            self.status = 'invalid URL'
            self.detail = str(e)
            return

        except socket.gaierror as e:
            if e.errno not in (socket.EAI_NONAME, socket.EAI_NODATA):
                raise
            self.status = 'hostname not found'
            return

        # We use a temporary file for the results, instead of a pipe,
        # so we don't have to worry about reading them until after the
        # child process exits.
        self.result_fd = tempfile.TemporaryFile("w+t", encoding="utf-8")
        self.errors_fd = tempfile.TemporaryFile("w+t", encoding="utf-8")
        self.proc = subprocess.Popen([
                "isolate",
                "env", "PHANTOMJS_DISABLE_CRASH_DUMPS=1", "MALLOC_CHECK_=0",
                "phantomjs",
                "--ssl-protocol=any",
                "--proxy-type=socks5",
                "--proxy=localhost:{}".format(proxyport),
                pj_trace_redir,
                "--capture",
                self.original_url
            ],
            stdin=subprocess.DEVNULL,
            stdout=self.result_fd,
            stderr=self.errors_fd)

    def parse_stdout(self, stdout):
        # Under conditions which are presently unclear, PhantomJS dumps
        # javascript console errors to stdout despite script logic which
        # is supposed to intercept them; so we need to scan through all
        # lines of output looking for something with the expected form.
        if not stdout:
            self.status = "crawler failure"
            self.detail = "no output from tracer"
            return False

        anomalous_stdout = []
        for line in stdout.strip().split("\n"):
            if _stdout_junk_re.match(line):
                continue

            try:
                results = json.loads(line)

                self.canon_url = results["canon"]
                self.status    = results["status"]
                self.detail    = results.get("detail", None)
                self.anomaly.update(results.get("log", {}))
                if 'content' in results:
                    self.content = zlib.compress(results['content']
                                                 .encode('utf-8'))
                if 'render' in results:
                    self.render = recompress_image(results['render'])

            except:
                anomalous_stdout.append(line)

        if anomalous_stdout:
            self.anomaly["stdout"] = anomalous_stdout
        if not self.status:
            self.status = "garbage output from tracer"
            return False
        return True

    def parse_stderr(self, stderr, valid_result):
        status = None
        anomalous_stderr = []

        for err in stderr:
            if err.startswith("isolate: env: "):
                # This is 'isolate' reporting the status of
                # the child process.  Certain signals are expected.

                status = err[len("isolate: env: "):]
                if status in ("Alarm clock", "Killed",
                              "CPU time limit exceeded"):
                    status = "timeout"

                # PJS occasionally segfaults on exit.  If there is a
                # valid report on stdout, don't count it as a crash.
                else:
                    if status != "Segmentation fault" or not valid_result:
                        self.detail = status
                        status = "crawler failure"

            elif "bad_alloc" in err:
                # PJS's somewhat clumsy way of reporting memory
                # allocation failure.
                if not status:
                    self.detail = "out of memory"
                    status = "crawler failure"
            else:
                anomalous_stderr.append(err)

        if not valid_result:
            if not status:
                status = "unexplained exit code 1";
            self.status = status

        if anomalous_stderr:
            self.anomaly["stderr"] = anomalous_stderr
        elif "stderr" in self.anomaly:
            del self.anomaly["stderr"]

    def pickup_results(self):

        if self.proc is None:
            return
        status = self.proc.wait()

        self.result_fd.seek(0)
        stdout = self.result_fd.read()
        self.result_fd.close()
        self.errors_fd.seek(0)
        stderr = self.errors_fd.read()
        self.result_fd.close()

        valid_result = self.parse_stdout(stdout)
        stderr = stderr.strip().split("\n")
        if stderr and (len(stderr) > 1 or stderr[0] != ''):
            self.anomaly["stderr"] = stderr

        if status == 0:
            pass
        elif status == 1:
            self.parse_stderr(stderr, valid_result)
        elif status > 0:
            self.status = "crawler failure"
            self.detail = "unexpected exit code {}".format(status)
        else:
            self.status = "crawler failure"
            self.detail = "Killed by " + strsignal(-status)

    def report(self):
        self.pickup_results()
        r = {
            'ourl':   self.original_url,
            'status': self.status,
            'detail': self.detail
        }
        if self.canon_url: r['canon'] = self.canon_url
        if self.anomaly:   r['anomaly'] = self.anomaly
        if self.content:   r['content'] = self.content
        if self.render:    r['render'] = self.render
        return r

class CaptureWorker:
    def __init__(self, disp, locale, hostname, port):
        self.disp = disp
        self.args = disp.args
        self.locale = locale
        self.hostname = hostname
        self.port = port
        self.batch_queue = queue.PriorityQueue()
        self.batch_queue_serializer = 0

        self.online = False
        self.nsuccess = 0
        self.nfailure = 0
        self.batch_time = None
        self.batch_avg  = None
        self.alpha = 2/11

    def report_status(self, msg):
        status = ("{}: {} | {} captured, {} failures"
                  .format(self.locale, self.hostname,
                          self.nsuccess, self.nfailure))

        if self.batch_time is not None:
            if self.batch_avg is None:
                self.batch_avg = self.batch_time
            else:
                self.batch_avg = \
                    self.batch_avg * (1-self.alpha) + \
                    self.batch_time * self.alpha
            status += (" | last batch {:.2f} sec/URL; avg: {:.2f} sec/URL"
                       .format(self.batch_time, self.batch_avg))

        self.mon.report_status(status + " | " + msg)


    # batch queue messages
    _MON_SAYS_STOP  = -1
    _PROXY_OFFLINE  = SshProxy.PROXY_OFFLINE  # known to be 0
    _PROXY_ONLINE   = SshProxy.PROXY_ONLINE   # known to be 1
    _CAPTURE_BATCH  = 10

    # dispatcher-to-worker API: this function is converted to a bound
    # method and handed to the dispatcher every time we call
    # request_batch().
    def queue_batch(self, batch):
        # Entries in a PriorityQueue must be totally ordered.
        # We don't care about the relative ordering of capture batches
        # and we don't want Python to waste time sorting them, so we
        # give those messages a serial number.
        self.batch_queue_serializer += 1
        self.batch_queue.put((self._CAPTURE_BATCH,
                              self.batch_queue_serializer,
                              batch))


    def __call__(self, mon, thr):
        self.mon = mon
        self.mon.register_event_queue(self.batch_queue, (self._MON_SAYS_STOP,))

        proxy_thread = SshProxy(self.port, self.hostname, self.args.login,
                                self.batch_queue)
        self.mon.add_work_thread(proxy_thread)

        self.report_status("waiting for proxy...")
        while True:
            msg = self.batch_queue.get()
            if msg[0] == self._MON_SAYS_STOP:
                self.mon.maybe_pause_or_stop()

            elif msg[0] == self._PROXY_ONLINE:
                self.online = True
                self.report_status("waiting for batch...")

            elif msg[0] == self._PROXY_OFFLINE:
                self.online = False
                self.report_status("waiting for proxy...")

            elif msg[0] == self._CAPTURE_BATCH:
                if len(msg[2]) == 0:
                    # No more work to do.
                    self.report_status("done")
                    proxy_thread.stop()
                    self.disp.complete_batch(self.locale, [])
                    break

                if self.online:
                    self.process_batch(msg[2])
                else:
                    self.disp.fail_batch(self.locale, msg[2])

            else:
                raise RuntimeError("invalid batch queue message {!r}"
                                   .format(msg))

            if self.online and self.batch_queue.empty():
                self.disp.request_batch(self.locale, self.queue_batch)

    def process_batch(self, batch):
        batchsize = len(batch)
        todo = collections.deque(batch)
        completed = collections.deque()

        start = time.time()

        # The appearance of any message on the batch queue means we need
        # to stop.
        while todo and self.batch_queue.empty():
            self.report_status("processing batch ({}/{})..."
                               .format(len(completed), batchsize))

            report = CaptureTask(todo.popleft(), self.port).report()
            completed.append(report)
            if 'content' in report:
                self.nsuccess += 1
            else:
                self.nfailure += 1

        stop = time.time()

        # Anything left in 'todo' needs to be pushed back to the dispatcher;
        # anything we have successfully completed needs to be recorded.
        if completed:
            self.batch_time = (stop - start)/len(completed)
            self.disp.complete_batch(self.locale, list(completed))

        if todo:
            self.disp.fail_batch(self.locale, todo)


class CaptureDispatcher:
    def __init__(self, args):
        self.args = args
        self.read_worker_list()
        self.error_log = open("capture-errors.txt", "wt")
        # defer further setup till we're on the right thread

    def __call__(self, mon, thr):
        self.mon = mon
        self.db = url_database.ensure_database(self.args)
        self.prepared_batches = {}
        self.processing = {}
        self.status_queue = queue.PriorityQueue()
        self.status_queue_serializer = 0
        self.mon.register_event_queue(self.status_queue,
                                      (self._MON_SAYS_STOP, -1))
        self.anomalies = open("capture-anomalies.txt", "w")

        # We can safely start the workers and get them connecting
        # before we go on to warm up the database.
        port = self.args.min_proxy_port
        self.workers = {}
        for tld, workers in self.locales.items():
            self.workers[tld] = []
            self.prepared_batches[tld] = collections.deque()
            self.processing[tld] = set()
            for n, w in enumerate(workers):
                wt = CaptureWorker(self, tld, w, port)
                self.mon.add_work_thread(wt)
                self.workers[tld].append(wt)
                port += 1

        self.prepare_database()
        self.dispatcher_loop()

    def read_worker_list(self):
        locales = collections.defaultdict(list)
        with open(self.args.worker_list) as f:
            for w in f:
                w = w.strip()
                if not w: continue
                if w[0] == '#': continue
                tld = w[w.rfind(".")+1 : ]
                locales[tld].append(w)
        self.locales = locales
        self.locale_list = list(self.locales.keys())

    # Status queue helper constants and methods.
    _COMPLETE      = 0
    _FAILED        = 1
    _MON_SAYS_STOP = 2 # Stop after handling all incoming work,
                       # but before pushing new work.
    _REQUEST       = 3

    # Entries in a PriorityQueue must be totally ordered.
    # We just want to service all COMPLETE and FAILED messages
    # ahead of all REQUEST messages, so give them all a serial number
    # which goes in the tuple right after the command code, before the data.
    # This also means we don't have to worry about unsortable data.
    def oq(self):
        self.status_queue_serializer += 1
        return self.status_queue_serializer

    # worker-to-dispatcher API
    def request_batch(self, locale, callback):
        self.status_queue.put((self._REQUEST, self.oq(), locale, callback))

    def complete_batch(self, locale, results):
        self.status_queue.put((self._COMPLETE, self.oq(), locale, results))

    def fail_batch(self, locale, batch):
        self.status_queue.put((self._FAILED, self.oq(), locale, batch))

    def log_worker_exc(self, locale, hostname, exctype, tb):
        self.error_log.write("Exception in worker {}:{}: {}\n"
                             .format(locale, hostname, exctype))
        self.error_log.write(tb)
        self.error_log.write('\n')
        self.error_log.flush()

    def dispatcher_loop(self):

        handlers = {
            self._REQUEST      : self.handle_request_batch,
            self._FAILED       : self.handle_failed_batch,
            self._COMPLETE     : self.handle_complete_batch,
            self._MON_SAYS_STOP: self.handle_stop,
        }
        def no_handler(*cmd):
            raise RuntimeError("invalid status queue message {!r}".format(cmd))

        while not self.mon.caller_is_only_active_thread():
            self.update_progress_statistics()
            for cmd in queue_iter(self.status_queue):
                handlers.get(cmd[0], no_handler)(*cmd)

    def handle_stop(self, *unused):
        self.mon.maybe_pause_or_stop()

    def handle_failed_batch(self, cmd, serial, locale, batch):
        self.prepared_batches[locale].append(batch)

    def handle_request_batch(self, cmd, serial, locale, callback):
        prepared = self.prepared_batches[locale]
        if not prepared:
            self.refill_prepared(locale, prepared)

        batch = prepared.pop()
        # If this batch is empty, that means we're done with this locale,
        # and we should push a finish message to every worker for this locale.
        # (If we don't do this, workers that are stuck trying to connect to
        # their proxy will never terminate.)
        if not batch:
            for w in self.workers[locale]:
                w.queue_batch(batch)
        else:
            callback(batch)

    def refill_prepared(self, locale, prepared):
        with self.db, self.db.cursor() as cr:

            ix = self.locale_index[locale]
            if ix >= self.jobsize:
                prepared.append([])
                return

            blocksize = self.args.batch_size * len(self.locales[locale])
            block = []
            while len(block) < blocksize:
                candidates = self.urls_to_do[ix:ix+blocksize]
                cr.execute("SELECT url FROM captured_urls"
                           "  WHERE locale = %s AND url = ANY(%s)",
                           (locale, candidates))
                for row in cr:
                    candidates.remove(row[0])

                if len(candidates) + len(block) <= blocksize:
                    block.extend(candidates)
                    ix += blocksize
                else:
                    block.extend(candidates[:blocksize - len(block)])
                    assert len(block) == blocksize
                    ix = self.urls_to_do.index(candidates[blocksize-len(block)],
                                               ix, ix+blocksize)

                if ix >= self.jobsize:
                    break

            self.locale_index[locale] = ix
            if not block:
                prepared.append([])
                return

            cr.execute("SELECT id, url FROM url_strings WHERE id = ANY(%s)",
                       (block,))

            uids = []
            urls = []
            for row in cr:
                uids.append(row.id)
                urls.append(row.url)

            prepared.extend(chunked(urls, self.args.batch_size))
            self.processing[locale] |= set(uids)

    def handle_complete_batch(self, cmd, serial, locale, results):
        with self.db, self.db.cursor() as cr:
            finished_urls = set()
            for r in results:

                if 'anomaly' in r:
                    self.anomalies.write(json.dumps(r['anomaly']))
                    self.anomalies.write('\n')
                    self.anomalies.flush()

                (url_id, surl) = url_database.add_url_string(cr, r['ourl'])
                redir_url_id = None
                if 'canon' in r:
                    redir_url = r['canon']
                    if redir_url == surl or redir_url == r['ourl']:
                        redir_url_id = url_id
                    elif redir_url is not None:
                        (redir_url_id, _) = \
                            url_database.add_url_string(cr, r['canon'])

                detail_id = self.canon_statuses.get(r['detail'])
                if detail_id is None and r['detail'] is not None:
                    cr.execute("INSERT INTO canon_statuses(id, detail) "
                               "  VALUES(DEFAULT, %s)"
                               "  RETURNING id", (r['detail'],))
                    detail_id = cr.fetchone()[0]
                    self.canon_statuses[r['detail']] = detail_id

                (_, result) = url_database.categorize_result(r['status'],
                                                             url_id,
                                                             redir_url_id)

                finished_urls.add(url_id)

                # stopgap OR IGNORE
                # not sure why duplicates
                cr.execute("SAVEPOINT ins1")
                try:
                    cr.execute("INSERT INTO captured_urls "
                               "(locale, url, access_time, result, "
                               " detail, redir_url, html_content, screenshot)"
                               "VALUES ("
                               "  %(locale)s,"
                               "  %(url)s,"
                               "  TIMESTAMP 'now',"
                               "  %(result)s,"
                               "  %(detail)s,"
                               "  %(redir_url)s,"
                               "  %(html_content)s,"
                               "  %(screenshot)s)",
                               {
                                    "locale":       locale,
                                    "url":          url_id,
                                    "result":       result,
                                    "detail":       detail_id,
                                    "redir_url":    redir_url_id,
                                    "html_content": r.get('content'),
                                    "screenshot":   r.get('render')
                               })
                except IntegrityError:
                    cr.execute("ROLLBACK TO SAVEPOINT ins1")
                else:
                    cr.execute("RELEASE SAVEPOINT ins1")

            self.processing[locale] -= finished_urls

    def update_progress_statistics(self):
        with self.db, self.db.cursor() as cr:
            cr.execute("SELECT COUNT(locale) AS nlocales"
                       "  FROM captured_urls c, capture_ptd p"
                       "  WHERE result IS NOT NULL"
                       "  AND locale = ANY(%s)"
                       "  AND c.url = p.url"
                       "     GROUP BY c.url",
                       (self.locale_list,))

            per_locale = [0]*(len(self.locales)+1)
            for row in cr:
                nloc = row[0]
                assert 1 <= nloc < len(per_locale)
                per_locale[nloc] += 1
            per_locale[0] = self.complete_jobsize - sum(per_locale)

        self.mon.report_status("Processing {}/{} URLs | "
                               .format(self.jobsize, self.complete_jobsize) +
                               " ".join("{}:{}".format(i, n)
                                        for i,n in enumerate(per_locale)))

    def prepare_database(self):
        with self.db, self.db.cursor() as cr:
            # Cache the status table in memory; it's reasonably small.
            self.mon.report_status("Loading database... (canon statuses)")
            cr.execute("SELECT detail, id FROM canon_statuses;")
            self.canon_statuses = { row.detail: row.id for row in cr }

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (analyzing)")
            cr.execute("ANALYZE canon_urls")
            cr.execute("ANALYZE captured_urls")

            # Identify all of the URLs that have yet to be entered into
            # the captured_urls table for all of the locales we care about.
            # This _can_ be done in one query, but it's painfully slow.
            # Adding DISTINCTs to the queries makes them slower.
            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (potentially to do)")
            cr.execute("CREATE TEMPORARY TABLE capture_ptd AS"
                       " SELECT DISTINCT canon AS url"
                       " FROM canon_urls, urls_citizenlab"
                       " WHERE canon_urls.url = urls_citizenlab.url"
                       "   AND canon IS NOT NULL"
                       "   AND result IN ('ok', 'ok (redirected)')")
            cr.execute("CREATE INDEX capture_ptd__url ON capture_ptd(url)")
            cr.execute("SELECT url FROM capture_ptd")
            all_canon_urls = set(row[0] for row in cr)
            self.complete_jobsize = len(all_canon_urls)

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (already done)")
            cr.execute("SELECT c.url FROM"
                       "  captured_urls c,"
                       "  capture_ptd p"
                       " WHERE locale = ANY(%s)"
                       "   AND c.url = p.url"
                       " GROUP BY c.url"
                       " HAVING COUNT(locale) = %s",
                       (self.locale_list, len(self.locale_list)))
            all_captured_urls = set(row[0] for row in cr)

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (to do)")
            all_canon_urls -= all_captured_urls

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (shuffling)")
            self.urls_to_do = list(all_canon_urls)
            random.shuffle(self.urls_to_do)
            self.jobsize = len(self.urls_to_do)
            self.locale_index = { loc: 0 for loc in self.locales }
