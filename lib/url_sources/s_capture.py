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
    ap.add_argument("-t", "--template-dir",
                    action="store", dest="template_dir",
                    help="Template directory for workers.  The contents of "
                    "this directory are rsynced to each worker every time it "
                    "is contacted with a new batch of URLs to process.")
    ap.add_argument("-b", "--batch-size",
                    action="store", dest="batch_size", type=int, default=20,
                    help="Number of URLs to feed to each worker at once.")

def run(args):
    Monitor(CaptureDispatcher(args),
            banner="Capturing content and screenshots of web pages")

import base64
import collections
import contextlib
import io
import itertools
import json
import os
import os.path
import queue
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import traceback
import zlib

from shared import url_database
from shared.monitor import Monitor

# Utilities

def rsync_quote(s):
    return shlex.quote(s.replace("%", "%%"))

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
    for group in itertools.zip_longest(*[iter(iterable)] * n, fillvalue=_marker):
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

# The zlib module - even in the latest 3.x - does not have filelikes.
# Hulk is not amused.  This code verrry vaguely based on
# http://effbot.org/librarybook/zlib/zlib-example-4.py.
# Extra special "gee thanks" to the Python core team for the complete
# absence of "how to write a custom IO class" in the io module documentation.

class ZlibRawInput(io.RawIOBase):
    def __init__(self, fp):
        self.fp = fp
        self.z  = zlib.decompressobj()
        self.data = bytearray()

    def __fill(self, nbytes):
        if self.z is None:
            return
        while len(self.data) < nbytes:
            data = self.fp.read(16384)
            if data:
                self.data.extend(self.z.decompress(data))
            else:
                self.data.extend(self.z.flush())
                self.fp.close()
                self.z = None # end of file reached
                break

    def readinto(self, buf):
        want = len(buf)
        self.__fill(want)
        have = len(self.data)

        if want <= have:
            buf[:] = self.data[:want]
            del self.data[:want]
            return want
        else:
            buf[:have] = self.data
            del self.data[:have]
            return have

    def readable(self):
        return True

def ZlibTextInput(fp, encoding="utf-8"):
    return io.TextIOWrapper(io.BufferedReader(ZlibRawInput(fp)),
                            encoding=encoding)

# End of utilities

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

class CaptureWorker:
    def __init__(self, disp, locale, hostname):
        self.disp = disp
        self.args = disp.args
        self.locale = locale
        self.hostname = hostname

        self.batch_queue = queue.Queue()
        self.ssh_master_socket = os.path.join(disp.sshsockdir,
                                              hostname + "_sock")
        self.ssh_master_active = False

        self.nsuccess = 0
        self.nfailure = 0
        self.batch_time = None
        self.batch_avg  = None
        self.alpha = 2/11

    def __call__(self, mon, thr):
        self.mon = mon
        self.mon.register_event_queue(self.batch_queue, [])
        try:
            while True:
                self.connect_to_worker_host()
                try:
                    self.prep_worker_host()
                    self.process_batches()
                    break

                except Exception as e:
                    excname = type(e).__name__
                    self.report_status(excname + " (retry in 5 minutes)")
                    self.disp.log_worker_exc(self.locale,
                                             self.hostname,
                                             excname,
                                             traceback.format_exc())
                    self.disconnect()
                    self.mon.idle(5 * 60)

        finally:
            self.disconnect()


    def report_status(self, msg):
        self.mon.report_status("{}: {}: {}".format(self.locale,
                                                   self.hostname,
                                                   msg))

    def summarize_progress(self):
        if self.batch_time is None:
            self.report_status("{} captured, {} failures"
                               .format(self.nsuccess,
                                       self.nfailure))
        else:
            if self.batch_avg is None:
                self.batch_avg = self.batch_time
            else:
                self.batch_avg = \
                    self.batch_avg * (1-self.alpha) + \
                    self.batch_time * self.alpha

            self.report_status("{} captured, {} failures in {:.2f} seconds "
                               " (moving average: {:.2f} seconds)"
                               .format(self.nsuccess,
                                       self.nfailure,
                                       self.batch_time,
                                       self.batch_avg))

    def connect_to_worker_host(self):
        backoff = 0
        self.ssh_master_active = False
        while True:
            self.report_status("connecting...")
            with tempfile.TemporaryFile(mode="w+t", prefix="errs") as errf:
                status = subprocess.call(
                    ["ssh", "-2afkNMTx", "-e", "none",
                     "-o", "BatchMode=yes", "-o", "ServerAliveInterval=30",
                     "-l", self.args.login,
                     "-S", self.ssh_master_socket,
                     self.hostname
                    ],
                    stdin  = subprocess.DEVNULL,
                    stdout = subprocess.DEVNULL,
                    stderr = errf
                )
                if not status:
                    self.report_status("connected.")
                    self.ssh_master_active = True
                    return

                errf.seek(0)
                errtext = errf.readlines()[-1]
                errtext = errtext[errtext.rfind(':')+1 : ].strip()

            # exponential backoff, starting at 15 minutes and going up to
            # eight hours
            idletime = 2**backoff * 15 * 60
            if idletime < 3600:
                human_idletime = str(idletime / 60) + " minutes"
            elif idletime == 3600:
                human_idletime = "1 hour"
            else:
                human_idletime = "{:.2g} hours".format(idletime/3600.).strip()

            self.report_status("connection failed: {} (retry in {})"
                               .format(errtext, human_idletime))

            self.mon.idle(idletime)
            if backoff < 6:
                backoff += 1

    def disconnect(self):
        if self.ssh_master_active:
            subprocess.call(["ssh", "-S", self.ssh_master_socket,
                             "-O", "stop", "."],
                            stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)

    def prep_worker_host(self):
        self.report_status("preparing worker environment...")
        with tempfile.NamedTemporaryFile(prefix="rsync-log-") as logfile, \
             tempfile.TemporaryFile(prefix="rsync-chatter-") as chatfile:
            rc = subprocess.call([
                "rsync",
                # Since there seems to be no way to tell ssh not to connect
                # directly if the multiplexor socket is unavailable, repeat
                # all our connection parameters here.
                # rsync-ssh-sudo is instead of having "%H sudo -n --" at the
                # end of the command line, because %H doesn't actually work
                # in -e, despite what the documentation says.
                "-e", ("rsync-ssh-sudo -2akTx -e none "
                       "-o BatchMode=yes -o ServerAliveInterval=30 "
                       "-S {} -l {}"
                       .format(rsync_quote(self.ssh_master_socket),
                               rsync_quote(self.args.login))),
                "-acmqz",
                "--log-file", logfile.name,
                self.args.template_dir + "/", self.hostname + ":/"],
                    stdin=subprocess.DEVNULL,
                    stdout=chatfile,
                    stderr=subprocess.STDOUT
            )
            chatfile.seek(0)
            chatter = chatfile.read().decode("utf-8")
            if rc != 0 or chatter != "":
                logfile.seek(0)
                raise CaptureBatchError(rc, "rsync",
                                        logfile.read().decode("utf-8"),
                                        chatter)

    def process_batches(self):
        batch = None
        try:
            while True:
                self.summarize_progress()
                self.mon.maybe_pause_or_stop()
                self.disp.request_batch(self.locale, self.batch_queue)

                batch = self.batch_queue.get()
                if len(batch) == 0:
                    self.disp.complete_batch(self.locale, [])
                    break

                self.process_batch(batch)
                batch = None

        except:
            if batch:
                self.disp.fail_batch(self.locale, batch)
            raise

    def process_batch(self, batch):

        def recompress_result(obj):
            if 'content' in obj:
                obj['content'] = zlib.compress(obj['content'].encode('utf-8'))
            if 'render' in obj:
                obj['render'] = recompress_image(obj['render'])
            return obj

        start = time.time()

        proc = subprocess.Popen(
            [
                # As above, we have to repeat all of the connection
                # parameters because there is no way to prevent
                # ssh from attempting a direct connection.
                "ssh", "-2akTx", "-e", "none",
                "-o", "BatchMode=yes", "-o", "ServerAliveInterval=30",
                "-S", self.ssh_master_socket,
                "-l", self.args.login,
                self.hostname,
                "python", "/usr/local/bin/planetlab-worker.py"
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        # Input to planetlab-worker.py is a zlib-compressed list of
        # URLs, one per line.  Output is a zlib-compressed JSON array.
        # There isn't supposed to be anything on stderr, and it consumes
        # its stdin completely before it produces any output.
        proc.stdin.write(zlib.compress("\n".join(batch).encode("ascii")))
        proc.stdin.close()

        problems = {}

        try:
            data = ZlibTextInput(proc.stdout).read()
            results = json.loads(data, object_hook=recompress_result)

        except Exception:
            problems['output'] = \
                "Output decoding error:\n" + traceback.format_exc() + \
                "Data:\n" + data + "\n"

        stderr = proc.stderr.read()
        if stderr:
            problems['stderr'] = stderr.decode("utf-8")

        rc = proc.wait()
        if rc or problems:
            problems['returncode'] = rc
            problems['cmd'] = proc.args
            raise CaptureBatchError(**problems)

        stop = time.time()
        self.batch_time = stop - start

        for r in results:
            if r.get('render'):
                self.nsuccess += 1
            else:
                self.nfailure += 1

        self.disp.complete_batch(self.locale, results)


class CaptureDispatcher:
    def __init__(self, args):
        self.args = args
        self.read_worker_list()
        self.sshsockdir = None
        self.error_log = open("capture-errors.txt", "wt")
        # defer further setup till we're on the right thread

    def __call__(self, mon, thr):
        self.mon = mon
        self.db = url_database.ensure_database(self.args)
        self.prepared_batches = {}
        self.processing = {}
        self.per_locale = [0]*(len(self.locales)+1)
        self.status_queue = queue.PriorityQueue()
        self.status_queue_serializer = 0
        self.mon.register_event_queue(self.status_queue,
                                      (self._MON_SAYS_STOP, -1))
        self.prepare_database()

        try:
            self.sshsockdir = tempfile.mkdtemp(prefix="capture-control")

            for tld, workers in self.locales.items():
                self.prepared_batches[tld] = collections.deque()
                self.processing[tld] = set()
                for w in workers:
                    self.mon.add_work_thread(CaptureWorker(self, tld, w))

            self.dispatcher_loop()

        finally:
            if self.sshsockdir is not None:
                shutil.rmtree(self.sshsockdir)

    def read_worker_list(self):
        locales = collections.defaultdict(list)
        with open(self.args.worker_list) as f:
            for w in f:
                w = w.strip()
                if not w: continue
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
    def request_batch(self, locale, reply_q):
        self.status_queue.put((self._REQUEST, self.oq(), locale, reply_q))

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

    def handle_request_batch(self, cmd, serial, locale, reply_q):
        prepared = self.prepared_batches[locale]
        if not prepared:
            self.refill_prepared(locale, prepared)

        reply_q.put(prepared.pop())

    def refill_prepared(self, locale, prepared):
        with self.db, self.db.cursor() as cr:
            cr.execute(
                "SELECT u.url AS uid, s.url AS url"
                "  FROM      captured_urls u,"
                "       capture_randomizer r,"
                "              url_strings s"
                "  WHERE u.locale = %(locale)s"
                "    AND u.result IS NULL"
                "    AND u.url <> ALL(%(processing)s)"
                "    AND u.url = s.id"
                "    AND u.url = r.url"
                " ORDER BY r.ix"
                "    LIMIT %(limit)s",
                { "locale": locale,
                  "processing": list(self.processing[locale]),
                  "limit": self.args.batch_size * len(self.locales[locale])
                })

            uids = []
            urls = []
            for row in cr:
                uids.append(row.uid)
                urls.append(row.url)

            if not urls:
                prepared.append([])
                return

            prepared.extend(chunked(urls, self.args.batch_size))
            self.processing[locale] |= set(uids)

    def handle_complete_batch(self, cmd, serial, locale, raw_results):
        with self.db, self.db.cursor() as cr:
            results = []
            finished_urls = set()
            for r in raw_results:
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

                results.append({
                    "locale":       locale,
                    "url":          url_id,
                    "result":       result,
                    "detail":       detail_id,
                    "redir_url":    redir_url_id,
                    "html_content": r.get('content'),
                    "screenshot":   r.get('render')
                })
                finished_urls.add(url_id)

            cr.executemany("UPDATE captured_urls "
                           "SET access_time = TIMESTAMP 'now',"
                           "         result = %(result)s,"
                           "         detail = %(detail)s,"
                           "      redir_url = %(redir_url)s,"
                           "   html_content = %(html_content)s,"
                           "     screenshot = %(screenshot)s "
                           ""
                           "WHERE    locale = %(locale)s"
                           "AND         url = %(url)s",
                           results)

            self.processing[locale] -= finished_urls

    def update_progress_statistics(self):
        with self.db, self.db.cursor() as cr:
            cr.execute("SELECT nlocales, COUNT(*) FROM"
                       "    (SELECT COUNT(locale) AS nlocales"
                       "     FROM captured_urls"
                       "     WHERE result IS NOT NULL"
                       "     AND locale = ANY(%s)"
                       "     GROUP BY url) AS counts"
                       "  GROUP BY nlocales ORDER BY nlocales",
                       (self.locale_list,))
            total = 0
            for nloc, count in cr:
                assert 1 <= nloc < len(self.per_locale)
                self.per_locale[nloc] = count
                total += count
            self.per_locale[0] = self.jobsize - total

        self.mon.report_status("Processing {} URLs | ".format(self.jobsize) +
                               " ".join("{}:{}".format(i, n)
                                        for i,n in enumerate(self.per_locale)))

    def prepare_database(self):
        with self.db, self.db.cursor() as cr:
            # Cache the status table in memory; it's reasonably small.
            self.mon.report_status("Loading database... (canon statuses)")
            cr.execute("SELECT detail, id FROM canon_statuses;")
            self.canon_statuses = { row.detail: row.id for row in cr }

            # Construct a temporary table containing all the URLs we have
            # yet to process, in a randomized order.
            # Note that, here and below, foreign key constraints are omitted
            # because postgres doesn't allow temp tables to declare foreign
            # keys pointing into permanent tables (?!)
            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (temporary tables)")
            cr.execute("CREATE TEMPORARY TABLE capture_randomizer ("
                       "  url INTEGER NOT NULL PRIMARY KEY,"
                       "  ix  SERIAL"
                       ")")

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (analyzing)")
            cr.execute("ANALYZE canon_urls")
            cr.execute("ANALYZE captured_urls")

            # Gather all canonicalized URLs from the canon table,
            # except those URLs for which we already have an answer
            # from every locale.  Note: value interpolation below is
            # known to be safe because 'ntlds' is, by construction, a
            # positive integer.
            # self.mon.maybe_pause_or_stop()
            # self.mon.report_status("Loading database... (to capture)")
            # cr.execute("INSERT INTO captured_urls (locale, url)"
            #             "  SELECT locale, url FROM"
            #             "    (SELECT DISTINCT canon FROM canon_urls"
            #             "       WHERE canon IS NOT NULL) x(url),"
            #             "    (SELECT UNNEST(%s)) y(locale)"
            #             "EXCEPT"
            #             "  SELECT locale, url FROM captured_urls",
            #             (self.locale_list,))

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database... (randomizer)")
            # The nested query here is only necessary to allow us to do
            # SELECT DISTINCT and ORDER BY random() simultaneously.
            cr.execute("INSERT INTO capture_randomizer (url)"
                       "  SELECT url FROM ("
                       "    SELECT DISTINCT url FROM captured_urls"
                       "      WHERE result IS NULL"
                       "        AND locale = ANY(%s)"
                       "  ) q ORDER BY random()",
                       (self.locale_list,))
            nrows = cr.rowcount
            if nrows in (-1, None):
                cr.execute("SELECT COUNT(*) FROM capture_randomizer")
                nrows = cr.fetchone()[0]
            self.jobsize = nrows

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database (indexing)...")
            cr.execute("CREATE INDEX capture_randomizer__ix "
                       " ON capture_randomizer(ix)")

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Loading database (analyzing)...")
            cr.execute("ANALYZE captured_urls")
            cr.execute("ANALYZE capture_randomizer")
