# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Capture the HTML content and a screenshot of each URL in an
existing database, from many locations simultaneously.  Locations are
defined by the config file passed as an argument, which is line-
oriented, each line having the general form

  locale method arguments ...

'locale' is an arbitrary word (consisting entirely of lowercase ASCII
letters) which names the location; it is what shows up in the 'locale'
column of the 'captured_pages' table.

'method' selects a general method for capturing pages from this
location.  Subsequent 'arguments' are method-specific.  There are
currently three supported methods:

  direct: The controller machine will issue HTTP requests directly.
          No arguments.

  ssh:    HTTP requests will be proxied via ssh -D.
          One argument, [user@]hostname; HOSTNAME must allow USER to
          log in via ssh with no password.

  ovpn:   HTTP requests will be proxied via openvpn.
          One or more arguments are passed to the 'openvpn-netns'
          helper program (see scripts/openvpn-netns.c).  The initial
          argument is treated as a glob pattern which should expand to
          one or more OpenVPN config files; if there's more than one,
          they are placed in a random order and then used round-robin
          (i.e. if connection with one config file fails or drops, the
          next one is tried).
"""

def setup_argp(ap):
    ap.add_argument("locations",
                    action="store",
                    help="List of location specifications.")
    ap.add_argument("-b", "--batch-size",
                    action="store", dest="batch_size", type=int, default=20,
                    help="Number of URLs to feed to each worker at once.")
    ap.add_argument("-w", "--workers-per-location",
                    action="store", dest="workers_per_loc", type=int, default=8,
                    help="Maximum number of concurrent workers per location.")
    ap.add_argument("-W", "--total-workers",
                    action="store", dest="total_workers", type=int, default=40,
                    help="Total number of concurrent workers to use.")
    ap.add_argument("-t", "--tables",
                    action="store", dest="tables",
                    help="Comma-separated list of url-source tables to "
                    "process, without the 'urls_' prefix. (default: all "
                    "of them)")
    ap.add_argument("-p", "--max-simultaneous-proxies",
                    action="store", type=int, default=10,
                    help="Maximum number of proxies to use simultaneously.")

def run(args):
    Monitor(CaptureDispatcher(args),
            banner="Capturing content and screenshots of web pages",
            error_log="capture-errors")

import base64
import collections
import contextlib
import io
import itertools
import json
import os
import os.path
import queue
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
from shared.monitor import Monitor, Worker
from shared.proxies import ProxySet
from shared.strsignal import strsignal

pj_trace_redir = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../../scripts/pj-trace-redir.js"))

# Utilities

def queue_iter(q, timeout=None):
    """Generator which yields messages pulled from a queue.Queue in
       sequence, until empty or the timeout expires.  Can block before
       yielding any items, but not after at least one item has been
       yielded.

    """
    try:
        yield q.get(timeout=timeout)
        while True:
            yield q.get(block=False)
    except queue.Empty:
        pass

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

class CaptureTask:
    """Representation of one capture job."""
    def __init__(self, url, proxy):
        self.proc         = None
        self.original_url = url
        self.canon_url    = None
        self.status       = None
        self.detail       = None
        self.log          = {}
        self.content      = None
        self.render       = None

        # Make sure the URL is not so mangled that phantomjs is just going
        # to give up and report nothing at all.
        try:
            self.original_url = \
                url_database.canon_url_syntax(url, want_splitresult = False)

        except ValueError as e:
            self.status = 'invalid URL'
            self.detail = str(e)
            return

        except UnicodeError as e:
            while e.__cause__ is not None: e = e.__cause__
            self.status = 'invalid URL'
            self.detail = 'invalid hostname: ' + str(e)
            return

        # We use a temporary file for the results, instead of a pipe,
        # so we don't have to worry about reading them until after the
        # child process exits.
        self.result_fd = tempfile.TemporaryFile("w+t", encoding="utf-8")
        self.errors_fd = tempfile.TemporaryFile("w+t", encoding="utf-8")

        self.proc = subprocess.Popen(
            proxy.adjust_command([
                "isolate",
                "ISOL_RL_MEM=unlimited",
                "ISOL_RL_STACK=8388608",
                "PHANTOMJS_DISABLE_CRASH_DUMPS=1",
                "MALLOC_CHECK_=0",
                "phantomjs",
                "--local-url-access=no",
                pj_trace_redir,
                "--capture",
                self.original_url
            ]),
            stdin=subprocess.DEVNULL,
            stdout=self.result_fd,
            stderr=self.errors_fd)

    def unpack_results(self, results):
        self.canon_url     = results["canon"]
        self.status        = results["status"]
        self.detail        = results.get("detail")
        if self.detail is None or self.detail == "":
            if self.status == "timeout":
                self.detail = "timeout"
            else:
                self.detail = self.status
                self.status = "crawler failure"

        self.log['events'] = results.get("log",    [])
        self.log['chain']  = results.get("chain",  [])
        self.log['redirs'] = results.get("redirs", None)

        if 'content' in results:
            self.content = zlib.compress(results['content']
                                         .encode('utf-8'))
        if 'render' in results:
            self.render = recompress_image(results['render'])

    def parse_stdout(self, stdout):
        if not stdout:
            # This may get overridden later, by analysis of stderr.
            self.status = "crawler failure"
            self.detail = "no output from tracer"
            return False

        # The output, taken as a whole, should be one complete JSON object.
        try:
            self.unpack_results(json.loads(stdout))
            return True
        except:
            # There is some sort of bug causing junk to be emitted along
            # with the expected output.  We used to try to clean up after
            # this but that caused its own problems.  Just fail.
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

    def pickup_results(self):

        if self.proc is None:
            return
        exitcode = self.proc.wait()

        self.result_fd.seek(0)
        stdout = self.result_fd.read()
        self.result_fd.close()
        self.errors_fd.seek(0)
        stderr = self.errors_fd.read()
        self.result_fd.close()

        # We parse stdout regardless of exit status, because sometimes
        # phantomjs prints a complete crawl result and _then_ crashes.
        valid_result = self.parse_stdout(stdout)

        # We only expect to get stuff on stderr with exit code 1.
        stderr = stderr.strip().splitlines()
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

    def report(self):
        self.pickup_results()
        return {
            'ourl':    self.original_url,
            'status':  self.status,
            'detail':  self.detail,
            'log':     zlib.compress(json.dumps(self.log).encode('utf-8')),
            'canon':   self.canon_url,
            'content': self.content,
            'render':  self.render
        }

def is_failure(report):
    return not (report.get('content') and
                report['status'] != 'crawler failure')

class CaptureWorker(Worker):
    def __init__(self, disp):
        Worker.__init__(self, disp)
        self._idle_prefix = "w"

    def process_batch(self, loc, batch):
        batchsize = len(batch)
        assert batchsize

        completed = []
        nsucc = 0
        nfail = 0
        start = time.time()

        self._mon.set_status_prefix("w " + loc.proxy.label())
        try:
            while batch and loc.proxy.online and not self.is_interrupted():
                self._mon.report_status("processing {}: "
                                        "{} captured, {} failures"
                                        .format(batchsize, nsucc, nfail))

                (url_id, url) = batch.pop()
                report = CaptureTask(url, loc.proxy).report()
                completed.append((url_id, report))
                if is_failure(report):
                    nfail += 1
                else:
                    nsucc += 1

        finally:
            stop = time.time()

            # If the proxy has gone offline, any string of failures at
            # the end of 'completed' should be retried later.  (We may
            # not notice promptly when the proxy has gone offline.)
            if not loc.proxy.online:
                while completed and is_failure(completed[-1][1]):
                    nfail -= 1
                    last_failure = completed.pop()
                    batch.append((last_failure[0], last_failure[1]['ourl']))

            self._disp.complete_batch(self, (completed, batch))

            if completed:
                sec_per_url = (stop - start)/len(completed)
            else:
                sec_per_url = 0
            loc.proxy.update_stats(nsucc, nfail, sec_per_url)


class PerLocaleState:
    def __init__(self, locale, proxy):
        self.locale       = locale
        self.proxy        = proxy
        self.in_progress  = set()
        self.n_workers    = 0
        self.todo         = 0

class CaptureDispatcher:
    def __init__(self, args):
        # complete initialization deferred till we're on the right thread
        self.args                    = args
        self.idle_workers            = set()
        self.active_workers          = {}
        self.locations               = {}
        self.overall_jobsize         = 0
        self.proxies                 = None
        self.mon                     = None
        self.db                      = None
        self.status_queue            = None
        self.status_queue_serializer = 0

    def __call__(self, mon, thr):
        self.mon = mon
        self.status_queue = queue.PriorityQueue()
        self.mon.register_event_queue(self.status_queue,
                                      (self._MON_SAYS_STOP, -1))

        self.mon.set_status_prefix("d")
        self.mon.report_status("loading...")

        self.proxies = ProxySet(self, self.mon, self.args,
                                self.proxy_sort_key)
        self.mon.report_status("loading... (proxies OK)")

        self.db = url_database.ensure_database(self.args)
        self.prepare_database()

        for _ in range(self.args.total_workers):
            wt = CaptureWorker(self)
            self.mon.add_work_thread(wt)
            self.idle_workers.add(wt)

        self.dispatcher_loop()

    # Status queue helper constants and methods.
    _PROXY_OFFLINE  = 1
    _PROXY_ONLINE   = 2
    _BATCH_COMPLETE = 3
    _BATCH_FAILED   = 4
    _DROP_WORKER    = 5
    _MON_SAYS_STOP  = 6 # Stop after handling all incoming work

    # Entries in a PriorityQueue must be totally ordered.  We just
    # want to service all COMPLETE messages ahead of all others, and
    # STOP messages after all others, so we give them all a serial
    # number which goes in the tuple right after the command code,
    # before the data.  This also means we don't have to worry about
    # unsortable data.
    def oq(self):
        self.status_queue_serializer += 1
        return self.status_queue_serializer

    # worker-to-dispatcher API
    def complete_batch(self, worker, result):
        self.status_queue.put((self._BATCH_COMPLETE, self.oq(),
                               worker, result))

    def fail_batch(self, worker, exc_info):
        self.status_queue.put((self._BATCH_FAILED, self.oq(), worker))

    def drop_worker(self, worker):
        self.status_queue.put((self._DROP_WORKER, self.oq(), worker))

    # proxy-to-dispatcher API
    def proxy_online(self, proxy):
        self.status_queue.put((self._PROXY_ONLINE, self.oq(), proxy))

    def proxy_offline(self, proxy):
        self.status_queue.put((self._PROXY_OFFLINE, self.oq(), proxy))

    def _invalid_message(self, *args):
        self.mon.report_error("invalid status queue message {!r}"
                              .format(args))

    def dispatcher_loop(self):

        # Kick things off by starting one proxy.
        (proxy, until_next, n_locations) = self.proxies.start_a_proxy()

        while n_locations:
            time_now = time.monotonic()
            # Technically, until_next being None means "wait for a proxy
            # to exit", but use an hour as a backstop.  (When a proxy does
            # exit, this will get knocked down to zero below.)
            if until_next is None: until_next = 3600
            time_next = time_now + until_next
            pending_stop = False

            while time_now < time_next:
                self.update_progress_statistics(n_locations, until_next)

                for msg in queue_iter(self.status_queue, until_next):
                    if msg[0] == self._PROXY_ONLINE:
                        self.proxies.note_proxy_online(msg[2])

                    elif msg[0] == self._PROXY_OFFLINE:
                        self.proxies.note_proxy_offline(msg[2])
                        # Wait no more than 5 minutes before trying to
                        # start another proxy.  (XXX This hardwires a
                        # specific provider's policy.)
                        time_now = time.monotonic()
                        time_next = min(time_next, time_now + 300)
                        until_next = time_next - time_now

                    elif msg[0] == self._BATCH_COMPLETE:
                        worker, result = msg[2], msg[3]
                        locstate, _ = self.active_workers[worker]
                        del self.active_workers[worker]
                        self.idle_workers.add(worker)
                        self.record_batch(locstate, *result)

                    elif msg[0] == self._BATCH_FAILED:
                        worker = msg[2]
                        # We might've already gotten a COMPLETE message
                        # with more precision.
                        if worker in self.active_workers:
                            locstate, batch = self.active_workers[worker]
                            del self.active_workers[worker]
                            self.idle_workers.add(worker)
                            self.record_batch(locstate, [], batch)

                    elif msg[0] == self._DROP_WORKER:
                        worker = msg[2]
                        self.idle_workers.discard(worker)
                        if worker in self.active_workers:
                            self.active_workers[worker].fail_job()
                            del self.active_workers[worker]

                    elif msg[0] == self._MON_SAYS_STOP:
                        self.mon.report_status("interrupt pending")
                        pending_stop = True

                    else:
                        self.mon.report_error("bogus message: {!r}"
                                              .format(message))

                for loc, state in self.locations.items():
                    if state.todo == 0 and loc in self.proxies.locations:
                        self.proxies.locations[loc].finished()

                if pending_stop:
                    self.mon.report_status("interrupted")
                    self.mon.maybe_pause_or_stop()
                    # don't start new work yet, the set of proxies
                    # available may be totally different now

                else:
                    # One-second delay before starting new work, because
                    # proxies aren't always 100% up when they say they are.
                    self.mon.idle(1)

                    while self.idle_workers:
                        assigned_work = False
                        for proxy in self.proxies.active_proxies:
                            if not proxy.online:
                                continue
                            state = self.locations[proxy.loc]
                            if state.n_workers >= self.args.workers_per_loc:
                                continue
                            batch = self.select_batch(state)
                            if not batch:
                                # All work for this location is
                                # assigned to other workers already.
                                continue

                            state.n_workers += 1
                            state.in_progress.update(row[0] for row in batch)
                            worker = self.idle_workers.pop()
                            self.active_workers[worker] = (state, batch)
                            worker.queue_batch(state, batch)
                            assigned_work = True
                            if not self.idle_workers:
                                break

                        if not assigned_work:
                            break

                time_now = time.monotonic()
                until_next = time_next - time_now

            # when we get to this point, it's time to start another proxy
            (proxy, until_next, n_locations) = self.proxies.start_a_proxy()

        # done, kill off all the workers
        self.mon.report_status("finished")
        assert not self.active_workers
        for w in self.idle_workers:
            w.finished()

    def proxy_sort_key(self, loc, method):
        # Consider locales that currently have no workers at all first.
        # Consider locales with more work to do first.
        # Consider locales whose proxy is 'direct' first.
        # Consider locales named 'us' first.
        # As a final tie breaker use alphabetical order of locale name.
        state = self.locations[loc]
        return (state.n_workers != 0,
                -state.todo,
                method != 'direct',
                loc != 'us',
                loc)

    def select_batch(self, loc):
        with self.db, self.db.cursor() as cr:

            query = ('SELECT c.url as uid, s.url as url'
                     '  FROM capture_progress c, url_strings s'
                     ' WHERE c.url = s.id')

            query += ' AND NOT c."l_{0}"'.format(loc.locale)

            if loc.in_progress:
                query += ' AND c.url NOT IN ('
                query += ','.join(str(u) for u in loc.in_progress)
                query += ')'

            query += ' LIMIT {0}'.format(self.args.batch_size)
            cr.execute(query)
            return cr.fetchall()

    def record_batch(self, loc, successes, failures):
        locale = loc.locale
        loc.n_workers -= 1
        for r in failures:
            loc.in_progress.remove(r[0])

        if not successes:
            return

        with self.db, self.db.cursor() as cr:
            for s in successes:
                url_id = s[0]
                r      = s[1]
                loc.in_progress.remove(url_id)

                redir_url = None
                redir_url_id = None
                if r['canon']:
                    redir_url = r['canon']
                    if redir_url == r['ourl']:
                        redir_url_id = url_id
                    elif redir_url is not None:
                        try:
                            (redir_url_id, _) = \
                                url_database.add_url_string(cr, redir_url)
                        except (ValueError, UnicodeError):
                            addendum = "invalid redir url: " + redir_url
                            if ('detail' not in r or r['detail'] is None):
                                r['detail'] = addendum
                            else:
                                r['detail'] += " | " + addendum

                detail_id = self.capture_detail.get(r['detail'])
                if detail_id is None:
                    cr.execute("INSERT INTO capture_detail(id, detail) "
                               "  VALUES(DEFAULT, %s)"
                               "  RETURNING id", (r['detail'],))
                    detail_id = cr.fetchone()[0]
                    self.capture_detail[r['detail']] = detail_id

                result = url_database.categorize_result(r['status'],
                                                        r['detail'],
                                                        url_id,
                                                        redir_url_id)

                to_insert = {
                    "locale":       locale,
                    "url":          url_id,
                    "result":       result,
                    "detail":       detail_id,
                    "redir_url":    redir_url_id,
                    "log":          r['log'],
                    "html_content": r['content'],
                    "screenshot":   r['render']
                }
                cr.execute("INSERT INTO captured_pages"
                           "(locale, url, access_time, result, detail,"
                           " redir_url, capture_log, html_content,"
                           " screenshot)"
                           "VALUES ("
                           "  %(locale)s,"
                           "  %(url)s,"
                           "  TIMESTAMP 'now',"
                           "  %(result)s,"
                           "  %(detail)s,"
                           "  %(redir_url)s,"
                           "  %(log)s,"
                           "  %(html_content)s,"
                           "  %(screenshot)s)",
                           to_insert)
                cr.execute('UPDATE capture_progress SET "l_{0}" = TRUE '
                           ' WHERE url = {1}'.format(locale, url_id))
                loc.todo -= 1

    def update_progress_statistics(self, n_locations, until_next):
        jobsize = 0
        plreport = []
        for plstate in self.locations.values():
            jobsize = max(jobsize, plstate.todo)
            plreport.append((-plstate.todo, plstate.locale))

        plreport.sort()
        plreport = " ".join("{}:{}".format(pl[1], -pl[0]) for pl in plreport)

        self.mon.report_status("Processing {}/{} URLs | {}/{}/{} active, {} till next | {}"
                               .format(jobsize, self.overall_jobsize,
                                       len(self.proxies.active_proxies),
                                       n_locations,
                                       len(self.locations),
                                       until_next,
                                       plreport))

    def prepare_database(self):
        self.locations = { loc: PerLocaleState(loc, proxy)
                           for loc, proxy in self.proxies.locations.items() }
        with self.db, self.db.cursor() as cr:
            # Cache the status table in memory; it's reasonably small.
            self.mon.report_status("Preparing database... (capture detail)")
            cr.execute("SELECT detail, id FROM capture_detail;")
            self.capture_detail = { row.detail: row.id for row in cr }

            # The capture_progress table tracks what we've done so far.
            # It is regenerated from scratch each time this program is run,
            # based on the contents of the urls_* and captured_pages tables.
            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Preparing database... "
                                   "(capture progress)")

            l_columns = ",\n  ".join(
                "\"l_{0}\" BOOLEAN NOT NULL DEFAULT FALSE"
                .format(loc) for loc in self.locations.keys())

            cr.execute("CREATE TEMPORARY TABLE capture_progress ("
                       "  url INTEGER PRIMARY KEY,"
                       + l_columns + ");")

            # Determine the set of URLs yet to be captured from the selected
            # tables.
            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Preparing database... "
                                   "(capture progress rows)")

            cr.execute("SELECT table_name FROM information_schema.tables"
                       " WHERE table_schema = %s"
                       "   AND table_type = 'BASE TABLE'"
                       "   AND table_name LIKE 'urls_%%'",
                       (self.args.schema,))
            all_url_tables = set(row[0] for row in cr)

            if self.args.tables is None:
                want_url_tables = all_url_tables
            else:
                want_url_tables = set("urls_"+t.strip()
                                      for t in self.args.tables.split(","))
                if not want_url_tables.issubset(all_url_tables):
                    raise RuntimeError("Requested URL tables do not exist: "
                                       + ", ".join(
                                           t[5:] for t in
                                           want_url_tables - all_url_tables))

            for tbl in want_url_tables:
                self.mon.maybe_pause_or_stop()
                self.mon.report_status("Preparing database... "
                                       "(capture progress rows: {})"
                                       .format(tbl))

                # Only one row per URL, even if it appears in more than one
                # source table.
                cr.execute("INSERT INTO capture_progress (url) "
                           "        SELECT url FROM "+tbl+
                           " EXCEPT SELECT url FROM capture_progress")

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Preparing database... (analyzing)")
            cr.execute("ANALYZE captured_pages")

            for loc in self.locations.keys():
                self.mon.maybe_pause_or_stop()
                self.mon.report_status("Preparing database... "
                                       "(capture progress values: {})"
                                       .format(loc))

                cr.execute('UPDATE capture_progress c SET "l_{0}" = TRUE'
                           '  FROM captured_pages p'
                           ' WHERE c.url = p.url AND p.locale = \'{0}\''
                           .format(loc))

                self.mon.maybe_pause_or_stop()
                self.mon.report_status("Preparing database... (indexing: {})"
                                       .format(loc))
                cr.execute("CREATE INDEX \"capture_progress_l_{0}_idx\""
                           "  ON capture_progress(\"l_{0}\");"
                           .format(loc))

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Preparing database... (analyzing)")
            cr.execute("ANALYZE capture_progress")

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Preparing database... (statistics)")

            query = "SELECT COUNT(*)"
            for loc in self.locations.keys():
                query += ', SUM("l_{0}"::INTEGER) AS "l_{0}"'.format(loc)
            query += " FROM capture_progress"
            cr.execute(query)

            # Compute the number of unvisited URLs for each locale,
            # and remove locales where that number is zero from the
            # working set.

            counts = cr.fetchone()
            self.overall_jobsize = counts[0]
            for loc, done in zip(self.locations.keys(), counts[1:]):
                todo = self.overall_jobsize - done
                assert todo >= 0
                if todo:
                    self.locations[loc].todo = todo
                else:
                    self.locations[loc].proxy.finished()

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Database prepared.")
