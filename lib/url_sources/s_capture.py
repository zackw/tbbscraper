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
from shared.proxies import ???
from shared.strsignal import strsignal

pj_trace_redir = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../../scripts/pj-trace-redir.js"))

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

        # PhantomJS seems to need 3GB of address space in order not to crash,
        # even on relatively simple pages.
        # 3 * 1024 * 1024 * 1024 = 3221225472
        self.proc = subprocess.Popen(
            proxy.adjust_command([
                "isolate",
                #"ISOL_RL_MEM=3221225472",
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
        completed = []
        nsucc = 0
        nfail = 0
        start = time.time()

        self._mon.set_status_prefix("w " + loc.proxy.label())
        try:
            while loc.proxy.online and not self.is_interrupted():
                self._mon.report_status("processing {}: "
                                        "{} captured, {} failures"
                                        .format(loc.proxy.label(),
                                                batchsize, nsucc, nfail))

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
                while completed and is_failure(completed[-1][1])):
                    nfail -= 1
                    last_failure = completed.pop()
                    batch.append((last_failure[0], last_failure[1]['ourl']))

            self._disp.complete_batch(loc, completed, batch)

            if completed:
                sec_per_url = (stop - start)/len(completed)
            else:
                sec_per_url = 0
            loc.proxy.update_stats(nsucc, nfail, sec_per_url)


class PerLocaleState:
    def __init__(self, locale, method, args):
        self.locale       = locale
        self.proxy_method = method
        self.proxy_args   = args
        self.proxy        = None
        self.in_progress  = set()
        self.n_workers    = 0
        self.todo         = 0

    def start_proxy(self, disp):
        self.proxy = self.proxy_method(disp, self.locale, *self.proxy_args)
        return self.proxy

class CaptureDispatcher:
    def __init__(self, args):
        self.args      = args
        self.state     = {}
        self.locales   = []
        self.workers   = []

        # complete initialization deferred till we're on the right thread
        self.mon = None
        self.db  = None
        self.status_queue = None
        self.status_queue_serializer = 0
        self.overall_jobsize = 0

    def __call__(self, mon, thr):
        self.mon = mon
        self.db = url_database.ensure_database(self.args)
        self.read_locations()
        self.prepare_database()

        self.status_queue = queue.PriorityQueue()
        self.mon.register_event_queue(self.status_queue,
                                      (self._MON_SAYS_STOP, -1))

        for loc in self.locales:
            proxy = self.state[loc].start_proxy(self)
            self.mon.add_work_thread(proxy)

        for _ in range(self.args.total_workers):
            wt = CaptureWorker(self)
            self.mon.add_work_thread(wt)
            self.workers.append(wt)

        while True:
            try:
                self.dispatcher_loop()
                break
            except Exception:
                self.mon.report_exception()

    def read_locations(self):
        raise NotImplemented#XXXXXX

    # Status queue helper constants and methods.
    _COMPLETE      = 0
    _MON_SAYS_STOP = 1 # Stop after handling all incoming work,
                       # but before pushing new work.
    _REQUEST       = 2

    # Entries in a PriorityQueue must be totally ordered.  We just
    # want to service all COMPLETE messages ahead of all STOP and
    # REQUEST messages, so give them all a serial number which goes
    # in the tuple right after the command code, before the data.
    # This also means we don't have to worry about unsortable data.
    def oq(self):
        self.status_queue_serializer += 1
        return self.status_queue_serializer

    # worker-to-dispatcher API
    def request_batch(self, worker):
        self.status_queue.put((self._REQUEST, self.oq(), worker))

    def complete_batch(self, loc, success, failure):
        self.status_queue.put((self._COMPLETE, self.oq(), loc,
                               success, failure))

    def drop_worker(self, worker):
        raise NotImplemented#XXXXXXX

    # proxy-to-dispatcher API
    def proxy_online(self, proxy):
        raise NotImplemented#XXXXXXX

    def proxy_offline(self, proxy):
        raise NotImplemented#XXXXXXX

    def dispatcher_loop(self):

        handlers = {
            self._REQUEST      : self.handle_request_batch,
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

    def handle_request_batch(self, cmd, serial, worker):

        # A locale is "chooseable" if it has some work to do, its
        # proxy is online, and it hasn't already been assigned the
        # maximum number of worker threads.  This loop is also a
        # convenient place to shut down proxies we are done using, and
        # to notice when we are _completely_ done.  Note that we are
        # not completely done until all locales reach todo 0, even if
        # we can't assign any more work right now (because all locales
        # with online proxies are full).
        chooseable_locales = []
        nothing_todo = True
        for loc in self.state.values():
            if loc.todo == 0:
                loc.proxy.finished()
            else:
                nothing_todo = False
                if (loc.proxy.online and
                    loc.n_workers < self.args.workers_per_loc):
                    chooseable_locales.append(loc)

        if nothing_todo:
            # We are completely done.
            worker.finished()
            return

        if not chooseable_locales:
            # All locales with work to do and online proxies already have
            # enough workers.  This message means "ask again in a while".
            callback(None, [None])

        # Sort key for locales to which work can be assigned.
        # Consider locales that currently have no workers at all first.
        # Consider locales with more work to do first.
        # Consider locales whose proxy is 'direct' first.
        # Consider locales named 'us' first.
        # As a final tie breaker use alphabetical order of locale name.
        def locale_order(l):
            return (l.n_workers != 0,
                    -l.todo,
                    l.proxy_method is not DirectProxy,
                    l.locale != 'us',
                    l.locale)

        chooseable_locales.sort(key=locale_order)

        # Try to pick a batch for each chooseable locale in turn.
        # We may discover that all available work for that locale has
        # already been assigned to other threads.
        with self.db, self.db.cursor() as cr:
            for loc in chooseable_locales:

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
                batch = cr.fetchall()

                if batch:
                    loc.n_workers += 1
                    loc.in_progress.update(row[0] for row in batch)
                    callback(loc, batch)
                    return

        # All locales with work to do and online proxies already have
        # all their work assigned to other workers.  As above: this
        # message means "ask again in a while".
        callback(None, [None])


    def handle_complete_batch(self, cmd, serial, loc, successes, failures):
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

    def update_progress_statistics(self):
        jobsize = 0
        plreport = []
        for plstate in self.state.values():
            jobsize = max(jobsize, plstate.todo)
            plreport.append((-plstate.todo, plstate.locale))

        plreport.sort()
        plreport = " ".join("{}:{}".format(pl[1], -pl[0]) for pl in plreport)

        self.mon.report_status("Processing {}/{} URLs | {}"
                               .format(jobsize, self.overall_jobsize,
                                       plreport))

    def prepare_database(self):
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
                .format(loc) for loc in self.locales)

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

            for loc in self.locales:
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
            for loc in self.locales:
                query += ', SUM("l_{0}"::INTEGER) AS "l_{0}"'.format(loc)
            query += " FROM capture_progress"
            cr.execute(query)

            # Compute the number of unvisited URLs for each locale,
            # and remove locales where that number is zero from the
            # working set.

            counts = cr.fetchone()
            self.overall_jobsize = counts[0]
            nlocales = []

            for loc, done in zip(self.locales, counts[1:]):
                todo = self.overall_jobsize - done
                assert todo >= 0
                if todo:
                    self.state[loc].todo = todo
                    nlocales.append(loc)
                else:
                    del self.state[loc]

            self.locales = nlocales

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Database prepared.")
