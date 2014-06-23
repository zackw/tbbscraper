# Copyright © 2010, 2013, 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Canonicalize URLs in an existing database by following redirections.

This operation also weeds out URLs that are no longer functional, or that
never were functional in the first place.  Peculiar responses are recorded
in a special "anomalies" table."""

def setup_argp(ap):
    ap.add_argument("-p", "--parallel",
                    action="store", dest="parallel", type=int, default=10,
                    help="number of simultaneous HTTP requests to issue")

def run(args):
    os.environ["PYTHONPATH"] = sys.path[0]
    cw = CanonizeWorker(args)
    curses.wrapper(cw)
    cw.report_final_statistics()

import contextlib
import curses
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

from psycopg2 import DatabaseError
from shared import url_database

pj_trace_redir = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../../scripts/pj-trace-redir.js"))

# Python does not provide strsignal() even in the very latest 3.x.
# This is a reasonable fake.
_sigtbl = []
def fake_strsignal(n):
    global _sigtbl
    if not _sigtbl:
        # signal numbers run 0 through NSIG-1; an array with NSIG members
        # has exactly that many slots
        _sigtbl = [None]*signal.NSIG
        for k in dir(signal):
            if (k.startswith("SIG") and not k.startswith("SIG_")
                # exclude obsolete aliases
                and k != "SIGCLD" and k != "SIGPOLL"):
              _sigtbl[getattr(signal, k)] = k
        # realtime signals mostly have no names
        if hasattr(signal, "SIGRTMIN") and hasattr(signal, "SIGRTMAX"):
            for r in range(signal.SIGRTMIN+1, signal.SIGRTMAX+1):
                _sigtbl[r] = "SIGRTMIN+" + str(r - signal.SIGRTMIN)
        # fill in any remaining gaps
        for i in range(signal.NSIG):
            if _sigtbl[i] is None:
                _sigtbl[i] = "unrecognized signal, number " + str(i)

    if n < 0 or n >= signal.NSIG:
        return "out-of-range signal, number "+str(n)
    return _sigtbl[n]

_stdout_junk_re = re.compile(
    r"^(?:"
    r"|[A-Z][a-z]+Error: .*"
    r"|[A-Z_]+?_ERR: .*"
    r"|Cannot init XMLHttpRequest object!"
    r"|Error requesting /.*"
    r"|Current location: https?://.*"
    r"|  (?:https?://.*?|undefined)?:[0-9]+(?: in \S+)?"
    r")$")

class CanonTask:
    """Representation of one canonicalization job."""
    def __init__(self, uid, url):
        self.original_uid = uid
        self.original_url = url
        self.idx          = -1
        self.canon_url    = None
        self.status       = None
        self.detail       = None
        self.anomaly      = {}
        self.pid          = None

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
                "--ignore-ssl-errors=true",
                "--load-images=false",
                pj_trace_redir, self.original_url
            ],
            stdin=subprocess.DEVNULL,
            stdout=self.result_fd,
            stderr=self.errors_fd)
        self.pid = self.proc.pid

    def terminate(self):
        self.proc.terminate()

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

    def pickup_results(self, status):

        if self.pid is None:
            return

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

        if os.WIFEXITED(status):
            exitcode = os.WEXITSTATUS(status)
            if exitcode == 0:
                pass
            elif exitcode == 1:
                self.parse_stderr(stderr, valid_result)
            else:
                self.status = "crawler failure"
                self.detail = "unexpected exit code {}".format(exitcode)

        elif os.WIFSIGNALED(status):
            self.status = "crawler failure"
            self.detail = "Killed by " + fake_strsignal(os.WTERMSIG(status))

        else:
            self.status = "crawler failure"
            self.detail = "Incomprehensible exit status {:04x}".format(status)

class CanonizeWorker:
    def __init__(self, args):
        self.args        = args
        self.in_progress = {}

        # Statistics counters.
        self.total       = 0
        self.processed   = 0
        self.successes   = 0
        self.failures    = 0
        self.anomalies   = 0

    def __exit__(self, *_):
        for job in self.in_progress.values():
            with contextlib.suppress(ProcessLookupError):
                job.terminate()
        self.db.commit()

    def __call__(self, screen):
        self.screen = screen
        self.bogus_results = open("bogus_results.txt", "at", encoding="utf-8")
        self.init_display()
        self.load_database()
        self.main_loop()

    def init_display(self):
        self.max_y, self.max_x = self.screen.getmaxyx()
        self.lines = [""]*(self.max_y - 1)
        self.prev_lines = [""]*(self.max_y - 1)
        curses.noecho()
        curses.nonl()
        curses.cbreak()
        curses.typeahead(-1)
        curses.curs_set(0)
        self.screen.attron(curses.A_REVERSE)

    def report_progress(self, msg):
        self.screen.addnstr(self.max_y - 1, 0, msg, self.max_x - 1)
        self.screen.clrtoeol()
        self.screen.refresh()

    def report_overall_progress(self):
        if self.processed == 0:
            self.report_progress("Processing {} URLs...".format(self.total))
        else:
            msg = (("Processed {} of {} URLs: {} canonized, "
                    "{} failures, {} anomalies")
                   .format(self.processed, self.total,
                           self.successes, self.failures, self.anomalies))
            self.report_progress(msg)

    def repaint_line(self, y):
        maxl = self.max_x // 2 - 1
        text = "{1:<{0}} {2:<{0}}".format(maxl,
                                          self.lines[y][:maxl],
                                          self.prev_lines[y][:maxl])
        self.screen.addnstr(y, 0, text, self.max_x - 1)
        self.screen.clrtoeol()

    def assign_display_index(self, url):
        for y, l in enumerate(self.lines):
            if not l:
                self.lines[y] = url
                self.repaint_line(y)
                self.screen.refresh()
                return y
        return -1

    def report_result(self, task, result):
        if task.idx == -1:
            return

        if hasattr(result, "decode"): result = result.decode("ascii")

        y = task.idx
        self.lines[y] = ""
        self.prev_lines[y] = task.original_url + " → " + result
        self.repaint_line(y)
        self.report_overall_progress()

    def report_final_statistics(self):
        # Called after curses shuts down, so it's ok to use stdout.
        sys.stdout.write("Processed {} URLs: {} canonized, {} failures, "
                         "{} anomalies\n"
                         .format(self.processed,
                                 self.successes, self.failures, self.anomalies))

    def load_database(self):
        self.report_progress("Loading database...")
        self.db = url_database.ensure_database(self.args)

        with self.db, self.db.cursor() as cr:
            # Cache the status table in memory; it's reasonably small.
            self.report_progress("Loading database... (canon statuses)")
            cr.execute("SELECT detail, id FROM canon_statuses")
            self.canon_statuses = { row.detail: row.id for row in cr }

            self.report_progress("Loading database... (sizing queue)")
            cr.execute("SELECT COUNT(*) FROM canon_urls"
                       "  WHERE result IS NULL")
            self.total = cr.fetchone()[0]

    def record_anomaly(self, result, exc=None):
        self.anomalies += 1
        self.bogus_results.write("{}\n".format(json.dumps({
                        "_1_original": result.original_url,
                        "_2_canon": result.canon_url,
                        "_3_status": result.status,
                        "_4_detail": result.detail,
                        "_5_exception": repr(exc),
                        "_6_anomaly": result.anomaly
                        })))

    def record_canonized(self, result):
        self.processed += 1
        with self.db, self.db.cursor() as cr:
            canon_id = None
            if result.canon_url is not None:
                try:
                    (canon_id, curl) = \
                        url_database.add_url_string(cr, result.canon_url)
                except ValueError as e:
                    # This happens when the canon URL is hopelessly
                    # ill-formed, like "httpons://host" or "http:/".
                    pass

                except DatabaseError as e:
                    # This happens, for instance, when the canon URL is
                    # so long that postgres refuses to index it.
                    result.status = "crawler failure"
                    result.detail = str(e)

            status_id = self.canon_statuses.get(result.detail)
            if status_id is None and result.detail is not None:
                cr.execute("INSERT INTO canon_statuses(id, detail) "
                           "  VALUES(DEFAULT, %s)"
                           "  RETURNING id", (result.detail,))
                status_id = cr.fetchone()[0]
                self.canon_statuses[result.detail] = status_id

            success, hlresult = \
                url_database.categorize_result(result.status,
                                               result.original_uid,
                                               canon_id)

            if success:
                self.successes += 1
                self.report_result(result, curl)
            else:
                self.failures += 1
                self.report_result(result,
                                   result.detail if result.detail
                                   else result.status)

            if result.anomaly:
                self.record_anomaly(result)

            cr.execute("UPDATE canon_urls"
                       "  SET canon  = %(canon)s,"
                       "      result = %(result)s,"
                       "      detail = %(detail)s"
                       "  WHERE url = %(url)s",
                       { 'url': result.original_uid,
                         'canon': canon_id,
                         'result': hlresult,
                         'detail': status_id })

    def next_batch(self):
        with self.db, self.db.cursor() as cr:
            cr.execute("SELECT u.url AS uid, v.url AS url"
                       "  FROM canon_urls u, url_strings v"
                       " WHERE u.url = v.id"
                       "   AND u.result IS NULL"
                       " LIMIT 500")
            return cr.fetchall()

    def urls_todo(self):
        while True:
            batch = self.next_batch()
            if not batch: break
            for b in batch: yield b

    def main_loop(self):
        self.report_overall_progress()

        all_read = False
        urls_todo = self.urls_todo()

        while self.in_progress or not all_read:
            while not all_read and len(self.in_progress) < self.args.parallel:

                uid, url = next(urls_todo)
                task = CanonTask(uid, url)
                task.idx = self.assign_display_index(task.original_url)
                if task.pid is None:
                    self.record_canonized(task)
                else:
                    self.in_progress[task.pid] = task

            try:
                (pid, status) = os.wait()
            except ChildProcessError:
                continue # no children to wait for: keep going

            task = self.in_progress.pop(pid)
            task.pickup_results(status)

            try:
                self.record_canonized(task)
            except Exception as e:
                self.report_result(result, "bogus")
                self.record_anomaly(result, e)
