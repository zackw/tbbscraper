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
    with CanonizeWorker(args) as cw:
        curses.wrapper(cw)
        cw.report_final_statistics()

import collections
import curses
import os
import pickle
import signal
import subprocess
import sys
import tempfile

from shared import url_database

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

class CanonTask:
    """Representation of one canonicalization job."""
    def __init__(self, uid, url, retries, idx):
        self.original_uid = uid
        self.original_url = url
        self.retries      = retries
        self.idx          = idx
        self.canon_url    = None
        self.status       = None
        self.anomaly      = None

        # We use a temporary file for the results, instead of a pipe,
        # so we don't have to worry about reading them until after the
        # child process exits.
        self.result_fd = tempfile.TemporaryFile()
        self.proc = subprocess.Popen([sys.executable, "-Os", "-m",
                                      "url_sources.canonize_single",
                                      self.original_url],
                                     stdin=subprocess.DEVNULL,
                                     stdout=self.result_fd,
                                     stderr=subprocess.DEVNULL)
        self.pid = self.proc.pid

    def terminate(self):
        self.proc.terminate()

    def pickup_results(self, status):

        if os.WIFSIGNALED(status):
            if os.WTERMSIG(status) == signal.SIGALRM:
                self.status = "Network timeout"
            else:
                self.status = "Killed by " + fake_strsignal(os.WTERMSIG(status))

        elif os.WIFEXITED(status):
            self.result_fd.seek(0)
            results = pickle.load(self.result_fd)
            if (results["status"] is None or
                (os.WEXITSTATUS(status) == 0 and results["url"] is None)):
                raise RuntimeError(repr(results))

            self.canon_url = results["url"]
            self.status    = results["status"]
            self.anomaly   = results["anomaly"]

        else:
            self.status = "Unexpected exit status {:04x}".format(status)

class CanonizeWorker:
    def __init__(self, args):
        self.args        = args
        self.in_progress = {}
        self.todo        = collections.deque()

        # Statistics counters.
        self.total       = 0
        self.processed   = 0
        self.successes   = 0
        self.failures    = 0
        self.anomalies   = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        for job in self.in_progress.values():
            try:
                job.terminate()
            except ProcessLookupError:
                pass
        self.dbw.commit()

    def __call__(self, screen):
        self.screen = screen
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
            total = str(self.total)
            wd = len(total)
            msg = (("Processed {1:>{0}} of {2}: {3} canonized, "
                   "{4} failures, {5} anomalies")
                   .format(wd, self.processed, total,
                           self.successes, self.failures, self.anomalies))
            self.report_progress(msg)

    def repaint_line(self, y):
        text = "{1:<{0}} {2:<{0}}".format(self.max_x // 2 - 1,
                                          self.lines[y],
                                          self.prev_lines[y])
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
        sys.stdout.write("Processed {} of {} URLs: {} canonized, {} failures, "
                         "{} anomalies\n"
                         .format(self.processed, self.total,
                                 self.successes, self.failures, self.anomalies))

    def load_database(self):
        self.report_progress("Loading database...")
        self.dbr = url_database.ensure_database(self.args)
        self.dbw = url_database.reconnect_to_database(self.args)

        cr = self.dbr.cursor()
        # Cache the status table in memory; it's reasonably small.
        self.report_progress("Loading database... (canon statuses)")
        cr.execute("SELECT id, status FROM canon_statuses;")
        self.canon_statuses = { row[1]: row[0]
                                for row in url_database.fetch_iter(cr) }

        # Load the list of URLs-to-do.
        self.report_progress("Loading database... (work queue)")
        cr.execute("SELECT DISTINCT u.url, v.url, 0"
                   "  FROM urls as u"
                   "  LEFT JOIN url_strings as v on u.url = v.id"
                   "  WHERE u.url NOT IN (SELECT url FROM canon_urls)")
        while True:
            rows = cr.fetchmany()
            if not rows: break
            self.total += len(rows)
            self.todo.extend(rows)
            self.report_progress("Loading database... (work queue {})"
                                 .format(self.total))

    def record_canonized(self, result):
        try:
            self.processed += 1
            cr = self.dbw.cursor()
            status_id = self.canon_statuses.get(result.status)
            if status_id is None:
                cr.execute("INSERT INTO canon_statuses VALUES(NULL, ?)",
                           (result.status,))
                status_id = cr.lastrowid
                self.canon_statuses[result.status] = status_id

            if result.anomaly is not None:
                cr.execute("INSERT INTO anomalies VALUES(?, ?, ?)",
                           (result.original_uid, status_id, result.anomaly))
                self.anomalies += 1

            if result.canon_url is None:
                canon_id = None
                self.failures += 1
                self.report_result(result, result.status)
            else:
                (canon_id, curl) = \
                    url_database.add_url_string(cr, result.canon_url)
                self.successes += 1
                self.report_result(result, curl)

            cr.execute("INSERT INTO canon_urls VALUES (?, ?, ?)",
                       (result.original_uid, canon_id, status_id))

            if self.processed % 1000 == 0:
                dbw.commit()

        except Exception as e:
            raise type(e)("Bogus result: {{ status: {!r} canon: {!r} anomaly: {!r} }}".format(result.status, result.canon_url, result.anomaly)) from e

    def main_loop(self):
        self.report_overall_progress()

        while self.todo or self.in_progress:
            while self.todo and len(self.in_progress) < self.args.parallel:
                uid, url, retries = self.todo.popleft()
                url = url_database.canon_url_syntax(url)
                idx = self.assign_display_index(url)
                task = CanonTask(uid, url, retries, idx)
                self.in_progress[task.pid] = task

            try:
                (pid, status) = os.wait()
            except ChildProcessError:
                continue # no children to wait for: keep going

            task = self.in_progress.pop(pid)
            task.pickup_results(status)

            # Retry network timeouts up to five times.
            if task.status == "Network timeout":
                if task.retries < 5:
                    self.report_result(task, task.status)
                    self.todo.append((task.original_uid,
                                      task.original_url,
                                      task.retries + 1))
                    continue

            self.record_canonized(task)
