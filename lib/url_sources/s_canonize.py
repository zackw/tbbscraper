# canonize - canonicalize and clean a list of URLs.
# Copyright © 2010, 2013, 2014 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import http.client
import operator
import optparse # FIXME: argparse
import queue
import requests
import socket
import sqlite3
import sys
import time
import urllib.parse
try:
    from urllib3 import exceptions as urllib3_exceptions
except ImportError:
    # thanks everso, MacPorts
    from requests.packages.urllib3 import exceptions as urllib3_exceptions

from shared.monitor import Monitor

#
# Logging.
#

def fmt_status(resp):
    return str(resp.status_code) + " " + str(resp.reason)

def fmt_cookies(jar):
    if not jar: return ""
    return " [" + " ".join(cookie.name + "=" + cookie.value
                           for cookie in jar) + "]"

def fmt_exception(exc):
    # Timeout exceptions are often much too verbose.
    if isinstance(exc, (requests.exceptions.Timeout,
                        socket.timeout)):
        return "timed out"

    # There are two different getaddrinfo() error codes that
    # mean essentially "host not found", and we don't care
    # about the difference.
    if (isinstance(exc, socket.gaierror) and
        exc.errno in (socket.EAI_NONAME, socket.EAI_NODATA)):
        return "unknown host"

    if hasattr(exc, 'strerror') and exc.strerror:
        msg = exc.strerror
        if hasattr(exc, 'filename') and exc.filename:
            msg = exc.filename + ": " + msg
        return msg

    # HTTPException subclasses often have a vague str().
    if isinstance(exc, http.client.HTTPException):
        return "HTTP error ({}): {}".format(exc.__class__.__name__,
                                            str(exc))

    return str(exc)

# Custom canonization function which forces "http://foo.example" to
# "http://foo.example/" and removes empty or vacuous components
# (e.g. "http://foo.example:80/" becomes "http://foo.example/").
def canonical_form(url):
    # Insist on working with purely ASCII URLs, because a site that
    # responds to http://foo.example/Br%e8ve probably won't accept
    # http://foo.example/Br%c3%a8ve as the same thing.
    # requests wants str, not bytes, though.
    if hasattr(url, "encode"):
        url = url.encode("ascii").decode("ascii")
    else:
        url = url.decode("ascii")

    exploded = urllib.parse.urlparse(url)
    scheme = exploded.scheme
    user   = exploded.username or ""
    passwd = exploded.password or ""
    host   = exploded.hostname or ""
    port   = str(exploded.port) if exploded.port else ""
    path   = exploded.path
    params = exploded.params
    query  = exploded.query
    frag   = exploded.fragment

    if (scheme == "http" or scheme == "https") and path == "":
        path = "/"
    netloc = exploded.hostname
    if passwd:
        netloc = user + ":" + passwd + "@" + netloc
    elif user:
        netloc = user + "@" + netloc
    if port and port != "80":
        netloc = netloc + ":" + port

    return urllib.parse.urlunparse((scheme, netloc, path, params, query, frag))

def load_enough_content(resp):
    # Load no more than 16KB of a response, in 1K chunks.
    # Allow this process to take no more than 5 seconds in total.
    # These numbers are arbitrarily chosen to defend against
    # teergrubes (intentional or not) while still allowing us a
    # useful amount of data for anomaly post-mortem.
    body = b""
    start = time.time()
    for chunk in resp.iter_content(chunk_size=1024):
        body += chunk
        if len(body) > 16*1024 or time.time() - start > 5:
            resp.close()
            break
    return body

# This is approximately what
#  namedtuple("CanonResult", "url_id canon_url status anomaly")
# would produce, but with some gunk we don't need stripped, and
# convenience constructors added.
class CanonResult(tuple):
    __slots__ = ()
    _fields = ('url_id', 'canon_url', 'status', 'anomaly')
    def __new__(cls, url_id, canon_url, status, anomaly):
        return tuple.__new__(cls, (url_id, canon_url, status, anomaly))

    def __repr__(self):
        return self.__class__.__name__ + \
            '(url_id=%r, canon_url=%r, status=%r, anomaly=%r)' % self

    url_id = property(operator.itemgetter(0))
    canon_url = property(operator.itemgetter(1))
    status = property(operator.itemgetter(2))
    anomaly = property(operator.itemgetter(3))

    @classmethod
    def success(cls, url_id, canon_url):
        return cls(url_id, canon_url, "200 OK", None)

    @classmethod
    def http_failure(cls, url_id, resp):
        return cls(url_id, None, fmt_status(resp), None)

    @classmethod
    def http_anomaly(cls, url_id, resp, body):
        # Response headers:
        headers = "\n".join("{}: {}".format(*kv)
                            for kv in sorted(resp.headers.items()))

        # The headers do not include the status line.
        status = fmt_status(resp)
        full_status = "HTTP/{} ".format(resp.raw.version/10.) + status

        return cls(url_id,
                   None,
                   status,
                   (full_status + "\n" + headers + "\n\n")
                       .encode("ascii", "backslashreplace")
                   + body)

    @classmethod
    def exception(cls, url_id, exc):
        return cls(url_id, None, fmt_exception(exc), None)

class HTTPWorker:
    def __init__(self, inq, outq):
        self.inq      = inq
        self.outq     = outq
        self.mon      = None
        self.sess     = None
        self.prev_msg = ""

    def report_status(self, msg):
        self.mon.report_status("{1:<{0}.{0}} {2}".format(self.mon._max_x // 2,
                                                         self.prev_msg, msg))
        self.prev_msg = msg

    def log_start(self, orig_url):
        self.report_status(orig_url + " ...")

    def log_success(self, orig_url, canon):
        self.report_status("{} => {}"
                           .format(orig_url, canon))

    def log_fail(self, orig_url, resp):
        self.report_status("{} => {}"
                           .format(orig_url, fmt_status(resp)))

    def log_good_redirect(self, orig_url, redir, resp, cookies):
        self.report_status("{} => {} to {}{}"
                           .format(orig_url, fmt_status(resp), redir,
                                   fmt_cookies(cookies)))

    def log_redirect_loop(self, orig_url, redir, resp):
        self.report_status("{} => {} to {}, loop detected"
                           .format(orig_url, fmt_status(resp), redir))

    def log_exception(self, orig_url, exc):
        self.report_status("{} => {}"
                           .format(orig_url, fmt_exception(exc)))

    def process_one_response(self, orig_id, orig_url, resp):
        # Only code 200 counts as success.
        if resp.status_code == 200:
            url = canonical_form(resp.url)
            self.log_success(orig_url, url)
            return CanonResult.success(orig_id, url)

        # Codes 400, 401, 403, 404, 410, 500, and 503 are "normal"
        # failures; they do not get recorded as anomalous.
        if resp.status_code in (400, 401, 403, 404, 410, 500, 503):
            self.log_fail(orig_url, resp)
            return CanonResult.http_failure(orig_id, resp)

        # This logic must match requests.session's idea of what a
        # redirect is.
        if (resp.status_code not in requests.sessions.REDIRECT_STATI
            or "location" not in resp.headers):
            self.log_fail(orig_url, resp)
            return CanonResult.http_anomaly(orig_id, resp,
                                            load_enough_content(resp))

        return None

    def chase_redirects(self, orig_id, orig_url):
        # In some circumstances we need to access the last-examined
        # response object and/or its body from an exception handler.
        last_resp = None
        try:
            url = orig_url = canonical_form(orig_url)
            self.log_start(orig_url)
            self.sess.cookies.clear()

            # We will manually iterate over the redirects.
            # SSL certs are not verified because we don't want to exclude
            # sites with self-signed certs at this stage.
            req = requests.Request('GET', orig_url)
            pr  = self.sess.prepare_request(req)
            r   = self.sess.send(pr, timeout=10, allow_redirects=False,
                                 verify=False, stream=True)

            # The resolve_redirects generator does not emit the very first
            # response; that's 'r'.  Don't bother invoking it if the first
            # response isn't a redirect.
            last_resp = r
            result = self.process_one_response(orig_id, orig_url, r)
            if result is not None: return result
            self.log_good_redirect(orig_url, r.headers["location"],
                                   r, self.sess.cookies)

            for resp in self.sess.resolve_redirects(r, pr,
                                                    timeout=10,
                                                    verify=False,
                                                    stream=True):
                last_resp = resp
                result = self.process_one_response(orig_id, orig_url, resp)
                if result is not None: return result
                self.log_good_redirect(orig_url, resp.headers["location"],
                                       resp, self.sess.cookies)

        # All exceptions are captured and recorded as failures. The most
        # common causes of exceptions are timeouts and DNS resolution
        # failures, neither of which are "anomalous".  When we do have a
        # genuine anomaly (e.g. HTTP response failed to parse) we may well
        # not have enough information to record it, but we do our best.
        except requests.exceptions.ConnectionError as e:
            # requests lumps a whole bunch of different network-layer
            # issues under this exception.  In all cases observed so far,
            # a ConnectionError wraps a urllib3 exception which wraps a
            # socket error, and the outer two layers are uninteresting.
            try:
                sockerr = e.args[0].reason
                self.log_exception(orig_url, sockerr)
                return CanonResult.exception(orig_id, sockerr)
            except:
                self.log_exception(orig_url, e)
                return CanonResult.exception(orig_id, e)

        # It is not clear to me why, but sometimes these show up bare,
        # not wrapped in a requests.ConnectionsError.
        except urllib3_exceptions.MaxRetryError as e:
            try:
                sockerr = e.reason
                self.log_exception(orig_url, sockerr)
                return CanonResult.exception(orig_id, sockerr)
            except:
                self.log_exception(orig_url, e)
                return CanonResult.exception(orig_id, e)

        except urllib3_exceptions.LocationParseError as e:
            # Redirect to bogus URL: treat the last response as anomalous.
            self.log_fail(orig_url, last_resp)
            return CanonResult.http_anomaly(orig_id, last_resp,
                                            load_enough_content(last_resp))


        except requests.exceptions.TooManyRedirects as e:
            # Redirect loop: treat the last response as anomalous.
            self.log_redirect_loop(orig_url, last_resp.url, last_resp)
            return CanonResult.http_anomaly(orig_id, last_resp,
                                            load_enough_content(last_resp))

        except Exception as e:
            self.log_exception(orig_url, e)
            return CanonResult.exception(orig_id, e)

    def __call__(self, mon, thr):
        self.mon = mon
        # separate sessions for each thread
        self.sess = requests.Session()
        # mimic a real browser's headers
        self.sess.headers.update({
           "Accept":
             "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Encoding": "gzip, deflate",
           "Accept-Language": "en-US,en;q=0.5",
           "User-Agent":
             "Mozilla/5.0 (Macintosh; rv:24.0) Gecko/20100101 Firefox/24.0",
        })
        try:
            while True:
                self.mon.maybe_pause_or_stop()
                task = self.inq.get(block=False)
                self.outq.put(self.chase_redirects(*task))
        except queue.Empty:
            self.report_status("")
            return

# Bizarrely, sqlite3.Cursor is not an iterator nor does it have a method
# to return an iterator over the current query result.
def fetch_iter(cursor):
    while True:
        rows = cursor.fetchmany()
        if not rows: break
        for row in rows: yield row

class DatabaseWorker:
    def __init__(self, db_filename, n_workers):
        self.db_filename = db_filename
        self.n_workers   = n_workers

        # We mustn't actually create any of these objects till we're
        # on the proper thread.
        self.db             = None
        self.mon            = None
        self.canon_statuses = None
        self.work_queue     = None
        self.result_queue   = None

        # Statistics counters.
        self.total     = 0
        self.processed = 0
        self.successes = 0
        self.failures  = 0
        self.anomalies = 0

    def __call__(self, mon, thr):
        self.mon = mon
        self.load_database()
        self.main_loop()

    def report_final_statistics(self):
        # Called after the Monitor shuts down, so it's ok to use stdout.
        sys.stderr.write("Processed {} of {} URLs: {} canonized, {} failures, "
                         "{} anomalies\n"
                         .format(self.processed, self.total,
                                 self.successes, self.failures, self.anomalies))

    def log_overall_progress(self):
        if self.processed == 0:
            self.mon.report_status("Processing {} URLs...".format(self.total))
        else:
            total = str(self.total)
            wd = len(total)
            msg = (("Processed {1:>{0}} of {2}: {3} canonized, "
                   "{4} failures, {5} anomalies")
                   .format(wd, self.processed, total,
                           self.successes, self.failures, self.anomalies))
            self.mon.report_status(msg)

    def load_database(self):
        self.mon.report_status("Loading database...")

        # FIXME: Use urldb.py.
        db = sqlite3.connect(self.db_filename)
        self.db = db
        cr = db.cursor()
        cr.executescript('PRAGMA encoding = "UTF-8";'
                         'PRAGMA foreign_keys = ON;'
                         'PRAGMA locking_mode = NORMAL;')

        # Cache the status table in memory; it's reasonably small.
        self.mon.report_status("Loading database... (canon statuses)")
        self.mon.maybe_pause_or_stop()
        cr.execute("SELECT id, status FROM canon_statuses;")
        self.canon_statuses = { row[1]: row[0]
                                for row in fetch_iter(cr) }

        # Load the list of URLs-to-do.
        # This must be done in advance so that multiprocessing doesn't try to
        # call the cursor on the wrong thread.
        self.mon.report_status("Loading database... (work queue)")
        self.mon.maybe_pause_or_stop()
        cr.execute("SELECT u.url, v.url"
                   "  FROM urls as u"
                   "  LEFT JOIN url_strings as v on u.url = v.id"
                   "  WHERE u.url NOT IN (SELECT url FROM canon_urls)"
                   "  ORDER BY v.url")


        work_queue = queue.Queue()
        total = 0
        for row in fetch_iter(cr):
            orig_id = row[0]

            work_queue.put(row)
            total += 1
        self.total = total
        self.work_queue = work_queue
        self.result_queue = queue.Queue()

    def record_canonized(self, result):
        cr = self.db.cursor()
        status_id = self.canon_statuses.get(result.status)
        if status_id is None:
            cr.execute("INSERT INTO canon_statuses VALUES(NULL, ?)",
                       (result.status,))
            status_id = cr.lastrowid
            self.canon_statuses[result.status] = status_id

        if result.anomaly is not None:
            cr.execute("INSERT INTO anomalies VALUES(?, ?, ?)",
                       (result.url_id, status_id, result.anomaly))
            self.anomalies += 1

        if result.canon_url is None:
            canon_id = None
            self.failures += 1
        else:
            self.successes += 1
            cr.execute("SELECT id FROM url_strings WHERE url = ?",
                       (result.canon_url,))
            row = cr.fetchone()
            if row is not None:
                canon_id = row[0]
            else:
                cr.execute("INSERT INTO url_strings VALUES(NULL, ?)",
                           (result.canon_url,))
                canon_id = cr.lastrowid

        cr.execute("INSERT INTO canon_urls VALUES (?, ?, ?)",
                   (result.url_id, canon_id, status_id))
        self.processed += 1

    def main_loop(self):
        self.log_overall_progress()
        self.mon.maybe_pause_or_stop()

        for _ in range(self.n_workers):
            self.mon.add_work_thread(HTTPWorker(self.work_queue,
                                                self.result_queue))

        while True:
            with self.db:
                try:
                    while True:
                        result = self.result_queue.get(timeout=1)
                        self.record_canonized(result)
                        self.log_overall_progress()
                        if self.processed % 1000 == 0:
                            self.db.commit()
                except queue.Empty:
                    pass

            # Only allow the database writer to pause or stop when the
            # result queue is completely drained and all other workers
            # have stopped.
            if self.mon.caller_is_only_active_thread():
                if self.work_queue.empty() and self.result_queue.empty():
                    return # completely done, hurrah!

                self.mon.maybe_pause_or_stop()

if __name__ == '__main__':

    op = optparse.OptionParser(
        usage="usage: %prog [options] database",
        version="%prog 1.0")
    op.add_option("-p", "--parallel",
                  action="store", dest="parallel", type="int", default=10,
                  help="number of simultaneous HTTP requests to issue")

    (options, args) = op.parse_args()
    dbw = DatabaseWorker(args[0], options.parallel)
    Monitor(dbw, banner="Canonicalizing URLs")
    dbw.report_final_statistics()
