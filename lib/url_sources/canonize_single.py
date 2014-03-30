# Copyright © 2010, 2013, 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Subprocess of s_canonize.py.  Canonicalizes one URL, provided as a
command-line argument, by following redirections.  The result is
reported as a pickled dictionary on stdout:

   { "canon":   "canonicalized URL",
     "status":  b"HTTP status phrase, or other one-line error message",
     "anomaly": b"details of any error that may have occurred" }

(Pickle is used instead of JSON because byte strings cannot be put into
JSON.)

The exit status is 0 if and only if a chain of redirections was
successfully followed all the way to a 200 OK.  An overall 30-second
timeout is enforced; if this lapses, the process will be killed by
SIGALRM and no output will appear."""

import http.client
import pickle
import pickletools
import re
import requests
import signal
import socket
import ssl
import sys
import time
import traceback

def main():
    signal.alarm(30)
    chase_redirects(sys.argv[1])

def chase_redirects(url):
    sess = requests.Session()
    # mimic a real browser's headers
    sess.headers.update({
        "Accept":
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.5",
        "User-Agent":
            "Mozilla/5.0 (Macintosh; rv:24.0) Gecko/20100101 Firefox/24.0",
    })

    try:
        # We will manually iterate over the redirects.
        # SSL certs are not verified because we don't want to exclude
        # sites with self-signed certs at this stage.
        req = requests.Request('GET', url)
        pr  = sess.prepare_request(req)
        r   = sess.send(pr, allow_redirects=False,
                        verify=False, stream=True)

        # The resolve_redirects generator does not emit the very first
        # response; that's 'r'.  Don't bother invoking it if the first
        # response isn't a redirect.
        process_one_response(r)

        # If we get here, it is a redirect.
        last_resp = r
        for resp in sess.resolve_redirects(r, pr,
                                           verify  = False,
                                           stream  = True):
            last_resp = resp
            process_one_response(resp)

    except requests.exceptions.TooManyRedirects as e:
        # Redirect loop: treat the previous response as anomalous.
        report_http_anomaly(last_resp)

    # requests lumps a whole bunch of different network-layer
    # issues under this exception.  In all cases observed so far,
    # a ConnectionError wraps a urllib3 exception which wraps the
    # actual error.
    except requests.exceptions.ConnectionError as e:
        try:
            neterr = e.args[0].reason
            report_neterror(neterr)
        except:
            report_exc_anomaly()

    # other expected failures
    except requests.exceptions.RequestException as e:
        report(None, str(e))

    except Exception:
        report_exc_anomaly()

def process_one_response(resp):
    # Only code 200 counts as success.
    if resp.status_code == 200:
        report_success(resp.url)

    # Codes 400, 401, 403, 404, 410, 500, and 503 are "normal"
    # failures; they do not get recorded as anomalous.
    if resp.status_code in (400, 401, 403, 404, 410, 500, 503):
        report_http_failure(resp)

    # This logic must match requests.session's idea of what a
    # redirect is.  (When 2.3.0 is out we can switch to resp.is_redirect.)
    if (resp.status_code not in requests.sessions.REDIRECT_STATI
        or "location" not in resp.headers):
        report_http_anomaly(resp)

def report(url, status, anomaly=None):
    # Sometimes the status is in a non-ASCII, non-Unicode, undeclared
    # encoding.
    if hasattr(status, "encode"):
        status = status.encode("ascii", "backslashreplace")
    if hasattr(anomaly, "encode"):
        anomaly = anomaly.encode("ascii", "backslashreplace")
    sys.stdout.buffer.write(pickletools.optimize(pickle.dumps({
        "url":     url,
        "status":  status,
        "anomaly": anomaly
    })))

    if url is not None:
        sys.exit(0)
    else:
        sys.exit(1)

def report_success(canon_url):
    report(canon_url, b"200 OK")

def report_http_failure(resp):
    report(None, "{} {}".format(resp.status_code, resp.reason))

def report_http_anomaly(resp):
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

    # Response headers:
    headers = "\n".join("{}: {}".format(*kv)
                        for kv in sorted(resp.headers.items()))

    # The headers do not include the status line.
    status = "{} {}".format(resp.status_code, resp.reason)
    full_status = "HTTP/{} ".format(resp.raw.version/10.) + status

    # Sometimes headers are in weird encodings.  Theoretically,
    # anyway, the body does not need this treatment because
    # (a) it's captured as a byte string, (b) the headers and/or the
    # first few bytes of the body should tell us what encoding it's in.
    anomaly = ((full_status + "\n" + headers + "\n\n")
               .encode("ascii", "backslashreplace")
               + body)

    report(None, status, anomaly)

def report_neterror(exc):
    # Timeout exceptions are often much too verbose.
    if isinstance(exc, (requests.exceptions.Timeout,
                        socket.timeout)):
        status = "Network timeout"

    # There are two different getaddrinfo() error codes that
    # mean essentially "host not found", and we don't care
    # about the difference.
    elif isinstance(exc, socket.gaierror):
        if exc.errno in (socket.EAI_NONAME, socket.EAI_NODATA):
            status = "DNS error: Unknown host"
        else:
            status = "DNS error: " + exc.strerror

    elif isinstance(exc, socket.error):
        status = "Network error: " + exc.strerror

    # SSL errors are annoyingly repetitive.
    elif isinstance(exc, ssl.SSLError):
        status = ("SSL error: " +
                  re.sub(r'^\[SSL(?:: [A-Z0-9_]+)?\] ', '',
                         re.sub(r' \(_ssl\.c:\d+\)$', '',
                                str(exc))))

    # HTTPException subclasses often have a vague str().
    # Also, str(BadStatusLine) often has a trailing newline.
    elif isinstance(exc, http.client.HTTPException):
        status = "HTTP error ({}): {}".format(exc.__class__.__name__,
                                              str(exc).strip())

    else:
        report_exc_anomaly()

    report(None, status.encode("ascii", "backslashreplace"))

def report_exc_anomaly():
    # This is an exception type that should not have happened.
    # Record a complete traceback in the anomaly field.
    # Relies on sys.exc_info.
    (last_type, last_value, _) = sys.exc_info()
    status = traceback.format_exception_only(last_type, last_value)
    anomaly = traceback.format_exc()
    report(None, status, anomaly)

#
# -----
#

if __name__ == '__main__':
    main()
