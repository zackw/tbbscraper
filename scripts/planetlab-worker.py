#! /usr/bin/python

# Worker script for PlanetLab.  Note: unlike most of the Python in
# this repo, this program is written specifically to run under Python
# 2.5 (because PlanetLab slices are FC8 unless you go the trouble of
# fixing that). This program is invoked after the bootstrap process
# completes, so we have a guarantee that all the ancillary programs we
# need, exist.  (See lib/url_sources/s_capture.py for the bootstrap.)
#
# There are no command-line arguments.
# There is a list of URLs to process waiting for us on standard input.
# We read them all in one gulp, feed back results as they come, and
# exit when done; this avoids having to worry about deadlocks.
# Incomplete batches are the dispatcher's problem.

from __future__ import with_statement

import cStringIO
import os
import re
import socket
import subprocess
import sys
import tempfile
import urlparse
import zlib

pj_trace_redir = os.path.realpath(os.path.join(
    os.path.dirname(__file__), "pj-trace-redir.js"))

# Python 2.5 does not have the json module.  This is a
# close-enough-for-our-purposes dictionary-to-JSON converter.
# It only handles string keys and numeric and string values.
# (We avoid decoding the JSON responses from pj-trace-redir.)
# Some code cribbed from the 2.7 json module.

ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')
ESCAPE_DCT = {
    '\x00':  '\\u0000',
    '\x01':  '\\u0001',
    '\x02':  '\\u0002',
    '\x03':  '\\u0003',
    '\x04':  '\\u0004',
    '\x05':  '\\u0005',
    '\x06':  '\\u0006',
    '\x07':  '\\u0007',
    '\x08':  '\\b',
    '\t':    '\\t',
    '\n':    '\\n',
    '\x0b':  '\\u000b',
    '\x0c':  '\\f',
    '\r':    '\\r',
    '\x0e':  '\\u000e',
    '\x0f':  '\\u000f',
    '\x10':  '\\u0010',
    '\x11':  '\\u0011',
    '\x12':  '\\u0012',
    '\x13':  '\\u0013',
    '\x14':  '\\u0014',
    '\x15':  '\\u0015',
    '\x16':  '\\u0016',
    '\x17':  '\\u0017',
    '\x18':  '\\u0018',
    '\x19':  '\\u0019',
    '\x1a':  '\\u001a',
    '\x1b':  '\\u001b',
    '\x1c':  '\\u001c',
    '\x1d':  '\\u001d',
    '\x1e':  '\\u001e',
    '\x1f':  '\\u001f',
    '"':     '\\"',
    '\\':    '\\\\'
}

def to_json_string(s):
    def replace(match):
        return ESCAPE_DCT[match.group(0)]
    return '"' + ESCAPE.sub(replace, s) + '"'

def write_json_string(f, s):
    f.write(to_json_string(s))

def write_json_dict(f, d):
    f.write('{')
    first = True
    for k, v in d.iteritems():
        if not first:
            f.write(',')
        first = False

        write_json_string(f, k)
        f.write(':')
        if isinstance(v, basestring):
            write_json_string(f, v)
        elif isinstance(v, (int, long, float)):
            f.write(str(v))
        elif isinstance(v, dict):
            write_json_dict(f, v)
        elif v is None:
            f.write("null")
        else:
            raise TypeError("cannot serialize: " + repr(v))
    f.write('}')

def string_dict_as_json(d):
    f = cStringIO.StringIO()
    write_json_dict(f, d)
    return f.getvalue()

# The zlib module - even in the latest 3.x - does not have filelikes.
# Hulk is not amused.

class ZlibOutputStream(object):
    def __init__(self, fp):
        self.fp = fp
        self.z  = zlib.compressobj()

    def write(self, data):
        self.fp.write(self.z.compress(data))

    def flush(self):
        self.fp.write(self.z.flush(zlib.Z_SYNC_FLUSH))
        self.fp.flush()

    def close(self):
        if self.z is not None:
            self.fp.write(self.z.flush(zlib.Z_FINISH))
            self.fp.close()
            self.fp = None
            self.z = None

    def __enter__(self):
        return self

    def __exit__(self, *dontcare):
        self.close()

    def __del__(self):
        self.close()


# This code largely shared with lib/url_sources/s_canonize.py.

_stdout_junk_re = re.compile(
    r"^(?:"
    r"|[A-Z][a-z]+Error: .*"
    r"|[A-Z_]+?_ERR: .*"
    r"|Cannot init XMLHttpRequest object!"
    r"|Error requesting /.*"
    r"|Current location: https?://.*"
    r"|  (?:https?://.*?|undefined)?:[0-9]+(?: in \S+)?"
    r")$")

class Task(object):
    def __init__(self, url):
        self.original_url = url
        self.proc = None
        self.result = None
        self.stub_result = {
            "ourl": url,
            "status": None,
            "detail": None,
            "canon": None,
            "redirs": {"http":0, "html":0, "js":0},
            "anomaly": {}
        }

        # Attempt a DNS lookup for the URL's hostname right now.  This
        # preloads the DNS cache, reduces overhead in the surprisingly
        # common case where the hostname is not found (2.85%), and most
        # importantly, catches the rare URL that is *so* mangled that
        # phantomjs just gives up and reports nothing at all.
        try:
            host = urlparse.urlsplit(url).hostname
            dummy = socket.getaddrinfo(host, 80)

        except ValueError, e:
            self.stub_result["status"] = "invalid URL"
            return

        except socket.gaierror, e:
            if e.errno not in (socket.EAI_NONAME, socket.EAI_NODATA):
                raise
            self.stub_result["status"] = "hostname not found"
            return

        # We use a temporary file for the results, instead of a pipe,
        # so we don't have to worry about reading them until after the
        # child process exits.
        self.result_fd = tempfile.TemporaryFile("w+t")
        self.errors_fd = tempfile.TemporaryFile("w+t")
        self.proc = subprocess.Popen(
            [
                "isolate",
                "env", "PHANTOMJS_DISABLE_CRASH_DUMPS=1", "MALLOC_CHECK_=0",
                "phantomjs",
                "--ssl-protocol=any",
                pj_trace_redir,
                "--capture",
                self.original_url
            ],
            stdout=self.result_fd,
            stderr=self.errors_fd)


    def parse_stdout(self, stdout):
        # Under conditions which are presently unclear, PhantomJS dumps
        # javascript console errors to stdout despite script logic which
        # is supposed to intercept them; so we need to scan through all
        # lines of output looking for something with the expected form.
        if not stdout:
            self.stub_result["status"] = "crawler failure"
            self.stub_result["detail"] = "no output from tracer"
            return False

        non_junk = []
        for line in stdout.splitlines():
            if _stdout_junk_re.match(line):
                continue
            non_junk.append(line)

        # After stripping junk, there should be exactly one line and it
        # should start with a curly brace.
        if len(non_junk) == 1 and non_junk[0][0] == '{':
            self.result = non_junk[0]
            return True
        else:
            self.stub_result["status"] = "crawler failure"
            self.stub_result["detail"] = "garbage output from tracer"
            self.stub_result["anomaly"]["stdout"] = stdout
            return False

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
                        self.stub_result["detail"] = status
                        status = "crawler failure"

            elif "bad_alloc" in err:
                # PJS's somewhat clumsy way of reporting memory
                # allocation failure.
                if not status:
                    self.stub_result["detail"] = "out of memory"
                    status = "crawler failure"
            else:
                anomalous_stderr.append(err)

        if not valid_result:
            if not status:
                self.stub_result["detail"] = "unexplained exit code 1"
                status = "crawler failure"
            self.stub_result["status"] = status

        if anomalous_stderr:
            self.stub_result["anomaly"]["stderr"] = anomalous_stderr
        elif "stderr" in self.stub_result["anomaly"]:
            del self.stub_result["anomaly"]["stderr"]

    def pickup_results(self, status):

        self.result_fd.seek(0)
        stdout = self.result_fd.read()
        self.result_fd.close()
        self.errors_fd.seek(0)
        stderr = self.errors_fd.read()
        self.result_fd.close()

        valid_result = self.parse_stdout(stdout)
        stderr = stderr.splitlines()
        if stderr and (len(stderr) > 1 or stderr[0] != ''):
            self.stub_result["anomaly"]["stderr"] = "\n".join(stderr)

        if status >= 0:
            exitcode = status
            if exitcode == 0:
                pass
            elif exitcode == 1:
                self.parse_stderr(stderr, valid_result)
            else:
                self.stub_result["status"] = "crawler failure"
                self.stub_result["detail"] = \
                    "unexpected exit code %d" % exitcode

        else:
            self.stub_result["status"] = "crawler failure"
            self.stub_result["detail"] = \
                "Killed by signal %d " % -status

    def report(self, outf):
        if self.proc is not None:
            self.pickup_results(self.proc.wait())

        if self.result:
            outf.write(self.result)
        else:
            if not self.stub_result["anomaly"]:
                del self.stub_result["anomaly"]
            write_json_dict(outf, self.stub_result)

def main():
    urls = [u for u in (v.strip() for v in
                        zlib.decompress(sys.stdin.read()).splitlines())
            if u]

    # Python !@#$% silently $!#@% ignores %@!$# sys.stdin.close().
    # But it seems to DTRT if we swap out the file descriptor underneath it.
    os.close(0)
    fd = os.open("/dev/null", os.O_RDONLY)
    assert fd == 0

    with ZlibOutputStream(sys.stdout) as output:

        # The output is expected to be a (compressed) JSON array.
        output.write("[")
        first = True

        for url in urls:
            if not first:
                output.write(",\n")
            first = False

            try:
                task = Task(url)
                task.report(output)

            except Exception, e:
                stub_result = {
                    "ourl": url,
                    "status": "crawler failure",
                    "detail": "Python exception: " + str(e),
                    "canon": None,
                    "redirs": {"http":0, "html":0, "js":0},
                }
                write_json_dict(output, stub_result)

        output.write("\n]")

main()
