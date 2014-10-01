# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Capture the HTML content and a screenshot of each URL in an
existing database, from many locations simultaneously.  Locations are
defined by the config file passed as the "-l" argument, which is line-
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
          helper program (see scripts/openvpn-netns.c).
"""

def setup_argp(ap):
    ap.add_argument("locations",
                    action="store",
                    help="List of location specifications.")
    ap.add_argument("-b", "--batch-size",
                    action="store", dest="batch_size", type=int, default=20,
                    help="Number of URLs to feed to each worker at once.")
    ap.add_argument("-w", "--workers",
                    action="store", dest="n_workers", type=int, default=3,
                    help="Number of concurrent workers per location.")
    ap.add_argument("-p", "--min-proxy-port",
                    action="store", dest="min_proxy_port", type=int,
                    default=9100,
                    help="Low end of range of TCP ports to use for "
                    "local proxy listeners.")
    ap.add_argument("-t", "--tables",
                    action="store", dest="tables",
                    help="Comma-separated list of url-source tables to "
                    "process, without the 'urls_' prefix. (default: all "
                    "of them)")

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
import select
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

def nonblocking_readlines(*files):
    """Generator which yields lines from one or more file objects, which
       are used only for their fileno() and as identifying labels,
       without blocking.  Files open in text mode are decoded
       according to their own stated encoding; files open in binary
       mode are decoded according to locale.getpreferredencoding().
       Newline determination is universal.  For convenience, any Nones
       in 'files' are silently ignored.

       Each yield is (fileobj, string); trailing whitespace is stripped
       from the string.  EOF on a particular file is indicated as
       (fileobj, None), after which that file is dropped from the poll
       set.  The generator will terminate once all files have closed.

       If there is no data available on any of the files still open,
       (None, "") is produced in an endless stream until there is data
       again; caller is expected to sleep for a while.
    """

    class NonblockingBuffer:
        def __init__(self, fp, default_encoding):
            self.fp  = fp
            self.fd  = fp.fileno()
            self.buf = bytearray()
            self.is_open = True
            try:
                self.enc = fp.encoding
            except AttributeError:
                self.enc = default_encoding

            fl = fcntl.fcntl(self.fd, fcntl.F_GETFL)
            fcntl.fcntl(self.fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        def absorb(self):
            while self.is_open:
                try:
                    block = os.read(fd, 8192)
                except BlockingIOError:
                    break

                if block:
                    self.buf.extend(block)
                else:
                    self.is_open = False

            return len(self.buf) > 0 or not self.is_open

        def emit(self):
            def emit1(chunk):
                return (self.fp, chunk.decode(self.enc).rstrip())

            buf = self.buf
            while len(buf) > 0:
                r = buf.find(b'\r')
                n = buf.find(b'\n')
                if r == -1 and n == -1:
                    if not self.is_open:
                        yield emit1(buf)
                        buf.clear()

                elif r == -1 or r > n:
                    yield emit1(buf[:n])
                    buf = buf[(n+1):]

                elif n == -1 or n > r:
                    yield emit1(buf[:r])
                    if n == r+1:
                        buf = buf[(r+2):]
                    else:
                        buf = buf[(r+1):]

            self.buf = buf
            if not self.is_open:
                yield (self.fp, None)

    default_encoding = locale.getpreferredencoding(False)
    poller = select.poll()
    buffers = {}
    for fp in files:
        if fp is None: continue
        fd = fp.fileno()
        buffers[fd] = NonblockingBuffer(fp, default_encoding)
        poller.register(fd, select.POLLIN)

    while buffers:
        # for efficiency, we do actually block for a short while here
        events = poller.poll(50)
        if not events:
            yield (None, "")
            continue

        may_emit = []
        for fd, ev in events:
            buf = buffers[fd]
            if buf.absorb():
                may_emit.append(buf)
            if not buf.is_open:
                buf.fp.close()
                del buffers[fd]
                poller.unregister(fd)

        for buf in may_emit:
            yield from buf.emit()

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

class Proxy:
    """Helper thread which runs and supervises a proxy mechanism that
       tunnels network traffic to another machine.  Events are posted
       whenever the proxy becomes available or unavailable."""

    # Event status codes.  Events are 1-tuples of the status code.
    OFFLINE = 0
    ONLINE  = 1

    def __init__(self, label):
        self._label  = label
        self._queues = []
        self._mon    = None
        self._proc   = None
        self._done   = False
        self._status = "offline"

    def report_status(self, msg):
        self._status = msg
        self._mon.report_status("{}: {}".format(self._label, msg))

    def adjust_command(self, cmd):
        """Adjust the command line vector CMD so that it will make use
           of the proxy.  Must return the modified command; allowed to
           modify it in place."""
        raise NotImplemented

    def stop(self):
        self._done = True
        if self._proc:
            self._stop_proxy()

    def add_queue(self, q):
        self._queues.append(q)

    def _post_online(self):
        for q in self._queues:
            q.put((Proxy.ONLINE,))

    def _post_offline(self):
        for q in self._queues:
            q.put((Proxy.OFFLINE,))

    def _start_proxy(self):
        """Start the proxy.  Must assign a subprocess.Popen object to
           self._proc."""
        raise NotImplemented

    def _stop_proxy(self):
        """Stop the proxy.  Does not wait for it to terminate."""
        raise NotImplemented

    def _handle_proxy_status(self, line, is_stderr, online_cb):
        """Handle a line of output from the proxy.  LINE is either a string,
           or None; the latter indicates EOF.  is_stderr is a boolean,
           indicating whether output was on stdout or stderr.
           Call online_cb with no arguments to signal that the proxy is now
           online and operational."""
        raise NotImplemented

    def __call__(self, mon, thr):
        """Main proxy-supervision loop."""

        self._mon = mon
        backoff = 0
        forced_disconnect = False

        def online_hook():
            self.report_status("online.")
            self._post_online()
            backoff = 0

        def disconnect_hook():
            self._stop_proxy()
            forced_disconnect = True

        while True:
            self.report_status("connecting...")
            self._start_proxy()

            for fp, line in nonblocking_readlines(self._proc.stdout,
                                                  self._proc.stderr):
                if fp is None:
                    self._mon.idle(1, disconnect_hook)
                else:
                    self._handle_proxy_status(line,
                                              fp is self._proc.stderr,
                                              online_hook)
                    self._mon.maybe_pause_or_stop(disconnect_hook)

            # EOF on both pipes indicates the proxy has exited.
            rc = self._proc.wait()
            self._proc = None
            self._post_offline()

            # If self._done is true at this point we should just exit as
            # quickly as possible.
            if self._done:
                self.report_status("shut down.")
                break

            # If forced_disconnect is true, we killed the proxy
            # because the monitor told us to suspend, and the
            # suspension is now over.  So we should restart the
            # proxy immediately.
            if forced_disconnect:
                forced_disconnect = False
                continue

            last_status = self._status
            if last_status == "online.":
                last_status = "disconnected."
            if rc < 0:
                last_status += " ({})".format(strsignal(-rc))
            elif rc > 0:
                last_status += " (exit {})".format(rc)

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

class DirectProxy(Proxy):
    """Stub 'proxy' that doesn't tunnel traffic, permitting it to emanate
       _directly_ from the local machine."""

    TYPE = 'direct'

    def adjust_command(self, cmd):
        return cmd

    def stop(self):
        self._done = True
        self._post_offline()

    def __call__(self, mon, thr):
        self._post_online()

class SshProxy(Proxy):
    """Helper thread which runs and supervises an ssh-based local SOCKS
       proxy that tunnels traffic to another machine."""

    TYPE = 'ssh'

    _next_local_port = None

    @classmethod
    def set_min_local_port(cls, port):
        cls._next_local_port = port

    def __init__(self, label, host_and_user):
        if SshProxy._next_local_port is None:
            raise RuntimeError("SshProxy.set_min_local_port wasn't called")

        self._remote_host = host_and_user
        self._local_port  = SshProxy._next_local_port
        SshProxy._next_local_port += 1

        Proxy.__init__(self, label)

    def adjust_command(self, cmd):
        assert cmd[0] == "isolate"
        for i in range(1, len(cmd)):
            if "=" not in cmd[i]:
                # This is hardwired for the proxy options used by phantomjs.
                assert cmd[i] == "phantomjs"
                cmd[i+1:i+1] = ["--proxy-type=socks5",
                                "--proxy=localhost:{}".format(self._local_port)]
                return cmd

        raise RuntimeError("Actual command not found in {!r}".format(cmd))

    def _start_proxy(self):
        self._proc = subprocess.Popen([
                "ssh", "-2akNTxv", "-e", "none",
                "-o", "BatchMode=yes", "-o", "ServerAliveInterval=30",
                "-D", "localhost:" + str(self._local_port),
                self._remote_host
            ],
            stdin  = subprocess.DEVNULL,
            stdout = subprocess.DEVNULL,
            stderr = subprocess.PIPE)

    def _stop_proxy(self):
        self._proc.terminate()

    def _handle_proxy_status(self, line, is_stderr, online_cb):
        if line is None:
            return

        # stdout was sent to /dev/null, so line is always from stderr.
        if (not line.startswith("debug1:") and
            not line.startswith("Transferred") and
            not line.startswith("OpenSSH_") and
            not line.endswith("closed by remote host.")):
            self.report_status(line)

        if line == "debug1: Entering interactive session.":
            online_cb()

class OpenVPNProxy(Proxy):
    """Helper thread which runs and supervises an OpenVPN-based
       netns proxy that tunnels traffic to another machine."""

    TYPE = 'ovpn'

    def __init__(self, label, *openvpn_args):
        if len(openvpn_args) < 1:
            raise RuntimeError("need at least an OpenVPN config file")

        Proxy.__init__(self, label)
        self._namespace         = "ns_" + label
        self._tried_gentle_stop = False
        self._openvpn_cmd       = [
            "openvpn-netns", self._namespace
        ]
        self._openvpn_cmd.extend(openvpn_args)
        self._state = 0

    def adjust_command(self, cmd):
        assert cmd[0] == "isolate"
        cmd.insert(1, "ISOL_NETNS="+self._namespace)
        return cmd

    def _start_proxy(self):
        self._proc = subprocess.Popen(self._openvpn_cmd,
                                      stdin  = subprocess.PIPE,
                                      stdout = subprocess.PIPE,
                                      stderr = subprocess.PIPE)
        self._tried_gentle_stop = False

    def _stop_proxy(self):
        if self._tried_gentle_stop:
            self._proc.terminate()
        else:
            self._proc.stdin.close()
            self._tried_gentle_stop = True

    def _handle_proxy_status(self, line, is_stderr, online_cb):
        if is_stderr:
            if line is not None:
                self.report_status(line)
        else:
            if line == "READY" and self._state == 0:
                self._state += 1
            elif line is None and self._state == 1:
                self._state += 1
                online_cb()
            else:
                self.report_status("[stdout] " + repr(line))

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
            surl = url_database.canon_url_syntax(url, want_splitresult = True)
            if not surl.hostname:
                self.status = 'invalid URL'
                self.detail = 'URL with no host: {!r}'.format(url)
                return
            else:
                # Will throw UnicodeError if the hostname is
                # syntactically invalid.
                surl.hostname.encode('idna')

            self.original_url = surl.geturl()

        except (ValueError, UnicodeError) as e:
            # The above IDN-encoding test may produce nested exceptions.
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
                "ISOL_RL_MEM=3221225472",
                "PHANTOMJS_DISABLE_CRASH_DUMPS=1",
                "MALLOC_CHECK_=0",
                "phantomjs",
                pj_trace_redir,
                "--capture",
                self.original_url
            ]),
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
            try:
                results = json.loads(line)

                self.canon_url = results["canon"]
                self.status    = results["status"]
                self.detail    = results.get("detail", None)
                self.log['events'] = results.get("log", [])
                if 'content' in results:
                    self.content = zlib.compress(results['content']
                                                 .encode('utf-8'))
                if 'render' in results:
                    self.render = recompress_image(results['render'])

            except:
                anomalous_stdout.append(line)

        if anomalous_stdout:
            self.log["stdout"] = anomalous_stdout
        if not self.status:
            self.status = "garbage output from tracer"
            return False
        return True

    def parse_stderr(self, stderr, valid_result):
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

        # Don't count a crash on exit as a failure if it happened
        # after a valid result was printed.
        if not valid_result:
            if not status:
                status = "crawler failure"
                detail = "unexplained unsuccessful exit"

            self.status = status
            self.detail = detail

        if anomalous_stderr:
            self.log["stderr"] = anomalous_stderr
        elif "stderr" in self.log:
            del self.log["stderr"]

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
            self.log["stderr"] = stderr

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
        if self.canon_url: r['canon']   = self.canon_url
        if self.content:   r['content'] = self.content
        if self.render:    r['render']  = self.render
        if self.log:
            r['log'] = zlib.compress(json.dumps(self.log).encode('utf-8'))
        return r

class CaptureWorker:
    def __init__(self, disp, locale, proxy):
        self.disp = disp
        self.args = disp.args
        self.locale = locale
        self.proxy = proxy
        self.batch_queue = queue.PriorityQueue()
        self.batch_queue_serializer = 0
        self.proxy.add_queue(self.batch_queue)

        self.online = False
        self.nsuccess = 0
        self.nfailure = 0
        self.batch_time = None
        self.batch_avg  = None
        self.alpha = 2/11

    def report_status(self, msg):
        status = ("{}: {} | {} captured, {} failures"
                  .format(self.locale, self.proxy.TYPE,
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

    # batch queue message types/priorities
    _MON_SAYS_STOP  = -1
    _PROXY_OFFLINE  = Proxy.OFFLINE  # known to be 0
    _PROXY_ONLINE   = Proxy.ONLINE   # known to be 1
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
                    self.proxy.stop()
                    self.disp.complete_batch(self.locale, [])
                    break

                if msg[2] == [None]:
                    # No more work to do right now.
                    if self.batch_queue.empty():
                        self.report_status(
                            "waiting for batch... (idle 5 minutes)")
                        self.mon.idle(300)
                else:
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

            report = CaptureTask(todo.popleft(), self.proxy).report()
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
        self.error_log = open("capture-errors.txt", "wt")
        # defer further setup till we're on the right thread

    def __call__(self, mon, thr):
        self.mon = mon
        self.db = url_database.ensure_database(self.args)
        self.prepared_batches = {}
        self.status_queue = queue.PriorityQueue()
        self.status_queue_serializer = 0
        self.mon.register_event_queue(self.status_queue,
                                      (self._MON_SAYS_STOP, -1))

        # We can safely start the workers and get them connecting
        # before we go on to warm up the database.
        SshProxy.set_min_local_port(self.args.min_proxy_port)
        self.read_locations()
        self.workers = {}
        for loc, proxy in self.locales.items():
            self.workers[loc] = []
            self.prepared_batches[loc] = collections.deque()
            for _ in range(self.args.n_workers):
                wt = CaptureWorker(self, loc, proxy)
                self.mon.add_work_thread(wt)
                self.workers[loc].append(wt)

        # Only start the proxies after we've started all the workers.
        for proxy in self.locales.values():
            self.mon.add_work_thread(proxy)

        self.prepare_database()
        self.dispatcher_loop()

    def read_locations(self):
        locales = {}
        valid_loc_re = re.compile("^[a-z]+$")
        with open(self.args.locations) as f:
            for w in f:
                w = w.strip()
                if not w: continue
                if w[0] == '#': continue
                w = w.split()
                if len(w) < 2 or not valid_loc_re.match(w[0]):
                    raise RuntimeError("invalid location: " + " ".join(w))
                loc = w[0]
                if loc in locales:
                    raise RuntimeError("duplicate location: " + " ".join(w))
                method = w[1]
                args = w[2:]
                if method == 'direct': method = DirectProxy
                elif method == 'ssh':  method = SshProxy
                elif method == 'ovpn': method = OpenVPNProxy
                else:
                    raise RuntimeError("unrecognized method: " + " ".join(w))

                locales[loc] = method(loc, *args)

        self.locales = locales
        self.locale_list = sorted(self.locales.keys())

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

            if not self.locale_todo[locale]:
                prepared.append([])
                return

            blocksize = self.args.batch_size * len(self.workers[locale])

            # Special case: all locales except 'us' draw from the pool of
            # URLs already processed in 'us'.
            if locale == 'us':
                cr.execute('SELECT s.url FROM capture_progress c, url_strings s'
                           '  WHERE c.url = s.id'
                           '  AND NOT c."l_{0}" LIMIT {1}'
                           .format(locale, blocksize))
            else:
                cr.execute('SELECT s.url FROM capture_progress c, url_strings s'
                           '  WHERE c.url = s.id'
                           '  AND c."l_us" AND NOT c."l_{0}" LIMIT {1}'
                           .format(locale, blocksize))
            block = [row[0] for row in cr]

            if not block:
                # [None] is a special message that means "I don't have
                # anything for you _now_, wait a while and ask again".
                prepared.append([None])
                return

            prepared.extend(chunked(block, self.args.batch_size))

    def handle_complete_batch(self, cmd, serial, locale, results):
        self.locale_todo[locale] -= len(results)

        with self.db, self.db.cursor() as cr:
            for r in results:

                (url_id, surl) = url_database.add_url_string(cr, r['ourl'])

                redir_url = None
                redir_url_id = None
                if 'canon' in r:
                    redir_url = r['canon']
                    if redir_url == surl or redir_url == r['ourl']:
                        redir_url_id = url_id
                    elif redir_url is not None:
                        try:
                            (redir_url_id, _) = \
                                url_database.add_url_string(cr, r['canon'])
                        except ValueError:
                            r['detail'] += " | invalid redir url: " + redir_url

                detail_id = self.capture_detail.get(r['detail'])
                if detail_id is None and (r['detail'] is not None and
                                          r['detail'] != ''):
                    cr.execute("INSERT INTO capture_detail(id, detail) "
                               "  VALUES(DEFAULT, %s)"
                               "  RETURNING id", (r['detail'],))
                    detail_id = cr.fetchone()[0]
                    self.capture_detail[r['detail']] = detail_id

                (_, result) = url_database.categorize_result(r['status'],
                                                             url_id,
                                                             redir_url_id)

                # stopgap OR IGNORE; not sure why duplicates
                cr.execute("SAVEPOINT ins1")
                to_insert = {
                    "locale":       locale,
                    "url":          url_id,
                    "result":       result,
                    "detail":       detail_id,
                    "redir_url":    redir_url_id,
                    "log":          r.get('log'),
                    "html_content": r.get('content'),
                    "screenshot":   r.get('render')
                }
                try:
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
                except IntegrityError as e:
                    self.error_log.write(
                        "Failed to record captured page: {}\n"
                        "  Original URL: {}\n"
                        "  Redir URL: {}\n"
                        "  Offending row:\n{}\n"
                        .format(e, r['ourl'], redir_url, repr(to_insert)))
                    self.error_log.flush()
                    cr.execute("ROLLBACK TO SAVEPOINT ins1")
                else:
                    cr.execute("RELEASE SAVEPOINT ins1")

    def update_progress_statistics(self):
        jobsize = max(self.locale_todo.values())
        self.mon.report_status("Processing {}/{} URLs | "
                               .format(jobsize, self.complete_jobsize) +
                               " ".join("{}:{}".format(l, t)
                                        for l, t in self.locale_todo.items()))

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
                .format(loc) for loc in self.locale_list)

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

            for loc in self.locale_list:
                self.mon.maybe_pause_or_stop()
                self.mon.report_status("Preparing database... "
                                       "(capture progress values: {})"
                                       .format(loc))

                cr.execute('UPDATE capture_progress c SET "l_{0}" = TRUE'
                           '  FROM captured_pages p'
                           ' WHERE c.url = p.url AND p.locale = \'{0}\''
                           .format(loc))

            for loc in self.locale_list:
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
            for loc in self.locale_list:
                query += ', SUM("l_{0}"::INTEGER) AS "l_{0}"'.format(loc)
            query += " FROM capture_progress"
            cr.execute(query)

            locale_todo = {}
            counts = cr.fetchone()
            self.complete_jobsize = counts[0]
            for loc, done in zip(self.locale_list, counts[1:]):
                locale_todo[loc] = self.complete_jobsize - done
            self.locale_todo = locale_todo

            self.mon.maybe_pause_or_stop()
            self.mon.report_status("Database prepared.")
