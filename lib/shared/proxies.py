# Proxy management.
#
# Copyright © 2014, 2015 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import fcntl
import glob
import locale
import os
import random
import select
import subprocess

# Utilities

# This is a module global because locale.getpreferredencoding(True) is
# not safe to call off-main-thread.
PREFERRED_ENCODING = locale.getpreferredencoding(True)

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

    global PREFERRED_ENCODING
    poller = select.poll()
    buffers = {}
    for fp in files:
        if fp is None: continue
        fd = fp.fileno()
        buffers[fd] = NonblockingBuffer(fp, PREFERRED_ENCODING)
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

# End of utilities

class Proxy:
    """Helper thread which runs and supervises a proxy mechanism that
       tunnels network traffic to another machine.  Also tracks
       performance statistics for that tunnel."""

    def __init__(self, disp, locale):
        self._disp   = disp
        self._locale = locale
        self._mon    = None
        self._proc   = None
        self._done   = False
        self._online = False
        self._detail = "offline"
        self._thrdid = None
        self._nsucc  = 0
        self._nfail  = 0
        self._pavg   = None
        self._alpha  = 2/11
        self._statm  = ""

    def __call__(self, mon, thr):
        """Main proxy-supervision loop."""

        self._mon    = mon
        self._thrdid = thr.ident

        while True:
            try:
                self._proxy_supervision_loop()
                break
            except Exception:
                self._mon.report_exception()
                self.stop()

    # Subclasses should (normally; note DirectProxy) implement these
    # methods.

    def adjust_command(self, cmd):
        """Adjust the command line vector CMD so that it will make use
           of the proxy.  Must return the modified command; allowed to
           modify it in place."""
        raise NotImplemented

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

    # External API
    @property
    def online(self):
        return self._online

    @property
    def locale(self):
        return self._locale

    @property
    def done(self):
        return self._done

    def label(self):
        return "{}:{}".format(self.locale, self.TYPE)

    def report_status(self, msg):
        self._detail = msg
        self._mon.report_status(msg, thread=self._thrdid)

    def update_stats(self, nsucc, nfail, sec_per_url):
        self._nsucc += nsucc
        self._nfail += nfail

        if self._pavg is None:
            self._pavg = sec_per_url
        else:
            self._pavg = self._pavg  * (1-self._alpha) + \
                         sec_per_url *    self._alpha

        self._statm = ("{} captured, {} failures | "
                       "last {:.2f}, avg {:.2f} URLs/hr/wkr | "
                       .format(self._nsucc,
                               self._nfail,
                               3600/sec_per_url,
                               3600/self._pavg))
        self._update_prefix()

    def stop(self):
        self._set_offline()
        if self._proc:
            self._stop_proxy()
            self._proc.wait()
            self._proc = None

    def finished(self):
        if not self._done:
            self._done = True
            self.stop()

    # Internal.
    def _update_prefix(self):
        self._mon.set_status_prefix("p {} {} | {} | "
                                    .format(self.label(),
                                            "up  " if self._online else "down",
                                            self._statm))

    def _set_online(self):
        self._online = True
        self._update_prefix()
        self.disp.proxy_online(self)

    def _set_offline(self):
        self._online = False
        self._update_prefix()
        self.disp.proxy_offline(self)

    def _proxy_supervision_loop(self):
        backoff = 0
        forced_disconnect = False

        def online_hook():
            self._set_online()
            backoff = 0

        def disconnect_hook():
            self._set_offline()
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
            self._online = False

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

            last_detail = self._detail
            if last_detail == "online.":
                last_detail = "disconnected."
            if rc < 0:
                last_detail += " ({})".format(strsignal(-rc))
            elif rc > 0:
                last_detail += " (exit {})".format(rc)

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
                               .format(last_detail, human_idletime))
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

    def __call__(self, mon, thr):
        self._mon = mon
        self._thrdid = thr.ident
        self._online = True
        self.report_status("online.")

        while self._online:
            mon.idle(1)

class OpenVPNProxy(Proxy):
    """Helper thread which runs and supervises an OpenVPN-based
       netns proxy that tunnels traffic to another machine."""

    TYPE = 'ovpn'

    def __init__(self, disp, locale, openvpn_cfg, *openvpn_args):
        Proxy.__init__(self, disp, locale)
        self._tried_gentle_stop = False
        self._namespace = "ns_" + locale
        self._state = 0
        openvpn_cfg = glob.glob(openvpn_cfg)
        random.shuffle(openvpn_cfg)
        self._openvpn_cfgs = collections.deque(openvpn_cfg)
        self._openvpn_args = openvpn_args

    def adjust_command(self, cmd):
        assert cmd[0] == "isolate"
        cmd.insert(1, "ISOL_NETNS="+self._namespace)
        return cmd

    def _start_proxy(self):

        cfg = self._openvpn_cfgs[0]
        self._openvpn_cfgs.rotate(-1)

        openvpn_cmd = [ "openvpn-netns", self._namespace, cfg ]
        openvpn_cmd.extend(self._openvpn_args)

        self._proc = subprocess.Popen(openvpn_cmd,
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
            elif line is not None:
                self.report_status("[stdout] " + repr(line))
