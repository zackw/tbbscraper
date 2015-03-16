# Proxy management.
#
# Copyright © 2014, 2015 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import datetime
import fcntl
import glob
import locale
import os
import random
import re
import select
import subprocess
import time

# Utilities

# This is a module global because locale.getpreferredencoding(True) is
# not safe to call off-main-thread.
DEFAULT_ENCODING = locale.getpreferredencoding(True)

def multiplex_readlines(*files, timeout=None):
    """Generator which yields lines from one or more file objects, which
       are used only for their fileno() and as identifying labels,
       in whatever order data becomes available on each.

       Files open in text mode are decoded according to their own
       stated encoding; files open in binary mode are decoded
       according to locale.getpreferredencoding().  Newline
       determination is universal.  For convenience, any Nones in
       'files' are silently ignored.

       Each yield is (fileobj, string); trailing whitespace is
       stripped from the string.  EOF on a particular file is
       indicated as (fileobj, None), which occurs only once; when it
       occurs, fileobj has already been closed and dropped from the
       pollset.  The generator will terminate once all files have
       closed.

       If timeout is not None, it is the maximum amount of time to
       block waiting for data, in milliseconds; if the timeout
       expires, (None, None) will be yielded.
    """

    class NonblockingBuffer:
        def __init__(self, fp):
            self.fp  = fp
            self.fd  = fp.fileno()
            self.buf = bytearray()
            self.is_open = True
            self.yielded_this_cycle = False
            try:
                self.enc = fp.encoding
            except AttributeError:
                global DEFAULT_ENCODING
                self.enc = DEFAULT_ENCODING

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
                    # We don't close the file here because the generator
                    # needs to remove the fd from the poll set first.

            return len(self.buf) > 0 or not self.is_open

        def emit(self):
            def emit1(chunk):
                return (self.fp, chunk.decode(self.enc).rstrip())

            buf = self.buf
            self.yielded_this_cycle = False
            while len(buf) > 0:
                r = buf.find(b'\r')
                n = buf.find(b'\n')
                if r == -1 and n == -1:
                    if not self.is_open:
                        self.yielded_this_cycle = True
                        yield emit1(buf)
                        buf.clear()

                elif r == -1 or r > n:
                    self.yielded_this_cycle = True
                    yield emit1(buf[:n])
                    buf = buf[(n+1):]

                elif n == -1 or n > r:
                    self.yielded_this_cycle = True
                    yield emit1(buf[:r])
                    if n == r+1:
                        buf = buf[(r+2):]
                    else:
                        buf = buf[(r+1):]

            self.buf = buf
            if not self.is_open:
                self.yielded_this_cycle = True
                yield (self.fp, None)

    poller = select.poll()
    buffers = {}
    for fp in files:
        if fp is None: continue
        fd = fp.fileno()
        buffers[fd] = NonblockingBuffer(fp)
        poller.register(fd, select.POLLIN)

    if timeout is not None:
        last_emit = time.clock_gettime(time.CLOCK_MONOTONIC)

    while buffers:
        events = poller.poll(timeout)
        emitted = False
        if events:
            may_emit = []
            for fd, ev in events:
                buf = buffers[fd]
                if buf.absorb():
                    may_emit.append(buf)
                if not buf.is_open:
                    del buffers[fd]
                    poller.unregister(fd)
                    buf.fp.close()

            for buf in may_emit:
                yield from buf.emit()
                emitted |= buf.yielded_this_cycle

        # If some of the file descriptors are slowly producing very
        # long lines, we might not actually emit any data for longer
        # than the timeout, even though the system call never blocks
        # for too long.  Therefore, we must manually check whether
        # any data has been emitted within the timeout interval.
        if timeout is not None:
            now = time.clock_gettime(time.CLOCK_MONOTONIC)
            if emitted:
                last_emit = now
            else:
                if now - last_emit > timeout:
                    yield (None, None)
                    last_emit = now


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
        self._log    = self._open_logfile()
        self._mpipe  = None

    def __call__(self, mon, thr):
        """Main proxy-supervision loop."""

        self._mon    = mon
        self._thrdid = thr.ident

        mpipe_r = None
        mpipe_w = None
        try:
            mpipe_r, mpipe_w = os.pipe()
            self._mon.register_event_pipe(mpipe_w, b"INT\n")
            self._mpipe = os.fdopen(mpipe_r, "rt", encoding="ascii")

            while True:
                try:
                    self._proxy_supervision_loop()
                    break
                except Exception:
                    self._mon.report_exception()
                    self.stop()

        finally:
            if mpipe_w is not None:
                os.close(mpipe_w)
            if self._mpipe is not None:
                self._mpipe.close()
            elif mpipe_r is not None:
                os.close(mpipe_r)

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

    def _handle_proxy_status(self, line, is_stderr):
        """Handle a line of output from the proxy.  LINE is either a string,
           or None; the latter indicates EOF.  is_stderr is a boolean,
           indicating whether output was on stdout or stderr.

           Return True to signal that the proxy is now online and
           operational, False otherwise.
        """
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

    def update_stats(self, nsucc, nfail, sec_per_job):
        self._nsucc += nsucc
        self._nfail += nfail

        if self._pavg is None:
            self._pavg = sec_per_job
        else:
            self._pavg = self._pavg  * (1-self._alpha) + \
                         sec_per_job *    self._alpha

        self._statm = ("{} complete, {} failures | "
                       "last {:.2f}, avg {:.2f} jobs/hr/wkr | "
                       .format(self._nsucc,
                               self._nfail,
                               3600/sec_per_job,
                               3600/self._pavg))
        self._update_prefix()

    def stop(self):
        if self._online:
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

    def _open_logfile(self):
        fp = open("proxy_" + self.label().replace(":","_") + ".log", "at")
        fp.write("\n")
        return fp

    def _log_proxy_status(self, line, is_stderr):
        self._log.write("{} [{}]: {}: {}\n".format(
            datetime.datetime.utcnow().isoformat(' '),
            "up" if self._online else "down",
            "err" if is_stderr else "out",
            line.strip()))

    def _set_online(self):
        self._online = True
        self._update_prefix()
        self._disp.proxy_online(self)

    def _set_offline(self):
        self._online = False
        self._update_prefix()
        self._disp.proxy_offline(self)

    def _proxy_supervision_loop(self):
        backoff = 0
        forced_disconnect = False

        while True:
            self.report_status("connecting...")
            self._start_proxy()

            stdout_closed = False
            stderr_closed = False
            for fp, line in multiplex_readlines(self._proc.stdout,
                                                self._proc.stderr,
                                                self._mpipe):
                if fp is self._mpipe:
                    assert line == "INT"
                    forced_disconnect = True
                    self._set_offline()
                    self._stop_proxy()
                    self._mon.maybe_pause_or_stop()

                else:
                    is_stderr = fp is self._proc.stderr
                    if line is None:
                        if is_stderr: stderr_closed = True
                        else:         stdout_closed = True

                    self._log_proxy_status(line, is_stderr)
                    if self._handle_proxy_status(line, is_stderr):
                        self._set_online()
                        backoff = 0

                    if stderr_closed and stdout_closed:
                        break

            # EOF on both pipes indicates the proxy has exited.
            rc = self._proc.wait()
            self._proc = None
            if self._online:
                self._set_offline()

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

    def _handle_proxy_status(self, line, is_stderr):
        if is_stderr:
            if line is not None:
                self.report_status(line)
        else:
            if line == "READY" and self._state == 0:
                self._state += 1
            elif line is None and self._state == 1:
                self._state += 1
                return True
            elif line is not None:
                self.report_status("[stdout] " + repr(line))
        return False

class ProxyConfig:
    """Configuration information for one proxy which may or may not
       be active right now."""
    def __init__(self, loc, method, args):
        self.loc    = loc
        self.method = method
        self.args   = args
        self.proxy  = None
        self.done   = False

    def start_proxy(self, disp):
        assert self.proxy is None
        self.proxy = self.method(disp, self.loc, *self.args)
        return self.proxy

    def stop_proxy(self, disp):
        assert self.proxy is not None
        self.proxy.stop()
        self.done = self.proxy.done
        self.proxy = None

    def finish_proxy(self, disp):
        assert self.proxy is not None
        self.proxy.finished()
        self.done = self.proxy.done
        self.proxy = None

class ProxySet:
    """Logic for starting proxies and tracking active proxies.  This is
    not itself the dispatcher object that proxies communicate with,
    but it helps to implement one.

    The 'args' object passed to the constructor must have these
    properties:

     - proxy_config: pathname of a proxy configuration file;
     - max_simultaneous_proxies: maximum number of proxies to allow to
       run simultaneously.

    Proxy configuration files are line-oriented, one proxy per line,
    in the format

        loc  method  args...

    'loc' is an arbitrary label, at least two characters,
    restricted to ASCII letters, digits, and underscores.
    It must be unique in the file.

    'method' should be one of the concrete proxy subclasses' TYPEs
    (currently either 'direct' or 'ovpn').

    'args' is split on whitespace and given to the proxy class's
    constructor as additional arguments.

    Entire lines may be commented out with a leading '#', but '#'
    elsewhere in a line is not special.

    """

    _VALID_LOC_RE  = re.compile("^[a-z0-9_]+$")
    _PROXY_METHODS = {
        cls.TYPE : cls
        for cls in globals().values()
        if (isinstance(cls, type)
            and cls is not Proxy
            and issubclass(cls, Proxy))
    }

    def __init__(self, args, proxy_sort_key=None):
        """Constructor.  ARGS is as described above.  PROXY_SORT_KEY takes two
           arguments, the 'loc' and 'method' fields of the proxy
           config file in that order, and controls the order in which
           proxies should be started; they are put in a sorted list
           using this as the sort key, and started low to high.  The
           default is to sort all 'direct' proxies first, and then
           alphabetically by 'loc'."""
        if proxy_sort_key is None:
            proxy_sort_key = lambda l, m: (m != 'direct', l)

        self.proxy_sort_key  = proxy_sort_kley
        self.args            = args
        self.proxy_configs   = {}
        self.active_proxies  = []
        self.active_configs  = []
        self.unused_configs  = []
        self.crashed_configs = []

        with open(self.args.locations) as f:
            for w in f:
                w = w.strip()
                if not w: continue
                if w[0] == '#': continue

                w = w.split()
                if len(w) < 2 or not self._VALID_LOC_RE.match(w[0]):
                    raise RuntimeError("invalid location: " + " ".join(w))

                loc = w[0]
                if loc in self.proxy_configs:
                    raise RuntimeError("duplicate location: " + " ".join(w))

                method = w[1]
                if method not in self._PROXY_METHODS:
                    raise RuntimeError("unrecognized method: " + " ".join(w))
                method = self._PROXY_METHODS[method]

                args = w[2:]
                self.proxy_configs[loc] = ProxyConfig(loc, method, args)

        self._load_unused_configs(self.proxy_configs.values())

    def _load_unused_configs(self, configs):
        """Internal: refill and sort the set of proxies that we could
           start up."""
        self.unused_configs.extend(configs)
        self.unused_configs.sort(
            key = lambda v: self.proxy_sort_key(v.loc, v.method.TYPE))
        self.unused_configs.reverse()

    def start_a_proxy(self, disp):
        """Pick a proxy and start it.  Returns the proxy object, or None if
           there are no more proxies that can be started now."""

        if len(self.active_proxies) >= self.args.max_simultaneous_proxies:
            return None

        if not self.unused_configs:
            if not self.crashed_configs:
                return None

            self._load_unused_configs(self.crashed_configs)
            self.crashed_configs.clear()

        cfg = self.unused_configs.pop()
        self.active_configs.append(cfg)

        proxy = cfg.start_proxy(disp)
        self.active_proxies.append(proxy)
        return proxy

    def drop_proxy(proxy):
        """Record that PROXY has gone or should go offline.  If it is not
           "done", its configuration is put in a "crashed" list; it
           will be retried by start_a_proxy only after we work through
           the full list of not-yet-used proxies.
        """
        self.active_proxies.remove(proxy)
        for i, cfg in enumerate(self.active_configs):
            if cfg.proxy is proxy:
                del self.active_configs[i]
                cfg.stop_proxy()
                if not cfg.done:
                    self.crashed_configs.append(cfg)
                break
