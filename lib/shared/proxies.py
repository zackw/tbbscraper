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
import heapq
import locale
import os
import random
import re
import selectors
import subprocess
import time

from .strsignal import strsignal

# Utilities

def format_exit_status(status):
    if status == 0:
        return "exited normally"
    elif status > 0:
        return "exited abnormally (code {})".format(status)
    else:
        return "killed by signal: " + strsignal(-status)

# This is a module global because locale.getpreferredencoding(True) is
# not safe to call off-main-thread.
DEFAULT_ENCODING = locale.getpreferredencoding(True)

class NonblockingLineBuffer:
    """Helper class used by LineMultiplexor."""

    def __init__(self, fp, lineno, priority):
        global DEFAULT_ENCODING

        self.fp = fp
        self.lineno = lineno
        self.priority = priority

        if hasattr(fp, 'fileno'):
            self.fd = fp.fileno()
            if hasattr(fp, 'encoding'):
                self.enc = fp.encoding
            else:
                self.enc = DEFAULT_ENCODING
        else:
            assert isinstance(fp, int)
            self.fd  = fp
            self.enc = DEFAULT_ENCODING

        self.buf = bytearray()
        self.at_eof = False
        self.carry_cr = False

        fl = fcntl.fcntl(self.fd, fcntl.F_GETFL)
        if not (fl & os.O_NONBLOCK):
            fcntl.fcntl(self.fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    def close(self):
        if hasattr(self.fp, 'close'):
            self.fp.close()
        else:
            os.close(self.fd)

    def absorb(self):
        while True:
            try:
                block = os.read(self.fd, 8192)
            except BlockingIOError:
                break
            if not block:
                self.at_eof = True
                break

            self.buf.extend(block)

        return bool(buf) or self.at_eof

    def emit(self):
        buf = self.buf

        if buf:
            # Deal with '\r\n' having been split between absorb() events.
            if self.carry_cr and buf[0] == b'\n':
                del buf[0]
            self.carry_cr = False

        if buf:
            lines = buf.splitlines()
            if self.is_open and buf[-1] not in (b'\r', b'\n'):
                buf = lines.pop()
            else:
                if buf[-1] == b'\r':
                    self.carry_cr = True
                del buf[:]

            for line in lines:
                yield (self.fp, line.decode(self.enc).rstrip())

        if self.at_eof:
            yield (self.fp, None)

        self.buf = buf


class LineMultiplexor:
    """Priority queue which produces lines from one or more file objects,
       which are used only for their fileno() and as identifying
       labels, as data becomes available on each.  If data is
       available on more than one file at the same time, the user can
       specify the order in which to return them.

       Files open in text mode are decoded according to their own
       stated encoding; files open in binary mode are decoded
       according to locale.getpreferredencoding().  Newline handling
       is universal.

       Files may be added or removed from the pollset with the
       add_file and drop_file methods (in the latter case, the file
       will not be closed).  You can pass in bare fds as well as file
       objects.  Files are automatically removed from the pollset and
       closed when they reach EOF.

       Each item produced by .get() or .peek() is (fileobj, string);
       trailing whitespace is stripped from the string.  EOF on a
       particular file is indicated as (fileobj, None), which occurs
       only once; when it occurs, fileobj has already been closed.

       If no data is available (either from .peek(), or after .get
       times out) the result is (None, None).

       If used as an iterator, iteration terminates when all files have
       reached EOF.  Adding more files will reactivate iteration.
    """

    def __init__(self, default_timeout=None):
        self.poller          = selectors.PollSelector()
        self.output_q        = []
        self.default_timeout = default_timeout
        self.seq             = 0

    def add_file(self, fp, priority=0):
        """Add FP to the poll set with priority PRIORITY (default 0).
           Larger priority numbers are _lower_ priorities.
        """
        buf = NonblockingLineBuffer(fp, lineno = 0, priority = priority)
        self.poller.register(fp, selectors.EVENT_READ, buf)

    def drop_file(self, fp):
        """Remove FP from the poll set.  Does not close the file."""
        self.poller.unregister(fp)

    def peek(self):
        """Returns the first item in the output queue, if any, without
           blocking and without removing it from the queue.
        """
        if not self.output_q:
            self._poll(0)
        return self._extract(False)

    def get(self, timeout=None):
        """Retrieve the first item from the output queue.  If there
           are none, blocks until data arrives or TIMEOUT expires."""
        self._poll(timeout)
        return self._extract(True)

    def __iter__(self):
        return self

    def __next__(self):
        """Iteration calls .get until all files are exhausted."""
        if not self.output_q and not self.poller.get_map():
            raise StopIteration
        return self.get()

    # Internal: queue management.
    def _insert(self, priority, lineno, line, fp):
        # self.seq ensures that everything in the queue is strictly
        # ordered before we get to 'fp', which prevents heapq from
        # trying to sort file objects.
        heapq.heappush(self.output_q, (priority, lineno, line, self.seq, fp))
        self.seq += 1

    def _extract(self, pop):
        if not self.output_q:
            return (None, None)
        if pop:
            qitem = heapq.heappop(self.output_q)
        else:
            qitem = self.output_q[0]
        return (qitem[4], qitem[2])

    # Internal: the core read loop.
    def _poll(self, timeout=None):
        if timeout is None:
            timeout = self.default_timeout

        while True:
            if timeout is not None and timeout > 0:
                entry = time.monotonic()

            events = self.poller.select(timeout)
            if events:
                may_emit = []
                for k, _ in events:
                    buf = k.data
                    if buf.absorb():
                        may_emit.append(buf)

                for buf in may_emit:
                    lineno = buf.lineno
                    prio = buf.priority
                    for line in buf.emit():
                        self._insert(prio, lineno, line, buf.fp)
                        lineno += 1

                    buf.lineno = lineno
                    if buf.at_eof:
                        self.drop_file(buf.fp)
                        buf.close()

            if self.output_q or timeout == 0:
                break

            # If some of the file descriptors are slowly producing very
            # long lines, we might not actually emit any data for longer
            # than the timeout, even though the system call never blocks
            # for too long.  Therefore, we must manually check whether
            # the timeout has expired, and adjust it downward if it hasn't.
            if timeout is not None and timeout > 0:
                now = time.monotonic()
                timeout -= now - entry
                if timeout <= 0:
                    break

# End of utilities

class ProxyMethod:
    """Abstract base class - configuration and state information for one
       proxy which may or may not be active right now.  Knows how to bring
       the proxy up and down and monitor its status.
    """
    def __init__(self, disp, loc):
        self.disp     = disp
        self.loc      = loc
        self.online   = False
        self.starting = False
        self.stopping = False
        self.done     = False
        self.proc     = None

    # Subclasses must implement:
    def adjust_command(self, cmd):
        """Adjust the command line vector CMD so that it will make use
           of the proxy.  Must return the modified command; allowed to
           modify it in place."""
        raise NotImplemented

    def start(self):
        """Start the proxy; do not wait for it to come up all the way.
           This method must adjust self.starting/stopping/online
           appropriately, and normally should also assign a
           subprocess.Popen object to self.proc.
        """
        raise NotImplemented

    def stop(self):
        """Stop the proxy; do not wait for it to terminate.
           This method must adjust state.starting/stopping/online
           appropriately.  If state.stopping is already true, should
           do something more aggressive than it did the first time.
        """
        raise NotImplemented

    def handle_proxy_output(self, line, is_stderr):
        """Handle a line of output from the proxy.  LINE is either a string,
           or None; the latter indicates EOF.  is_stderr is a boolean,
           indicating whether output was on stdout or stderr.
           self.starting/stopping/online should be updated as appropriate.
           Return True if a significant state change has occurred.
        """
        raise NotImplemented

    def handle_proxy_exit(self):
        """Handle the proxy having exited.  waitpid() has already been called.
           self.starting/stopping/online should be updated as appropriate.
           Return True if a significant state change has occurred.
        """
        raise NotImplemented

class DirectMethod(ProxyMethod):
    """Stub 'proxy' that doesn't tunnel traffic, permitting it to emanate
       _directly_ from the local machine."""

    TYPE = 'direct'

    def adjust_command(self, cmd):
        return cmd

    def start(self):
        state.online = True

    def stop(self):
        state.online = False

    # handle_proxy_output should never be called

class OpenVPNMethod(ProxyMethod):
    """OpenVPN-based proxy that tunnels traffic to another machine."""

    TYPE = 'ovpn'

    def __init__(self, disp, loc, cfg, *args):
        ProxyMethod.__init__(self, disp, loc)
        self._namespace    = "ns_" + loc
        self._openvpn_args = args

        openvpn_cfg = glob.glob(openvpn_cfg)
        random.shuffle(openvpn_cfg)
        self._openvpn_cfgs = collections.deque(openvpn_cfg)

    def adjust_command(self, cmd):
        assert cmd[0] == "isolate"
        cmd.insert(1, "ISOL_NETNS="+self._namespace)
        return cmd

    def start(self):
        if self.online or self.starting or self.stopping or self.done:
            return
        assert self.proc is None

        cfg = self._openvpn_cfgs[0]
        self._openvpn_cfgs.rotate(-1)

        openvpn_cmd = [ "openvpn-netns", self._namespace, cfg ]
        openvpn_cmd.extend(self._openvpn_args)

        self.starting = True
        self.proc = subprocess.Popen(openvpn_cmd,
                                     stdin  = subprocess.PIPE,
                                     stdout = subprocess.PIPE,
                                     stderr = subprocess.PIPE)

    def stop(self):
        if self.done or not self.online:
            return
        assert self.proc is not None

        self.starting = False
        if not self.stopping:
            self.proc.stdin.close() # this should trigger a clean shutdown
            self.stopping = True

        else:
            # we already tried a clean shutdown and it didn't work
            self.proc.terminate()

    def handle_proxy_output(self, line, is_stderr):
        assert self.proc is not None
        if line is None:
            if self.proc.stdout.closed and self.proc.stderr.closed:
                # This should only happen if the process is about to exit.
                if not self.stopping:
                    self.proc.stdin.close()
                    self.stopping = True

                if self.proc.returncode is not None:
                    self.stopping = False
                    self.online = False
                    self.proc = None

                return True

        elif line == "READY" and self.starting and not is_stderr:
            self.starting = False
            self.online = True
            return True

        return False

    def handle_proxy_exit(self):
        assert self.proc is not None
        if not self.stopping:
            self.proc.stdin.close()
            self.stopping = True

        if self.proc.stdout.closed and self.proc.stderr.closed:
            self.stopping = False
            self.online = False
            self.proc = None

        return True


class ProxyRunner:
    """Helper thread which runs and supervises a proxy mechanism that
       tunnels network traffic to another machine.  Also tracks
       performance statistics for that tunnel."""

    # Control pipe messages.  All three of _BRING_DOWN, _FINISHED, and
    # _INTERRUPT cause the proxy to be brought down; _INTERRUPT and
    # _FINISHED also have other effects.  Note that it is necessary to
    # tack on a newline whenever you write one of these to the control
    # pipe, because of the way LineMultiplexor works.
    _BRING_DOWN = b"d"
    _FINISHED   = b"f"
    _INTERRUPT  = b"i"
    _BRING_UP   = b"u"

    # Initialization and finalization.

    def __init__(self, disp, locale):
        self._disp    = disp
        self._locale  = locale
        self._mon     = None
        self._proc    = None
        self._done    = False
        self._online  = False
        self._detail  = "offline"
        self._thrdid  = None
        self._nsucc   = 0
        self._nfail   = 0
        self._pavg    = None
        self._poller  = None
        self._alpha   = 2/11
        self._statm   = ""
        self._log     = self._open_logfile()
        self._cpipe_r, self._cpipe_w = os.pipe2(os.O_NONBLOCK|os.O_CLOEXEC)

    def __del__(self):
        os.close(self._cpipe_r)
        os.close(self._cpipe_w)

    # Subclasses should (normally; note DirectProxy) implement these
    # methods.

    def adjust_command(self, cmd):
        raise NotImplemented

    def _start_proxy(self):
        raise NotImplemented

    def _stop_proxy(self):
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

    def start(self):
        self._cpipe_w.write(self._BRING_UP + b'\n')

    def stop(self):
        self._cpipe_w.write(self._BRING_DOWN + b'\n')

    def finished(self):
        self._cpipe_w.write(self._FINISHED + b'\n')

    # Internal.
    def _state_tag(self):
        if self._online:
            return "up"
        elif self._done:
            return "closed"
        elif self._proc is not None:
            return "starting"
        else:
            return "down"

    def _update_prefix(self):
        self._mon.set_status_prefix("p {} {} | {} | "
                                    .format(self.label(),
                                            self._state_tag(),
                                            self._statm))

    def _open_logfile(self):
        fp = open("proxy_" + self.label().replace(":","_") + ".log",
                  mode="at",
                  buffering=1, # line-buffered
                  encoding="utf-8",
                  errors="backslashreplace") # in case of miscoded proxy chatter
        fp.write("\n")
        return fp

    def _log_proxy_status(self, line, is_stderr):
        if line is None: line = "<<EOF>>"
        self._log.write("{} [{}]: {}: {}\n".format(
            datetime.datetime.utcnow().isoformat(' '),
            self._state_tag(),
            "err" if is_stderr else "out",
            line.strip()))

    def _log_proxy_exit(self, status):
        self._log.write("{} [{}]: proxy {}\n".format(
            datetime.datetime.utcnow().isoformat(' '),
            self._state_tag(),
            format_exit_status(status)))

    def _do_start(self):
        ...

    def _do_stop(self):
        ...

    def _drain(self):
        # This is a secondary event loop which runs when we are trying
        # to bring the proxy down.  Requests to bring it back up are
        # discarded, but requests to interrupt or finish will be
        # reported to caller, which must honor them.  Output from the
        # proxy is logged but not passed to _handle_proxy_status.

        have_proxy = self._proc is not None
        pending_interrupt = False
        pending_finish = False

        while True:
            fp, data = self._poller.get(None if have_proxy else 0)
            if fp is None:
                if not have_proxy: break

            if fp is self._cpipe_r:
                if   data == _FINISHED:  pending_finish    = True
                elif data == _INTERRUPT: pending_interrupt = True
            else:
                is_stderr = fp is self._proc.stderr
                self._log_proxy_status(data, is_stderr)
                if data is None:
                    # We assume that the proxy will only close
                    # both its stdout and stderr if it's about
                    # to exit.
                    if (self._proc.stdout.closed and
                        self._proc.stderr.closed):
                        self._set_offline()
                        self._log_proxy_exit(self._proc.wait())
                        self._proc = None
                        have_proxy = False

        return pending_finish, pending_interrupt

    def _set_online(self):
        self._online = True
        self._update_prefix()
        self._disp.proxy_online(self)

    def _set_offline(self):
        self._online = False
        self._update_prefix()
        self._disp.proxy_offline(self)

    # Main loop.
    def __call__(self, mon, thr):
        self._mon    = mon
        self._thrdid = thr.ident
        self._mon.register_event_pipe(self._cpipe_w, self._INTERRUPT+b'\n')
        self._poller = LineMultiplexor()
        # The control pipe is given _lower_ priority than the output
        # from the proxy client.
        self._poller.add_file(self._cpipe_r, priority=1)
        self._update_prefix()

        stdout_closed = True
        stderr_closed = True

        while True:
            try:
                # There is no deduplication of messages on the control
                # pipe, so we need to drain it, recording all of the
                # messages received, and only then process them.  This
                # loop handles status messages from the proxy as a
                # side effect.
                pending_start     = False
                pending_stop      = False
                pending_interrupt = False
                pending_finish    = False
                fp, data = self._poller.get()
                while fp is not None:
                    if fp is self._cpipe_r:
                        if   data == _BRING_DOWN: pending_stop      = True
                        elif data == _FINISHED:   pending_finish    = True
                        elif data == _INTERRUPT:  pending_interrupt = True
                        elif data == _BRING_UP:   pending_start     = True
                    else:
                        is_stderr = fp is self._proc.stderr
                        self._log_proxy_status(data, is_stderr)
                        if self._handle_proxy_status(data, is_stderr):
                            self._set_online()

                        if data is None:
                            # We assume that the proxy will only close
                            # both its stdout and stderr if it's about
                            # to exit.
                            if (self._proc.stdout.closed and
                                self._proc.stderr.closed):
                                self._set_offline()
                                self._log_proxy_exit(self._proc.wait())
                                self._proc = None

                    fp, data = self._poller.get(0)

                # Now we know everything we're supposed to do, do it, in the
                # appropriate order.
                if ((pending_stop or pending_interrupt or pending_finish)
                    and self._proc):
                    self._do_stop()
                    pf, pi = self._drain()
                    pending_finish |= pf
                    pending_interrupt |= pi

                if pending_finish:
                    self._done = True
                    self._update_prefix()
                    return

                if pending_interrupt:
                    self._mon.maybe_pause_or_stop()

                if pending_start:
                    self._do_start()

            except Exception:
                self._mon.report_exception()
                self._do_stop()
                self._drain()
                pf, pi = self._drain()
                if pf:
                    self._done = True
                    self._update_prefix()
                    return
                if pi:
                    self._mon.maybe_pause_or_stop()

class DirectProxy(Proxy):

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
