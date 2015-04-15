# Proxy management.
#
# Copyright © 2014, 2015 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import collections
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

import threading
PROXY_SET_LOG = open("proxy-set-operations.log",
                     mode="at", buffering=1, encoding="utf-8",
                     errors="backslashreplace")

def PSL(s):
    PROXY_SET_LOG.write("{:x}: {}\n".format(threading.get_ident(), s))

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

        return bool(self.buf) or self.at_eof

    def emit(self):
        buf = self.buf
        NL = ord(b'\n')
        CR = ord(b'\r')

        if buf:
            # Deal with '\r\n' having been split between absorb() events.
            if self.carry_cr and buf[0] == b'\n':
                del buf[0]
            self.carry_cr = False

        if buf:
            lines = buf.splitlines()
            if buf[-1] != NL and buf[-1] != CR and not self.at_eof:
                iline = lines.pop()
                if len(buf) >= 2 and buf[-2] == CR and buf[-2] == NL:
                    iline.append(CR)
                    iline.append(NL)
                else:
                    iline.append(buf[-1])
                buf = iline

            else:
                if buf[-1] == b'\r':
                    self.carry_cr = True
                del buf[:]

            for line in lines:
                s = line.decode(self.enc).rstrip()
                yield s

        if self.at_eof:
            yield None

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
    def __init__(self, loc):
        self.loc      = loc
        self.online   = False
        self.starting = False
        self.stopping = False
        self.done     = False
        self.proc     = None

    @property
    def fully_offline(self):
        return not (self.online or self.starting or self.stopping)

    def state_tag(self):
        if   self.starting: return "starting"
        elif self.stopping: return "stopping"
        elif self.online:   return "up"
        elif self.done:     return "closed"
        else:               return "down"

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

    def handle_proxy_output(self, line, fp):
        """Handle a line of output from the proxy.  LINE is either a string,
           or None; the latter indicates EOF.  FP is the file object that
           produced LINE.
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
        self.online = True

    def stop(self):
        self.online = False

    # handle_proxy_output should never be called
    # handle_proxy_exit should never be called

class OpenVPNMethod(ProxyMethod):
    """OpenVPN-based proxy that tunnels traffic to another machine."""

    TYPE = 'ovpn'

    def __init__(self, loc, cfg, *args):
        ProxyMethod.__init__(self, loc)
        self._namespace    = "ns_" + loc
        self._openvpn_args = args

        openvpn_cfg = glob.glob(cfg)
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

    def handle_proxy_output(self, line, fp):
        if self.proc is None: return True

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

        elif line == "READY" and self.starting and fp is self.proc.stdout:
            self.starting = False
            self.online = True
            return True

        return False

    def handle_proxy_exit(self):
        if self.proc is None: return True

        if not self.stopping:
            self.proc.stdin.close()
            self.stopping = True

        if self.proc.stdout.closed and self.proc.stderr.closed:
            self.stopping = False
            self.online = False
            self.proc = None

        return True

class ProxyRunner:
    """Helper thread which runs and supervises a proxy, and tracks
       performance statistics.
    """

    # Control pipe messages.  All three of _BRING_DOWN, _FINISHED, and
    # _INTERRUPT cause the proxy to be brought down; _INTERRUPT and
    # _FINISHED also have other effects.  Note that it is necessary to
    # tack on a newline whenever you write one of these to the control
    # pipe, because of the way LineMultiplexor works.
    _BRING_DOWN   =  "d"
    _BRING_DOWN_W = b"d\n"

    _FINISHED     =  "f"
    _FINISHED_W   = b"f\n"

    _INTERRUPT    =  "i"
    _INTERRUPT_W  = b"i\n"

    _BRING_UP     =  "u"
    _BRING_UP_W   = b"u\n"


    def __init__(self, disp, loc, method, *, alpha=2/11):
        self.disp    = disp
        self.loc     = loc
        self.method  = method

        self.mon     = None
        self.poller  = None
        self.thrdid  = None
        self.log     = self.open_logfile()

        self.nsucc   = 0
        self.nfail   = 0
        self.pavg    = None
        self.alpha   = alpha
        self.statm   = ""
        self.detail  = ""

        # These fields are used by ProxySet.
        self.cycle        = 0
        self.backoff      = 0
        self.last_attempt = 0

    def __call__(self, mon, thr):
        self.mon    = mon
        self.thrdid = thr.ident
        try:
            self.cpipe_r, self.cpipe_w = os.pipe2(os.O_NONBLOCK|os.O_CLOEXEC)
            self.mon.register_event_pipe(self.cpipe_w, self._INTERRUPT_W)
            self.poller = LineMultiplexor()
            # The control pipe is given _lower_ priority than the output
            # from the proxy client.
            self.poller.add_file(self.cpipe_r, priority=1)
            self.update_prefix()

            self.pending_start = False
            self.pending_stop  = False
            self.pending_intr  = False
            self.pending_fini  = False

            while not self.method.done:
                try:
                    self.proxy_supervision_cycle()
                except Exception:
                    self.mon.report_exception()

        finally:
            os.close(self.cpipe_r)
            os.close(self.cpipe_w)

    # External API
    @property
    def online(self):
        return self.method.online and not self.method.stopping

    @property
    def done(self):
        return self.method.done

    def adjust_command(self, command):
        return self.method.adjust_command(command)

    def label(self):
        return "{}:{}".format(self.loc, self.method.TYPE)

    def report_status(self, msg):
        self.detail = msg
        self.mon.report_status(msg, thread=self.thrdid)

    def update_stats(self, nsucc, nfail, sec_per_job):
        self.nsucc += nsucc
        self.nfail += nfail

        if self.pavg is None:
            self.pavg = sec_per_job
        else:
            self.pavg = self.pavg    * (1-self.alpha) + \
                         sec_per_job *    self.alpha

        self.statm = ("{} complete, {} failures | "
                      "last {:.2f}, avg {:.2f} jobs/hr/wkr | "
                      .format(self.nsucc,
                              self.nfail,
                              3600/sec_per_job,
                              3600/self.pavg))
        self.update_prefix()

    def start(self):
        os.write(self.cpipe_w, self._BRING_UP_W)

    def stop(self):
        os.write(self.cpipe_w, self._BRING_DOWN_W)

    def finished(self):
        os.write(self.cpipe_w, self._FINISHED_W)

    # Internal.
    def update_prefix(self):
        self.mon.set_status_prefix("p {} {} | {} | "
                                   .format(self.label(),
                                           self.method.state_tag(),
                                           self.statm))

    def open_logfile(self):
        fp = open("proxy_" + self.label().replace(":","_") + ".log",
                  mode="at",
                  buffering=1, # line-buffered
                  encoding="utf-8",
                  errors="backslashreplace") # in case of miscoded proxy chatter
        fp.write("\n")
        return fp

    def log_raw(self, line):
        self.mon.report_status(line)
        self.log.write("{} [{}]: {}\n".format(
            datetime.datetime.utcnow().isoformat(' '),
            self.method.state_tag(),
            line))

    def log_proxy_status(self, line):
        if line is None: line = "<<EOF>>"
        self.log_raw(line)

    def log_proxy_exit(self, status):
        self.log_raw("exit: " + format_exit_status(status))

    def log_proxy_ctl(self, what):
        self.log_raw("ctl: " + what)

    def do_start(self):
        if (not self.method.fully_offline) or self.method.done:
            return

        self.method.start()
        self.update_prefix()
        self.pending_start = False

        if self.method.proc is not None:
            self.poller.add_file(self.method.proc.stdout, priority=0)
            self.poller.add_file(self.method.proc.stderr, priority=0)

        if (self.method.online and not self.method.stopping):
            self.disp.proxy_online(self)

    def do_stop(self):
        if self.method.done:
            return

        self.method.stop()
        self.update_prefix()
        self.pending_stop = False

        if self.method.fully_offline:
            if self.pending_fini:
                self.method.done = True
                self.pending_fini = False
            self.disp.proxy_offline(self)

    def process_proxy_message(self, fp, data):
        self.log_proxy_status(data)
        if self.method.handle_proxy_output(data, fp):
            self.update_prefix()
            if self.method.online and not self.method.stopping:
                self.disp.proxy_online(self)

            if self.method.online and self.method.stopping:
                status = self.method.proc.wait()
                self.log_proxy_exit(status)
                self.method.handle_proxy_exit()

                assert self.method.fully_offline
                if self.pending_fini:
                    self.method.done = True
                    self.pending_fini = False
                self.disp.proxy_offline(self)

    # Main loop.
    def proxy_supervision_cycle(self):
        # Process actions from the previous cycle.
        if self.pending_stop or self.pending_intr or self.pending_fini:
            self.log_proxy_ctl("stop requested")
            self.do_stop()

        if self.method.done:
            self.log_proxy_ctl("finished")
            return

        # Delay start and interrupt actions until the proxy is
        # fully offline.
        if self.method.fully_offline:
            if self.pending_intr:
                self.pending_intr = False
                self.log_proxy_ctl("interrupted")
                self.mon.maybe_pause_or_stop()

            if self.pending_start:
                self.log_proxy_ctl("start requested")
                self.do_start()
        else:
            if self.pending_intr:
                self.log_proxy_ctl("interrupt pending")
            if self.pending_start:
                self.log_proxy_ctl("start pending")

        # There is no deduplication of messages on the control
        # pipe, so we need to drain it, recording all of the
        # messages received, and only then process them.
        # This loop handles status messages from the proxy
        # as a side effect.
        fp, data = self.poller.get()
        while fp is not None:
            if fp is self.cpipe_r:
                self.log_proxy_ctl("control message: " + repr(data))
                if   data == self._BRING_DOWN: self.pending_stop  = True
                elif data == self._FINISHED:   self.pending_fini  = True
                elif data == self._INTERRUPT:  self.pending_intr  = True
                elif data == self._BRING_UP:   self.pending_start = True
                else:
                    raise RuntimeError("invalid control message: " + repr(data))
            else:
                self.process_proxy_message(fp, data)
            fp, data = self.poller.get(0)


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

    'loc' is an arbitrary label, which must begin with two or three
    lowercase ASCII letters (an ISO country code) and then optionally
    an underscore followed by any sequence of lowercase ASCII letters,
    digits, and underscores.  It must be unique in the file.

    'method' should be one of the concrete proxy subclasses' TYPEs
    (currently either 'direct' or 'ovpn').

    'args' is split on whitespace and given to the proxy class's
    constructor as additional arguments.

    Entire lines may be commented out with a leading '#', but '#'
    elsewhere in a line is not special.

    """

    _VALID_LOC_RE  = re.compile("^[a-z]{2,3}(?:_[a-z0-9_]+)?$")
    _PROXY_METHODS = {
        cls.TYPE : cls
        for cls in globals().values()
        if (isinstance(cls, type)
            and issubclass(cls, ProxyMethod)
            and hasattr(cls, 'TYPE'))
    }

    def __init__(self, disp, mon, args, proxy_sort_key=None):
        """Constructor.  ARGS is as described above.  PROXY_SORT_KEY takes two
           arguments, the 'loc' and 'method' fields of the proxy
           config file in that order, and controls the order in which
           proxies should be started; they are put in a sorted list
           using this as the sort key, and started low to high.  The
           default is to sort all 'direct' proxies first, and then
           alphabetically by 'loc'."""
        if proxy_sort_key is None:
            proxy_sort_key = lambda l, m: (m != 'direct', l)

        self.proxy_sort_key  = proxy_sort_key
        self.args            = args
        self.locations       = {}
        self.active_proxies  = set()
        self.waiting_proxies = collections.deque()
        self.crashed_proxies = set()

        with open(self.args.locations) as f:
            proxies = []
            for w in f:
                w = w.strip()
                if not w: continue
                if w[0] == '#': continue

                w = w.split()
                #if len(w) < 2 or not self._VALID_LOC_RE.match(w[0]):
                #    raise RuntimeError("invalid location: " + " ".join(w))

                loc = w[0]
                if loc in self.locations:
                    raise RuntimeError("duplicate location: " + " ".join(w))

                method = w[1]
                if method not in self._PROXY_METHODS:
                    raise RuntimeError("unrecognized method: " + " ".join(w))
                method = self._PROXY_METHODS[method]

                args = w[2:]
                method = method(loc, *args)
                runner = ProxyRunner(disp, loc, method)
                mon.add_work_thread(runner)
                self.locations[loc] = runner
                proxies.append(runner)

        self.args.max_simultaneous_proxies = \
            min(self.args.max_simultaneous_proxies, len(proxies))
        self._refill_waiting_proxies(proxies)

    def _refill_waiting_proxies(self, new_proxies=None):
        """Internal: refill and sort the set of proxies that we could start."""

        def full_proxy_sort_key(proxy):
            disp_key = self.proxy_sort_key(proxy.loc, proxy.method.TYPE)
            return (proxy.cycle, proxy.backoff, disp_key)

        if new_proxies is None: new_proxies = []
        for proxy in self.crashed_proxies:
            proxy.cycle += 1
            # Exponential backoff, starting at 15 seconds and going up to
            # one hour.
            if proxy.backoff == 0:
                proxy.backoff = 15
            elif proxy.backoff < 3600:
                proxy.backoff *= 2
            new_proxies.append(proxy)

        self.crashed_proxies.clear()

        new_proxies.sort(key=full_proxy_sort_key)
        self.waiting_proxies.extend(new_proxies)

    def start_a_proxy(self):
        """Pick a proxy and start it.  Returns a 3-tuple: the first
           entry is the proxy object, or None if there are no more
           proxies that can be started now; the second entry is how
           long to wait before calling this method again, or None if
           caller should wait until at least one proxy has exited;
           the third entry is how many proxies are still not 'done'.
        """

        PSL("active {} max {}".format(len(self.active_proxies),
                                      self.args.max_simultaneous_proxies))
        if len(self.active_proxies) >= self.args.max_simultaneous_proxies:
            return (None, None, len(self.locations))

        if not self.waiting_proxies:
            PSL("no proxies waiting")
            if not self.crashed_proxies:
                PSL("no proxies crashed")
                return (None, None, len(self.locations))
            self._refill_waiting_proxies()
            PSL("after refill {} waiting".format(len(self.waiting_proxies)))
        else:
            PSL("{} waiting".format(len(self.waiting_proxies)))

        now = time.monotonic()
        proxy = None
        min_backoff = 3600
        for i, cand in enumerate(self.waiting_proxies):
            remaining = (cand.last_attempt + cand.backoff) - now
            PSL("cand {} last_attempt {} backoff {} remaining {}"
                .format(cand.label(), cand.last_attempt, cand.backoff,
                        remaining))
            if remaining <= 0:
                PSL("{} selected".format(cand.label()))
                proxy = cand
                del self.waiting_proxies[i]
                break
            else:
                min_backoff = min(min_backoff, remaining)

        if proxy is None:
            PSL("no proxy selected")
            return (None, max(min_backoff, 5), len(self.locations))

        proxy.last_attempt = now
        self.active_proxies.add(proxy)
        proxy.start()

        return (proxy, 5, len(self.locations))

    def note_proxy_online(self, proxy):
        assert proxy in self.active_proxies
        proxy.backoff = 0

    def note_proxy_offline(self, proxy):
        """Record that PROXY has gone offline.  If it is not "done", its
           configuration is put in a "crashed" list; it will be
           retried by start_a_proxy only after we work through the
           full list of not-yet-used proxies.
        """
        self.active_proxies.discard(proxy)
        if proxy.done:
            del self.locations[proxy.loc]
        else:
            self.crashed_proxies.add(proxy)
