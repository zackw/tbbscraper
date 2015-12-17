# Proxy management (asyncio version).
#
# Copyright © 2014, 2015 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import asyncio
import collections
import glob
import locale
import random
import re
import shlex
import subprocess
import sys
import time

# Utilities

from .strsignal import strsignal
def format_exit_status(status):
    if status == 0:
        return "exited normally"
    elif status > 0:
        return "exited abnormally (code {})".format(status)
    else:
        return "killed by signal: {}" + strsignal(-status)

# This is a module global because locale.getpreferredencoding(True) is
# not safe to call off-main-thread.
DEFAULT_ENCODING = locale.getpreferredencoding(True)

class LineBuffer:
    def __init__(self, enc=None):
        self.buf      = bytearray()
        self.at_eof   = False
        self.carry_cr = False
        self.enc      = enc or DEFAULT_ENCODING

    def absorb(self, data):
        self.at_eof = False
        self.buf.extend(data)

    def absorb_eof(self):
        self.at_eof = True

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

        self.buf = buf

class ProxySubprocessError(subprocess.CalledProcessError):
    def __str__(self):
        return "Proxy management subprocess '{}' {}".format(
            self.cmd, format_exit_status(self.returncode))

class NamespaceManager:
    """Runs the 'tunnel-ns' program to create namespaces for the proxies
    to use."""

    class TunnelNSProtocol(asyncio.SubprocessProtocol):
        def __init__(self, result_f, exit_f):
            self._result_f = result_f
            self._exit_f = exit_f
            self._output = bytearray()
            self._transport = None

        def connection_made(self, transport):
            self._transport = transport

        def stop(self):
            self._transport.get_pipe_transport(0).write_eof()

        def pipe_data_received(self, fd, data):
            if self._result_f.done(): return
            if fd != 1:
                self._result_f.set_exception(RuntimeError(
                    "received data on unexpected fd, {} != 1".format(fd)))
                return
            self._output.extend(data)

        def pipe_connection_lost(self, fd, exc):
            if self._result_f.done(): return
            if exc is not None:
                self._result_f.set_exception(exc)
                return
            if fd != 1:
                self._result_f.set_exception(RuntimeError(
                    "received EOF on unexpected fd, {} != 1".format(fd)))
                return

            self._result_f.set_result(self._output.decode("ascii").split())
            self._output = None

        def process_exited(self):
            if self._exit_f.done(): return
            status = self._transport.get_returncode()
            if status:
                self._exit_f.set_exception(ProxySubprocessError(
                    'tunnel-ns', status))
            else:
                self._exit_f.set_result(None)

            if not self._result_f.done():
                self._result_f.set_exception(RuntimeError(
                    "tunnel-ns subprocess exited prematurely"))
                self._output = None


    def __init__(self, nsprefix, nnamespaces, loop=None):
        if loop is None: loop = asyncio.get_event_loop()

        self.ready = False
        self.namespaces = []

        self._loop = loop
        self._nns = nnamespaces
        self._nsp = nsprefix
        self._result_f = None
        self._exit_f = None
        self._sp_t = None
        self._sp_p = None

    @asyncio.coroutine
    def start(self):
        if self._exit_f is not None:
            raise RuntimeError("misuse: start() called while running")

        self._result_f = asyncio.Future(loop=self._loop)
        self._result_f.add_done_callback(self._become_ready)

        self._exit_f = asyncio.Future(loop=self._loop)
        self._exit_f.add_done_callback(self._become_unready)

        self._sp_t, self._sp_p = yield from self._loop.subprocess_exec(
            lambda: NamespaceManager.TunnelNSProtocol(self._result_f,
                                                      self._exit_f),
            'tunnel-ns', self._nsp, str(self._nns),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=2)

        yield from self._result_f
        return self.namespaces

    @asyncio.coroutine
    def stop(self):
        if self._sp_p is not None:
            self._sp_p.stop()
        if self._exit_f is not None:
            try:
                yield from self._exit_f
            finally:
                self._exit_f = None

    def _become_ready(self, f):
        try:
            self.namespaces = f.result()
            self.ready = True
        except:
            if self._sp_p is not None:
                self._sp_p.stop()

    def _become_unready(self, f):
        self.ready = False
        self.namespaces = []
        self._sp_t.close()
        self._sp_t = None
        self._sp_p = None
        self._result_f = None
        # don't clear self._exit_f yet, stop() may not yet have been called

class BaseProxyManager:
    """Base class -- runs and supervises some kind of proxy, and tracks
       performance statistics.  Subclasses are responsible for implementing
       proxy-specific behavior.
    """
    def __init__(self, loop, loc):
        self._loop        = loop
        self.loc          = loc
        self.online       = False
        self.starting     = False
        self.stopping     = False
        self.done         = False
        self.cycle        = 0
        self.backoff      = 0
        self.last_attempt = 0

    @property
    def fully_offline(self):
        return not (self.online or self.starting or self.stopping)

    def state_tag(self):
        if   self.starting: return "starting"
        elif self.stopping: return "stopping"
        elif self.online:   return "up"
        elif self.done:     return "closed"
        else:               return "down"

    def label(self):
        return "{} ({})".format(self.loc, self.TYPE)

    def close(self):
        """Client should call this method when it is completely done using
           this proxy."""
        self.done = True
        self.stop()

    # Subclasses must implement:
    def adjust_command(self, cmd):
        """Adjust the command line vector CMD so that it will make use
           of the proxy.  Must return the modified command; allowed to
           modify it in place."""
        raise NotImplemented

    @asyncio.coroutine
    def start(self, ns):
        """Start the proxy and wait for it to come up all the way.
           This method must adjust self.starting/stopping/online
           appropriately.  NS is the network namespace to use.
        """
        raise NotImplemented

    def stop(self):
        """Signal the proxy to stop.
           This method is _not_ a coroutine and may not yield.
           This method must adjust state.starting/stopping/online
           appropriately.  If state.stopping is already true, should
           do something more aggressive than it did the first time.
        """
        raise NotImplemented

    @asyncio.coroutine
    def wait(self):
        """Wait for the proxy to terminate."""
        raise NotImplemented

class DirectProxyManager(BaseProxyManager):
    """Stub 'proxy' that doesn't tunnel traffic, permitting it to emanate
       _directly_ from the local machine."""

    TYPE = 'direct'

    def __init__(self, loop, loc, *args):
        BaseProxyManager.__init__(self, loop, loc)
        self._stop_e = asyncio.Event(loop=loop)
        self._stop_e.set() # proxy is not running

    def adjust_command(self, cmd):
        return cmd

    @asyncio.coroutine
    def start(self, ns):
        sys.stderr.write(self.label() + ": online.\n")
        self.online = True
        self._stop_e.clear()

    def stop(self):
        sys.stderr.write(self.label() + ": offline.\n")
        self.online = False
        self._stop_e.set()

    @asyncio.coroutine
    def wait(self):
        yield from self._stop_e.wait()

class OpenVPNProxyManager(BaseProxyManager):
    """Proxy that tunnels traffic to another machine using OpenVPN."""

    TYPE = 'ovpn'

    class OpenVPNProxyProtocol(asyncio.SubprocessProtocol):
        def __init__(self, ready_f, exit_f, label):
            self._label   = label
            self._ready_f = ready_f
            self._exit_f  = exit_f
            self._obuf    = LineBuffer()
            self._ebuf    = LineBuffer()

        def connection_made(self, transport):
            self._transport = transport

        def stop(self, try_harder):
            if try_harder:
                self._transport.terminate()
            else:
                self._transport.get_pipe_transport(0).write_eof()

        def pipe_data_received(self, fd, data):
            if fd == 1:
                self._obuf.absorb(data)
                for line in self._obuf.emit():
                    self.handle_proxy_output(line)
            elif fd == 2:
                self._ebuf.absorb(data)
                for line in self._ebuf.emit():
                    sys.stderr.write("{}: {}\n".format(self._label, line))
            else:
                sys.stderr.write("{}: unexpected data on fd {}: {!r}\n"
                                 .format(self._label, fd, data))

        def pipe_connection_lost(self, fd, exc):
            if fd == 1:
                self._obuf.absorb_eof()
                for line in self._obuf.emit():
                    self.handle_proxy_output(line)

            elif fd == 2:
                self._ebuf.absorb_eof()
                for line in self._ebuf.emit():
                    sys.stderr.write("{}: {}\n".format(self._label, line))

            elif fd == 0:
                # This fires when we write EOF on the stdin pipe, even
                # though it really shouldn't.
                pass

            else:
                sys.stderr.write("{}: unexpected EOF on fd {}\n"
                                 .format(self._label, fd))

        def handle_proxy_output(self, line):
            if line == "READY" and not self._ready_f.done():
                self._ready_f.set_result(True)

        def process_exited(self):
            if self._exit_f.done(): return
            status = self._transport.get_returncode()
            # Proxy subprocesses crash all the time and it's no big deal.
            if status:
                sys.stderr.write("{}: {}\n".format(
                    self._label, format_exit_status(status)))
            if not self._ready_f.done():
                sys.stderr.write("{}: quit during startup\n"
                                 .format(self._label))
                self._ready_f.set_result(False)
            self._exit_f.set_result(None)


    def __init__(self, loop, loc, cfg, *args):
        BaseProxyManager.__init__(self, loop, loc)
        self._namespace    = None
        self._openvpn_args = args
        self._exit_f       = None
        self._ready_f      = None
        self._sp_t         = None
        self._sp_p         = None

        openvpn_cfg = glob.glob(cfg)
        if not openvpn_cfg:
            raise RuntimeError("{!r} does not match any config files"
                               .format(cfg))
        random.shuffle(openvpn_cfg)
        self._openvpn_cfgs = collections.deque(openvpn_cfg)

    def adjust_command(self, cmd):
        assert cmd[0] == "isolate"
        assert self._namespace is not None
        cmd.insert(1, "ISOL_NETNS="+self._namespace)
        return cmd

    def _become_online(self, fut):
        self.starting = False
        # _become_offline could have fired already (in the
        # crash-during-startup case). The future is never
        # set to an exception.
        if fut.result() and self._ready_f is not None:
            sys.stderr.write(self.label() + ": online.\n")
            self.online = True

    def _become_offline(self, fut):
        self.starting = False
        self.stopping = False
        self.online   = False
        sys.stderr.write(self.label() + ": offline.\n")

        self._sp_t.close()
        self._sp_t = None
        self._sp_p = None
        self._ready_f = None
        self._namespace = None
        # don't clear self._exit_f yet, wait() may not yet have been called

    @asyncio.coroutine
    def start(self, ns):
        if self._exit_f is not None:
            raise RuntimeError("misuse: start() called while running")

        self._namespace = ns

        sys.stderr.write(self.label() + ": starting.\n")
        self.starting = True

        self._ready_f = asyncio.Future(loop=self._loop)
        self._ready_f.add_done_callback(self._become_online)

        self._exit_f = asyncio.Future(loop=self._loop)
        self._exit_f.add_done_callback(self._become_offline)

        cfg = self._openvpn_cfgs[0]
        self._openvpn_cfgs.rotate(-1)
        command = [ "openvpn-netns", self._namespace, cfg ]
        command.extend(self._openvpn_args)

        self._sp_t, self._sp_p = yield from self._loop.subprocess_exec(
            lambda: self.OpenVPNProxyProtocol(self._ready_f,
                                              self._exit_f,
                                              self.label()),
            *command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        yield from self._ready_f

    def stop(self):
        if self._exit_f is None:
            return
        sys.stderr.write(self.label() + ": stopping.\n")
        self._sp_p.stop(self.stopping)
        self.stopping = True

    @asyncio.coroutine
    def wait(self):
        if self._exit_f is not None:
            try:
                yield from self._exit_f
            finally:
                self._exit_f = None

def ProxyManager(loop, loc, method, args):
    if method == 'direct': return DirectProxyManager(loop, loc, *args)
    if method == 'ovpn':   return OpenVPNProxyManager(loop, loc, *args)

    raise RuntimeError("unrecognized method: " + method)


class ProxySet:
    """Runs proxies and assigns them to namespaces.
    Owns a NamespaceManager and one or more ProxyManagers.

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

    _VALID_LOC_RE   = re.compile("^[a-z]{2,3}(?:_[a-z0-9_]+)?$")
    _VALID_NSTAG_RE = re.compile("^[a-z]+$")

    def __init__(self, args, *,
                 nstag="t",
                 loop=None,
                 proxy_sort_key=None,
                 include_locations=None):
        """Constructor.  ARGS is as described above.  NSTAG is a label
           for all of the namespaces created by this program; it must
           consist entirely of lowercase ASCII letters. LOOP is an event
           loop.

           PROXY_SORT_KEY takes two arguments, the 'loc' and 'method'
           fields of the proxy config file in that order, and controls
           the order in which proxies should be started; they are put
           in a sorted list using this as the sort key, and started
           low to high.  The default is to sort all 'direct' proxies
           first, and then alphabetically by 'loc'.

           If INCLUDE_LOCATIONS is not None, it must be a set of
           locations (anything for which "'str' in X" works) and only
           the proxies for those locations will be activated.
        """
        if not self._VALID_NSTAG_RE.match(nstag):
            raise ValueError("namespace tag must be entirely ASCII lowercase")

        if proxy_sort_key is None:
            proxy_sort_key = lambda l, m: (m != 'direct', l)

        if loop is None: loop = asyncio.get_event_loop()

        self.loop            = loop
        self.proxy_sort_key  = proxy_sort_key
        self.nstag           = nstag
        self.nsmgr           = None
        self.avail_nss       = None
        self.args            = args
        self.locations       = {}
        self.active_proxies  = set()
        self.proxy_runners   = set()
        self.waiting_proxies = collections.deque()
        self.crashed_proxies = set()
        self.proxy_crash_evt = asyncio.Event(loop=loop)

        with open(self.args.locations) as f:
            proxies = []
            for w in f:
                w = w.strip()
                if not w: continue
                if w[0] == '#': continue

                w = shlex.split(w)
                loc    = w[0]
                method = w[1]
                args   = w[2:]
                if loc in self.locations:
                    raise RuntimeError("duplicate location: " + " ".join(w))

                if include_locations is None or loc in include_locations:
                    proxy = ProxyManager(self.loop, loc, method, args)
                    self.locations[loc] = proxy
                    self.crashed_proxies.add(proxy)

        self.max_simultaneous_proxies = \
            min(self.args.max_simultaneous_proxies, len(self.crashed_proxies))

    def _refill_waiting_proxies(self):
        """Internal: refill and sort the set of proxies that we could start."""

        def full_proxy_sort_key(proxy):
            disp_key = self.proxy_sort_key(proxy.loc, proxy.TYPE)
            return (proxy.cycle, proxy.backoff, disp_key)

        new_proxies = []
        for proxy in self.crashed_proxies:
            if proxy.done:
                try: del self.locations[proxy.loc]
                except KeyError: pass
                continue

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

        # It's possible that the dispatcher has decided some of the
        # waiting proxies are no longer required, since we were last here.
        waiting_proxies = []
        waiting_proxies.extend(self.waiting_proxies)
        waiting_proxies.extend(new_proxies)
        self.waiting_proxies.clear()

        for cand in waiting_proxies:
            if cand.done:
                try: del self.locations[cand.loc]
                except KeyError: pass
            else:
                self.waiting_proxies.append(cand)

        # Also discard all completed _run_one_proxy tasks.
        # This is much simpler.
        self.proxy_runners = [r for r in self.proxy_runners if not r.done()]

        # No proxies are crashed anymore.
        self.proxy_crash_evt.clear()

    def _select_proxy_to_start(self):
        # This is a backstop; under no circumstances will the main loop
        # suspend itself for longer than this.
        min_backoff = 3600
        proxy = None

        nwait    = len(self.waiting_proxies)
        nactive  = len(self.active_proxies)
        sys.stderr.write("pset: {} proxies active, {} waiting\n"
                         .format(nactive, nwait))

        if nactive >= self.max_simultaneous_proxies:
            sys.stderr.write("pset: no more simultaneous proxies allowed\n")
            return (proxy, min_backoff)

        # collections.deque needs to grow a _mutable_ iterator,
        # one that supports "delete the element I'm looking at now"
        # without getting confused about how long the list is.
        nrotate  = 0
        now = time.monotonic()
        while nrotate < nwait:
            cand = self.waiting_proxies[0]
            if cand.done:
                self.waiting_proxies.popleft()
                nwait -= 1
                try: del self.locations[cand.loc]
                except KeyError: pass
                continue

            remaining = (cand.last_attempt + cand.backoff) - now
            if remaining <= 0 and proxy is None:
                proxy = cand
                self.waiting_proxies.popleft()
                nwait -= 1
            else:
                # Hardwired minimum 5-second delay between starting proxies.
                min_backoff = min(min_backoff, max(remaining, 5))
                self.waiting_proxies.rotate(-1)
                nrotate += 1

        # We have now rotated waiting_proxies all the way around.
        # We may or may not have a proxy to start, and we definitely
        # do have a time to wait before starting another one.
        return (proxy, min_backoff)

    @asyncio.coroutine
    def _run_one_proxy(self, client, proxy):
        """Internal: start and monitor one proxy."""
        posted_online = False

        self.active_proxies.add(proxy)
        ns = self.avail_nss.pop()

        yield from proxy.start(ns)
        if proxy.online:
            posted_online = True
            yield from client.proxy_online(proxy)
            proxy.backoff = 0

        yield from proxy.wait()
        if posted_online:
            yield from client.proxy_offline(proxy)

        proxy.last_attempt = time.monotonic()
        self.active_proxies.discard(proxy)
        self.crashed_proxies.add(proxy)
        self.avail_nss.add(ns)
        self.proxy_crash_evt.set()

    @asyncio.coroutine
    def run(self, client):
        """Run proxies until they are all done.  CLIENT is an object with
        these callbacks, both of which are expected to be coroutines:

        proxy_online(proxy)  - PROXY has come fully online and can be used.
        proxy_offline(proxy) - PROXY has shut down; any work in progress
                               via to that proxy will need to be cancelled.
        """

        # This is just a wrapper which ensures that everything gets
        # torn down properly, since context managers don't play nice
        # with asyncio yet.
        try:
            yield from self._run(client)
        finally:
            yield from self._teardown(client)

    @asyncio.coroutine
    def _run(self, client):
        # We must bring up the namespace manager before doing anything else.
        # Proxies are not obliged to use a namespace, but we don't know which
        # ones need them and which don't, so assume the worst.
        self.nsmgr = NamespaceManager(self.nstag,
                                      self.max_simultaneous_proxies,
                                      self.loop)
        avail_nss = yield from self.nsmgr.start()
        self.avail_nss = set(avail_nss)

        # Proxies are removed from self.locations when they become 'done'.
        while self.locations:
            if self.crashed_proxies:
                self._refill_waiting_proxies()

            (proxy, till_next) = self._select_proxy_to_start()

            if proxy:
                sys.stderr.write("pset: selected {}\n".format(proxy.label()))
                self.proxy_runners.append(
                    self.loop.create_task(self._run_one_proxy(client, proxy)))

            if self.locations:
                sys.stderr.write("pset: next start in {}s\n"
                                 .format(till_next))
                try:
                    yield from asyncio.wait_for(self.proxy_crash_evt.wait(),
                                                timeout=till_next,
                                                loop=self.loop)
                    n_done = sum(1 for proxy in self.crashed_proxies
                                 if proxy.done)
                    sys.stderr.write("pset: {} proxies stopped, {} done\n"
                                     .format(len(self.crashed_proxies),
                                             n_done))
                except asyncio.TimeoutError:
                    pass

    @asyncio.coroutine
    def _teardown(self, client):

        for proxy in self.active_proxies:
            proxy.stop()

        # synchronize with all the _run_one_proxy tasks
        # For some damn reason asyncio.wait raises an exception if called
        # with zero things to wait for, instead of just returning.
        if self.proxy_runners:
            yield from asyncio.wait(self.proxy_runners, loop=self.loop)
        self._refill_waiting_proxies()

        if self.nsmgr:
            yield from self.nsmgr.stop()
