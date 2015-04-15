# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Perform a traceroute (using 'scamper', which must be in the PATH
and must be invokeable by an unprivileged user) to a specified set of
destinations, through a specified set of proxies.  There are three
mandatory arguments: a "location specifications" config file, a
"traceroute destinations" config file, and an output directory (which
will be created if it doesn't exist), in that order.  The location
specifications file has the same format that `capture` uses.  The
traceroute destinations config file is simply a list of hostnames, one
per line, possibly followed by an IPv4 address in parentheses (which
overrides any attempt to look up that host's DNS name); # anywhere
introduces a comment; non-ASCII domain names may either be written
human-readably in UTF-8, or IDNA-coded.  In addition to the
destinations listed in the file, this program will also carry out
traceroutes via each proxy to each DNS server configured for that
proxy.

Output is to scamper 'warts' files named output_dir/YYYY-MM-DD.N/LOCATION.warts
aggregating all of the scans performed via proxy LOCATION.  YYY-MM-DD.N
is unique for each run of this program.  .../LOCATION.dns will contain the
result of all DNS lookups (which are not done by scamper).

"""

def setup_argp(ap):
    ap.add_argument("locations", action="store",
                    help="List of location specifications")
    ap.add_argument("destinations", action="store",
                    help="List of traceroute destinations")
    ap.add_argument("output", action="store",
                    help="Output directory")
    ap.add_argument("-p", "--max-simultaneous-proxies",
                    action="store", type=int, default=10,
                    help="Maximum number of proxies to use simultaneously.")

def run(args):
    Monitor(TracerouteDispatcher(args),
            banner="Capturing traceroutes",
            error_log="traceroute-errors")

import datetime
import os
import queue
import subprocess
import sys
import time

from shared.monitor import Monitor, Worker
from shared.proxies import ProxySet

def create_output_subdir(output_dir):
    datestamp = datetime.date.today().isoformat()
    i = 1
    while True:
        try:
            path = os.path.join(output_dir, datestamp + "." + str(i))
            os.makedirs(path)
            return path
        except FileExistsError:
            if i < 1000:
                i += 1
                continue
            raise

def rename_out(fname):
    i = 0
    while True:
        try:
            os.link(fname, fname + "." + str(i))
            os.unlink(fname)
            return
        except FileNotFoundError:
            return
        except FileExistsError:
            i += 1
            if i == 1000: raise

def queue_iter(q, timeout=None):
    """Generator which yields messages pulled from a queue.Queue in
       sequence, until empty or the timeout expires. Can block before
       yielding any items, but not after at least one item has been
       yielded.

    """
    try:
        yield q.get(timeout=timeout)
        while True:
            yield q.get(block=False)
    except queue.Empty:
        pass


class TracerouteWorker(Worker):

    BATCH_IP_LOOKUP = os.path.realpath(
        os.path.join(os.path.dirname(__file__),
                     "../../scripts/batch_ip_lookup.py"))

    DNS_JOB   = 1
    TRACE_JOB = 2

    def __init__(self, disp):
        Worker.__init__(self, disp)
        self._idle_prefix = "w"

    def process_batch(self, jobtype, *args):
        if jobtype == self.DNS_JOB:
            return self.process_dns(*args)
        elif jobtype == self.TRACE_JOB:
            return self.process_trace(*args)
        else:
            raise RuntimeError("unknown batch job type " + repr(jobtype))

    def process_dns(self, proxy, input_fname, mapping_fname, addrlist_fname):
        """Invoke .../scripts/batch_ip_lookup.py on INPUT_FNAME, writing
           results to MAPPING_FNAME; then reduce that to a list of
           bare addresses and write _that_ to ADDRLIST_FNAME.
        """

        self.set_status_prefix("d " + proxy.label())

        with open(input_fname, "rt") as in_f, \
             open(mapping_fname, "x+t") as out_f, \
             open(addrlist_fname, "xt") as list_f:

            cmd = proxy.adjust_command(["isolate",
                                        sys.executable,
                                        self.BATCH_IP_LOOKUP])
            proc = subprocess.Popen(cmd,
                                    stdin  = in_f,
                                    stdout = out_f,
                                    stderr = subprocess.PIPE)
            self.report_status(" ".join(cmd))
            for line in proc.stderr:
                self.report_status(
                    line.decode("utf-8", errors="backslashreplace").strip())

            proc.wait()
            if proc.returncode:
                raise subprocess.CalledProcessError(proc.returncode, cmd)

            out_f.seek(0)
            for line in out_f:
                ip = line.strip().split(' ', 1)[1]
                if ip != '127.0.0.1':
                    list_f.write(ip)
                    list_f.write("\n")

        return proxy

    def process_trace(self, proxy, listfile, wartsfile):
        listfile = os.path.realpath(listfile)
        wartsfile = os.path.realpath(wartsfile)
        self.set_status_prefix("t " + proxy.label())
        cmd = proxy.adjust_command(["isolate",
                                    "ISOL_RL_WALL=7200",
                                    "scamper",
                                    "-l", proxy.label(),
                                    "-f", listfile,
                                    "-O", "warts",
                                    "-c", "tracelb"])
        # We have to create the .warts file in advance and pass it as
        # standard output, because an isolate-d subprocess won't be
        # able to open it otherwise.
        with open(wartsfile, "xb") as warts_fp:
            proc = subprocess.Popen(cmd,
                                    stdin  = subprocess.DEVNULL,
                                    stdout = warts_fp,
                                    stderr = subprocess.PIPE)
            self.report_status(" ".join(cmd))
            for line in proc.stderr:
                self.report_status(
                    line.decode("utf-8", errors="backslashreplace").strip())
            proc.wait()

        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, cmd)

        return proxy

class LocationState:
    def __init__(self, location, destinations, output_dir):
        self.location     = location
        self.destinations = destinations
        self.dns_log      = os.path.join(output_dir, location + ".dns")
        self.ip_list      = os.path.join(output_dir, location + ".ips")
        self.trace_log    = os.path.join(output_dir, location + ".warts")
        self.next_task    = TracerouteWorker.DNS_JOB
        self.active_task  = False

    def queue_job(self, worker, proxy):
        if self.active_task: return
        if self.next_task == TracerouteWorker.DNS_JOB:
            worker.queue_batch(TracerouteWorker.DNS_JOB,
                               proxy,
                               self.destinations,
                               self.dns_log,
                               self.ip_list)
            self.active_task = True
        elif self.next_task == TracerouteWorker.TRACE_JOB:
            worker.queue_batch(TracerouteWorker.TRACE_JOB,
                               proxy,
                               self.ip_list,
                               self.trace_log)
            self.active_task = True
        else:
            assert self.next_task is None

    def complete_job(self):
        self.active_task = False
        if self.next_task == TracerouteWorker.DNS_JOB:
            self.next_task = TracerouteWorker.TRACE_JOB
        else:
            assert self.next_task == TracerouteWorker.TRACE_JOB
            self.next_task = None

    def fail_job(self):
        self.active_task = False
        if self.next_task == TracerouteWorker.DNS_JOB:
            rename_out(self.dns_log)
            rename_out(self.ip_list)
        elif self.next_task == TracerouteWorker.TRACE_JOB:
            rename_out(self.trace_log)

class TracerouteDispatcher:
    def __init__(self, args):
        self.args                    = args
        self.mon                     = None
        self.proxies                 = None
        self.locations               = {}
        self.idle_workers            = set()
        self.active_workers          = {}
        self.status_queue            = None
        self.output_dir              = create_output_subdir(args.output)

    _PROXY_OFFLINE  = 1
    _PROXY_ONLINE   = 2
    _BATCH_COMPLETE = 3
    _BATCH_FAILED   = 4
    _DROP_WORKER    = 5
    _MON_SAYS_STOP  = 6

    def proxy_online(self, proxy):
        self.status_queue.put((self._PROXY_ONLINE, proxy))

    def proxy_offline(self, proxy):
        self.status_queue.put((self._PROXY_OFFLINE, proxy))

    def complete_batch(self, worker, result):
        self.status_queue.put((self._BATCH_COMPLETE, worker, result))

    def fail_batch(self, worker, exc_info):
        self.status_queue.put((self._BATCH_FAILED, worker))

    def drop_worker(self, worker):
        self.status_queue.put((self._DROP_WORKER, worker))

    def __call__(self, mon, thr):
        self.mon = mon
        self.status_queue = queue.Queue()
        self.mon.register_event_queue(self.status_queue,
                                      (self._MON_SAYS_STOP,))

        self.mon.set_status_prefix("d")
        self.mon.report_status("loading...")

        self.proxies = ProxySet(self, mon, self.args)
        self.mon.report_status("loading... (proxies OK)")


        self.locations = { loc: LocationState(loc, self.args.destinations,
                                              self.output_dir)
                           for loc in self.proxies.locations.keys() }
        self.mon.report_status("loading... (locations OK)")

        # We only need one worker thread per proxy, because scamper
        # parallelizes work internally.
        for _ in range(self.args.max_simultaneous_proxies):
            wt = TracerouteWorker(self)
            self.mon.add_work_thread(wt)
            self.idle_workers.add(wt)
        self.mon.report_status("loading... (work threads OK)")

        # kick things off by starting one proxy
        (proxy, until_next, n_locations) = self.proxies.start_a_proxy()
        self.mon.report_status("{}/{}/{} locations active, {} started, "
                               "{} till next"
                               .format(len(self.proxies.active_proxies),
                                       n_locations,
                                       len(self.locations),
                                       proxy.label() if proxy else None,
                                       until_next))

        while n_locations:
            time_now = time.monotonic()
            # Technically, until_next being None means "wait for a proxy
            # to exit", but use an hour as a backstop.  (When a proxy does
            # exit, this will get knocked down to zero below.)
            if until_next is None: until_next = 3600
            time_next = time_now + until_next
            pending_stop = False
            while time_now < time_next:
                for msg in queue_iter(self.status_queue, until_next):
                    if msg[0] == self._PROXY_ONLINE:
                        self.proxies.note_proxy_online(msg[1])
                        self.mon.report_status("proxy {} online"
                                               .format(msg[1].label()))
                        self.mon.idle(1)

                    elif msg[0] == self._PROXY_OFFLINE:
                        self.mon.report_status("proxy {} offline"
                                               .format(msg[1].label()))
                        self.proxies.note_proxy_offline(msg[1])
                        # Wait no more than 5 minutes before trying to
                        # start another proxy.  (XXX This hardwires a
                        # specific provider's policy.)
                        time_now = time.monotonic()
                        time_next = min(time_next, time_now + 300)
                        until_next = time_next - time_now

                    elif msg[0] == self._BATCH_COMPLETE:
                        locstate = self.active_workers[msg[1]]
                        del self.active_workers[msg[1]]
                        self.idle_workers.add(msg[1])
                        locstate.complete_job()
                        self.mon.report_status("{} batch complete"
                                               .format(locstate.location))

                    elif msg[0] == self._BATCH_FAILED:
                        locstate = self.active_workers[msg[1]]
                        del self.active_workers[msg[1]]
                        self.idle_workers.add(msg[1])
                        locstate.fail_job()
                        self.mon.report_status("{} batch failed"
                                               .format(locstate.location))

                    elif msg[0] == self._DROP_WORKER:
                        self.idle_workers.discard(worker)
                        if worker in self.active_workers:
                            self.active_workers[worker].fail_job()
                            del self.active_workers[worker]

                    elif msg[0] == self._MON_SAYS_STOP:
                        self.mon.report_status("interrupt pending")
                        pending_stop = True

                    else:
                        self.mon.report_error("bogus message: {!r}"
                                              .format(message))

                for loc, state in self.locations.items():
                    if state.next_task is None:
                        self.mon.report_status("{} finished".format(loc))
                        if loc in self.proxies.locations:
                            self.proxies.locations[loc].finished()

                if pending_stop:
                    self.mon.report_status("interrupted")
                    self.mon.maybe_pause_or_stop()
                    # don't start new work yet, the set of proxies
                    # available may be totally different now

                else:
                    for proxy in self.proxies.active_proxies:
                        if not self.idle_workers:
                            break
                        if not proxy.online:
                            continue
                        state = self.locations[proxy.loc]
                        if not state.active_task and \
                           state.next_task is not None:
                            worker = self.idle_workers.pop()
                            self.active_workers[worker] = state
                            state.queue_job(worker, proxy)
                            self.mon.report_status("queuing job for {}"
                                                   .format(proxy.label()))

                time_now = time.monotonic()
                until_next = time_next - time_now

            # when we get to this point, it's time to start another proxy
            (proxy, until_next, n_locations) = self.proxies.start_a_proxy()
            self.mon.report_status("{}/{}/{} locations active, {} started, "
                                   "{} till next"
                                   .format(len(self.proxies.active_proxies),
                                           n_locations,
                                           len(self.locations),
                                           proxy.label() if proxy else None,
                                           until_next))

        # done, kill off all the workers
        self.mon.report_status("finished")
        assert not self.active_workers
        for w in self.idle_workers:
            w.finished()
