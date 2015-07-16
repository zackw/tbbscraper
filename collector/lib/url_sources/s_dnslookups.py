# Copyright © 2015 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Look up many names in the DNS, via proxies.

Four command-line arguments are expected:

 * A list of location specifications in the same format as `capture`.
 * A list of DNS server IP addresses, classified by location: each line
   of the form

   LOCATION  IP IP IP ...

   where LOCATION also appears in the location-specifications file.
   (Locations in either file that don't appear in the other will be
   ignored, so you can disable a location by commenting it out in
   either file.)  Each IP on the line for LOCATION will be queried via
   the proxy for LOCATION.

 * A list of hostnames to look up, one per line.  This file may be
   gzipped.  If it is, the output files will also be gzipped.

 * The name of the output directory, which will be created if it
   doesn't exist.  All output is to files named

       output_dir/YYYY-MM-DD.N/LOCATION.IP.dns(.gz)
"""

def setup_argp(ap):
    ap.add_argument("locations", action="store",
                    help="List of location specifications")
    ap.add_argument("dns_servers", action="store",
                    help="List of DNS servers")
    ap.add_argument("hostnames", action="store",
                    help="List of hostnames to look up")
    ap.add_argument("output", action="store",
                    help="Output directory")
    ap.add_argument("-p", "--max-simultaneous-proxies",
                    action="store", type=int, default=10,
                    help="Maximum number of proxies to use simultaneously.")

def run(args):
    Monitor(DNSLookupDispatcher(args),
            banner="Performing DNS lookups",
            error_log="dnslookup-errors")

import datetime
import gzip
import os
import queue
import socket
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

def is_valid_ipaddr(addr):
    try:
        socket.inet_pton(socket.AF_INET6 if ':' in addr else socket.AF_INET,
                         addr)
        return True
    except OSError:
        return False

def parse_dns_servers(server_f):
    result = {}
    with open(server_f, "rt") as fp:
        for line in fp:
            ot = line.find('#')
            if ot >= 0: line = line[:ot]

            line = line.split()
            if len(line) < 2: continue

            loc = line[0]
            addrs = []
            for addr in line[1:]:
                if is_valid_ipaddr(addr):
                    addrs.append(addr)
                else:
                    raise RuntimeError("invalid IP address for {}: {}"
                                       .format(loc, addr))

            result[loc] = addrs

    return result

def parse_hostnames(hostnames_f):
    result = set()
    if hostnames_f.endswith(".gz"):
        opener = gzip.open
    else:
        opener = open

    with opener(hostnames_f, mode="rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith('#'): continue
            if '.' not in line or '..' in line or line.endswith('.'):
                sys.stderr.write("invalid hostname skipped: {!r}\n"
                                 .format(line))
            try:
                line = line.encode('idna')
            except UnicodeError as e:
                sys.stderr.write("invalid hostname skipped ({}): {!r}\n"
                                 .format(e, line))
            line += b'\n'
            result.add(line)

    # Sort the list by suffix; this means we will look up every entry
    # in a particular domain all at once, maximizing DNS cache efficiency.
    return sorted(result, key=lambda v: tuple(reversed(v.split(b'.'))))

class DNSWorker(Worker):

    def __init__(self, disp):
        Worker.__init__(self, disp)
        self._idle_prefix = "w"

    def writelines_rate_limited(self, fp, lines, per_sec):
        """Write LINES to FP at a rate of PER_SEC lines per second.
           LINES are assumed to have trailing newlines.
        """
        interval = 1.0 / float(per_sec)
        last = 0.0
        for line in lines:
            elapsed = time.monotonic() - last
            remain  = interval - elapsed
            if remain > 0:
                self._mon.idle(remain)
                last = time.monotonic()
            else:
                last += elapsed
            try:
                fp.write(line)
            except BrokenPipeError:
                break
        try:
            fp.close()
        except BrokenPipeError:
            pass

    def process_batch(self, proxy, dns_server, namelist, mapping_fname):
        """Invoke adnshost and feed it NAMELIST, writing
           results to MAPPING_FNAME, speaking to DNS_SERVER.
        """

        self.set_status_prefix("d " + proxy.label())
        cmd = proxy.adjust_command(["isolate",
                                    "ISOL_RL_WALL=3600",
                                    "adnshost",
                                    "-a", "-f", "-A4",
                                    "--config",
                                    "nameserver " + dns_server])
        self.report_status(" ".join(cmd))

        with open(mapping_fname, "x+t") as out_f:
            p_lu = subprocess.Popen(cmd,
                                    stdin  = subprocess.PIPE,
                                    stdout = subprocess.PIPE,
                                    stderr = self._log)
            p_cp = subprocess.Popen(["gzip"],
                                    stdin  = p_lu.stdout,
                                    stdout = out_f,
                                    stderr = subprocess.DEVNULL)
            p_lu.stdout.close()

        self.writelines_rate_limited(p_lu.stdin, namelist, 100)

        p_lu.wait()
        p_cp.wait()
        #if p_lu.returncode:
        #    raise subprocess.CalledProcessError(p_lu.returncode, cmd)

        return proxy, dns_server

class LocationState:
    def __init__(self, location, dns_servers, namelist, output_dir):
        self.location     = location
        self.dns_servers  = set(dns_servers)
        self.namelist     = namelist
        self.dns_log_base = os.path.join(output_dir, location) + "."
        self.active_task  = None
        self.active_task_o= None

    def idle_p(self):
        return (self.active_task is None and
                len(self.dns_servers) > 0)

    def finished_p(self):
        return (len(self.dns_servers) == 0)

    def queue_job(self, worker, proxy):
        if self.active_task is not None: return
        if self.dns_servers:
            self.active_task = self.dns_servers.pop()
            self.active_task_o = (self.dns_log_base +
                                  self.active_task +
                                  ".dns.gz")

            worker.queue_batch(proxy,
                               self.active_task,
                               self.namelist,
                               self.active_task_o)

    def complete_job(self):
        self.active_task = None
        self.active_task_o = None

    def fail_job(self):
        rename_out(self.active_task_o)
        self.dns_servers.add(self.active_task)
        self.active_task = None
        self.active_task_o = None

class DNSLookupDispatcher:
    def __init__(self, args):
        self.args                    = args
        self.mon                     = None
        self.proxies                 = None
        self.locations               = {}
        self.idle_workers            = set()
        self.active_workers          = {}
        self.status_queue            = None
        self.output_dir              = create_output_subdir(args.output)
        self.dns_servers             = parse_dns_servers(args.dns_servers)
        self.hostnames               = parse_hostnames(args.hostnames)

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

        self.proxies = ProxySet(self, mon, self.args,
                                include_locations=self.dns_servers)
        self.mon.report_status("loading... (proxies OK)")

        for loc in list(self.dns_servers.keys()):
            if loc not in self.proxies.locations:
                del self.dns_servers[loc]

        assert list(self.dns_servers.keys()) == \
               list(self.proxies.locations.keys())

        self.locations = { loc: LocationState(loc,
                                              self.dns_servers[loc],
                                              self.hostnames,
                                              self.output_dir)
                           for loc in self.dns_servers.keys() }
        self.mon.report_status("loading... (locations OK)")

        # One work thread per active proxy.
        for _ in range(self.args.max_simultaneous_proxies):
            wt = DNSWorker(self)
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
                    if state.finished_p():
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
                        if state.idle_p():
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
