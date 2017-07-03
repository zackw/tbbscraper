# Copyright © 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Perform traceroutes --- implementation."""

import asyncio
import datetime
import os
import subprocess
import sys

from shared.aioproxies import ProxySet

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


BATCH_IP_LOOKUP = os.path.realpath(
    os.path.join(os.path.dirname(__file__),
                 "../../scripts/batch_ip_lookup.py"))

@asyncio.coroutine
def process_dns_job(proxy, input_fname, mapping_fname, addrlist_fname):
    """For every hostname in INPUT_FNAME, look it up in the DNS, through
       the proxy.  Write a hostname-to-IP mapping to MAPPING_FNAME and
       a list of bare addresses to ADDRLIST_FNAME.  Only IPv4 addresses
       are looked up.

       The addresses of the configured name servers (as determined by
       manually parsing /etc/resolv.conf) are also included in both
       output files, under the dummy name "nameserver".

       N.B. This uses an external script (scripts/batch_ip_lookup.py)
       because we only know how to run an entire process under a proxy.
    """
    label = proxy.label()

    sys.stderr.write("{}: batch DNS lookups...\n".format(label))

    with open(input_fname, "rt") as in_f, \
         open(mapping_fname, "x+t") as out_f, \
         open(addrlist_fname, "xt") as list_f:

        cmd = proxy.adjust_command(["isolate",
                                    sys.executable,
                                    BATCH_IP_LOOKUP])

        proc = yield from asyncio.create_subprocess_exec(
            *cmd, stdin=in_f, stdout=out_f, stderr=subprocess.PIPE)

        while True:
            line = yield from proc.stderr.readline()
            if not line: break
            line = line.decode("utf-8", errors="backslashreplace").strip()
            sys.stderr.write("{}: {}\n".format(label, line))

        rc = yield from proc.wait()
        if rc:
            raise subprocess.CalledProcessError(rc, cmd)

        out_f.seek(0)
        for line in out_f:
            ip = line.strip().split(' ', 1)[1]
            if ip != '127.0.0.1':
                list_f.write(ip)
                list_f.write("\n")

@asyncio.coroutine
def process_trace_job(proxy, listfile, wartsfile):
    listfile = os.path.realpath(listfile)
    wartsfile = os.path.realpath(wartsfile)

    label = proxy.label()
    sys.stderr.write("{}: traceroutes...\n".format(label))

    cmd = proxy.adjust_command(["isolate",
                                "ISOL_RL_WALL=7200",
                                "scamper",
                                "-l", label,
                                "-f", listfile,
                                "-O", "warts",
                                "-c", "tracelb"])

    # We have to create the .warts file in advance and pass it as
    # standard output, because an isolate-d subprocess won't be
    # able to open it otherwise.
    with open(wartsfile, "xb") as warts_fp:
        proc = yield from asyncio.create_subprocess_exec(
            *cmd,
            stdin  = subprocess.DEVNULL,
            stdout = warts_fp,
            stderr = subprocess.PIPE)

        while True:
            line = yield from proc.stderr.readline()
            if not line: break
            line = line.decode("utf-8", errors="backslashreplace").strip()
            sys.stderr.write("{}: {}\n".format(label, line))

        rc = yield from proc.wait()
        if rc:
            raise subprocess.CalledProcessError(rc, cmd)

@asyncio.coroutine
def process_jobs_for_location(proxy, location, destinations, output_dir):
    dns_log   = os.path.join(output_dir, location + ".dns")
    ip_list   = os.path.join(output_dir, location + ".ips")
    trace_log = os.path.join(output_dir, location + ".warts")

    if not os.path.exists(dns_log):
        try:
            yield from process_dns_job(proxy, destinations,
                                       dns_log, ip_list)
        except Exception as e:
            sys.stderr.write("{}: {}\n".format(proxy.label(), e))
            rename_out(dns_log)
            rename_out(ip_list)
            proxy.stop()
            return

    if not os.path.exists(trace_log):
        try:
            yield from process_trace_job(proxy, ip_list, trace_log)
        except Exception as e:
            sys.stderr.write("{}: {}\n".format(proxy.label(), e))
            rename_out(trace_log)
            proxy.stop()
            return

    proxy.close()

class TracerouteClient:
    def __init__(self, args, loop=None):
        if loop is None: loop = asyncio.get_event_loop()
        self.args       = args
        self.loop       = loop
        self.proxies    = ProxySet(args, loop=loop)
        self.locations  = set(self.proxies.locations.keys())
        self.output_dir = create_output_subdir(args.output)
        self.jobs       = {}

    @asyncio.coroutine
    def proxy_online(self, proxy):
        self.jobs[proxy.loc] = \
            self.loop.create_task(process_jobs_for_location(
                proxy, proxy.loc, self.args.destinations, self.output_dir))

    @asyncio.coroutine
    def proxy_offline(self, proxy):
        job = self.jobs.get(proxy.loc)
        if job is not None:
            del self.jobs[proxy.loc]
            job.cancel()
            # swallow cancellation exception
            try: yield from asyncio.wait_for(job)
            except: pass

    @asyncio.coroutine
    def run(self):
        yield from self.proxies.run(self)
        if self.jobs:
            yield from asyncio.wait(self.jobs, loop=self.loop)
