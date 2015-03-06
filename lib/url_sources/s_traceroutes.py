# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Perform a traceroute (using 'paris-traceroute', which must be in
the PATH and must be invokeable by an unprivileged user) to a
specified set of destinations, through a specified set of proxies.
There are three mandatory arguments: a "location specifications"
config file, a "traceroute destinations" config file, and an output
directory (which will be created if it doesn't exist), in that order.
The location specifications file has the same format that `capture`
uses.  The traceroute destinations config file is simply a list of
hostnames, one per line, possibly followed by an IPv4 address in
parentheses (which overrides any attempt to look up that host's DNS
name); # anywhere introduces a comment; non-ASCII domain names may
either be written human-readably in UTF-8, or IDNA-coded.  Only one
traceroute runs at a time for each proxy.  In addition to the
destinations listed in the file, this program will also carry out
traceroutes via each proxy to each DNS server configured for that proxy.

Output is to individual files named output_dir/YYYY-MM-DD.N/LOCATION/DNS.NAME
(regardless of input syntax, SITENAME will be human-readable).  YYY-MM-DD.N
is unique for each run of this program."""

def setup_argp(ap):
    ap.add_argument("locations", action="store",
                    help="List of location specifications")
    ap.add_argument("destinations", action="store",
                    help="List of traceroute destinations")
    ap.add_argument("output", action="store",
                    help="Output directory")

def run(args):
    Monitor(TracerouteDispatcher(args),
            banner="Capturing traceroutes",
            error_log="traceroute-errors")

import socket
import subprocess

from shared.monitor import Monitor, Worker

class ReverseDNSWorker(Worker):
    def process_batch(self, batch):
        # A reverse DNS batch is just a list of IP addresses to feed
        # to getnameinfo().

        prev = None
        results = []
        for addr in batch:
            if prev:
                self._mon.report_status(prev + " | " + addr)
            else:
                self._mon.report_status(addr)

            try:
                result = socket.getnameinfo((addr, 0), 0)[0]
            except OSError:
                # These should be rare, so we report them.
                self._mon.report_exception()
                result = addr

            results.append((addr, result))
            prev = addr + " = " + result

        return results

class TracerouteWorker(Worker):
    def process_batch(self, proxy, destination):
        # For simplicity's sake, traceroute "batches" are a single
        # destination.
