# Copyright © 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Perform traceroutes (using 'scamper', which must be in the PATH and
must be invokeable by an unprivileged user) to a specified set of
destinations, through a specified set of proxies.

There are three mandatory arguments: a "location specifications"
config file, a "traceroute destinations" config file, and an output
directory (which will be created if it doesn't exist), in that order.
The location specifications file has the same format that `capture` uses.
The traceroute destinations config file is simply a list of hostnames,
one per line, possibly followed by an IPv4 address in parentheses
(which overrides any attempt to look up that host's DNS name); #
anywhere introduces a comment; non-ASCII domain names may either be
written human-readably in UTF-8, or IDNA-coded.  In addition to the
destinations listed in the file, this program will also carry out
traceroutes via each proxy to each DNS server configured for that proxy.

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
    import asyncio
    loop = asyncio.get_event_loop()

    from url_sources.traceroutes import TracerouteClient
    loop.run_until_complete(TracerouteClient(args, loop).run())
    loop.close()
