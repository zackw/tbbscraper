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
    from shared.monitor import Monitor
    from url_sources.dnslookups import DNSLookupDispatcher
    Monitor(DNSLookupDispatcher(args),
            banner="Performing DNS lookups",
            error_log="dnslookup-errors")
