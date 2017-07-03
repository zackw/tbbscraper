# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Capture the HTML content and a screenshot of each URL in a list,
using PhantomJS, from many locations simultaneously.

Locations are defined by the config file passed as an argument, which
is line- oriented, each line having the general form

  locale method arguments ...

'locale' is an arbitrary word (consisting entirely of lowercase ASCII
letters) which names the location.

'method' selects a general method for capturing pages from this
location.  Subsequent 'arguments' are method-specific.  There are
currently two supported methods:

  direct: The controller machine will issue HTTP requests directly.
          No arguments.

  ovpn:   HTTP requests will be proxied via openvpn.
          One or more arguments are passed to the 'openvpn-netns'
          helper program (see scripts/openvpn-netns.c).  The initial
          argument is treated as a glob pattern which should expand to
          one or more OpenVPN config files; if there's more than one,
          they are placed in a random order and then used round-robin
          (i.e. if connection with one config file fails or drops, the
          next one is tried).

The second non-optional argument is the list of URLs to process, one per
line.

The third non-optional argument is the directory in which to store
results.  Each result will be written to its own file in this
directory; the directory hierarchy has the structure

  ${OUTPUT_DIR}/${RUN}/AB/CDE/FGH.${LOCALE}

where RUN starts at zero and is incremented by one each time the program is
invoked, and AB,CDE,FGH is a 8-digit decimal number assigned to each URL.
This number is not meaningful; you must look in each file to learn which
URL goes with which result.

The output files are binary; see CaptureResult.write_result for the format.

"""

def setup_argp(ap):
    ap.add_argument("locations",
                    action="store",
                    help="List of location specifications.")
    ap.add_argument("urls",
                    action="store",
                    help="List of URLs to process.")
    ap.add_argument("output_dir",
                    action="store",
                    help="Directory in which to store output.")
    ap.add_argument("-w", "--workers-per-location",
                    action="store", dest="workers_per_loc", type=int, default=8,
                    help="Maximum number of concurrent workers per location.")
    ap.add_argument("-W", "--total-workers",
                    action="store", dest="total_workers", type=int, default=40,
                    help="Total number of concurrent workers to use.")
    ap.add_argument("-p", "--max-simultaneous-proxies",
                    action="store", type=int, default=10,
                    help="Maximum number of proxies to use simultaneously.")
    ap.add_argument("-q", "--quiet", action="store_true",
                    help="Don't print progress messages.")

def run(args):
    import os
    if os.environ.get("PYTHONASYNCIODEBUG"):
        import logging
        logging.basicConfig(level=logging.DEBUG)
        import warnings
        warnings.simplefilter('default')

    import asyncio
    loop = asyncio.get_event_loop()
    asyncio.get_child_watcher()

    from url_sources.capture_phantom import CaptureDispatcher
    loop.run_until_complete(CaptureDispatcher(args).run())
    loop.close()
