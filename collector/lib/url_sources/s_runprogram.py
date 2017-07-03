# Copyright © 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Run a program under a specified set of proxies.

   Takes two arguments: the usual proxy config file and then the
   program to run; all subsequent arguments are passed to the program.
   Stdout/stderr of the program are written to log files named
   <PROGRAM_BASENAME>-<PROXY>-<SERIAL>.log.
"""

def setup_argp(ap):
    from argparse import REMAINDER

    ap.add_argument("-l", "--log-dir", action="store",
                    help="Directory in which to put log files.")
    ap.add_argument("locations", action="store",
                    help="List of location specifications")
    ap.add_argument("program", action="store", nargs=REMAINDER,
                    help="Program to run, and its arguments")
    ap.add_argument("-p", "--max-simultaneous-proxies",
                    action="store", type=int, default=10,
                    help="Maximum number of proxies to use simultaneously.")
    ap.add_argument("-P", "--prefix",
                    action="store", default="t",
                    help="Network namespace prefix to use.")

def run(args):
    import asyncio
    loop = asyncio.get_event_loop()

    from url_sources.runprogram import RunProgramClient
    loop.run_until_complete(RunProgramClient(args, loop).run())
    loop.close()
