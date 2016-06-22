# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Run a program under a specified set of proxies.  Takes two arguments:
the usual proxy config file and then the program to run; all subsequent
arguments are passed to the program.  Stdout/stderr of the program are
written to log files named <PROGRAM_BASENAME>-<PROXY>-<SERIAL>.log."""


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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(RunProgramClient(args, loop).run())

import asyncio
import datetime
import os
import subprocess
import sys
import tempfile

from shared.aioproxies import ProxySet

@asyncio.coroutine
def run_program_for_location(proxy, program, log_dir):
    try:
        label = proxy.label()
        progname = os.path.splitext(os.path.basename(program[0]))[0]

        fd, logfile = tempfile.mkstemp(
            dir=log_dir,
            prefix="{}-{}-".format(progname, proxy.loc),
            suffix=".log"
        )
        os.fchmod(fd, 0o0644)

        start = datetime.datetime.now()
        sys.stderr.write("{}: {}: running {}...\n".format(
            start.isoformat(sep=' '), label, progname))

        cmd = ["isolate", "ISOL_RL_WALL=28800", "ISOL_RL_CPU=3600"]
        cmd.extend(arg.replace("$LOCATION", proxy.loc)
                   for arg in program)
        cmd = proxy.adjust_command(cmd)

        proc = yield from asyncio.create_subprocess_exec(
                *cmd,
                stdin  = subprocess.DEVNULL,
                stdout = fd,
                stderr = fd)
        os.close(fd)
        rc = yield from proc.wait()
        stop = datetime.datetime.now()

        sys.stderr.write("{}: {}: exit {}, {} elapsed\n".format(
            stop.isoformat(' '), label, rc, stop - start))

        if rc:
            raise subprocess.CalledProcessError(rc, cmd)

    finally:
        proxy.close()

class RunProgramClient:
    def __init__(self, args, loop=None):
        if loop is None: loop = asyncio.get_event_loop()
        self.args       = args
        self.loop       = loop
        self.proxies    = ProxySet(args, loop=loop, nstag=args.prefix)
        self.jobs       = {}
        if self.args.log_dir:
            os.makedirs(self.args.log_dir, exist_ok=True)

    @asyncio.coroutine
    def proxy_online(self, proxy):
        self.jobs[proxy.loc] = \
            self.loop.create_task(run_program_for_location(
                proxy, self.args.program, self.args.log_dir or "."))

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
