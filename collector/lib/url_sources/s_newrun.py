# Copyright © 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Initialize database tables for a new run, possibly copying
URL-source tables from a previous one."""

def setup_argp(ap):
    ap.add_argument("-c", "--copy-from", action="store", type=int, metavar="N",
                    help="Copy URL-source tables from run number N.")
    ap.add_argument("-x", "--exclude",
                    help="Comma-separated list of tables *not* to copy."
                    " (captured_pages and capture_detail are never copied.)")
    ap.add_argument("-q", "--quiet",
                    help="Don't print any progress messages.")

def run(args):
    from url_sources.newrun import make_new_run
    make_new_run(args)
