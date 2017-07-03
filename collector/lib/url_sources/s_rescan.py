# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Populate the table of URLs to rescan, from a CSV file."""

def setup_argp(ap):
    ap.add_argument("to_rescan", help="CSV file listing URLs to rescan.")

def run(args):
    from url_sources.rescan import rescan
    rescan(args)
