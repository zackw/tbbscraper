# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Download the current CitizenLab potentially-censored sites lists and
   add it to the URL database."""

def setup_argp(ap):
    ap.add_argument("source",
                    help="Source directory for the sites lists. "
                    "If it doesn't exist, it is created.")
    ap.add_argument("--repo", "-r",
                    help="Remote Git repository to check out into the "
                    "source directory if it doesn't already exist.",
                    default="https://github.com/citizenlab/test-lists/")

def run(args):
    from url_sources.citizenlab import CitizenLabExtractor
    CitizenLabExtractor(args)()
