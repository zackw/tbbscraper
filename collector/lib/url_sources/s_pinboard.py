# Copyright © 2013, 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Import URLs from a Pinboard JSON dump."""

def setup_argp(ap):
    ap.add_argument("user", help="Pinboard user whose bookmarks these are.")
    ap.add_argument("file", help="File to import.")

def run(args):
    from url_sources.pinboard import PinboardExtractor
    PinboardExtractor(args)()

