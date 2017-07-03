# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Import a list of 'interesting' URLs from a flat file.

The file should start with three metadata lines reading:
# Label:   [short_name_for_this_list]
# Source:  [URL where list was downloaded from]
# Date:    [date of last update, YYYY-MM-DD format]

and should otherwise be a list of one URL per line."""

def setup_argp(ap):
    ap.add_argument("file",
                    help="File to import.")

def run(args):
    from url_sources.staticlist import StaticListExtractor
    StaticListExtractor(args)()
