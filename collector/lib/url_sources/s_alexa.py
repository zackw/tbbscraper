# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Download the current Alexa top-1-million-sites list and add it to the
   URL database."""

def setup_argp(ap):
    ap.add_argument("--src", "-s", metavar="URL",
                help="Source URL for the sites list. "
                     "Assumed to name a zipfile.",
                default="http://s3.amazonaws.com/alexa-static/top-1m.csv.zip")
    ap.add_argument("--src-name", "-n", metavar="NAME",
                help="Name of the file to extract from the zipfile.",
                default="top-1m.csv")
    ap.add_argument("--cache", "-c", metavar="DIR",
                help="Directory in which to cache downloaded site lists.",
                default="alexa")
    ap.add_argument("--top-n", "-t", metavar="N",
                help="How many sites (from the top down) to add.",
                type=int, default=0)
    ap.add_argument("--http-only", "-H",
                    help="Only add http:// URLs.",
                    action="store_true")
    ap.add_argument("--www-only", "-w",
                    help="Only add URLs with 'www.' prefixed to the hostname.",
                    action="store_true")


def run(args):
    from shared.monitor import Monitor
    from url_sources.alexa import AlexaExtractor

    Monitor(AlexaExtractor(args),
            banner="Extracting URLs from Alexa top 1,000,000")
