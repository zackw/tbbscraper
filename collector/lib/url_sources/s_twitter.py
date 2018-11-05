# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Extract URLs from Twitter streams.

You can choose to examine the stream of a single user, a snowball
sample of all users within some follow-graph distance of one user, or
a frontier sample of users from the entire population.  Or you can
examine a random sample of all tweets, filtered with search
parameters."""

def setup_argp(ap):
    def positive_int(arg):
        val = int(arg)
        if val <= 0:
            raise TypeError("argument must be positive")
        return val

    ap.add_argument("mode", metavar="extraction-mode",
                    choices=("single", "snowball", "frontier",
                             "firehose", "resume", "urls"),
                    default="single",
                    help="single=one user.\n"
                    "snowball=all users within some distance of a seed user.\n"
                    "frontier=random sample of the entire user population.\n"
                    "firehose=random sample of all tweets as they go by "
                    "(possibly filtered with search parameters).\n"
                    "resume=continue an interrupted scan.\n"
                    "urls=extract new URLs from users already in the database.")

    ap.add_argument("-l", "--limit",
                    type=positive_int, default=1,
                    help="How 'big' of a sample to take, in some sense. "
                    "For snowball mode, the distance from the seed user. "
                    "For frontier and firehose mode, the number of unique "
                    "users to pick before stopping.")

    ap.add_argument("-p", "--parallel",
                    type=positive_int, default=1,
                    help="Parallelism: only relevant for frontier sampling, "
                    "where it controls the number of simultaneous random "
                    "walks.")

    ap.add_argument("seed", nargs="*",
                    help="Starting point for the scan. "
                    "For 'single' and 'snowball' modes, you must supply one "
                    "Twitter handle (leading @ not required).  For 'frontier' "
                    "and 'firehose' modes, you may supply a search query which "
                    "will limit the initial stream request. "
                    "For 'resume' mode, you must supply the tag of a previous "
                    "scan (specify --mode=resume with no seed to get a list "
                    "of resumable scans). ")

def run(args):
    from url_sources.twitter import extract_from_twitter
    extract_from_twitter(args)
