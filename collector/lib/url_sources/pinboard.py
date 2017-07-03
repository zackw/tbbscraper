# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Import URLs from a Pinboard JSON dump --- implementation."""

import sys
import json
import traceback
from shared import url_database

class PinboardExtractor:
    def __init__(self, args):
        self.args = args
        self.delayed_failure = False

    def __call__(self):
        db = url_database.ensure_database(self.args)
        with open(self.args.file, "rt") as fp:
            self.load_urls(db, fp)
        if self.delayed_failure:
            raise SystemExit(1)

    def load_urls(self, db, fp):
        to_insert = set()

        uname = self.args.user
        sys.stderr.write("Importing {} ({})... /"
                         .format(uname, self.args.file))
        sys.stderr.flush()

        spinner = "/-\\|"
        c = 0
        with db, db.cursor() as cur:
            for entry in json.load(fp):
                try:
                    url   = url_database.add_url_string(cur, entry['href'])[0]
                    atime = entry['time']
                    title = entry['description']
                    annot = entry['extended']
                    tags  = entry['tags']
                except Exception as e:
                    sys.stderr.write("\nInvalid entry: {}\n"
                                     .format(json.dumps(entry)))
                    for l in traceback.format_exception_only(type(e), e):
                        sys.stderr.write(l)

                to_insert.add(cur.mogrify("(%s,%s,TIMESTAMP %s,%s,%s,%s)",
                                          (uname, url, atime, title,
                                           annot, tags)))
                sys.stderr.write("\b" + spinner[c % 4])
                sys.stderr.flush()
                c += 1

            sys.stderr.write(" (insert)")
            sys.stderr.flush()
            cur.execute(b"INSERT INTO urls_pinboard"
                        b"(username, url, access_time, title, annotation, tags)"
                        b"VALUES"
                        + b",".join(sorted(to_insert)))

            sys.stderr.write(" (commit)")
            sys.stderr.flush()

        sys.stderr.write("\n")
