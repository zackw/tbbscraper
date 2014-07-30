# Copyright © 2013, 2014 Zack Weinberg
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
    StaticListExtractor(args)()

import sys
import re

from shared import url_database

class StaticListExtractor:
    def __init__(self, args):
        self.args = args
        self.delayed_failure = False
        self.last_update = None
        self.source_url = None
        self.source_label = None
        self.import_id = None
        self.lineno = 0

    def __call__(self):
        db = url_database.ensure_database(self.args)
        with open(self.args.file) as fp:
            self.load_metadata(db, fp)
            self.load_urls(db, fp)

        self.update_canon_queue(db)
        if self.delayed_failure:
            raise SystemExit(1)

    def load_metadata(self, db, fp):
        while (self.last_update is None or
               self.source_url is None or
               self.source_label is None):
            line = fp.readline().strip()
            self.lineno += 1

            if line == "":
                continue
            if not line.startswith("#"):
                sys.stderr.write("{}:{}: missing metadata tags:{}{}{}\n"
                                 .format(self.args.file,
                                         self.lineno,
                                         "" if self.last_update else " Date",
                                         "" if self.source_url else " Source",
                                         "" if self.source_label else " Label"))
                raise SystemExit(1)

            line = line[1:].lstrip()
            if line.startswith("Label:"):
                self.source_label = line[len("Label:"):].lstrip()
            elif line.startswith("Source:"):
                self.source_url = line[len("Source:"):].lstrip()
            elif line.startswith("Date:"):
                self.last_update = line[len("Date:"):].lstrip()
            else:
                sys.stderr.write("{}:{}: unrecognized metadata line: {!r}\n"
                                 .format(self.args.file, self.lineno, line))

        with db, db.cursor() as cur:
            metatuple = (self.source_label, self.source_url, self.last_update)
            cur.execute("SELECT id FROM static_list_metadata"
                        " WHERE label = %s AND url = %s AND last_update = %s",
                        metatuple)
            row = cur.fetchone()
            if row is not None:
                self.import_id = row[0]
            else:
                cur.execute("INSERT INTO static_list_metadata "
                            "  (id, label, url, last_update)"
                            "  VALUES (DEFAULT, %s, %s, %s)"
                            "  RETURNING id",
                            metatuple)
                self.import_id = cur.fetchone()[0]

    _has_scheme = re.compile(r"(?i)^[a-z]+://")

    def load_urls(self, db, fp):
        to_insert = set()

        sys.stderr.write("Importing {}...".format(self.source_label))
        sys.stderr.flush()

        with db, db.cursor() as cur:
            for line in fp:
                line = line.strip()
                self.lineno += 1

                if line == "" or line[0] == "#":
                    continue

                if self._has_scheme.match(line):
                    if (not line.startswith("http://") and
                        not line.startswith("https://")):
                        sys.stderr.write("{}:{}: non-HTTP(S) URL: {!r}\n"
                                         .format(self.args.file,
                                                 self.lineno, line))
                        self.delayed_failure = True
                        continue

                    try:
                        (url_id, _) = url_database.add_url_string(cur, line)

                    except Exception as e:
                        sys.stderr.write("{}:{}: {}\n"
                                         .format(self.args.file, self.lineno,
                                                 str(e)))
                        self.delayed_failure = True
                        continue

                    to_insert.add(cur.mogrify("(%s, %s)",
                                              (url_id, self.import_id)))

                else:
                    try:
                        urls = url_database.add_site(cur, line)

                    except Exception as e:
                        sys.stderr.write("{}:{}: {}\n"
                                         .format(self.args.file, self.lineno,
                                                 str(e)))
                        self.delayed_failure = True
                        continue

                    for pair in urls:
                        to_insert.add(cur.mogrify("(%s, %s)",
                                                  (pair[0], self.import_id)))

            if self.delayed_failure:
                raise SystemExit(1)

            sys.stderr.write(" (insert)")
            sys.stderr.flush()
            cur.execute(b"INSERT INTO urls_staticlist "
                        b"(url, listid) VALUES "
                        + b",".join(sorted(to_insert)))

            sys.stderr.write(" (commit)")
            sys.stderr.flush()
        sys.stderr.write("\n")
