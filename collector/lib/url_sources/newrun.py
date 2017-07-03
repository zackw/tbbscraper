# Copyright © 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Initialize database tables for a new run, possibly copying
URL-source tables from a previous one --- implementation."""

import os
import os.path
import sys
import time
import math
from shared import url_database

def make_new_run(args):
    global quiet
    quiet = args.quiet

    progress(None)
    db = url_database.ensure_database(args)
    if args.copy_from is not None:
        old_run = find_old_run(db, args)

    new_run = initialize_new_schema(db, args)
    if args.copy_from is not None:
        copy_sources(db, args, old_run, new_run)

quiet = False
start_time = None
def progress(msg):
    global start_time, quiet
    if quiet:
        return

    if start_time is None:
        start_time = time.time()
    if msg is None:
        return

    elapsed = time.time() - start_time
    hours   = math.floor(elapsed / 3600)
    minutes = math.floor((elapsed - hours*3600) / 60)
    seconds = elapsed - (hours*3600 + minutes*60)
    sys.stderr.write("[{:02}:{:02}:{:06.3f}] {}\n"
                     .format(hours, minutes, seconds, msg))

def find_old_run(db, args):
    old_run = "ts_run_{}".format(args.copy_from)
    with db, db.cursor() as cur:
        cur.execute("SELECT EXISTS("
                    "  SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
                    "  WHERE TABLE_SCHEMA = %s)",
                    (old_run,))
        exists = cur.fetchone()[0]
    if not exists:
        raise RuntimeError("Old run {} does not exist.".format(args.copy_from))
    return old_run

def initialize_new_schema(db, args):
    schema_sql = os.path.join(os.path.dirname(__file__),
                              "../../scripts/ts_schema.sql")
    with open(schema_sql) as schema_f:
        tablespec = schema_f.read()

    with db, db.cursor() as cur:
        cur.execute("SELECT MAX(CAST(SUBSTRING(TABLE_SCHEMA FROM"
                    "                          'ts_run_([0-9]+)') AS INTEGER))"
                    "  FROM INFORMATION_SCHEMA.TABLES"
                    " WHERE TABLE_SCHEMA LIKE 'ts_run_%'")
        max_existing = cur.fetchone()[0]
        new_schema = 'ts_run_{}'.format(max_existing + 1)

        cur.execute('CREATE SCHEMA "{}"'.format(new_schema))
        cur.execute('SET search_path TO "{}"'.format(new_schema))
        cur.execute(tablespec)

    progress("New schema {} initialized.".format(new_schema))
    return new_schema

def copy_sources(db, args, old_run, new_run):
    dont_copy = ["capture_detail", "captured_pages"]
    if args.exclude:
        for t in args.exclude.split(","):
            t = t.strip()
            if t: dont_copy.append(t)

    with db, db.cursor() as cur:
        cur.execute("SET SEARCH_PATH TO " + new_run)
        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
                    " WHERE TABLE_SCHEMA = %s AND TABLE_NAME <> ALL(%s)",
                    (old_run, dont_copy))

        urls_tables = []
        ancillary_tables = []
        for row in cur:
            tbl = row[0]

            # The url_strings table is handled specially.
            if tbl == "url_strings":
                pass
            elif tbl.startswith("urls_"):
                urls_tables.append(tbl)
            else:
                ancillary_tables.append(tbl)

        ancillary_tables.sort()
        urls_tables.sort()
        progress("Will copy ancillary tables: " + " ".join(ancillary_tables))
        progress("Will copy URL tables: "       + " ".join(urls_tables))

        for tbl in ancillary_tables:
            copy_ancillary(cur, tbl, old_run, new_run)

        with URLRenumberer(cur, old_run, new_run) as state:
            for tbl in urls_tables:
                state.copy_urls(tbl)

    # Intentionally outside the with: so it fires after commit.
    progress("Copy complete. Analyzing...")

    with db, db.cursor() as cur:
        cur.execute("ANALYZE")

    progress("ANALYZE complete.")

def copy_ancillary(cur, tbl, old_run, new_run):
    cur.execute('INSERT INTO "{new}"."{tbl}" SELECT * FROM "{old}"."{tbl}"'
                .format(tbl=tbl, old=old_run, new=new_run))
    progress(tbl + ": ancillary table copied")

class URLRenumberer:
    def __init__(self, cur, old_run, new_run):
        self.cur = cur
        self.old_run = old_run
        self.new_run = new_run
        self.id_seq = 1
        self.url_strings = {}
        self.new_ids = {}

    def __enter__(self):
        cur = self.cur
        cur.execute('SELECT id, url FROM "{}".url_strings'
                    .format(self.old_run))
        for row in cur:
            self.url_strings[row.id] = row.url
        progress("URL strings loaded")
        return self

    def __exit__(self, *ignored):
        self.cur.execute('ALTER SEQUENCE "{}".url_strings_id_seq'
                         ' RESTART WITH {}'.format(self.new_run,
                                                   self.id_seq))
        return False

    def copy_urls(self, tbl):
        cur = self.cur
        old_tbl = '"{}"."{}"'.format(self.old_run, tbl)
        new_tbl = '"{}"."{}"'.format(self.new_run, tbl)

        cur.execute('SELECT * FROM ' + old_tbl)
        rows = cur.fetchall()

        # Define new IDs for all the URLs in this table that haven't
        # already been encountered.
        old_ids_this_table = sorted((row.url for row in rows),
                                    key=lambda u: self.url_strings[u])

        to_insert = []
        for old_id in old_ids_this_table:
            if old_id not in self.new_ids:
                self.new_ids[old_id] = self.id_seq
                to_insert.append(cur.mogrify("(%s, %s)",
                                             (self.id_seq,
                                              self.url_strings[old_id])))
                self.id_seq += 1
        if to_insert:
            cur.execute(('INSERT INTO "{}".url_strings (id, url)'
                         ' VALUES ').format(self.new_run).encode("ascii")
                        + b",".join(to_insert))

        progress(tbl + ": URL id map established")

        pattern = "(" + ",".join("%s" for _ in rows[0]._fields) + ")"
        to_insert = [cur.mogrify(pattern,
                                 row._replace(url=self.new_ids[row.url]))
                     for row in rows]

        cur.execute("INSERT INTO {} ({}) VALUES "
                    .format(new_tbl, ",".join(rows[0]._fields)).encode("ascii")
                    + b",".join(to_insert))
        progress(tbl + ": URL table copied")
