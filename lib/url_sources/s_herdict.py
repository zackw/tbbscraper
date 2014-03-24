# encoding: utf-8
# Copyright © 2013, 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Extract URLs logged as inaccessible by herdict.org."""

def setup_argp(ap):
    pass

def run(args):
    ext = HerdictExtractor(args)
    Monitor(ext, banner="Extracting URLs from herdict.org")
    ext.report_final_statistics()

import datetime
import queue
import re
import requests
import requests.exceptions
import sys
import time

from shared.url_database import ensure_database
from shared.monitor import Monitor

class HerdictExtractor:
    def __init__(self, args):
        # Do not load the database until we are on the correct thread.
        self.args = args
        self.summary = None

    def __call__(self, mon, thr):
        db, oid, start_date, end_date = self.prepare_database()
        self.db = db
        cur = db.cursor()
        pageq = queue.Queue()
        mon.add_work_thread(HerdictReader(pageq, start_date, end_date))

        n_accessible = 0
        n_inaccessible = 0
        n_total = 0
        lo_timestamp = time.time() + 86400
        hi_timestamp = 0

        while True:
            page = pageq.get()
            if not page:
                db.commit()
                break
            for row in page:
                if ("url" not in row or
                    "reportDate" not in row or
                    "reportType" not in row):
                    continue

                timestamp = (datetime.datetime.strptime(row["reportDate"],
                                                        "%Y-%m-%dT%H:%M:%S %z")
                             .timestamp())
                lo_timestamp = min(timestamp, lo_timestamp)
                hi_timestamp = max(timestamp, hi_timestamp)

                url = row["url"]
                if "/" not in url:
                    url = url + "/"
                if "protocol" not in row:
                    url = "HTTP://" + url
                else:
                    url = row["protocol"] + "://" + url
                accessible = (row["reportType"] != "INACCESSIBLE")
                if "country" in row and "shortName" in row["country"]:
                    country = row["country"]["shortName"]
                else:
                    country = "??"

                # It is a damned shame that there is no way to do this
                # in one SQL operation.
                cur.execute("SELECT id FROM url_strings WHERE url = ?",
                            (url,))
                uid = cur.fetchone()
                if uid is not None:
                    uid = uid[0]
                else:
                    cur.execute("INSERT INTO url_strings VALUES(NULL, ?)",
                                (url,))
                    uid = cur.lastrowid

                rid = cur.execute("INSERT INTO herdict_reports "
                                  "VALUES (NULL,?,?,?)",
                                  (timestamp, accessible, country)).lastrowid
                cur.execute("INSERT INTO urls VALUES(?,?,?)",
                            (oid, rid, uid))

                n_total += 1
                if accessible: n_accessible += 1
                else: n_inaccessible += 1
                mon.report_status("Processed {} URLs; "
                                  "{} accessible, {} inaccessible"
                                  .format(n_total, n_accessible,
                                          n_inaccessible))

            mon.report_status("Processed {} URLs; "
                              "{} accessible, {} inaccessible; checkpointing"
                              .format(n_total, n_accessible,
                                      n_inaccessible))
            db.commit()
            mon.maybe_pause_or_stop()

        self.summary = (lo_timestamp, hi_timestamp,
                        n_total, n_accessible, n_inaccessible)

    def report_final_statistics(self):
        if not self.summary: return
        f = sys.stdout
        s = self.summary
        f.write("Earliest report: {}\n".format(time.ctime(s[0])))
        f.write("Latest report:   {}\n".format(time.ctime(s[1])))
        f.write("Processed {} urls; {} accessible, {} inaccessible\n"
                .format(s[2], s[3], s[4]))

    def prepare_database(self):
        # Herdict reports have several more keys than this, but none
        # of them appear to be terribly trustworthy.
        herdict_schema = """\
CREATE TABLE herdict_reports (
    uid         INTEGER PRIMARY KEY,
    timestamp   INTEGER,
    accessible  INTEGER, -- (boolean)
    country     TEXT
);
CREATE INDEX herdict_reports__timestamp ON herdict_reports(timestamp);
"""
        db = ensure_database(self.args)
        with db:
            # FIXME: More sophisticated way of detecting presence of our
            # ancillary schema.
            s_tables = frozenset(re.findall("(?m)(?<=^CREATE TABLE )[a-z_]+",
                                            herdict_schema))
            s_indices = frozenset(re.findall("(?m)(?<=^CREATE INDEX )[a-z_]+",
                                             herdict_schema))
            d_tables = frozenset(r[0] for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE "
                    "  type = 'table' AND name LIKE 'herdict_%'"))
            d_indices = frozenset(r[0] for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE "
                    "  type = 'index' AND name LIKE 'herdict_%'"))

            if not d_tables and not d_indices:
                db.executescript(herdict_schema)
                db.commit()
            elif d_tables != s_tables or d_indices != s_indices:
                raise RuntimeError("ancillary schema mismatch - "
                                   "migration needed")

            oid = db.execute("SELECT id FROM origins"
                             "  WHERE label = 'herdict'").fetchone()
            if oid is None:
                oid = db.execute("INSERT INTO origins"
                                 "  VALUES(NULL, 'herdict')").lastrowid
            else:
                oid = oid[0]

            # Find the latest date already in the table.  We don't
            # need to process dates before that point.  Note that
            # Herdict's fsd= and fed= parameters are both inclusive, so
            # we need to step to the next day.
            db.execute("ANALYZE");
            start_date = db.execute("SELECT COALESCE(MIN(timestamp), 0) "
                                    "FROM herdict_reports").fetchone()[0];
            if start_date == 0:
                start_date = None
            else:
                start_date = ((datetime.date.fromtimestamp(start_date)
                               + datetime.timedelta(days=1))
                              .strftime("%Y-%m-%d"))

        # Herdict raw reports do not have serial numbers, and the API
        # only lets you ask for reports up to a certain _date_, not a
        # date and time.  So, to avoid ever getting duplicate dates
        # upon requerying the API, ask for reports up to and including
        # yesterday (UTC).  Note that datetime.date seems to be
        # unaware that "today (local)" and "today (UTC)" are not the
        # same thing, feh.
        end_date = ((datetime.datetime.utcnow()
                     - datetime.timedelta(days=1))
                    .strftime("%Y-%m-%d"))

        return db, oid, start_date, end_date

class HerdictReader:
    def __init__(self, pageq, start_date, end_date):
        self.pageq = pageq
        self.start_date = start_date
        self.end_date = end_date

    def __call__(self, mon, thr):
        if self.start_date is None:
            base_url = ("http://herdict.org/api/query?fed={}&page="
                        .format(self.end_date))
            base_status = ("Until {}: loading page ".format(self.end_date))
        else:
            base_url = ("http://herdict.org/api/query?fsd={}&fed={}&page="
                        .format(self.start_date, self.end_date))
            base_status = ("From {} through {}: loading page "
                           .format(self.start_date, self.end_date))

        session = requests.Session()
        try:
            page = 1
            backoff = 5
            while True:
                ps = str(page)
                mon.report_status(base_status + ps)
                try:
                    resp = session.get(base_url + ps)
                    blob = resp.json()
                except (ValueError, UnicodeDecodeError,
                        requests.exceptions.ConnectionError):
                    # Herdict seems to indicate that it wants you to
                    # back off a bit on the queries by sending a bogus
                    # HTTP response.  Le sigh.
                    mon.report_status(base_status + ps +
                                      " [connection error, retry in {}s]"
                                      .format(backoff))
                    mon.maybe_pause_or_stop()
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                if not blob:
                    break
                mon.report_status(base_status + ps +
                                  " [{} rows]".format(len(blob)))
                self.pageq.put(blob)
                page += 1
                backoff = 5
                mon.maybe_pause_or_stop()
        finally:
            self.pageq.put(None)
