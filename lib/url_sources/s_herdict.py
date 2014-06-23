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

from shared import url_database
from shared.monitor import Monitor

class HerdictExtractor:
    def __init__(self, args):
        # Do not load the database until we are on the correct thread.
        self.args = args
        self.summary = None

    def __call__(self, mon, thr):
        db, start_date, end_date = self.prepare_database()
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
                break
            batch = []
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

                # Herdict reports have several more keys than this,
                # but none of them appear to be terribly trustworthy.
                accessible = (row["reportType"] != "INACCESSIBLE")
                if "country" in row and "shortName" in row["country"]:
                    country = row["country"]["shortName"]
                else:
                    country = "??"

                (uid, url) = url_database.add_url_string(cur, url)
                batch.append((uid, timestamp, accessible, country))

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

            cur.execute(b"INSERT INTO urls_herdict "
                        b"(url, \"timestamp\", accessible, country) VALUES "
                        + b",".join(cur.mogrify("(%s,%s,%s,%s)", row)
                                    for row in batch))
            db.commit()
            mon.maybe_pause_or_stop()

        mon.report_status("Flushing duplicates...")
        # The urls_herdict table doesn't have any uniquifier.
        # Flush any duplicate rows that may have occurred.
        cur.execute(
            'DELETE FROM urls_herdict WHERE ctid IN (SELECT ctid FROM ('
            '  SELECT ctid, row_number() OVER ('
            '    PARTITION BY url,"timestamp",accessible,country'
            '    ORDER BY ctid) AS rnum FROM urls_herdict) t'
            '  WHERE t.rnum > 1)')
        db.commit()

        mon.report_status("Adding URLs to be canonicalized...")
        cur.execute("INSERT INTO canon_urls (url) "
                    "  SELECT DISTINCT url FROM urls_herdict"
                    "  EXCEPT SELECT url FROM canon_urls")
        db.commit()
        self.summary = (lo_timestamp, hi_timestamp,
                        n_total, n_accessible, n_inaccessible)

    def report_final_statistics(self):
        if not self.summary: return
        f = sys.stdout
        s = self.summary
        f.write("Earliest report: {}\n".format(time.ctime(s[1])))
        f.write("Latest report:   {}\n".format(time.ctime(s[0])))
        f.write("Processed {} urls; {} accessible, {} inaccessible\n"
                .format(s[2], s[3], s[4]))

    def prepare_database(self):
        db = url_database.ensure_database(self.args)
        cur = db.cursor()
        # Find the latest date already in the table.  We don't
        # need to process dates before that point.
        cur.execute("SELECT coalesce(max(timestamp), 0) "
                    "FROM urls_herdict")
        start_date = cur.fetchone()[0];
        if start_date == 0:
            start_date = None
        else:
            start_date = (datetime.date.fromtimestamp(start_date)
                          .strftime("%Y-%m-%d"))

        end_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        return db, start_date, end_date

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
