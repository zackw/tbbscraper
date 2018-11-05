# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Populate the table of URLs to rescan, from a CSV file --- implementation."""

from shared import url_database
import csv

def rescan(args):
    db = url_database.ensure_database(args)
    cur = db.cursor()
    cur.execute("SET search_path TO ts_run_4")
    with open(args.to_rescan, "rt") as f:
        rd = csv.DictReader(f)
        process_urls(db, rd)

def process_urls(db, rd):
    batch = []
    for row in rd:
        (uid, _) = url_database.add_url_string(db, row['url'])
        batch.append( (uid, row['result'], row['locales']) )

    with db, db.cursor() as cur:
        batch_str = b",".join(cur.mogrify("(%s,%s,%s)", row)
                              for row in batch)
        cur.execute(b"INSERT INTO urls_rescan (url, result, locales)"
                    b"VALUES " + batch_str)
        db.commit()
