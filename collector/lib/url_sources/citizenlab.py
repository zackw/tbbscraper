# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Download the current CitizenLab potentially-censored sites lists and
   add it to the URL database --- implementation."""


import csv
import glob
import os
import os.path
import stat
import subprocess
import sys
import time

from shared import url_database

class CitizenLabExtractor:
    def __init__(self, args):
        self.args = args
        self.delayed_failure = False

    def __call__(self):
        datestamp = time.strftime("%Y-%m-%d", time.gmtime())
        db        = url_database.ensure_database(self.args)
        to_import = self.update_srcdir(db, self.args.source, self.args.repo)
        self.ensure_category_codes(db, self.args.source)
        self.process_imports(db, datestamp, to_import)
        if self.delayed_failure:
            raise SystemExit(1)

    def update_srcdir(self, db, source, repo):
        need_update = True

        # Ensure that every file that Git touches has a timestamp
        # which is strictly greater than this, even if filesystem
        # timestamps are imprecise.
        before_update = time.time()
        time.sleep(2)

        if not os.path.exists(source):
            sys.stderr.write("Cloning {} into {}...\n"
                             .format(repo, source))
            subprocess.check_call(["git", "clone", repo, source])
            need_update = False

        if not os.path.isdir(os.path.join(source, ".git")):
            raise RuntimeError("{!r} exists but is not a Git checkout"
                               .format(source))
        if not os.path.isdir(os.path.join(source, "csv")):
            raise RuntimeError("{!r} exists but its contents are "
                               "not as expected"
                               .format(source))

        if need_update:
            sys.stderr.write("Updating {}...\n".format(source))
            subprocess.check_call(["git", "pull"], cwd=source)

        to_import = []

        with db, db.cursor() as cur:
            # for f in ${source}/csv/*.csv
            for f in glob.iglob(os.path.join(glob.escape(source),
                                             "csv", "*.csv")):
                b = os.path.basename(f)
                if b.startswith("00-LEGEND-"):
                    continue
                country_code = os.path.splitext(b)[0].upper()
                # for the database's sake, we replace 'global' and
                # 'cis' with two-letter codes in the "user-assigned"
                # ISO 3166-1a2 space.
                if country_code == "GLOBAL": country_code = "ZZ"
                elif country_code == "CIS":  country_code = "XC"
                elif len(country_code) != 2:
                    sys.stderr.write("{!r}: name doesn't contain a 2-letter "
                                     "country code or recognized exception\n"
                                     .format(f))
                    self.delayed_failure = True
                    continue

                cur.execute("SELECT 1 AS one FROM urls_citizenlab "
                            "WHERE country = %s "
                            "LIMIT 1", (country_code,))
                prev_import = (cur.fetchone() is not None)

                st = os.lstat(f)
                if (stat.S_ISREG(st.st_mode) and
                    (not prev_import or st.st_mtime_ns > before_update)):
                    to_import.append((f, country_code))

        return to_import

    def ensure_category_codes(self, db, source):
        f = os.path.join(source, "csv", "00-LEGEND-category_codes.csv")
        with open(f, newline='', encoding='utf-8') as fp:
            reader = csv.DictReader(fp)
            if reader.fieldnames != ['CategoryCode', 'CategoryName']:
                sys.stderr.write("{!r}: field names {!r} not as expected\n"
                                 .format(f, reader.fieldnames))
                raise SystemExit(1)
            with db, db.cursor() as cur:
                values = []
                for row in reader:
                    values.append(cur.mogrify("(%s,%s)",
                                              (row['CategoryCode'],
                                               row['CategoryName'])))
                cur.execute(
                    "CREATE TEMP TABLE clab_categories_new ("
                    "  code TEXT, description TEXT)")
                cur.execute(
                    b"INSERT INTO clab_categories_new "
                    b"VALUES " + b",".join(values))
                cur.execute(
                    "INSERT INTO clab_categories"
                    " SELECT DISTINCT code,description FROM clab_categories_new"
                    " EXCEPT SELECT code,description FROM clab_categories")

    def process_imports(self, db, datestamp, to_import):
        for f, country_code in to_import:
            with open(f, newline='', encoding='utf-8') as fp:
                with db, db.cursor() as cur:
                    self.process_one_import(cur, datestamp, country_code,
                                            csv.DictReader(fp))
            sys.stderr.write('\n')

    def process_one_import(self, cur, datestamp, country_code, reader):
        sys.stderr.write("Importing {}...".format(country_code))
        sys.stderr.flush()
        values = []
        for row in reader:
            category_code = row['category_code']
            uid, url = url_database.add_url_string(cur, row['url'])
            values.append(cur.mogrify("(%s,%s,%s,%s)",
                                      (uid, country_code, category_code,
                                       datestamp)))

        if not values:
            return

        sys.stderr.write(" (insert)")
        sys.stderr.flush()
        cur.execute(b"INSERT INTO urls_citizenlab "
                    b"(url, country, category, retrieval_date) "
                    b"VALUES "
                    + b",".join(values))

        sys.stderr.write(" (commit)")
        sys.stderr.flush()
