# Copyright © 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Merge several URL databases into one.  The -d option specifies the
   destination database (which should not yet exist); give source
   databases as non-option arguments."""

def setup_argp(ap):
    ap.add_argument("sources", metavar="SOURCE", nargs="+",
                    help="Source databases to merge into the database "
                    "specified with the -d option.")

def run(args):
    merger = Merger(args)
    Monitor(merger, banner="Merging databases")
    merger.dump()

from collections import defaultdict
from shared import url_database
from shared.monitor import Monitor

class argshim:
    def __init__(self, db):
        self.database = db

class Merger:
    def __init__(self, args):
        self.args = args
        self.tables_processed = set()

    def dump(self):
        pass

    def __call__(self, mon, thr):
        self.mon = mon

        srcdbs = { src: url_database.reconnect_to_database(argshim(src))
                   for src in self.args.sources }

        destdb = url_database.ensure_database(self.args)

        self.uidmap = self.merge_url_strings(destdb, srcdbs)
        self.oidmap = self.merge_origins(destdb, srcdbs)
        self.cidmap = self.merge_canon_statuses(destdb, srcdbs)

        self.merge_urls(destdb, srcdbs)
        self.merge_canon_urls(destdb, srcdbs)
        self.merge_anomalies(destdb, srcdbs)
        self.merge_ancillary(destdb, srcdbs)

    def merge_url_strings(self, destdb, srcdbs):
        # It is expected to be more efficient to merge the strings in
        # memory.
        stringmap = defaultdict(dict)
        uidmap = {}

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Reading URL strings ({})..."
                                   .format(tag))
            self.mon.maybe_pause_or_stop()
            scur = sdb.cursor()
            n = 0
            for row in url_database.fetch_iter(
                    scur.execute("SELECT * FROM url_strings")):
                stringmap[row['url']][tag] = row['id']
                n = max(n, row['id'])
                self.mon.report_status("Reading URL strings ({})... {}"
                                       .format(tag, n))
                self.mon.maybe_pause_or_stop()
            uidmap[tag] = [None]*(n+1)

        self.mon.report_status("Sorting URL strings...")
        self.mon.maybe_pause_or_stop()
        merged = sorted(stringmap.keys())
        self.mon.report_status("Writing URL strings...")
        self.mon.maybe_pause_or_stop()
        destdb.executemany("INSERT INTO url_strings VALUES(?,?)",
                           enumerate(merged, 1))
        self.mon.report_status("Writing URL strings (commit)...")
        self.mon.maybe_pause_or_stop()
        destdb.commit()
        self.mon.report_status("Constructing uidmap...")
        self.mon.maybe_pause_or_stop()
        for uid, url in enumerate(merged, 1):
            smap = stringmap[url]
            self.mon.report_status("Constructing uidmap... {}".format(uid))
            self.mon.maybe_pause_or_stop()
            for tag in srcdbs.keys():
                sid = smap.get(tag, None)
                if sid is not None:
                    uidmap[tag][sid] = uid

        self.tables_processed.add("url_strings")
        return uidmap

    def merge_origins(self, destdb, srcdbs):
        # The logic here is essentially the same as for URL strings,
        # but the origins lists are much, much shorter.

        stringmap = defaultdict(dict)
        oidmap = {}

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Reading origin list ({})..."
                                   .format(tag))
            scur = sdb.cursor()
            n = 0
            for row in url_database.fetch_iter(
                    scur.execute("SELECT * FROM origins")):
                stringmap[row['label']][tag] = row['id']
                n += 1
                self.mon.report_status("Reading origin list ({})... {}"
                                       .format(tag, n))
            oidmap[tag] = [None]*(n+1)

        self.mon.report_status("Sorting origin list...")
        merged = sorted(stringmap.keys())
        self.mon.report_status("Writing origin list...")
        destdb.executemany("INSERT INTO origins VALUES(?,?)",
                           enumerate(merged, 1))
        self.mon.report_status("Writing origins (commit)...")
        destdb.commit()
        self.mon.report_status("Constructing oidmap...")
        for oid, label in enumerate(merged, 1):
            smap = stringmap[label]
            for tag in srcdbs.keys():
                sid = smap.get(tag, None)
                if sid is not None:
                    oidmap[tag][sid] = oid

        self.tables_processed.add("origins")
        return oidmap

    def merge_canon_statuses(self, destdb, srcdbs):
        # Similarly.

        stringmap = defaultdict(dict)
        cidmap = {}

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Reading canon status list ({})..."
                                   .format(tag))
            scur = sdb.cursor()
            n = 0
            for row in url_database.fetch_iter(
                    scur.execute("SELECT * FROM canon_statuses")):
                stringmap[row['label']][tag] = row['id']
                n += 1
                self.mon.report_status("Reading canon_status list ({})... {}"
                                       .format(tag, n))
            cidmap[tag] = [None]*(n+1)

        self.mon.report_status("Sorting canon status list...")
        merged = sorted(stringmap.keys())
        self.mon.report_status("Writing canon status list...")
        destdb.executemany("INSERT INTO canon_statuses VALUES(?,?)",
                           enumerate(merged, 1))
        self.mon.report_status("Writing canon_statuses (commit)...")
        destdb.commit()
        self.mon.report_status("Constructing cidmap...")
        for oid, label in enumerate(merged, 1):
            smap = stringmap[label]
            for tag in srcdbs.keys():
                sid = smap.get(tag, None)
                if sid is not None:
                    cidmap[tag][sid] = oid

        self.tables_processed.add("canon_statuses")
        return cidmap

    def merge_urls(self, destdb, srcdbs):

        writer = destdb.cursor()
        write_batch = []

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Merging URLs ({})..."
                                   .format(tag))
            self.mon.maybe_pause_or_stop()

            scur = sdb.cursor()
            omap = self.oidmap[tag]
            umap = self.uidmap[tag]
            for row in url_database.fetch_iter(
                    scur.execute("SELECT * FROM urls")):
                m_origin = omap[row['origin']]
                m_origin_id = row['origin_id']
                m_url = umap[row['url']]
                write_batch.append((m_origin, m_origin_id, m_url))
                self.mon.report_status("Merging URLs ({})... {}.{}"
                                       .format(tag, m_origin, m_origin_id))
                self.mon.maybe_pause_or_stop()

                if len(write_batch) > 10000:
                    self.mon.report_status("Merging URLs ({})... writing"
                                           .format(tag))
                    writer.executemany("INSERT INTO urls VALUES(?,?,?)",
                                       write_batch)
                    write_batch = []
                    destdb.commit()
                    self.mon.maybe_pause_or_stop()

        self.mon.report_status("Merging URLs ({})... writing"
                               .format(tag))
        writer.executemany("INSERT INTO urls VALUES(?,?,?)",
                           write_batch)
        destdb.commit()
        self.tables_processed.add("urls")

    def merge_canon_urls(self, destdb, srcdbs):
        writer = destdb.cursor()
        write_batch = []

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Merging canon URLs ({})..."
                                   .format(tag))
            self.mon.maybe_pause_or_stop()

            scur = sdb.cursor()
            umap = self.uidmap[tag]
            cmap = self.cidmap[tag]
            for row in url_database.fetch_iter(
                    scur.execute("SELECT * FROM canon_urls")):
                m_url = umap[row['url']]
                m_canon = umap[row['canon']]
                m_status = cmap[row['status']]
                write_batch.append((m_url, m_canon, m_status))
                self.mon.report_status("Merging canon URLs ({})... {}.{}"
                                       .format(tag, m_url))
                self.mon.maybe_pause_or_stop()

                if len(write_batch) > 10000:
                    self.mon.report_status("Merging canon URLs ({})... writing"
                                           .format(tag))
                    writer.executemany("INSERT INTO canon_urls VALUES(?,?,?)",
                                       write_batch)
                    write_batch = []
                    destdb.commit()
                    self.mon.maybe_pause_or_stop()

        self.mon.report_status("Merging canon URLs ({})... writing"
                               .format(tag))
        writer.executemany("INSERT INTO canon_urls VALUES(?,?,?)",
                           write_batch)
        destdb.commit()
        self.tables_processed.add("canon_urls")

    def merge_anomalies(self, destdb, srcdbs):
        writer = destdb.cursor()
        write_batch = []

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Merging anomalies ({})..."
                                   .format(tag))
            self.mon.maybe_pause_or_stop()

            scur = sdb.cursor()
            umap = self.uidmap[tag]
            cmap = self.cidmap[tag]
            for row in url_database.fetch_iter(
                    scur.execute("SELECT * FROM anomalies")):
                m_url = umap[row['url']]
                m_status = cmap[row['status']]
                write_batch.append((m_url, m_status, row['response']))
                self.mon.report_status("Merging anomalies ({})... {}.{}"
                                       .format(tag, m_url))
                self.mon.maybe_pause_or_stop()

                if len(write_batch) > 10000:
                    self.mon.report_status("Merging anomalies ({})... writing"
                                           .format(tag))
                    writer.executemany("INSERT INTO anomalies VALUES(?,?,?)",
                                       write_batch)
                    write_batch = []
                    destdb.commit()
                    self.mon.maybe_pause_or_stop()

        self.mon.report_status("Merging anomalies ({})... writing"
                               .format(tag))
        writer.executemany("INSERT INTO anomalies VALUES(?,?,?)",
                           write_batch)
        destdb.commit()
        self.tables_processed.add("anomalies")

    def merge_ancillary(self, destdb, srcdbs):
        # The ancillary table merge currently assumes that we don't
        # need to do any ID fixups.  This is accurate for the present
        # data set just because each database-to-be-merged has only one
        # origin in it, but may become a problem later.

        writer = destdb.cursor()
        already_created = set(row[0] for row in writer.execute(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                ).fetchall())

        for tag, sdb in srcdbs.items():
            self.mon.report_status("Merging ancillary tables ({})..."
                                   .format(tag))
            self.mon.maybe_pause_or_stop()
            scur = sdb.cursor()
            for name, sql in scur.execute(
                    "SELECT name, sql FROM sqlite_master "
                    "WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%'").fetchall():
                if name in self.tables_processed:
                    continue

                self.mon.report_status("Merging ancillary tables ({})... {}"
                                       .format(tag, name))
                self.mon.maybe_pause_or_stop()

                if name not in already_created:
                    writer.executescript(sql)
                    for isql in scur.execute(
                            "SELECT sql FROM sqlite_master "
                            "WHERE type = 'index' "
                            "AND tbl_name = ?", (name,)).fetchall():
                        writer.executescript(isql[0])
                    already_created.add(name)

                # This is the least bad available way to find out how
                # many columns the table has.  It doesn't work if the
                # table is empty, but if the table is empty, we don't
                # need to do anything.
                row = scur.execute(
                    "SELECT * FROM \""+name+"\" LIMIT 1").fetchone()
                if row is None:
                    continue

                cols = len(row)
                insertion = ("INSERT INTO \""+name+"\" VALUES(" +
                             ",".join(["?"]*cols) + ")")
                selection = "SELECT * FROM \""+name+"\""
                # sqlite3.Row objects cannot be passed directly to execute().
                # Feh. Feh, I say.
                for row in url_database.fetch_iter(scur.execute(selection)):
                    writer.execute(insertion, tuple(row))
                destdb.commit()
