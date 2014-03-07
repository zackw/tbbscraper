# Copyright © 2013, 2014 Zack Weinberg
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
    extractors = {
        'single':   SingleExtractor,
        'snowball': SnowballExtractor,
        'frontier': FrontierExtractor,
        'firehose': FirehoseExtractor,
        'urls':     UrlsOnlyExtractor,
        'resume':   resume_extraction
    }
    args.seed = " ".join(args.seed)
    db, oid = ensure_database(args)
    extractor = extractors[args.mode](args, db, oid)
    extractor.run()

import calendar
import email.utils
import os
import pickle
import pickletools
import pkgutil
import re
import shutil
import sys
import time
import twython
import urllib.parse

import shared.url_database

def fatal(message, *args, **kwargs):
    import os.path
    import textwrap
    prog = os.path.basename(sys.argv[0])
    sys.stderr.write(textwrap.fill(message.format(*args, prog=prog, **kwargs)))
    sys.stderr.write('\n')
    sys.exit(1)

def connect_to_twitter_api():
    cred = pkgutil.get_data("url_sources", "twitter_credential.txt")
    (app_key, app_secret, oauth_token, oauth_secret) = cred.split("\n", 4)
    return twython.Twython(app_key, app_secret, oauth_token, oauth_secret)

def ensure_database(args):
    twitter_schema = """\
CREATE TABLE twitter_users (
    uid                 INTEGER PRIMARY KEY,
    created_at          INTEGER,
    verified            INTEGER,
    protected           INTEGER,
    highest_tweet_seen  INTEGER,
    screen_name         TEXT,
    full_name           TEXT,
    lang                TEXT,
    location            TEXT,
    description         TEXT
);
CREATE INDEX twitter_users__sn ON twitter_users(screen_name);

CREATE TABLE twitter_relations (
    follow_from INTEGER NOT NULL REFERENCES twitter_users(uid),
    follow_to   INTEGER NOT NULL REFERENCES twitter_users(uid)
);
CREATE INDEX twitter_relations__from ON twitter_relations(follow_from);
CREATE INDEX twitter_relations__to ON twitter_relations(follow_to);

CREATE TABLE twitter_tweets (
    tid                INTEGER PRIMARY KEY,
    uid                INTEGER NOT NULL REFERENCES twitter_users(uid),
    timestamp          INTEGER,
    retweets           INTEGER,
    possibly_sensitive INTEGER,
    lang               TEXT,
    withheld           TEXT,
    hashtags           TEXT
);

CREATE TABLE twitter_scans (
    scan               INTEGER PRIMARY KEY,
    mode               TEXT    NOT NULL,
    limit_             INTEGER NOT NULL,
    parallel           INTEGER NOT NULL,
    seed               TEXT,
    state              BLOB
);
"""

    db = shared.url_database.ensure_database(args)
    with db:
        # FIXME: More sophisticated way of detecting presence of our
        # ancillary schema.
        s_tables = frozenset(re.findall("(?m)(?<=^CREATE TABLE )[a-z_]+",
                                        twitter_schema))
        s_indices = frozenset(re.findall("(?m)(?<=^CREATE INDEX )[a-z_]+",
                                         twitter_schema))
        d_tables = frozenset(r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE "
                "  type = 'table' AND name LIKE 'twitter_%'"))
        d_indices = frozenset(r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE "
                "  type = 'index' AND name LIKE 'twitter_%'"))

        if not d_tables and not d_indices:
            db.executescript(twitter_schema)
            db.commit()

        elif d_tables != s_tables or d_indices != s_indices:
            raise RuntimeError("ancillary schema mismatch - migration needed")

        oid = db.execute("SELECT id FROM origins"
                         "  WHERE label = 'twitter'").fetchone()
        if oid is None:
            oid = db.execute("INSERT INTO origins"
                             "  VALUES(NULL, 'twitter')").lastrowid
        else:
            oid = oid[0]

        return db, oid

def dump_resumables_and_exit(db):
    resumable = db.execute("SELECT scan, mode, limit_, parallel, seed "
                           "FROM twitter_scans "
                           "WHERE state NOT NULL "
                           "ORDER BY mode").fetchall()
    if not resumable:
        fatal("{prog}: No scans can be resumed.")

    # we need to munge the list
    resumable = list(list(row) for row in resumable)

    colheads = ["scan", "mode", "limit", "par", "seed"]
    colwidths = [len(x) for x in colheads]
    maxwidth = shutil.get_terminal_size().columns

    for row in resumable:
        row[2] = str(row[2])
        row[3] = str(row[3])
        for i, col in enumerate(row):
            colwidths[i] = max(len(col), colwidths[i])

    separator = ["-"*n for n in colwidths]
    resumable.insert(0, separator)
    resumable.insert(0, colheads)

    sys.stderr.write("Scans that can be resumed:\n")
    for row in resumable:
        sys.stderr.write("{1:<{0}} {3:>{2}} {5:>{4}} {7:>{6}} {9:<{8}}\n"
                         .format(colwidths[0], row[0],
                                 colwidths[1], row[1],
                                 colwidths[2], row[2],
                                 colwidths[3], row[3],
                                 colwidths[4], row[4][:colwidths[4]]))
    fatal("Use '{prog} twitter resume SCAN' to resume an "
          "interrupted scan.")

def resume_extraction(args, db, oid):
    if not args.seed:
        dump_resumables_and_exit(db)

    if len(args.seed) > 1:
        fatal("{prog}: too many arguments for 'twitter resume' mode")

    state = db.execute("SELECT * FROM twitter_scans WHERE scan = ?",
                       (args.seed[0],)).fetchall()
    assert len(state) <= 1
    if not state:
        fatal("{prog}: no scan '{scan}' to resume.\n"
              "Use '{prog} twitter resume' with no further arguments "
              "for a list of resumable scans.", scan=args.seed[0])

    extractor = Extractor.reload(*state[0])
    return extractor

class Extractor:
    """Base class for extraction algorithms.  Note: subclasses should
       call Extractor.__init__ at the _end_ of their own __init__
       (if they need one), because it does an immediate checkpoint
       (via set_scan_number)."""
    def __init__(self, args, db, oid):
        self.db       = db
        self.oid      = oid
        self.db_name  = args.database
        self.mode     = args.mode
        self.limit    = args.limit
        self.parallel = args.parallel
        self.seed     = args.seed
        self.set_scanno()

    def __getstate__(self):
        # Don't attempt to pickle the database handle, or anything that
        # is stored in the database outside the pickle.
        state = self.__dict__.copy()
        for k in ('db', 'db_name', 'oid', 'mode', 'limit', 'parallel',
                  'seed', 'scanno'):
            try: del state[k]
            except KeyError: pass

        return state

    @classmethod
    def reload(cls, args, db, oid, scan, mode, limit, parallel, seed, state):
        this = pickle.loads(state)
        assert isinstance(this, cls)

        this.db       = db
        this.oid      = oid
        this.db_name  = args.database
        this.scanno   = scan
        this.mode     = mode
        this.limit    = limit
        this.seed     = seed
        this.parallel = parallel

        if this.limit != args.limit and args.limit != 1:
            fatal("{prog}: Cannot change --limit when resuming a scan.")
        if this.parallel != args.parallel and args.parallel != 1:
            fatal("{prog}: Cannot change --parallel when resuming a scan.")
        if this.seed != args.seed and args.seed != "":
            fatal("{prog}: Cannot change seed when resuming a scan.")

        return this

    def set_scanno(self):
        cur = self.db.execute("INSERT INTO twitter_scans "
                              "VALUES (NULL, ?, ?, ?, ?, ?)",
                              (self.mode, self.limit, self.parallel, self.seed,
                               pickletools.optimize(pickle.dumps(self))))
        self.scanno = cur.lastrowid
        self.db.commit()

    def checkpoint(self):
        self.db.execute("UPDATE twitter_scans SET state = ? WHERE scan = ?",
                        (pickletools.optimize(pickle.dumps(self)), self.scanno))
        self.db.commit()

    def complete(self):
        self.db.execute("UPDATE twitter_scans SET state = NULL WHERE scan = ?",
                        (self.scanno,))
        self.db.commit()

    def abandon(self, message, *args, **kwargs):
        self.db.execute("DELETE FROM twitter_scans WHERE scan = ?",
                        (self.scanno,))
        self.db.commit()
        fatal(message, *args, **kwargs)

    def run(self):
        """The main logic of each subclass goes here."""
        raise NotImplementedError

class SingleExtractor(Extractor):
    def __init__(self, args, db, oid):
        if not args.seed:
            fatal("{prog}: Must specify a Twitter handle from which to begin.")
        Extractor.__init__(self, args, db, oid)

class SnowballExtractor(Extractor):
    def __init__(self, args, db, oid):
        if not args.seed:
            fatal("{prog}: Must specify a Twitter handle from which to begin.")
        Extractor.__init__(self, args, db, oid)

class FrontierExtractor(Extractor):
    pass

class FirehoseExtractor(Extractor):
    pass

class UrlsOnlyExtractor(Extractor):
    pass
