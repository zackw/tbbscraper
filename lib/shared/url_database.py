# URL database management.  Shared among all the sources and the
# scraper controller.
#
# Copyright © 2014 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import re
import sqlite3
import threading
import time
import urllib.parse
from shared import sql_lexer

#
# Database, table, and index creation.
#

class TableSpec:
    """A specification for a single table that should be created."""
    def __init__(self, name, raw_columns):
        self.name = name

        # Canonicalize the whitespace and then put back a little bit
        # of pretty-printing, so that '.schema' is readable.
        cnamelen = 0
        columns1 = []
        constraints = []
        for c in raw_columns:
            c = sql_lexer.canonicalize(c)
            if sql_lexer.is_constraint(c):
                n = c.find("(")
                c = "    " + c[:n] + " " + c[n:]
                constraints.append(c)
            else:
                n = c.find(" ")
                cnamelen = max(cnamelen, n)
                columns1.append(c)

        if not columns1:
            raise ValueError("table "+name+" has no columns")

        columns = []
        for c in columns1:
            n = c.find(" ")
            columns.append("    {1:<{0}}{2}".format(cnamelen, c[:n], c[n:]))

        self.schema = "CREATE TABLE "+name+" (\n"
        self.schema += ",\n".join(columns)
        if constraints:
            self.schema += ",\n"
            self.schema += ",\n".join(constraints)
        self.schema += "\n)"

    def ensure(self, db):
        """Ensure that a table with this specification exists in
           database DB."""

        existing = db.execute("SELECT type, sql FROM sqlite_master "
                              "WHERE name = ?", (self.name,)).fetchall()
        if not existing:
            db.executescript(self.schema)
            return

        if len(existing) > 1:
            raise RuntimeError("more than one item named '{}'?!"
                               .format(self.name))
        if existing[0] != "table":
            raise RuntimeError("existing item named '{}' is not a table"
                               .format(self.name))

        canon_schema = sql_lexer.canonicalize(self.schema)
        existing_schema = sql_lexer.canonicalize(existing[1])
        if canon_schema != existing_schema:
            raise RuntimeError("schema mismatch for {}:\n"
                               "want: {}\n"
                               "got:  {}"
                               .format(canon_schema, existing_schema))

class IndexSpec:
    """A specification for a single external index that should be
       created.  TABLE is the name of the table, COLUMNS is a list of
       column names to index, and UNIQUE specifies whether the index
       ought to be unique."""
    def __init__(self, table, columns, unique=False):
        self.name = table + "__" + "_".join(columns)
        self.schema = ("CREATE {}INDEX {} ON {}({})"
                       .format("UNIQUE " if unique else "",
                               self.name, table, ",".join(columns)))

    def ensure(self, db):
        """Ensure that an index with this specification exists in
           database DB."""

        existing = db.execute("SELECT type, sql FROM sqlite_master "
                              "WHERE name = ?", (self.name,)).fetchall()
        if not existing:
            db.executescript(self.schema)
            return

        if len(existing) > 1:
            raise RuntimeError("more than one item named '{}'?!"
                               .format(self.name))
        if existing[0] != "index":
            raise RuntimeError("existing item named '{}' is not an index"
                               .format(self.name))
        canon_schema = sql_lexer.canonicalize(self.schema)
        existing_schema = sql_lexer.canonicalize(existing[1])
        if canon_schema != existing_schema:
            raise RuntimeError("schema mismatch for {}:\n"
                               "want: {}\n"
                               "got:  {}"
                               .format(canon_schema, existing_schema))

def ensure_database(args):
    """Ensure that the database specified by args.database exists and
       has an up-to-date schema.  `args` would normally be an
       argparse.Namespace object, but we don't care as long as
       "database" is an attribute (nor do we care how the argument
       actually shows up on the command line).

       WHEN NEEDED: implement schema migration."""

    # We need to make a couple substitutions into the raw SQL.
    assert len(application_id) == 4
    numeric_application_id = (ord(application_id[0]) << 24 |
                              ord(application_id[1]) << 16 |
                              ord(application_id[2]) << 8  |
                              ord(application_id[3]))
    schema_pragmata_ = schema_pragmata.format(
        current_schema_version=current_schema_version,
        application_id=numeric_application_id)

    db = sqlite3.connect(args.database)
    db.executescript(connection_pragmata)

    schema_version = int(db.execute("PRAGMA user_version;").fetchone()[0])
    if schema_version == 0:
        db.executescript(schema_pragmata_)
        for tbl in generic_tables:
            tbl.ensure(db)
        db.commit()
        schema_version = int(db.execute("PRAGMA user_version;").fetchone()[0])

    if schema_version != current_schema_version:
        raise RuntimeError("schema version mismatch: exp %d got %d"
                           % (current_schema_version, schema_version))

    return db

def reconnect_to_database(args):
    """Establish a second connection to the database in args.database,
       but don't create any tables.  This is used for cursor isolation
       when reading and writing from the database simultaneously (see
       e.g. s_canonize.py) and by Checkpointer."""
    db = sqlite3.connect(args.database)
    db.executescript(connection_pragmata)

    schema_version = int(db.execute("PRAGMA user_version;").fetchone()[0])
    if schema_version != current_schema_version:
        raise RuntimeError("schema version mismatch: exp %d got %d"
                           % (current_schema_version, schema_version))

    return db

class Checkpointer:
    """Thread that runs under Monitor and periodically forces a
       database checkpoint."""
    def __init__(self, args):
        self._args = args
        self._stopEvent = threading.Event()

    def __call__(self, mon, thr):
        db = reconnect_to_database(self._args)
        db.execute("PRAGMA wal_autocheckpoint=0;")
        while True:
            if self._stopEvent.is_set(): return
            mon.maybe_pause_or_stop()
            time.sleep(5)

            if self._stopEvent.is_set(): return
            mon.maybe_pause_or_stop()
            mon.report_status("Checkpointing...")
            start = time.time()
            stats = db.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchone()
            finish = time.time()
            mon.report_status("Checkpointed {}/{} pages in {:.4} seconds"
                              .format(stats[1], stats[2], finish-start))

    def stop(self):
        self._stopEvent.set()

#
# Utilities for working with the shared schema.
#

def canon_url_syntax(url):
    """Syntactically canonicalize a URL.  This makes the following
       transformations:
         - scheme and hostname are lowercased
         - hostname is punycoded if necessary
         - vacuous user, password, and port fields are stripped
         - ports redundant to the scheme are also stripped
         - path becomes '/' if empty

       You can provide either a string or a SplitResult, and you
       get back what you put in.
    """

    if isinstance(url, urllib.parse.SplitResult):
        exploded = url
        want_splitresult = True
    else:
        # Insist on working with purely ASCII URLs (but in 'str', for
        # convenience), because a site that responds to
        # http://foo.example/Br%e8ve probably won't accept
        # http://foo.example/Br%c3%a8ve as the same thing.
        # This hasn't hitherto been an issue, but if it does come up
        # we want to catch it post haste.
        if hasattr(url, "encode"):
            url = url.encode("ascii").decode("ascii")
        else:
            url = url.decode("ascii")

        exploded = urllib.parse.urlsplit(url)
        if not exploded.hostname:
            # Remove extra slashes after the scheme and retry.
            corrected = re.sub(r'(?i)^([a-z]+):///+', r'\1://', url)
            exploded = urllib.parse.urlsplit(corrected)

        want_splitresult = False

    if not exploded.hostname:
        raise ValueError("url with no host - " + repr(url))

    scheme = exploded.scheme
    if scheme != "http" and scheme != "https":
        raise ValueError("url with non-http(s) scheme - " + repr(url))

    host   = exploded.hostname.lower()
    user   = exploded.username or ""
    passwd = exploded.password or ""
    port   = exploded.port
    path   = exploded.path
    query  = exploded.query
    frag   = exploded.fragment

    if path == "":
        path = "/"

    if port is None:
        port = ""
    elif ((port == 80  and scheme == "http") or
          (port == 443 and scheme == "https")):
        port = ""
    else:
        port = ":{}".format(port)

    # We don't have to worry about ':' or '@' in the user and password
    # strings, because urllib.parse does not do %-decoding on them.
    if user == "" and passwd == "":
        auth = ""
    elif passwd == "":
        auth = "{}@".format(user)
    else:
        auth = "{}:{}@".format(user, passwd)
    netloc = auth + host + port

    result = urllib.parse.SplitResult(scheme, netloc, path, query, frag)
    if want_splitresult:
        return result
    else:
        return result.geturl()

def add_url_string(db, url):
    """Add an URL to the url_strings table for DB, if it is not already there.
       Returns a pair (id, url) where ID is the table identifier, and URL
       is the URL as returned by canon_url_syntax()."""

    url = canon_url_syntax(url)

    # Accept either a database connection or a cursor.
    if isinstance(db, sqlite3.Cursor):
        cur = db
    else:
        cur = db.cursor()

    # It is a damned shame that there is no way to do this in one SQL
    # operation.
    cur.execute("SELECT id FROM url_strings WHERE url = ?", (url,))
    row = cur.fetchone()
    if row is not None:
        id = row[0]
    else:
        cur.execute("INSERT INTO url_strings VALUES(NULL, ?)", (url,))
        id = cur.lastrowid

    return (id, url)


#
# Common schema used by all sources and the scraper controller.
#

application_id = "urls"

# Increment this number every time the overall schema changes!
# Origins are responsible for tracking their additional tables' schemas.
current_schema_version = 2

# Some pragmata are not persistent, so they must be repeated for every
# database connection.
connection_pragmata = r"""
PRAGMA encoding = 'UTF-8';
PRAGMA foreign_keys = ON;
PRAGMA locking_mode = NORMAL;
"""

# Persistent pragmata applied when the database is created.
schema_pragmata = r"""
PRAGMA legacy_file_format = OFF;
PRAGMA application_id = {application_id};
PRAGMA user_version = {current_schema_version};

-- this needs to be last, as it interacts with the above in a complicated way
PRAGMA journal_mode = WAL;
"""

# Generic tables for the URL database.
# We currently use SQLite for this database, so the columns are not as
# annotated as they could be.
# "Source" is a SQL keyword, so we refer instead to "origins" throughout
# this schema.

generic_tables = [
    # Every origin has an entry in this table; it currently just tags the
    # origin with a human-readable label.  Origins are also encouraged to
    # create a metadata table, including anything that seems relevant,
    # with a name recognizably derived from the label, and indexed by
    # their per-origin id for the URL.  The schema for that table will be
    # with the code for the origin itself.
    #
    # Origins can either be external sources of URLs (Alexa, Twitter,
    # etc) or transformation passes applied to URLs already in the table
    # (such as 'try accessing the top level of every site with and
    # without a leading "www."', 'try converting http: to https: and vice
    # versa', and 'attempt to see through geographic or language
    # variants of a site').
    TableSpec("origins", [
        "id    INTEGER PRIMARY KEY",  # automatically assigned serial number
        "label TEXT NOT NULL UNIQUE"  # human-readable label
    ]),

    # This table holds the actual text of every URL.
    # Other tables refer to URLs by id number in this table.
    # URLs must be 7-bit ASCII (percent-encoded as necessary).
    TableSpec("url_strings", [
        "id  INTEGER PRIMARY KEY",  # automatically assigned serial number
        "url TEXT NOT NULL UNIQUE", # text of URL
    ]),

    # Master index of URLs with their origins.
    # origin:    index into "origins" table
    # origin_id: origin-specific serial number; may have metadata embedded
    #            into it, or may index an ancillary metadata table
    # url:       URL as retrieved from the origin and lightly canonicalized;
    #            indexes url_strings
    TableSpec("urls", [
        "origin    INTEGER NOT NULL REFERENCES origins(id)",
        "origin_id INTEGER NOT NULL",
        "url       INTEGER NOT NULL REFERENCES url_strings(id)",
        "UNIQUE (origin, origin_id)"
    ]),

    # Full canonicalization of URLs (via redirection-following) is a
    # separate pass and has different uniqueness requirements, so it
    # gets its own tables.  (Maybe s_canon.py should create these
    # tables?)

    # Status lines received during canonicalization.
    # These are not necessarily *HTTP* statuses: for instance,
    # DNS lookup failure gets recorded here too.
    # Status lines are supposed to be ASCII, but sometimes we see data in
    # unspecified legacy encodings, so we use a BLOB for the text.
    TableSpec("canon_statuses", [
        "id     INTEGER PRIMARY KEY",
        "status BLOB NOT NULL UNIQUE"
    ]),

    # URLs and their canonical forms (i.e. after chasing all redirects).
    # If both 'canon' and 'status' are NULL, the URL has not yet been
    # canonized.  If 'canon' is NOT NULL, 'status' should be also.
    # If 'canon' is NULL but 'status' isn't, canonization failed
    # (this happens often - dead sites, redirect loops, etc)
    TableSpec("canon_urls", [
        "url    INTEGER PRIMARY KEY REFERENCES url_strings(id)",
        "canon  INTEGER REFERENCES url_strings(id)",
        "status INTEGER REFERENCES canon_statuses(id)",
    ]),

    # Anomalous HTTP responses are logged in this table.
    # They have been partially interpreted and then reserialized, but may
    # contain data in arbitrary legacy encodings or even be truly binary
    # (nothing says one can't make the body of a 5xx response be an image)
    # so the content is a BLOB.
    TableSpec("anomalies", [
        "url        INTEGER PRIMARY KEY REFERENCES url_strings(id)",
        "status     INTEGER NOT NULL REFERENCES canon_statuses(id)",
        "response   BLOB NOT NULL"
    ])
]
