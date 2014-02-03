# URL database management.

import sqlite3

application_id = "urls"

# Increment this number every time the overall schema changes!
# Origins are responsible for tracking their additional tables' schemas.
current_schema_version = 2

# Some pragmata are not persistent, so they must be repeated for every
# database connection.
connection_pragmata = r"""
PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;
PRAGMA locking_mode = NORMAL;
"""

# Overall schema for the URL database, used by the various URL sources and
# transformers in this directory.
# We currently use SQLite for this database, so the columns are not as
# annotated as they could be.
# "Source" is a SQL keyword, so we refer instead to "origins" throughout
# this schema.
schema = r"""
PRAGMA legacy_file_format = OFF;
PRAGMA application_id = {application_id};
PRAGMA user_version = {current_schema_version};

-- this needs to be last, as it interacts with the above in a complicated way
PRAGMA journal_mode = WAL;

-- Every origin has an entry in this table; it currently just tags the
-- origin with a human-readable label.  Origins are also encouraged to
-- create a metadata table, including anything that seems relevant,
-- with a name recognizably derived from the label, and indexed by
-- their per-origin id for the URL.  The schema for that table will be
-- with the code for the origin itself.
--
-- Origins can either be external sources of URLs (Alexa, Twitter,
-- etc) or transformation passes applied to URLs already in the table
-- (such as 'try accessing the top level of every site with and
-- without a leading "www."', 'try converting http: to https: and vice
-- versa', and 'attempt to see through geographic or language
-- variants of a site').
--
-- id:    automatically assigned serial number for this origin.
-- label: human-readable label.
CREATE TABLE origins (
  id         INTEGER PRIMARY KEY,
  label      TEXT NOT NULL UNIQUE
);

-- This table holds the actual text of every URL.
-- Other tables refer to URLs by id number in this table.
CREATE TABLE url_strings (
  id         INTEGER PRIMARY KEY,
  url        TEXT NOT NULL UNIQUE
);

-- Table tracking URLs with their origins.
-- origin:    index into the "origins" table
-- origin_id: origin-specific identifier; may index an ancillary
--            metadata table created by the origin
-- url:       URL as retrieved from the origin; indexes url_strings
CREATE TABLE urls (
  origin     INTEGER NOT NULL REFERENCES origins(id),
  origin_id  INTEGER NOT NULL,
  url        INTEGER NOT NULL REFERENCES url_strings(id),
  UNIQUE (origin, origin_id)
);

-- Canonicalization of URLs is a separate pass and has different uniqueness
-- requirements, so it gets its own tables.
-- (Maybe this should move into canonize.py?)

-- Status lines received during canonicalization.
-- These are not necessarily *HTTP* statuses: for instance,
-- DNS lookup failure gets recorded here too.
CREATE TABLE canon_statuses (
  id         INTEGER PRIMARY KEY,
  status     TEXT NOT NULL UNIQUE
);

-- URLs and their canonical forms (i.e. after chasing all redirects).
-- If both 'canon' and 'status' are NULL, the URL has not yet been
-- canonized.  If 'canon' is NOT NULL, 'status' should be also.
-- If 'canon' is NULL but 'status' isn't, canonization failed
-- (this happens often - dead sites, redirect loops, etc)
CREATE TABLE canon_urls (
  url        INTEGER PRIMARY KEY REFERENCES url_strings(id),
  canon      INTEGER REFERENCES url_strings(id),
  status     INTEGER REFERENCES canon_statuses(id)
);

-- Anomalous HTTP responses are logged in this table.
CREATE TABLE anomalies (
  url        INTEGER PRIMARY KEY REFERENCES url_strings(id),
  status     INTEGER NOT NULL REFERENCES canon_statuses(id),
  response   TEXT NOT NULL
);
"""

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
    schema_ = schema.format(current_schema_version=current_schema_version,
                            application_id=numeric_application_id)

    db = sqlite3.connect(args.database)
    schema_version = int(db.execute("PRAGMA user_version;").fetchone()[0])
    if schema_version == 0:
        db.executescript(schema_)
        db.commit()
        schema_version = int(db.execute("PRAGMA user_version;").fetchone()[0])

    if schema_version != current_schema_version:
        raise RuntimeError("schema version mismatch: exp %d got %d"
                           % (current_schema_version, schema_version))

    db.executescript(connection_pragmata)
    return db
