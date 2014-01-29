# URL database management.

import sqlite3

application_id = "urls"

# Increment this number every time the overall schema changes!
# Origins are responsible for tracking their additional tables' schemas.
current_schema_version = 1

# Overall schema for the URL database, used by the various URL sources and
# transformers in this directory.
# We currently use SQLite for this database, so the columns are not as
# annotated as they could be.
# "Source" is a SQL keyword, so we refer instead to "origins" throughout
# this schema.
schema = r"""
PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;
PRAGMA legacy_file_format = OFF;
PRAGMA locking_mode = NORMAL;
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
CREATE TABLE origins (
  id         INTEGER PRIMARY KEY, -- ID number for this origin.
  label      TEXT NOT NULL        -- Human-readable label for this origin.
);

-- Table tracking URLs with their origins.
CREATE TABLE urls (
  origin     INTEGER NOT NULL, -- The origin of the URL;
                               -- indexes the 'origins' table.
  origin_id  INTEGER NOT NULL, -- Origin-specific identifier for the URL;
                               -- indexes that origin's metadata table, if any.
  url        TEXT NOT NULL,    -- URL as retrieved from the origin
                               -- The same URL may have been retrieved
                               -- from many origins, so this is not UNIQUE.
  UNIQUE (origin, origin_id),
  FOREIGN KEY (origin) REFERENCES origins(id)
);
CREATE INDEX urls__url ON urls(url);

-- Canonicalization of URLs is a separate pass and has different uniqueness
-- requirements, so it gets its own table.
CREATE TABLE canon_urls (
  url        TEXT NOT NULL PRIMARY KEY,  -- Original URL.
  canon      TEXT,                       -- Canonicalized URL.
  status     INTEGER NOT NULL DEFAULT(0) -- Zero if not yet canonicalized,
                                         -- otherwise an HTTP status code
                                         -- or -1 for DNS lookup failure.
) WITHOUT ROWID;
CREATE INDEX canon_urls__canon ON canon_urls(canon);
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

    return db
