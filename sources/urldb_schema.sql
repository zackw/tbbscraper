-- Overall schema for the URL database, used by the various URL sources and
-- transformers in this directory.
-- We currently use SQLite for this database, so the columns are not as
-- annotated as they could be.
-- "Source" is a SQL keyword, so we refer instead to "origins" throughout
-- this file.

PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;
PRAGMA legacy_file_format = OFF;
PRAGMA locking_mode = NORMAL;

-- 'urls' == 0x75726C73
PRAGMA application_id = 1970433139;

-- increment this number every time this file is changed!
-- origins are responsible for tracking their own schemas.
PRAGMA user_version = 1;

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
