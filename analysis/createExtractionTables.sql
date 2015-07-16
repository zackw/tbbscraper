-- -*- sql-product: postgres -*-
--
-- Create tables for basic information extracted from the raw page
-- captures.  This includes, for instance, text content, outbound
-- links, and language assessment.  Some metadata from the
-- captured_pages table is also replicated, for convenience.

SET search_path TO ts_analysis;

BEGIN;

-- Mapping from ISO 639 language codes to common English names.
CREATE TABLE language_codes (
   code  TEXT NOT NULL PRIMARY KEY,
   name  TEXT NOT NULL
);

-- This url_strings table holds all of the URLs in these tables.
-- For convenience, it also holds crossreference columns which give
-- the matching URL numbers in the other schemas.  Note that not all
-- URLs are necessarily _in_ those tables.

CREATE TABLE url_strings (
    id    SERIAL   NOT NULL PRIMARY KEY,
    url   TEXT     NOT NULL UNIQUE CHECK (url <> ''),
    r1id  INTEGER,
    r2id  INTEGER,
    r3id  INTEGER
);

-- Merge of all the capture_detail tables.  Because capture_detail IDs
-- are not used to index anything else, we don't bother with
-- crossreference columns.
CREATE TABLE capture_detail (
    id     SERIAL   NOT NULL PRIMARY KEY,
    detail TEXT     NOT NULL UNIQUE CHECK (detail <> '')
);

-- Many pages contain exactly the same text content, even if their
-- HTML was not the same.  We store page text and properties derived
-- from it in this table, uniquely.
CREATE TABLE page_text (
    -- Serial number, for joins
    id       SERIAL  NOT NULL PRIMARY KEY,

    -- Does this text include site boilerplate?
    has_boilerplate BOOLEAN NOT NULL,

    -- ISO 639 two- or three-letter code: best estimate of the
    -- text's language.
    lang_code       TEXT NOT NULL,

    -- Confidence in the language estimate (percentage)
    lang_conf       REAL NOT NULL,

    -- Actual contents, zlib-compressed.
    contents BYTEA   NOT NULL
);

-- Do not waste effort trying to recompress 'contents' in the storage
-- layer, since it is already compressed.
-- lang_code is guaranteed to be two or three characters at most.
ALTER TABLE page_text
   ALTER COLUMN contents SET STORAGE EXTERNAL,
   ALTER COLUMN lang_code SET STORAGE PLAIN;

-- Index for deduplication.  The 'contents' field is frequently too
-- large to be the target of a regular index, so we use a function
-- index instead.  MD5 is the only strong hash built into postgres;
-- application code does not assume there are no collisions.
CREATE INDEX page_text_contents_idx
    ON page_text (md5(contents));

-- Backward map from page contents to field observations.  The primary
-- key is (document, url, locale, run) where 'document' is the text after
-- boilerplate removal (index into the page_texts table), 'url' is the
-- URL at which this text was observed, 'locale' is the country code
-- from which it was observed, and 'run' is the run number.  There are
-- then some ancillary data specific to the observation rather than the
-- document, mostly copied over from ts_run_<run>.captured_pages.
--
-- If you need to join back to ts_run_<run>.whatever to get at data
-- that's not captured here, beware that the URL numbers are different
-- in each of those schemas! You will need to look up the correct
-- number in url_strings.r<run>id in *this* schema.
--
CREATE TABLE page_observations (
   -- Note: this field always points to a row of 'page_text' for which
   -- has_boilerplate is FALSE.
   document    INTEGER NOT NULL REFERENCES page_text(id),
   url         INTEGER NOT NULL REFERENCES url_strings(id),

   -- This field is an ISO 631 country code, possibly with a
   -- disambiguating suffix.  It is not declared as a foreign key for
   -- the locale_data table because of the suffixes.
   locale      TEXT    NOT NULL CHECK (locale <> ''),

   -- Which data collection run is this from?  This tells you which
   -- ts_run_<n> schema to look in for more data.
   run         INTEGER NOT NULL CHECK (run >= 1),

   -- Which sources provided us with this URL?  One or more of the
   -- following codes: a=Alexa, c=Citizenlab, h=Herdict, p=Pinboard,
   -- s=Staticlist, t=Tweeted, u=Twitter user profile.
   sources     TEXT    NOT NULL CHECK (sources <> ''),

   -- More extracted data.
   -- 'document_with_bp' always points to a row of page_text for which
   -- has_boilerplate is TRUE.
   -- The other four are compressed JSON blobs.
   document_with_bp  INTEGER NOT NULL REFERENCES page_text(id),
   links        BYTEA NOT NULL,
   resources    BYTEA NOT NULL,
   headings     BYTEA NOT NULL,
   dom_stats    BYTEA NOT NULL,

   -- metadata denormalized from captured_pages
   access_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
   result      ts_run_1.capture_result NOT NULL,
   detail      INTEGER NOT NULL REFERENCES capture_detail(id),
   redir_url   INTEGER NOT NULL REFERENCES url_strings(id),
   html_length INTEGER NOT NULL, -- byte length of uncompressed UTF-8-coded HTML
   html_sha2   BYTEA   NOT NULL, -- SHA256 hash of uncompressed UTF-8-coded HTML

   -- The ordering on this index is chosen to facilitate lookups and joins.
   UNIQUE (document, url, locale, run)
);
CREATE UNIQUE INDEX page_observations_rlu_idx
    ON page_observations(run, locale, url);

ALTER TABLE page_observations
      -- These fields are guaranteed to be two or three characters at most.
      ALTER COLUMN locale    SET STORAGE PLAIN,
      ALTER COLUMN sources   SET STORAGE PLAIN,
      -- These fields are already compressed by the application.
      ALTER COLUMN links     SET STORAGE EXTERNAL,
      ALTER COLUMN resources SET STORAGE EXTERNAL,
      ALTER COLUMN headings  SET STORAGE EXTERNAL,
      ALTER COLUMN dom_stats SET STORAGE EXTERNAL,
      -- Secure hashes are incompressible.
      ALTER COLUMN html_sha2 SET STORAGE EXTERNAL;

-- Per-language, corpus-wide statistics.
-- The 'data' column will always be a compressed JSON blob;
-- currently, that blob is always a { word: number } dictionary.
-- Current values of 'stat' are:
--   cwf - Corpus word frequency: total occurrences of this word in the corpus.
--   rdf - Raw document frequency: total number of documents containing this word
--   idf - Inverse document frequency: log(n_docs/rdf) per word
CREATE TABLE corpus_stats (
   stat            TEXT    NOT NULL CHECK (stat <> ''),
   lang            TEXT    NOT NULL CHECK (lang <> ''),
   has_boilerplate BOOLEAN NOT NULL,
   n_documents     INTEGER NOT NULL CHECK (n_documents >= 1),
   data            BYTEA   NOT NULL,
   PRIMARY KEY(stat, lang, has_boilerplate)
);
ALTER TABLE corpus_stats
  ALTER COLUMN stat SET STORAGE PLAIN,
  ALTER COLUMN lang SET STORAGE PLAIN,
  ALTER COLUMN data SET STORAGE EXTERNAL;

-- Per-document statistics.
-- As above, the 'data' column will always be a compressed JSON blob.
-- Current values of 'stat' are:
--   tfidf - Term frequency-inverse document frequency
-- Note 'data' is allowed to be NULL to facilitate concurrent population
-- of this table.
CREATE TABLE page_text_stats (
  stat            TEXT    NOT NULL CHECK (stat <> ''),
  text_id         INTEGER NOT NULL REFERENCES page_text(id),
  data            BYTEA,
  PRIMARY KEY(stat, text_id)
);
ALTER TABLE page_text_stats
  ALTER COLUMN stat SET STORAGE PLAIN,
  ALTER COLUMN data SET STORAGE EXTERNAL;

-- so we can efficiently look up all stats for a page
CREATE INDEX page_text_stats_tid_idx ON page_text_stats(text_id);

COMMIT;
