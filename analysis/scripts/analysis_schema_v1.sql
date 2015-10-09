-- -*- sql-product: postgres -*-

--
-- PostgreSQL doesn't have a "median" aggregate as stock.
-- This is inefficient but the best one can do without a compiled-code
-- extension.
--

CREATE FUNCTION _final_median(numeric[]) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    AS $_$
   SELECT AVG(val)
   FROM (
     SELECT val
     FROM unnest($1) val
     ORDER BY 1
     LIMIT  2 - MOD(array_upper($1, 1), 2)
     OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
   ) sub;
$_$;
CREATE AGGREGATE median(numeric) (
    SFUNC = array_append,
    STYPE = numeric[],
    INITCOND = '{}',
    FINALFUNC = _final_median
);

-- Ancillary: metadata about ISO language and country codes
CREATE TABLE language_codes (
    code text NOT NULL UNIQUE,
    name text NOT NULL UNIQUE
);

CREATE TABLE locale_data (
    name      TEXT NOT NULL UNIQUE,
    cc3       TEXT NOT NULL UNIQUE,
    cc2       TEXT NOT NULL UNIQUE,
    rwb_score REAL,
    rwb_rank  INTEGER,
    fh_score  INTEGER
);

-- Capture ancillary: URL strings
CREATE TABLE url_strings (
    id   SERIAL  NOT NULL PRIMARY KEY,
    url  TEXT    NOT NULL UNIQUE CHECK (url <> ''),
    r1id INTEGER,
    r2id INTEGER,
    r3id INTEGER,
);

-- Capture ancillary: "detail" strings (like "200 OK")
CREATE TABLE capture_detail (
    id       SERIAL  NOT NULL PRIMARY KEY,
    detail   TEXT    NOT NULL UNIQUE CHECK (detail <> '')
);

-- Capture derived/ancillary: text of pages.
-- Space optimizations: 'contents' is compressed by the application,
-- 'lang_code' is known to be short.
CREATE TABLE page_text (
    id                SERIAL  NOT NULL PRIMARY KEY,
    has_boilerplate   BOOLEAN NOT NULL,
    lang_code         TEXT    NOT NULL,
    lang_conf         REAL    NOT NULL,
    contents          BYTEA   NOT NULL
);
ALTER TABLE page_text
  ALTER COLUMN lang_code SET STORAGE PLAIN,
  ALTER COLUMN contents SET STORAGE EXTERNAL;

-- Capture derived: all detected languages.
CREATE MATERIALIZED VIEW captured_languages AS
 SELECT DISTINCT page_text.lang_code AS code
   FROM page_text
  WITH NO DATA;

-- Capture derived: result of postprocessing on the data collected.
CREATE TABLE page_observations (
    document         INTEGER NOT NULL REFERENCES page_text(id),
    url              INTEGER NOT NULL REFERENCES url_strings(id),
    locale           TEXT    NOT NULL CHECK (locale <> ''),
    run              INTEGER NOT NULL CHECK (run >= 1),
    sources          TEXT    NOT NULL CHECK (sources <> ''),
    document_with_bp INTEGER NOT NULL REFERENCES page_text(id),
    links            BYTEA   NOT NULL,
    resources        BYTEA   NOT NULL,
    headings         BYTEA   NOT NULL,
    dom_stats        BYTEA   NOT NULL,
    access_time      TIMESTAMP WITHOUT TIME ZONE   NOT NULL,
    result           ts_run_1.capture_result NOT NULL,
    detail           INTEGER REFERENCES capture_detail(id),
    redir_url        INTEGER REFERENCES url_strings(id),
    html_length      INTEGER NOT NULL,
    html_sha2        BYTEA NOT NULL,
    UNIQUE (run, locale, url)
);
-- Space optimization: locale and sources are short, html_sha2 is
-- incompressible, links/resources/headings/dom_stats are compressed by
-- the application.
ALTER TABLE page_observations
  ALTER COLUMN locale    SET STORAGE PLAIN,
  ALTER COLUMN sources   SET STORAGE PLAIN,
  ALTER COLUMN links     SET STORAGE EXTERNAL,
  ALTER COLUMN resources SET STORAGE EXTERNAL,
  ALTER COLUMN headings  SET STORAGE EXTERNAL,
  ALTER COLUMN dom_stats SET STORAGE EXTERNAL,
  ALTER COLUMN html_sha2 SET STORAGE EXTERNAL;

CREATE INDEX page_observations_document_idx ON page_observations USING btree (document);
CREATE INDEX page_observations_document_with_bp_idx ON page_observations USING btree (document_with_bp);
CREATE INDEX page_observations_url_idx ON page_observations USING btree (url);
CREATE INDEX page_text_contents_idx ON page_text USING btree (md5(contents));
CREATE INDEX page_text_lang_idx ON page_text USING btree (lang_code);


-- All locales collected-from
CREATE MATERIALIZED VIEW captured_locales AS
 SELECT DISTINCT page_observations.locale
   FROM page_observations
  WITH NO DATA;


-- Various derived data tables
CREATE TABLE corpus_stats (
    stat            TEXT    NOT NULL CHECK (stat <> ''),
    lang            TEXT    NOT NULL CHECK (lang <> ''),
    has_boilerplate BOOLEAN NOT NULL,
    n_documents     INTEGER NOT NULL CHECK (n_documents >= 1),
    data            BYTEA   NOT NULL,
    PRIMARY KEY (stat, lang, has_boilerplate)
);
ALTER TABLE corpus_stats
  ALTER COLUMN stat SET STORAGE PLAIN,
  ALTER COLUMN lang SET STORAGE PLAIN,
  ALTER COLUMN data SET STORAGE EXTERNAL;


CREATE TABLE features_old (
    locale text NOT NULL,
    url integer NOT NULL,
    tfidf double precision[],
    tfidf_row double precision[],
    tfidf_column double precision[],
    usercontent text[],
    tags text[],
    code text,
    detail text,
    isredir integer,
    redirdomain text,
    html_length integer,
    content_length integer,
    dom_depth integer,
    number_of_tags integer,
    unique_tags integer,
    PRIMARY KEY (locale, url)
);

CREATE TABLE features_test (
    locale text NOT NULL,
    url integer NOT NULL,
    tfidf double precision[],
    tfidf_row double precision[],
    tfidf_column double precision[],
    usercontent text[],
    tags text[],
    code text,
    detail text,
    isredir integer,
    redirdomain text,
    html_length integer,
    content_length integer,
    dom_depth integer,
    number_of_tags integer,
    unique_tags integer,
    PRIMARY KEY (locale, url)
);

CREATE TABLE features_test_old (
    locale text NOT NULL,
    url integer NOT NULL,
    tfidf double precision[],
    tfidf_row double precision[],
    tfidf_column double precision[],
    usercontent text[],
    tags text[],
    code text,
    detail text,
    isredir integer,
    redirdomain text,
    html_length integer,
    content_length integer,
    dom_depth integer,
    number_of_tags integer,
    unique_tags integer,
    PRIMARY KEY (locale, url)
);

CREATE TABLE lang_assignment (
    document  integer NOT NULL PRIMARY KEY,
    lang_code text,
    contents  text
);

CREATE TABLE page_text_stats (
    stat    TEXT    NOT NULL CHECK (stat <> ''),
    text_id INTEGER NOT NULL REFERENCES page_text(id),
    data    BYTEA,
    PRIMARY KEY (stat, text_id)
);
ALTER TABLE page_text_stats
  ALTER COLUMN stat SET STORAGE PLAIN,
  ALTER COLUMN data SET STORAGE EXTERNAL;

CREATE INDEX page_text_stats_tid_idx ON page_text_stats USING btree (text_id);
