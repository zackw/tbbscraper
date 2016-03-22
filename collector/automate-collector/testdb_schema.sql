-- -*- sql-product: postgres -*-

BEGIN;

CREATE SCHEMA automateCollection;
SET search_path TO automateCollection, public;

CREATE TABLE country_codes (
    name      TEXT NOT NULL UNIQUE CHECK (name <> ''),
    cc3       TEXT NOT NULL UNIQUE CHECK (cc3  <> ''),
    cc2       TEXT NOT NULL UNIQUE CHECK (cc2  <> ''),
    rwb_score REAL,
    rwb_rank  INTEGER,
    fh_score  INTEGER
);

-- The URLs, their sources, and the metadata for them.
CREATE TABLE url_strings (
    id    SERIAL  NOT NULL PRIMARY KEY,
    url   TEXT    NOT NULL UNIQUE CHECK (url <> '')
);

-- Results of page captures
CREATE TABLE capture_coarse_result (
  id     SERIAL NOT NULL PRIMARY KEY,
  result TEXT   NOT NULL UNIQUE CHECK (result <> '')
);

CREATE TABLE capture_fine_result (
  id     SERIAL  NOT NULL PRIMARY KEY,
  result INTEGER NOT NULL REFERENCES capture_coarse_result(id),
  detail TEXT    NOT NULL UNIQUE CHECK (detail <> '')
);
CREATE INDEX capture_fine_result_result_idx ON capture_fine_result(result);

CREATE TABLE capture_html_content (
  id                    SERIAL   NOT NULL PRIMARY KEY,
  hash                  BYTEA    NOT NULL UNIQUE,
  content               BYTEA    NOT NULL,
  extracted             INTEGER,
  is_parked             BOOLEAN,
  parking_rules_matched TEXT[]
);
ALTER TABLE capture_html_content
  ALTER COLUMN hash    SET STORAGE PLAIN,    -- incompressible (SHA256)
  ALTER COLUMN content SET STORAGE EXTERNAL; -- compressed by application

-- Old, nonstandard-format capture logs
CREATE TABLE capture_logs_old (
  id      SERIAL   NOT NULL PRIMARY KEY,
  hash    BYTEA    NOT NULL UNIQUE,
  log     BYTEA    NOT NULL
);
ALTER TABLE capture_logs_old
  ALTER COLUMN hash SET STORAGE PLAIN,    -- incompressible (SHA256)
  ALTER COLUMN log  SET STORAGE EXTERNAL; -- compressed by application


-- New, HAR-format capture logs
CREATE TABLE capture_logs (
      id      SERIAL   NOT NULL PRIMARY KEY,
      hash    BYTEA    NOT NULL UNIQUE,
      log     BYTEA    NOT NULL
);
ALTER TABLE capture_logs
  ALTER COLUMN hash SET STORAGE PLAIN,    -- incompressible (SHA256)
  ALTER COLUMN log  SET STORAGE EXTERNAL; -- compressed by application

-- Main capture table.
-- The "id" column is meaningless, and exists almost entirely
-- to facilitate shuffling data over from the collection host to the
-- analysis host.
CREATE TABLE captured_pages (
  id              SERIAL  NOT NULL PRIMARY KEY,
  url             INTEGER NOT NULL REFERENCES url_strings(id),
  country         TEXT    NOT NULL REFERENCES country_codes(cc2),
  vantage         TEXT    NOT NULL DEFAULT(''),
  access_time     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  elapsed_time    REAL,
  result          INTEGER NOT NULL REFERENCES capture_fine_result(id),
  redir_url       INTEGER NOT NULL REFERENCES url_strings(id),
  capture_log     INTEGER          REFERENCES capture_logs(id),
  capture_log_old INTEGER          REFERENCES capture_logs_old(id),
  html_content    INTEGER NOT NULL REFERENCES capture_html_content(id),
  UNIQUE (url, country, vantage, access_time)
);
CREATE INDEX captured_pages_url_idx ON captured_pages(url);
CREATE INDEX captured_pages_url_country_idx ON captured_pages(url, country);
CREATE INDEX captured_pages_url_result_idx ON captured_pages(url, result);

COMMIT;
