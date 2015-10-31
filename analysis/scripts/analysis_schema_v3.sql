-- -*- sql-product: postgres -*-

CREATE SCHEMA n_analysis;
GRANT USAGE ON SCHEMA n_analysis TO PUBLIC;
SET search_path TO n_analysis, public;
BEGIN;

-- These tables hold all of the data that ExtractedContent pulls out of
-- a raw HTML page.  They're split up like this because there can be
-- lots of duplicates.

-- 'segmented' is non-null only for pruned texts; it holds the result
-- of applying language and then word segmentation to 'plaintext'.
-- It's an array of pairs, {"l": "xx", "t": [...]} where 'l' is a
-- language code and 't' is word-segmented text believed to be in
-- language 'l'.
CREATE TABLE extracted_plaintext (
  id          SERIAL   NOT NULL PRIMARY KEY,
  hash        BYTEA    NOT NULL UNIQUE,
  plaintext   TEXT     NOT NULL,
  segmented   JSONB
);
ALTER TABLE extracted_plaintext
  ALTER COLUMN hash      SET STORAGE PLAIN;    -- incompressible (SHA256)

CREATE TABLE extracted_urls (
  id          SERIAL   NOT NULL PRIMARY KEY,
  hash        BYTEA    NOT NULL UNIQUE,
  urls        JSONB    NOT NULL
);
ALTER TABLE extracted_urls
  ALTER COLUMN hash      SET STORAGE PLAIN;    -- incompressible (SHA256)

CREATE TABLE extracted_headings (
  id          SERIAL   NOT NULL PRIMARY KEY,
  hash        BYTEA    NOT NULL UNIQUE,
  headings    JSONB    NOT NULL
);
ALTER TABLE extracted_headings
  ALTER COLUMN hash      SET STORAGE PLAIN;    -- incompressible (SHA256)

CREATE TABLE extracted_dom_stats (
  id          SERIAL   NOT NULL PRIMARY KEY,
  hash        BYTEA    NOT NULL UNIQUE,
  dom_stats   JSONB    NOT NULL
);
ALTER TABLE extracted_dom_stats
  ALTER COLUMN hash      SET STORAGE PLAIN;    -- incompressible (SHA256)

CREATE TABLE extracted_content_ov (
  id          SERIAL   NOT NULL PRIMARY KEY,
  content_len INTEGER  NOT NULL CHECK (content_len >= 0),
  raw_text    INTEGER  NOT NULL REFERENCES extracted_plaintext(id),
  pruned_text INTEGER  NOT NULL REFERENCES extracted_plaintext(id),
  links       INTEGER  NOT NULL REFERENCES extracted_urls(id),
  resources   INTEGER  NOT NULL REFERENCES extracted_urls(id),
  headings    INTEGER  NOT NULL REFERENCES extracted_headings(id),
  dom_stats   INTEGER  NOT NULL REFERENCES extracted_dom_stats(id)
);
CREATE VIEW extracted_content AS
SELECT ov.id          AS id,
       ov.content_len AS content_len,
       er.plaintext   AS raw_text,
       ep.plaintext   AS pruned_text,
       ep.segmented   AS segmented_text,
       el.urls        AS links,
       es.urls        AS resources,
       eh.headings    AS headings,
       ed.dom_stats   AS dom_stats
  FROM extracted_content_ov ov,
       extracted_plaintext  er,
       extracted_plaintext  ep,
       extracted_urls       el,
       extracted_urls       es,
       extracted_headings   eh,
       extracted_dom_stats  ed
 WHERE ov.raw_text    = er.id
   AND ov.pruned_text = ep.id
   AND ov.links       = el.id
   AND ov.resources   = es.id
   AND ov.headings    = eh.id
   AND ov.dom_stats   = ed.id
;

CREATE VIEW page_text AS
SELECT e.id      AS extracted_id,
       h.id      AS document_id,
       e.content_len AS html_content_len,
       h.hash    AS html_content_hash,
       h.content AS html_content_compressed,
       e.raw_text, e.pruned_text, e.segmented_text,
       e.links, e.resources, e.headings, e.dom_stats
  FROM extracted_content e,
       collection.capture_html_content h
 WHERE e.id = h.extracted
;

CREATE VIEW page_observations AS
  SELECT cp.id           AS id,
         cp.run          AS run,
          u.url          AS orig_url,
          v.url          AS redir_url,
         cp.country      AS country,
         cc.name         AS country_name,
         cp.vantage      AS vantage,
         cp.access_time  AS access_time,
         cp.elapsed_time AS elapsed_time,
          r.result       AS result,
          r.detail       AS detail,
         cl.log          AS capture_log,
         cl.hash         AS capture_log_hash,
         co.log          AS capture_log_old,
         co.hash         AS capture_log_old_hash,
         cp.html_content AS document_id
       FROM collection.captured_pages       cp
  LEFT JOIN collection.url_strings          u  ON cp.url             = u.id
  LEFT JOIN collection.url_strings          v  ON cp.redir_url       = v.id
  LEFT JOIN collection.capture_result       r  ON cp.result          = r.id
  LEFT JOIN collection.capture_logs         cl ON cp.capture_log     = cl.id
  LEFT JOIN collection.capture_logs_old     co ON cp.capture_log_old = co.id
  LEFT JOIN collection.country_codes        cc ON cp.country         = cc.cc2
;

CREATE TABLE pruned_content_stats (
    stat    TEXT    NOT NULL CHECK (stat <> ''),
    text_id INTEGER NOT NULL,
    runs    INTEGER[] NOT NULL,
    data    BYTEA,
    PRIMARY KEY (stat, text_id, runs)
);
ALTER TABLE pruned_content_stats
  ALTER COLUMN stat SET STORAGE PLAIN,
  ALTER COLUMN data SET STORAGE EXTERNAL;

CREATE TABLE corpus_stats (
    stat            TEXT    NOT NULL CHECK (stat <> ''),
    lang            TEXT    NOT NULL CHECK (lang <> ''),
    runs            INTEGER[] NOT NULL,
    n_documents     INTEGER NOT NULL CHECK (n_documents >= 1),
    data            BYTEA   NOT NULL,
    PRIMARY KEY (stat, lang, runs)
);
ALTER TABLE corpus_stats
  ALTER COLUMN stat SET STORAGE PLAIN,
  ALTER COLUMN lang SET STORAGE PLAIN,
  ALTER COLUMN data SET STORAGE EXTERNAL;

COMMIT;
