-- -*- sql-product: postgres -*-

CREATE SCHEMA analysis;
GRANT USAGE ON SCHEMA analysis TO PUBLIC;
SET search_path TO analysis, public;
BEGIN;

CREATE TABLE capture_pruned_content (
  id          SERIAL   NOT NULL PRIMARY KEY,
  origin      INTEGER  NOT NULL REFERENCES collection.capture_html_content(id),
  content_len INTEGER  NOT NULL CHECK (content_len >= 0),
  lang_code   TEXT     NOT NULL,
  lang_conf   REAL     NOT NULL,
  hash        BYTEA    NOT NULL UNIQUE,
  content     BYTEA    NOT NULL,
  links       BYTEA    NOT NULL,
  resources   BYTEA    NOT NULL,
  headings    BYTEA    NOT NULL,
  dom_stats   BYTEA    NOT NULL
);
ALTER TABLE capture_pruned_content
  ALTER COLUMN hash      SET STORAGE PLAIN,    -- incompressible (SHA256)
  ALTER COLUMN content   SET STORAGE EXTERNAL, -- compressed by application
  ALTER COLUMN links     SET STORAGE EXTERNAL, -- compressed by application
  ALTER COLUMN resources SET STORAGE EXTERNAL, -- compressed by application
  ALTER COLUMN headings  SET STORAGE EXTERNAL, -- compressed by application
  ALTER COLUMN dom_stats SET STORAGE EXTERNAL; -- compressed by application

CREATE INDEX captured_pruned_content_origin_idx ON capture_pruned_content(origin);

CREATE VIEW page_text AS
    SELECT p.id        AS id,
           h.id        AS origin,
           p.lang_code AS lang_code,
           l.name      AS lang_name,
           p.lang_conf AS lang_conf,
           h.hash      AS raw_hash,
           h.content   AS raw_contents,
           p.hash      AS hash,
           p.content   AS contents,
           p.links     AS links,
           p.resources AS resources,
           p.headings  AS headings,
           p.dom_stats AS dom_stats
      FROM analysis.capture_pruned_content p
      JOIN collection.capture_html_content h ON p.origin = h.id
 LEFT JOIN collection.language_codes l ON (p.lang_code = l.lc2 OR
                                           p.lang_code = l.lc3)
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
