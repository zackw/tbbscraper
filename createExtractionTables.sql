-- -*- sql-product: postgres -*-
--
-- Create tables for basic information extracted from the raw page
-- captures.  This includes, for instance, text content, outbound
-- links, and language assessment.  Some metadata from the
-- captured_pages table is also replicated, for convenience.
--
-- This url_strings table is not to be confused with the url_strings
-- table in any ts_run_<N> schema; it holds ONLY outbound links.

CREATE TABLE url_strings (
    id    SERIAL  NOT NULL PRIMARY KEY,
    url   TEXT    NOT NULL UNIQUE CHECK (url <> '')
);

-- Many pages contain exactly the same text content, even if their
-- HTML was not the same.  Therefore, text content is stored in an
-- ancillary table so it can be deduplicated.
CREATE TABLE page_text_content (
    -- foreign key for joins
    id       SERIAL  NOT NULL PRIMARY KEY,

    -- SHA256 (raw) hash of uncompressed text, for deduplication
    hash     BYTEA   NOT NULL UNIQUE CHECK (hash <> ''),

    -- Actual contents, zlib-compressed.
    contents BYTEA   NOT NULL
);
-- Do not waste effort trying to recompress 'contents' in the storage
-- layer, since it is already compressed.  Similarly for 'hash', which
-- is incompressible and should be inline anyway.
ALTER TABLE page_text_content
   ALTER COLUMN hash     SET STORAGE PLAIN,
   ALTER COLUMN contents SET STORAGE EXTERNAL;

-- Many pages also contain exactly the same set of outbound links.
-- (Even more pages contain *nearly* the same set of outbound links,
-- but taking that into account would substantially complicate usage.)
-- Each element of the urls array is an entry in url_strings; postgres
-- does not currently support declaring that as a foreign-key
-- constraint.  The database relies on the application always to
-- generate url vectors with no duplicates, sorted in ascending order
-- by code number.
CREATE TABLE page_link_sets (
   -- foreign key for joins
   id   SERIAL    NOT NULL PRIMARY KEY,
   urls INTEGER[] NOT NULL
);
-- The 'urls' field can easily become too long to be the target of a
-- regular UNIQUE CONSTRAINT, so we do this instead.
-- MD5 is the only strong hash built into postgres.
CREATE UNIQUE INDEX page_link_clusters_urls_idx
    ON page_link_clusters (md5(array_to_string(urls,',')));

-- This is the main table.  Pay attention to the 'run' field.
-- The 'url' and 'redir_url' fields index "url_strings", and
-- (locale, url) indexes "captured_pages", IN THE SCHEMA
-- CORRESPONDING TO THE RUN NUMBER.
--
-- Yes, this is awkward (and prevents us applying a foreign-key
-- constraint to the url field), but the collection process can't
-- easily be changed now.  pagedb.py will paper over this wrinkle
-- as long as you don't need to look at anything *besides* the text
-- of the URL(s).
CREATE TABLE page_extracted_content (
   run         INTEGER NOT NULL CHECK (run >= 1),
   locale      TEXT    NOT NULL CHECK (locale <> ''),
   url         INTEGER NOT NULL CHECK (url >= 1),

   -- metadata denormalized from captured_pages
   access_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
   result      ts_run_1.capture_result NOT NULL,
   redir_url   INTEGER NOT NULL CHECK (redir_url >= 1),

   -- extracted data; add more columns as necessary
   text_content INTEGER NOT NULL REFERENCES page_text_content(id),
   links        INTEGER NOT NULL REFERENCES page_link_sets(id),
   resources    INTEGER NOT NULL REFERENCES page_link_sets(id),

   lang_code    TEXT,
   lang_conf    REAL,

   -- The ordering on this index is chosen to facilitate lookups and joins.
   UNIQUE (locale, url, run)
);
-- These fields are guaranteed to be two or three characters at most.
ALTER TABLE page_extracted_content
      ALTER COLUMN locale SET STORAGE PLAIN,
      ALTER COLUMN lang_code SET STORAGE PLAIN;
