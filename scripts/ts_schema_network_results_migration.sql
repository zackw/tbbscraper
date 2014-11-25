-- -*- sql-product: postgres -*-
-- Migration to more detailed network-layer results in capture_result.

-- Infuriatingly, the only way to rename an enum value is to replace the
-- entire type.  And you can't do type manipulations inside a transaction.
ALTER TYPE capture_result RENAME TO old_capture_result;

-- Make sure this stays in sync with ts_schema.sql.
CREATE TYPE capture_result AS ENUM (
    'ok',
    'ok (redirected)',
    'redirection loop',
    'bad request (400)',
    'authentication required (401)',
    'forbidden (403)',
    'page not found (404/410)',
    'server error (500)',
    'service unavailable (503)',
    'proxy error (502/504/52x)',
    'other HTTP response',
    'invalid URL',
    'host not found',
    'server unreachable',
    'connection refused',
    'TLS handshake failed',
    'connection interrupted',
    'other network error',
    'timeout',
    'proxy failure',
    'crawler failure'
);

BEGIN;

ALTER TABLE captured_pages RENAME COLUMN result TO old_result;
ALTER TABLE captured_pages    ADD COLUMN result capture_result;

UPDATE captured_pages SET result = CAST ((CASE old_result
  WHEN 'ok'                            THEN 'ok'
  WHEN 'ok (redirected)'               THEN 'ok (redirected)'
  WHEN 'redirection loop'              THEN 'redirection loop'
  WHEN 'bad request (400)'             THEN 'bad request (400)'
  WHEN 'authentication required (401)' THEN 'authentication required (401)'
  WHEN 'forbidden (403)'               THEN 'forbidden (403)'
  WHEN 'page not found (404/410)'      THEN 'page not found (404/410)'
  WHEN 'proxy error (502/504/52x)'     THEN 'proxy error (502/504/52x)'
  WHEN 'server error (500)'            THEN 'server error (500)'
  WHEN 'service unavailable (503)'     THEN 'service unavailable (503)'
  WHEN 'other HTTP response'           THEN 'other HTTP response'
  WHEN 'network or protocol error'     THEN 'other network error'
  WHEN 'timeout'                       THEN 'timeout'
  WHEN 'crawler failure'               THEN 'crawler failure'
  WHEN 'hostname not found'            THEN 'host not found'
  WHEN 'invalid URL'                   THEN 'invalid URL'
  END) AS capture_result);

ALTER TABLE captured_pages DROP COLUMN old_result CASCADE; -- to the index
CREATE INDEX captured_pages_result_idx ON captured_pages(result);

COMMIT;
DROP TYPE old_capture_result;

-- Result reclassification.
BEGIN;

-- Older crawler left NULL in detail when result was 'timeout'.
-- This turns out to be a great way to shoot yourself in the foot on queries.
INSERT INTO capture_detail VALUES (DEFAULT, 'Client timeout');
UPDATE captured_pages
   SET detail = (SELECT id FROM capture_detail WHERE detail = 'Client timeout')
 WHERE result = 'timeout' AND detail IS NULL;

-- "N202 Cannot open file:///..." was misclassified as a network error.
-- (See also https://github.com/ariya/phantomjs/issues/12752 .)
-- This can't reasonably be done inside a CASE.
UPDATE captured_pages c
   SET result = 'invalid URL'
  FROM capture_detail d
WHERE c.detail = d.id and d.detail LIKE '%file:///%';

UPDATE captured_pages c
  SET result = CAST ((CASE d.detail
   WHEN 'N1 Connection refused'                   THEN 'connection refused'
   WHEN 'N2 Connection closed'                    THEN 'connection interrupted'
   WHEN 'N4 Socket operation timed out'           THEN 'timeout'
   WHEN 'N5 Operation canceled'                   THEN 'connection interrupted'
   WHEN 'N6 SSL handshake failed'                 THEN 'TLS handshake failed'
   WHEN 'N99 Connection to proxy refused'             THEN 'proxy failure'
   WHEN 'N99 Host unreachable'                        THEN 'server unreachable'
   WHEN 'N99 Network unreachable'                     THEN 'server unreachable'
   WHEN 'N99 Unknown error'                           THEN 'other network error'
   WHEN 'N102 Connection to proxy closed prematurely' THEN 'proxy failure'
   WHEN 'N205 Unknown error'                          THEN 'other network error'
  END) AS capture_result)
  FROM capture_detail d
 WHERE c.result = 'other network error'
   AND c.detail = d.id;

COMMIT;
VACUUM (ANALYZE);
