---- -*- sql-product: postgres -*-

---- Note: the data in ts_run_0 is not carried over because it is
---- generally inconsistent with the data in the other three runs.

SET search_path TO collection, public;
-- BEGIN;

-- INSERT INTO url_strings (url)
--       SELECT DISTINCT url FROM ts_analysis.url_strings
-- UNION SELECT DISTINCT url FROM ts_run_1.url_strings
-- UNION SELECT DISTINCT url FROM ts_run_2.url_strings
-- UNION SELECT DISTINCT url FROM ts_run_3.url_strings;

CREATE TEMP TABLE url_remap (
  nid  INTEGER,
  r1id INTEGER,
  r2id INTEGER,
  r3id INTEGER,
  rAid INTEGER
);
INSERT INTO url_remap (nid, r1id, r2id, r3id, rAid)
  SELECT un.id AS nid, u1.id AS r1id, u2.id AS r2id,
         u3.id AS r3id, uA.id AS rAid
    FROM url_strings un
  LEFT JOIN ts_run_1.url_strings u1    ON un.url = u1.url
  LEFT JOIN ts_run_2.url_strings u2    ON un.url = u2.url
  LEFT JOIN ts_run_3.url_strings u3    ON un.url = u3.url
  LEFT JOIN ts_analysis.url_strings uA ON un.url = uA.url;
CREATE INDEX url_remap_r1 ON url_remap(r1id);
CREATE INDEX url_remap_r2 ON url_remap(r2id);
CREATE INDEX url_remap_r3 ON url_remap(r3id);
CREATE INDEX url_remap_ra ON url_remap(raid);
CREATE INDEX url_remap_rn ON url_remap(nid);

-- ---- Source metadata was formerly only partially stored in the database.
-- ---- Also, source labeling was formerly somewhat inconsistent.

-- INSERT INTO url_sources (name, last_updated, meta)
--    SELECT
--      CASE label
--        WHEN 'Germany 2014 (BPjM hashbusting)' THEN 'Germany 2014 (#BPjMleak)'
--        WHEN 'India (2012; Assam riots)'       THEN 'India 2012 (Assam riots)'
--        WHEN 'India (Anonymous, 2012)'         THEN 'India 2012 (Anonymous)'
--        WHEN 'Italy (2009; Wikileaks)'         THEN 'Italy 2009 (Wikileaks)'
--        WHEN 'Norway (2009; Wikileaks)'        THEN 'Norway 2009 (Wikileaks)'
--        WHEN 'Russia (2014; rublacklist.net)'  THEN 'Russia 2014 (rublacklist.net)'
--        WHEN 'Thailand (2007; Wikileaks)'      THEN 'Thailand 2007 (Wikileaks)'
--        ELSE label END
--      AS name,
--      last_update AS last_updated,
--      json_build_object('url', url)::jsonb AS meta
--    FROM (SELECT DISTINCT label, url, last_update FROM (
--            SELECT label, url, last_update FROM ts_run_1.static_list_metadata
--  UNION ALL SELECT label, url, last_update FROM ts_run_2.static_list_metadata
--  UNION ALL SELECT label, url, last_update FROM ts_run_3.static_list_metadata
--        ) _
--     ) __ ORDER BY name;

-- INSERT INTO url_sources (name, last_updated, meta) VALUES
--   ('Alexa 2014',                 '2014-10-24', '{"url":"http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"}'),
--   ('CitizenLab 2014',            '2014-08-31', '{"url":"https://github.com/citizenlab/test-lists"}'),
--   ('Herdict 2014',               '2014-08-31', '{"url":"http://herdict.org/explore/data/view"}'),
--   ('Pinboard 2014',              '2014-09-05', '{"url":"http://pinboard.in/"}'),
--   ('Tweets 2014',                '2014-03-23', '{"url":"https://dev.twitter.com/streaming/reference/get/statuses/sample"}'),
--   ('Twitter user profiles 2014', '2014-03-23', '{"url":"https://dev.twitter.com/streaming/reference/get/statuses/sample"}');

-- ---- It is known that the urls_* tables in ts_run_{1,2,3} are identical.

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object('rank', u.rank)::jsonb AS meta
--     FROM ts_run_1.urls_alexa u,
--          url_sources s,
--          url_remap r
--    WHERE u.retrieval_date = '2014-10-24'
--      AND u.url = r.r1id
--      AND s.name = 'Alexa 2014';

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object('country', LOWER(u.country),
--                            'category', u.category)::jsonb AS meta
--     FROM ts_run_1.urls_citizenlab u,
--          url_sources s,
--          url_remap r
--    WHERE u.retrieval_date = '2014-08-31'
--      AND u.url = r.r1id
--      AND s.name = 'CitizenLab 2014';

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object(
--              'country',    LOWER(u.country),
--              'timestamp',  to_timestamp(u.timestamp) AT TIME ZONE 'utc',
--              'accessible', u.accessible
--          )::jsonb AS meta
--     FROM ts_run_1.urls_herdict u,
--          url_sources s,
--          url_remap r
--    WHERE u.url = r.r1id
--      AND s.name = 'Herdict 2014';

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object(
--              'username',   u.username,
--              'timestamp',  u.access_time,
--              'title',      u.title,
--              'annotation', u.annotation,
--              'tags',       u.tags
--          )::jsonb AS meta
--     FROM ts_run_1.urls_pinboard u,
--          url_sources s,
--          url_remap r
--    WHERE u.url = r.r1id
--      AND s.name = 'Pinboard 2014';

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object(
--              'username',   t.screen_name,
--              'userloc',    t.location,
--              'timestamp',  to_timestamp(u.timestamp) AT TIME ZONE 'utc',
--              'lang',       u.lang,
--              'withheld',   u.withheld,
--              'psensitive', u.possibly_sensitive,
--              'tags',       u.hashtags
--          )::jsonb AS meta
--     FROM ts_run_1.urls_tweeted u,
--          url_sources s,
--          url_remap r,
--          ts_run_1.twitter_users t
--    WHERE u.url = r.r1id
--      AND u.uid = t.uid
--      AND s.name = 'Tweets 2014';

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object(
--              'username',   t.screen_name,
--              'userfname',  t.full_name,
--              'userloc',    t.location,
--              'created',    to_timestamp(t.created_at) AT TIME ZONE 'utc',
--              'verified',   t.verified,
--              'protected',  t.protected,
--              'lang',       t.lang,
--              'description',t.description
--          )::jsonb AS meta
--     FROM ts_run_1.urls_twitter_user_profiles u,
--          url_sources s,
--          url_remap r,
--          ts_run_1.twitter_users t
--    WHERE u.url = r.r1id
--      AND u.uid = t.uid
--      AND s.name = 'Twitter user profiles 2014';

-- INSERT INTO urls (url, src, meta)
--   SELECT r.nid AS url,
--          s.id  AS src,
--          json_build_object('country', c.cc2)::jsonb AS meta
--     FROM ts_run_1.urls_staticlist u,
--          ts_run_1.static_list_metadata m,
--          url_sources s,
--          url_remap r,
--          country_codes c
--    WHERE u.url = r.r1id
--      AND u.listid = m.id
--      AND s.name = (CASE m.label
--        WHEN 'Germany 2014 (BPjM hashbusting)' THEN 'Germany 2014 (#BPjMleak)'
--        WHEN 'India (2012; Assam riots)'       THEN 'India 2012 (Assam riots)'
--        WHEN 'India (Anonymous, 2012)'         THEN 'India 2012 (Anonymous)'
--        WHEN 'Italy (2009; Wikileaks)'         THEN 'Italy 2009 (Wikileaks)'
--        WHEN 'Norway (2009; Wikileaks)'        THEN 'Norway 2009 (Wikileaks)'
--        WHEN 'Russia (2014; rublacklist.net)'  THEN 'Russia 2014 (rublacklist.net)'
--        WHEN 'Thailand (2007; Wikileaks)'      THEN 'Thailand 2007 (Wikileaks)'
--        ELSE m.label END)
--      AND SUBSTRING(s.name FROM '([^ ]+) ') = c.name;

-- COMMIT;

-- BEGIN;

-- INSERT INTO capture_coarse_result (result)
--   SELECT enumlabel AS result FROM (
--     SELECT DISTINCT enumsortorder, enumlabel FROM pg_enum
--            ORDER BY enumsortorder
--   ) _ WHERE enumlabel <> 'ok (redirected)';

-- ---- There are some cases in the oldest data where 'detail' is
-- ---- inconsistent with 'result', which cannot be represented in the
-- ---- new schema.  Resolve such cases by discarding the old (result, detail)
-- ---- pair.

-- INSERT INTO capture_fine_result (result, detail)
--   SELECT DISTINCT c.id, d.detail
--     FROM ts_analysis.capture_detail d
--     JOIN ts_analysis.page_observations p ON d.id = p.detail
--     JOIN capture_coarse_result c ON
--       (CASE WHEN p.result = 'ok (redirected)' THEN 'ok'
--             ELSE CAST(p.result AS TEXT) END) = c.result;

-- INSERT INTO capture_fine_result (result, detail)
--   SELECT DISTINCT c.id, m.detail FROM (
--     SELECT CAST(p.result AS TEXT), d.detail
--       FROM ts_run_1.capture_detail d
--       JOIN ts_run_1.captured_pages p ON d.id = p.detail
--     UNION ALL
--     SELECT CAST(p.result AS TEXT), d.detail
--       FROM ts_run_2.capture_detail d
--       JOIN ts_run_2.captured_pages p ON d.id = p.detail
--     UNION ALL
--     SELECT DISTINCT CAST(p.result AS TEXT), d.detail
--       FROM ts_run_3.capture_detail d
--       JOIN ts_run_3.captured_pages p ON d.id = p.detail
--   ) m
--   JOIN capture_coarse_result c ON
--     (CASE WHEN m.result = 'ok (redirected)' THEN 'ok' ELSE m.result END)
--     = c.result
--   WHERE m.detail NOT IN (SELECT detail FROM capture_fine_result);

CREATE TEMP TABLE detail_remap (
  nid  INTEGER,
  r1id INTEGER,
  r2id INTEGER,
  r3id INTEGER,
  rAid INTEGER
);
INSERT INTO detail_remap (nid, r1id, r2id, r3id, rAid)
  SELECT un.id AS nid, u1.id AS r1id, u2.id AS r2id,
         u3.id AS r3id, uA.id AS rAid
    FROM capture_fine_result un
  LEFT JOIN ts_run_1.capture_detail u1    ON un.detail = u1.detail
  LEFT JOIN ts_run_2.capture_detail u2    ON un.detail = u2.detail
  LEFT JOIN ts_run_3.capture_detail u3    ON un.detail = u3.detail
  LEFT JOIN ts_analysis.capture_detail uA ON un.detail = uA.detail;
CREATE INDEX detail_remap_r1 ON detail_remap(r1id);
CREATE INDEX detail_remap_r2 ON detail_remap(r2id);
CREATE INDEX detail_remap_r3 ON detail_remap(r3id);
CREATE INDEX detail_remap_ra ON detail_remap(raid);
CREATE INDEX detail_remap_rn ON detail_remap(nid);

-- COMMIT;

-- BEGIN;
-- INSERT INTO capture_logs_old (hash, log)
--   SELECT DISTINCT ON (hash) digest(capture_log, 'sha256') AS hash,
--                             capture_log AS log FROM ts_run_1.captured_pages
--                             WHERE capture_log IS NOT NULL
--   UNION
--   SELECT DISTINCT ON (hash) digest(capture_log, 'sha256') AS hash,
--                             capture_log AS log FROM ts_run_2.captured_pages
--                             WHERE capture_log IS NOT NULL
--   UNION
--   SELECT DISTINCT ON (hash) digest(capture_log, 'sha256') AS hash,
--                             capture_log AS log FROM ts_run_3.captured_pages
--                             WHERE capture_log IS NOT NULL;
-- COMMIT;

-- INSERT INTO capture_html_content (hash, content)
--   SELECT DISTINCT ON (hash) digest(html_content, 'sha256') AS hash,
--                             html_content AS content FROM ts_run_1.captured_pages
--                             WHERE html_content IS NOT NULL
--   UNION
--   SELECT DISTINCT ON (hash) digest(html_content, 'sha256') AS hash,
--                             html_content AS content FROM ts_run_2.captured_pages
--                             WHERE html_content IS NOT NULL
--   UNION
--   SELECT DISTINCT ON (hash) digest(html_content, 'sha256') AS hash,
--                             html_content AS content FROM ts_run_3.captured_pages
--                             WHERE html_content IS NOT NULL;

---- INSERT IF NOT EXISTS INTO capture_html_content (hash, content)
----        VALUES (digest('', 'sha256'), '');
-- INSERT INTO capture_html_content (hash, content)
--   WITH empty(hash, content) AS (VALUES (digest('', 'sha256'), ''::BYTEA))
--   SELECT hash, content FROM empty
--   EXCEPT SELECT hash, content FROM capture_html_content
--          WHERE content = ''::BYTEA;

INSERT INTO captured_pages (url, country, vantage, access_time, result,
                            redir_url, capture_log_old, html_content)
SELECT
  u.nid AS url,
  SUBSTRING(cp.locale FOR 2) AS country,
  COALESCE(SUBSTRING(cp.locale FROM '^[a-z][a-z]_(.+)$'), '') AS vantage,
  cp.access_time AS access_time,
  d.nid AS result,
  COALESCE(v.nid, u.nid) AS redir_url,
  cl.id AS capture_log_old,
  COALESCE(ch.id, (SELECT id FROM capture_html_content
                    WHERE hash = digest('', 'sha256'))) AS html_content
  FROM ts_run_1.captured_pages cp
LEFT JOIN url_remap u ON cp.url = u.r1id
LEFT JOIN url_remap v ON cp.redir_url = v.r1id
LEFT JOIN detail_remap d ON cp.detail = d.r1id
LEFT JOIN capture_logs_old cl ON digest(cp.capture_log, 'sha256') = cl.hash
LEFT JOIN capture_html_content ch ON digest(cp.html_content, 'sha256') = ch.hash
UNION ALL
SELECT
  u.nid AS url,
  SUBSTRING(cp.locale FOR 2) AS country,
  COALESCE(SUBSTRING(cp.locale FROM '^[a-z][a-z]_(.+)$'), '') AS vantage,
  cp.access_time AS access_time,
  d.nid AS result,
  COALESCE(v.nid, u.nid) AS redir_url,
  cl.id AS capture_log_old,
  COALESCE(ch.id, (SELECT id FROM capture_html_content
                    WHERE hash = digest('', 'sha256'))) AS html_content
  FROM ts_run_2.captured_pages cp
LEFT JOIN url_remap u ON cp.url = u.r2id
LEFT JOIN url_remap v ON cp.redir_url = v.r2id
LEFT JOIN detail_remap d ON cp.detail = d.r2id
LEFT JOIN capture_logs_old cl ON digest(cp.capture_log, 'sha256') = cl.hash
LEFT JOIN capture_html_content ch ON digest(cp.html_content, 'sha256') = ch.hash
UNION ALL
SELECT
  u.nid AS url,
  SUBSTRING(cp.locale FOR 2) AS country,
  COALESCE(SUBSTRING(cp.locale FROM '^[a-z][a-z]_(.+)$'), '') AS vantage,
  cp.access_time AS access_time,
  d.nid AS result,
  COALESCE(v.nid, u.nid) AS redir_url,
  cl.id AS capture_log_old,
  COALESCE(ch.id, (SELECT id FROM capture_html_content
                    WHERE hash = digest('', 'sha256'))) AS html_content
  FROM ts_run_3.captured_pages cp
LEFT JOIN url_remap u ON cp.url = u.r3id
LEFT JOIN url_remap v ON cp.redir_url = v.r3id
LEFT JOIN detail_remap d ON cp.detail = d.r3id
LEFT JOIN capture_logs_old cl ON digest(cp.capture_log, 'sha256') = cl.hash
LEFT JOIN capture_html_content ch ON digest(cp.html_content, 'sha256') = ch.hash
;
