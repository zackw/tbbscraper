-- -*- sql-product: postgres -*-

--CREATE SCHEMA ts_replmeta;
--SET search_path TO ts_replmeta, ts_run_1;

CREATE TABLE r_capture_detail (
    id INTEGER PRIMARY KEY
);
CREATE VIEW d_capture_detail AS
   SELECT * FROM ts_run_1.capture_detail
      WHERE id NOT IN (SELECT id FROM ts_replmeta.r_capture_detail);

CREATE TABLE r_captured_pages (
   locale TEXT,
   url INTEGER,
   PRIMARY KEY (locale, url)
);
CREATE VIEW d_captured_pages AS
   SELECT * FROM ts_run_1.captured_pages
      WHERE (locale, url) NOT IN
         (SELECT locale, url FROM ts_replmeta.r_captured_pages);

CREATE TABLE r_clab_categories (
   code TEXT PRIMARY KEY
);
CREATE VIEW d_clab_categories AS
   SELECT * FROM ts_run_1.clab_categories
      WHERE code NOT IN (SELECT code FROM ts_replmeta.r_clab_categories);

CREATE TABLE r_static_list_metadata (
   id INTEGER PRIMARY KEY
);
CREATE VIEW d_static_list_metadata AS
   SELECT * FROM ts_run_1.static_list_metadata
      WHERE id NOT IN (SELECT id FROM ts_replmeta.r_static_list_metadata);

CREATE TABLE r_twitter_users (
   uid BIGINT PRIMARY KEY
);
CREATE VIEW d_twitter_users AS
   SELECT * FROM ts_run_1.twitter_users
      WHERE uid NOT IN (SELECT uid FROM ts_replmeta.r_twitter_users);

CREATE TABLE r_url_strings (
   id INTEGER PRIMARY KEY
);
CREATE VIEW d_url_strings AS
   SELECT * FROM ts_run_1.url_strings
      WHERE id NOT IN (SELECT id FROM ts_replmeta.r_url_strings);

CREATE TABLE r_urls_alexa (
   retrieval_date DATE,
   url INTEGER,
   PRIMARY KEY (retrieval_date, url)
);
CREATE VIEW d_urls_alexa AS
   SELECT * FROM ts_run_1.urls_alexa
      WHERE (retrieval_date, url) NOT IN
         (SELECT retrieval_date, url FROM ts_replmeta.r_urls_alexa);

CREATE TABLE r_urls_citizenlab (
   retrieval_date DATE,
   country CHAR(2),
   url INTEGER,
   PRIMARY KEY (retrieval_date, country, url)
);
CREATE VIEW d_urls_citizenlab AS
   SELECT * FROM ts_run_1.urls_citizenlab
      WHERE (retrieval_date, country, url) NOT IN
         (SELECT retrieval_date, country, url FROM
            ts_replmeta.r_urls_citizenlab);

CREATE TABLE r_urls_herdict (
   url INTEGER,
   "timestamp" BIGINT,
   accessible BOOLEAN,
   country CHAR(2),
   PRIMARY KEY (url, "timestamp", accessible, country)
);
CREATE VIEW d_urls_herdict AS
   SELECT * FROM ts_run_1.urls_herdict
      WHERE (url, "timestamp", accessible, country) NOT IN
         (SELECT url, "timestamp", accessible, country
            FROM ts_replmeta.r_urls_herdict);

CREATE TABLE r_urls_pinboard (
   username TEXT,
   url INTEGER,
   PRIMARY KEY (username, url)
);
CREATE VIEW d_urls_pinboard AS
   SELECT * FROM ts_run_1.urls_pinboard
      WHERE (username, url) NOT IN
         (SELECT username, url FROM ts_replmeta.r_urls_pinboard);

CREATE TABLE r_urls_tweeted (
   uid BIGINT,
   url INTEGER,
   PRIMARY KEY (uid, url)
);
CREATE VIEW d_urls_tweeted AS
   SELECT * FROM ts_run_1.urls_tweeted
      WHERE (uid, url) NOT IN
        (SELECT uid, url FROM ts_replmeta.r_urls_tweeted);

CREATE TABLE r_urls_staticlist (
   listid INTEGER,
   url INTEGER,
   PRIMARY KEY (listid, url)
);
CREATE VIEW d_urls_staticlist AS
   SELECT * FROM ts_run_1.urls_staticlist
      WHERE (listid, url) NOT IN
        (SELECT listid, url FROM ts_replmeta.r_urls_staticlist);

CREATE TABLE r_urls_twitter_user_profiles (
   uid BIGINT,
   url INTEGER,
   PRIMARY KEY (uid, url)
);
CREATE VIEW d_urls_twitter_user_profiles AS
   SELECT * FROM ts_run_1.urls_twitter_user_profiles
      WHERE (uid, url) NOT IN
         (SELECT uid, url FROM ts_replmeta.r_urls_twitter_user_profiles);
