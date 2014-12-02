-- -*- sql-product: postgres -*-

-- URL ancillary
CREATE TABLE url_strings (
    id    SERIAL  NOT NULL PRIMARY KEY,
    url   TEXT    NOT NULL UNIQUE CHECK (url <> '')
);

-- URL source ancillary
CREATE TABLE clab_categories (
    code        TEXT NOT NULL PRIMARY KEY,
    description TEXT NOT NULL UNIQUE
);

CREATE TABLE static_list_metadata (
    id          SERIAL NOT NULL PRIMARY KEY,
    label       TEXT   NOT NULL,
    url         TEXT   NOT NULL,
    last_update DATE   NOT NULL,
    UNIQUE (label, url, last_update)
);

CREATE TABLE twitter_relations (
    follow_from BIGINT NOT NULL,
    follow_to   BIGINT NOT NULL
);
CREATE INDEX twitter_relations_follow_from_idx ON twitter_relations(follow_from);
CREATE INDEX twitter_relations_follow_to_idx ON twitter_relations(follow_to);

CREATE TABLE twitter_scans (
    scan     INTEGER NOT NULL PRIMARY KEY,
    mode     TEXT    NOT NULL,
    limit_   INTEGER NOT NULL,
    parallel INTEGER NOT NULL,
    seed     TEXT,
    state    BYTEA
);

CREATE TABLE twitter_users (
    uid                 BIGINT NOT NULL PRIMARY KEY,
    created_at          BIGINT,
    verified            INTEGER,
    protected           INTEGER,
    highest_tweet_seen  BIGINT,
    screen_name         TEXT,
    full_name           TEXT,
    lang                TEXT,
    location            TEXT,
    description         TEXT
);
CREATE INDEX twitter_users_screen_name_idx ON twitter_users(screen_name);

-- URL sources
CREATE TABLE urls_alexa (
    retrieval_date DATE    NOT NULL,
    url            INTEGER NOT NULL REFERENCES url_strings(id),
    rank           INTEGER NOT NULL,
    UNIQUE(retrieval_date, url)
);

CREATE TABLE urls_citizenlab (
    retrieval_date DATE    NOT NULL,
    country        CHAR(2) NOT NULL CHECK(country<>''),
    url            INTEGER NOT NULL REFERENCES url_strings(id),
    category       TEXT    NOT NULL REFERENCES clab_categories(code),
    UNIQUE(retrieval_date, country, url)
);

CREATE TABLE urls_herdict (
    "timestamp"    BIGINT  NOT NULL,
    url            INTEGER NOT NULL REFERENCES url_strings(id),
    country        CHAR(2) NOT NULL CHECK(country<>''),
    accessible     BOOLEAN NOT NULL
);

CREATE TABLE urls_pinboard (
    username     TEXT NOT NULL,
    url          INTEGER NOT NULL REFERENCES url_strings(id),
    access_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    title        TEXT NOT NULL DEFAULT(''),
    annotation   TEXT NOT NULL DEFAULT(''),
    tags         TEXT NOT NULL DEFAULT(''),
    UNIQUE (username, url)
);

CREATE TABLE urls_staticlist (
    listid         INTEGER NOT NULL REFERENCES static_list_metadata(id),
    url            INTEGER NOT NULL REFERENCES url_strings(id),
    UNIQUE(listid, url)
);

CREATE TABLE urls_tweeted (
    uid                 BIGINT NOT NULL REFERENCES twitter_users(uid),
    url                 INTEGER NOT NULL REFERENCES url_strings(id),
    "timestamp"         BIGINT,
    retweets            INTEGER,
    possibly_sensitive  BOOLEAN,
    lang                CHAR(3),
    withheld            TEXT,
    hashtags            TEXT,
    UNIQUE (uid, url)
);

CREATE TABLE urls_twitter_user_profiles (
    uid                 BIGINT NOT NULL REFERENCES twitter_users(uid),
    url                 INTEGER NOT NULL REFERENCES url_strings(id),
    UNIQUE (uid, url)
);

-- Capture results
CREATE TYPE capture_result AS ENUM (
    -- HTTP: success
    'ok',
    'ok (redirected)',

    -- HTTP: failure
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

    -- Network-level failure
    'host not found',
    'server unreachable', -- lumping EHOSTUNREACH and ENETUNREACH
    'connection refused',
    'TLS handshake failed',
    'connection interrupted',
    'other network error',

    -- Crawler timeout.
    -- Note that HTTP responses containing the regex /timed?-?out/i are
    -- classified as site failures rather than client-side timeouts,
    -- because that normally means the *site* timed out talking to its
    -- own backend.
    'timeout',

    -- Something went wrong with the proxy, not the site.
    -- This category mostly gets used for legacy data from SOCKS proxies,
    -- for which we would get "N103 Connection to proxy closed prematurely"
    -- for everything from DNS lookup failure to an actual disconnect.
    -- It is still relevant in cases where, say, the censors notice what
    -- we are doing and terminate our control connection to the proxy itself.
    'proxy failure',

    -- The crawler crashed (usually this means it ran out of memory, but
    -- WebKit does still have an awful lot of bugs).
    'crawler failure'
);

CREATE TABLE capture_detail (
    id      SERIAL  NOT NULL PRIMARY KEY,
    detail  TEXT    NOT NULL UNIQUE CHECK (detail <> '')
);

CREATE TABLE captured_pages (
    locale       TEXT NOT NULL CHECK (locale <> ''),
    url          INTEGER NOT NULL REFERENCES url_strings(id),
    access_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    result       capture_result NOT NULL,
    detail       INTEGER NOT NULL REFERENCES capture_detail(id),
    redir_url    INTEGER REFERENCES url_strings(id),
    capture_log  BYTEA,
    html_content BYTEA,
    screenshot   BYTEA,
    PRIMARY KEY(locale, url)
);
CREATE INDEX captured_pages_url_idx ON captured_pages(url);
CREATE INDEX captured_pages_result_idx ON captured_pages(result);
