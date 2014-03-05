#! /usr/bin/python

# This script extracts URLs from a Twitter stream and writes them to a CSV
# file with some of the per-tweet metadata.  The output format is like this:
#
# id,user.id,timestamp,retweet_count,lang,possibly_sensitive,withheld,hashtags,urls
#
# corresponding to fields from
# https://dev.twitter.com/docs/platform-objects/tweets
# https://dev.twitter.com/docs/platform-objects/entities
#
# The "withheld", "hashtags", and "urls" fields are |-separated lists.
# Vertical bars in the URLs, if any, are %-encoded.
#
# You get the expanded URLs, not the t.co URLs.

import argparse
import calendar
import email.utils
import os
import pickle
import re
import sys
import time
#import twitter
import urllib.parse

import url_sources.urldb

def parse_args():
    def positive_int(arg):
        val = int(arg)
        if val <= 0:
            raise TypeError("argument must be positive")
        return val

    ap = argparse.ArgumentParser(description=
                                 "Extract URLs from Twitter streams, "
                                 "either for a single user, breadth-"
                                 "first to some depth starting with a "
                                 "single user, or frontier sampled from "
                                 "the entire user population.")

    ap.add_argument("--database", "-d", metavar="DB",
                    default="urls.db",
                    help="The database to update.")
    ap.add_argument("--creds", "-c", metavar="FILE",
                    default="sources/twitter_credential.txt",
                    help="read Twitter API credentials from this file "
                    "(four lines: consumer key, consumer secret, "
                    " access token, access secret)")

    ap.add_argument("--mode", "-m", metavar="EXTRACTION_MODE",
                    choices=("single", "breadth", "frontier", "resume",
                             "urls"),
                    default="single",
                    help="Extraction mode.  Use 'resume' to resume a "
                    "previous, interrupted scan.  Use 'urls' to just "
                    "extract more URLs from all users already in the "
                    "database.")

    ap.add_argument("--number", "-n", metavar="N",
                    type=positive_int, default=1,
                    help="Numeric parameter used by some modes: "
                    "For 'breadth' mode, the depth of the search "
                    "(1=friends, 2=friends of friends, and so on). "
                    "For 'frontier' mode, the number of simultaneous "
                    "random walks.")

    ap.add_argument("--quiet", "-q", action="store_true",
                    help="disable progress messages")

    ap.add_argument("seed", nargs="?",
                    help="Starting point for the scan. "
                    "For 'single' and 'breadth' modes, this should be a "
                    "Twitter handle (leading @ not required).  For 'resume' "
                    "mode, the tag of a previous scan (specify --mode=resume "
                    "with no seed to get a list of resumable scans). "
                    "Not used in frontier mode.")

    return ap.parse_args()

def ensure_database(args):
    twitter_schema = """\
CREATE TABLE twitter_users (
    uid                 INTEGER PRIMARY KEY,
    created_at          INTEGER,
    verified            INTEGER,
    protected           INTEGER,
    highest_tweet_seen  INTEGER,
    screen_name         TEXT,
    full_name           TEXT,
    lang                TEXT,
    location            TEXT,
    description         TEXT
);
CREATE INDEX twitter_users__sn ON twitter_users(screen_name);

CREATE TABLE twitter_relations (
    follow_from INTEGER NOT NULL REFERENCES twitter_users(uid),
    follow_to   INTEGER NOT NULL REFERENCES twitter_users(uid)
);
CREATE INDEX twitter_relations__from ON twitter_relations(follow_from);
CREATE INDEX twitter_relations__to ON twitter_relations(follow_to);

CREATE TABLE twitter_tweets (
    tid                INTEGER PRIMARY KEY,
    uid                INTEGER NOT NULL REFERENCES twitter_users(uid),
    timestamp          INTEGER,
    retweets           INTEGER,
    possibly_sensitive INTEGER,
    lang               TEXT,
    withheld           TEXT,
    hashtags           TEXT
);

CREATE TABLE twitter_scans (
    tag                TEXT PRIMARY KEY,
    mode               TEXT NOT NULL,
    number             INTEGER,
    seed               TEXT,
    state              BLOB
) WITHOUT ROWID;
"""

    db = urldb.ensure_database(args)
    with db:
        # FIXME: More sophisticated way of detecting presence of our
        # ancillary schema.
        s_tables = frozenset(re.findall("(?m)(?<=^CREATE TABLE )[a-z_]+",
                                        twitter_schema))
        s_indices = frozenset(re.findall("(?m)(?<=^CREATE INDEX )[a-z_]+",
                                         twitter_schema))
        d_tables = frozenset(r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE "
                "  type = 'table' AND name LIKE 'twitter_%'"))
        d_indices = frozenset(r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE "
                "  type = 'index' AND name LIKE 'twitter_%'"))

        if not d_tables and not d_indices:
            db.executescript(twitter_schema)
            db.commit()

        elif d_tables != s_tables or d_indices != s_indices:
            raise RuntimeError("ancillary schema mismatch - migration needed")

        oid = db.execute("SELECT id FROM origins"
                         "  WHERE label = 'twitter'").fetchone()
        if oid is None:
            oid = db.execute("INSERT INTO origins"
                             "  VALUES(NULL, 'twitter')").lastrowid
        else:
            oid = oid[0]

        return db, oid

class Session(object):
    def __init__(self, fname):
        self.api = None
        # I would like to use application-only auth but it appears that
        # python-twitter does not support this.
        with open(fname) as cf:
            self.consumer_key    = cf.readline().strip()
            self.consumer_secret = cf.readline().strip()
            self.access_token    = cf.readline().strip()
            self.access_secret   = cf.readline().strip()

    def __enter__(self):
        self.api = twitter.Api(consumer_key=self.consumer_key,
                               consumer_secret=self.consumer_secret,
                               access_token_key=self.access_token,
                               access_token_secret=self.access_secret)
        return self.api

    def __exit__(self, *ignored):
        if self.api is not None:
            self.api.ClearCredentials()
            self.api = None

class Extraction(object):
    """All of the state associated with an ongoing extraction run."""

    def __init__(self, tag, number, seed, db, oid, api, verbose):
        self._tag     = tag
        self._n       = number
        self._seed    = seed
        self._db      = db
        self._oid     = oid
        self._api     = api
        self._verbose = verbose

        # fully constructed rows, queued for insertion
        self.pending_tweets = []
        self.pending_urls   = []

    def flush_tweets(force=False):
        """Batch insert the pending_tweets and pending_urls into the
           database, if they're big enough to be worth it."""
        if force: minrows = 1
        else:     minrows = 1000

        if (len(self.pending_tweets) < minrows and
            len(self.pending_urls) < minrows):
            return

        with self._db:
            if self.pending_tweets:
                # We might have duplicates, e.g. from retweeting.
                self._db.executemany("INSERT OR IGNORE INTO twitter_tweets "
                                    "  VALUES(?,?,?,?,?,?,?,?)",
                                     self.pending_tweets)
                self.pending_tweets = []
            if self.pending_urls:
                # We might have duplicates, e.g. from retweeting.
                self._db.executemany("INSERT OR IGNORE INTO urls VALUES(?,?,?)",
                                     self.pending_urls)
                self.pending_urls = []

    def process_tweet(self, t):
        """Record everything we care to know about tweet T.  May be
           extended by subclasses."""

        lang = t.lang if t.lang else ""
        sensitive = "1" if t.possibly_sensitive else 0

        withheld = []
        if t.withheld_copyright:
            withheld.append("copy")
        if t.withheld_in_countries:
            withheld.extend(c.lower() for c in t.withheld_in_countries)
        withheld.sort()
        withheld = "|".join(withheld)

        hashtags = "|".join(sorted(h.text.replace("|", "_")
                                   for h in t.hashtags))

        self.pending_tweets.append((t.id, t.user.id, t.created_at_in_seconds,
                                    t.retweet_count,
                                    lang,
                                    sensitive,
                                    withheld,
                                    hashtags))

        self.pending_urls.extend((self._oid,
                                  t.id << 1, # from a tweet
                                  urlparse.urlparse(u.expanded_url).geturl())
                                 for u in t.urls)
        self.flush_tweets()

    def process_tweets_for_user(self, uid, screen_name, since):
        """Retrieve as many tweets as possible for user UID and populate
           the twitter_tweets and urls tables from them.  If SINCE is
           nonzero, we have already processed this user and we need only
           check for tweets since that serial number."""

        time.sleep(self._api.GetSleepTime("/statuses/user_timeline"))
        tweets = self._api.GetUserTimeline(user_id=uid, since_id=since)
        if not tweets: return

        new_highest_tid = tweets[0].id
        total = 0
        while tweets:
            for t in tweets:
                if t.urls:
                    total += len(t.urls)
                    self.process_tweet(t)

            max_id = tweets[-1].id - 1
            time.sleep(self._api.GetSleepTime("/statuses/user_timeline"))
            tweets = self._api.GetUserTimeline(user_id=uid,
                                               max_id=max_id, since_id=since)

        self._db.execute("UPDATE twitter_users SET highest_tweet_seen = ?"
                         "  WHERE uid = ?", (new_highest_tid, uid))

        if self._verbose:
            sys.stderr.write("added {} urls from @{}\n"
                             .format(total, screen_name))


    def ensure_user(self, uid=None, screen_name=None):
        """Make sure the user with the given UID has an entry in the
           twitter_users table.  Does NOT load this user's relations.
           Returns the row vector for the user."""

        assert (uid is None or screen_name is None)
        if uid:
            row = self._db.execute("SELECT * FROM twitter_users"
                                   "  WHERE uid = ?", (uid,)).fetchone()
            if row is not None: return row

        # python-twitter drops user entities on the floor, feh, so
        # there's no point asking for them.
        time.sleep(self._api.GetSleepTime("/users/show"))
        u = self._api.GetUser(user_id=uid, screen_name=screen_name,
                              include_entities=False)

        with self._db:
            self._db.execute("INSERT INTO twitter_users VALUES "
                             "(?,?,?,?,0,?,?,?,?,?,?,?)",
                             (uid,
                              # no created_at_in_seconds for users :-(
                              calendar.timegm(
                                  email.utils.parsedate(u.created_at)),
                              u.verified,
                              u.protected,
                              u.screen_name,
                              u.name,
                              u.lang,
                              u.location,
                              u.description))

        if u.url:
            self.pending_urls.append((self._oid,
                                      (uid << 1) | 1, # from a user
                                      urlparse.urlparse(u.url).geturl()))
            self.flush_tweets()

    def process_user(self, uid):
        """Load the user indicated by UID fully into the database; in
           addition to what ensure_user does, this populates the
           twitter_relations, twitter_tweets, and urls tables from the
           selected user."""
        urow = self.ensure_user(uid)
        assert urow[0] == uid
        self.process_tweets_for_user(urow[0], # uid
                                     urow[5], # screen_name
                                     urow[4]) # highest_tweet_seen

        with self._db:
            self.process_edges(uid, False)
            self.process_edges(uid, True)

    def process_edges(self, uid, followers):
        if followers:
            get_ids = self._api.GetFollowerIDs
            marshal = lambda f: (f, uid)
        else:
            get_ids = self._api.GetFriendIDs
            marshal = lambda f: (uid, f)

        # GetFollowerIDs and GetFriendIDs internally handle cursoring
        # and rate-limiting!  How nice.
        users = get_ids(user_id=uid)
        relations = []
        for u in users:
            self.ensure_user(u)
            relations.append(marshal(u))
        self._db.executemany("INSERT INTO twitter_relations VALUES(?,?)",
                             relations)


def main():
    args = parse_args()
    db, oid = ensure_database(args)
    with Session(args.creds) as sess:
        extractor = Extraction("test", args.number, args.seed,
                               db, oid, sess, not args.quiet)
        urow = extractor.ensure_user(screen_name=args.seed)
        extractor.process_user(urow[0])

if __name__ == "__main__": main()
