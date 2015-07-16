# Copyright © 2013, 2014 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Extract URLs from Twitter streams.

You can choose to examine the stream of a single user, a snowball
sample of all users within some follow-graph distance of one user, or
a frontier sample of users from the entire population.  Or you can
examine a random sample of all tweets, filtered with search
parameters."""

def setup_argp(ap):
    def positive_int(arg):
        val = int(arg)
        if val <= 0:
            raise TypeError("argument must be positive")
        return val

    ap.add_argument("mode", metavar="extraction-mode",
                    choices=("single", "snowball", "frontier",
                             "firehose", "resume", "urls"),
                    default="single",
                    help="single=one user.\n"
                    "snowball=all users within some distance of a seed user.\n"
                    "frontier=random sample of the entire user population.\n"
                    "firehose=random sample of all tweets as they go by "
                    "(possibly filtered with search parameters).\n"
                    "resume=continue an interrupted scan.\n"
                    "urls=extract new URLs from users already in the database.")

    ap.add_argument("-l", "--limit",
                    type=positive_int, default=1,
                    help="How 'big' of a sample to take, in some sense. "
                    "For snowball mode, the distance from the seed user. "
                    "For frontier and firehose mode, the number of unique "
                    "users to pick before stopping.")

    ap.add_argument("-p", "--parallel",
                    type=positive_int, default=1,
                    help="Parallelism: only relevant for frontier sampling, "
                    "where it controls the number of simultaneous random "
                    "walks.")

    ap.add_argument("seed", nargs="*",
                    help="Starting point for the scan. "
                    "For 'single' and 'snowball' modes, you must supply one "
                    "Twitter handle (leading @ not required).  For 'frontier' "
                    "and 'firehose' modes, you may supply a search query which "
                    "will limit the initial stream request. "
                    "For 'resume' mode, you must supply the tag of a previous "
                    "scan (specify --mode=resume with no seed to get a list "
                    "of resumable scans). ")

def run(args):
    extractors = {
        'single':   SingleExtractor,
        'snowball': SnowballExtractor,
        'frontier': FrontierExtractor,
        'firehose': FirehoseExtractor,
        'urls':     UrlsOnlyExtractor,
        'resume':   resume_extraction
    }
    args.seed = " ".join(args.seed)
    db = url_database.ensure_database(args)
    twi = connect_to_twitter_api()
    extractor = extractors[args.mode](args, db, twi)
    extractor.run()

import calendar
import email.utils
import os
import pickle
import pickletools
import pkgutil
import re
import shutil
import sys
import time
import twython
import urllib.parse

from shared import url_database

def fatal(message, *args, **kwargs):
    import os.path
    import textwrap
    prog = os.path.basename(sys.argv[0])
    sys.stderr.write(textwrap.fill(message.format(*args, prog=prog, **kwargs)))
    sys.stderr.write('\n')
    sys.exit(1)

def connect_to_twitter_api():
    cred = pkgutil.get_data("url_sources", "twitter_credential.txt").strip()
    (app_key, app_secret, oauth_token, oauth_secret) = cred.split()
    return twython.Twython(app_key, app_secret, oauth_token, oauth_secret)


def dump_resumables_and_exit(cur):
    cur.execute("SELECT scan, mode, limit_, parallel, seed "
                "FROM twitter_scans "
                "WHERE state NOT NULL "
                "ORDER BY mode")
    resumable = cur.fetchall()
    if not resumable:
        fatal("{prog}: No scans can be resumed.")

    # we need to munge the list
    resumable = list(list(row) for row in resumable)

    colheads = ["scan", "mode", "limit", "par", "seed"]
    colwidths = [len(x) for x in colheads]
    maxwidth = shutil.get_terminal_size().columns

    for row in resumable:
        row[2] = str(row[2])
        row[3] = str(row[3])
        for i, col in enumerate(row):
            colwidths[i] = max(len(col), colwidths[i])

    separator = ["-"*n for n in colwidths]
    resumable.insert(0, separator)
    resumable.insert(0, colheads)

    sys.stderr.write("Scans that can be resumed:\n")
    for row in resumable:
        sys.stderr.write("{1:<{0}} {3:>{2}} {5:>{4}} {7:>{6}} {9:<{8}}\n"
                         .format(colwidths[0], row[0],
                                 colwidths[1], row[1],
                                 colwidths[2], row[2],
                                 colwidths[3], row[3],
                                 colwidths[4], row[4][:colwidths[4]]))
    fatal("Use '{prog} twitter resume SCAN' to resume an "
          "interrupted scan.")

def resume_extraction(args, cur, twi):
    if not args.seed:
        dump_resumables_and_exit(db)

    if len(args.seed) > 1:
        fatal("{prog}: too many arguments for 'twitter resume' mode")

    cur.execute("SELECT * FROM twitter_scans WHERE scan = ?",
                (args.seed[0],))
    state = cur.fetchall()
    assert len(state) <= 1
    if not state:
        fatal("{prog}: no scan '{scan}' to resume.\n"
              "Use '{prog} twitter resume' with no further arguments "
              "for a list of resumable scans.", scan=args.seed[0])

    extractor = Extractor.reload(args, db, twi, *state[0])
    return extractor

class Extractor:
    """Base class for extraction algorithms.  Note: subclasses should
       call Extractor.__init__ at the _end_ of their own __init__
       (if they need one), because it does an immediate checkpoint
       (via set_scan_number)."""
    def __init__(self, args, db, twi):
        self.twi      = twi
        self.db       = db
        self.cr       = db.cursor()
        self.db_name  = args.database
        self.mode     = args.mode
        self.limit    = args.limit
        self.parallel = args.parallel
        self.seed     = args.seed
        self.last_checkpoint = time.time()
        self.set_scanno()

    def __getstate__(self):
        # Don't attempt to pickle the database handle, the Twitter API
        # handle, or anything that is stored in the database already.
        state = self.__dict__.copy()
        for k in ('twi', 'db', 'db_name',
                  'mode', 'limit', 'parallel', 'seed',
                  'scanno', 'last_checkpoint'):
            try: del state[k]
            except KeyError: pass

        return state

    @classmethod
    def reload(cls, args, db, twi,
               scan, mode, limit, parallel, seed, state):
        this = pickle.loads(state)
        assert isinstance(this, cls)

        this.twi      = twi
        this.db       = db
        this.db_name  = args.database
        this.scanno   = scan
        this.mode     = mode
        this.limit    = limit
        this.seed     = seed
        this.parallel = parallel
        this.last_checkpoint = time.time()

        if this.limit != args.limit and args.limit != 1:
            fatal("{prog}: Cannot change --limit when resuming a scan.")
        if this.parallel != args.parallel and args.parallel != 1:
            fatal("{prog}: Cannot change --parallel when resuming a scan.")
        if this.seed != args.seed and args.seed != "":
            fatal("{prog}: Cannot change seed when resuming a scan.")

        return this

    def set_scanno(self):
        self.cur.execute("INSERT INTO twitter_scans "
                         "VALUES (DEFAULT, %s, %s, %s, %s, %s)"
                         "RETURNING scan",
                         (self.mode, self.limit, self.parallel, self.seed,
                          pickletools.optimize(pickle.dumps(self))))
        self.scanno = self.cur.fetchone()[0]
        self.db.commit()

    def checkpoint(self):
        self.cur.execute("UPDATE twitter_scans SET state = %s WHERE scan = %s",
                         (pickletools.optimize(pickle.dumps(self)),
                          self.scanno))
        self.db.commit()

    def complete(self):
        self.cur.execute(
            "UPDATE twitter_scans SET state = NULL WHERE scan = %s",
            (self.scanno,))
        self.db.commit()

    def abandon(self, message, *args, **kwargs):
        self.cur.execute("DELETE FROM twitter_scans WHERE scan = %s",
                        (self.scanno,))
        self.db.commit()
        fatal(message, *args, **kwargs)

    def ensure_user(self, *, uid=None, screen_name=None, row=None):
        """Make sure the user with the given UID (or screen name, or
           preloaded row vector, but no more than one of these) has an
           entry in the twitter_users table.  Does NOT load this
           user's relations.  Returns the row vector for the user."""

        assert (uid is None) + (screen_name is None) + (row is None) == 2
        cur = self.db.cursor()

        if uid is not None:
            row = self.db.execute("SELECT * FROM twitter_users"
                                  "  WHERE uid = ?", (uid,)).fetchone()
        elif screen_name is not None:
            row = self.db.execute("SELECT * FROM twitter_users"
                                  "  WHERE screen_name = ?",
                                  (screen_name,)).fetchone()
        else:
            pass
            
        if row is not None:
            return row

        u = self.twi.show_user(user_id=uid, screen_name=screen_name)
        return self.note_user(u)

    def note_user(self, u):
        row = (u['id'],
               # no created_at_in_seconds for users :-(
               calendar.timegm(email.utils.parsedate(u['created_at'])),
               int(u.get('verified', False)),
               int(u.get('protected', False)),
               0,
               u['screen_name'],
               u.get('name', ""),
               u.get('lang', ""),
               u.get('location', ""),
               u.get('description', ""))

        self.db.execute("INSERT OR IGNORE INTO twitter_users "
                        "VALUES(?,?,?,?,?,?,?,?,?,?)",
                        row)
        for thing in u.get('entities', {}).values():
            for url in thing.get('urls', []):
                self.note_url(url['expanded_url'], 'user', u['id'])

        return row

    def note_tweet(self, t):
        """Record one Tweet in the database, if it is interesting.
           For our purposes, tweets are interesting if and only if
           they contain URLs."""

        entities = t.get("entities", {})
        urls = entities.get("urls", [])
        if not urls:
            return

        lang      = t.get("lang", "")
        sensitive = int(t.get("possibly_sensitive", 0))
        withheld = []
        if t.get("withheld_copyright", False):
            # Use the reserved-for-user-use country code ZZ to
            # indicate withholding for copyright violation.  Twitter
            # uses XX and XY for related purposes (withheld everywhere,
            # withheld due to DMCA respectively).
            withheld.append("ZZ")
        withheld.extend(c.lower() for c in t.get("withheld_in_countries", []))
        withheld.sort()
        withheld = "|".join(withheld)

        hashtags = "|".join(sorted(h["text"].replace("|", "_")
                                   for h in entities.get("hashtags", [])))

        user = self.note_user(t["user"])

        sys.stderr.write("{user}: {text}...\n"
                         .format(user=user[5], # screen name
                                 tid=t["id"],
                                 text=t["text"][:60]))

        try:
            created_at = t["created_at_in_seconds"]
        except KeyError:
            created_at = calendar.timegm(email.utils.parsedate(t["created_at"]))

        # We may encounter the same tweet multiple times due to retweeting.
        self.db.execute("INSERT OR IGNORE INTO twitter_tweets "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (t["id"],
                         user[0], # uid
                         created_at,
                         t.get("retweet_count", 0),
                         sensitive,
                         lang,
                         withheld,
                         hashtags))
        for u in urls:
            self.note_url(u["expanded_url"], "tweet", t["id"])

        now = time.time()
        if (now - self.last_checkpoint) > 60:
            self.checkpoint()
            self.last_checkpoint = now

    def run(self):
        """The main logic of each subclass goes here."""
        raise NotImplementedError

class SingleExtractor(Extractor):
    def __init__(self, args, db, twi):
        if not args.seed:
            fatal("{prog}: Must specify a Twitter handle from which to begin.")

        try:
            # replicated from Extractor.__init__ to allow ensure_user to work
            self.db = db
            self.twi = twi
            user = self.ensure_user(screen_name=args.seed)
        except twython.TwythonError as e:
            # most likely scenario:
            if e.error_code == 404:
                fatal("{prog}: No such Twitter handle: {seed}", seed=args.seed)

        self.seed_uid     = user[0]
        self.since_id     = user[4]
        self.new_since_id = 0
        self.max_id       = None
        Extractor.__init__(self, args, db, twi)

    def run(self):
        while True:
            params = { "user_id":  self.seed_uid,
                       "trim_user": True,
                       "exclude_replies": False,
                       "include_rts": True }

            if self.since_id > 0:
                params["since_id"] = self.since_id
            if self.max_id is not None:
                params["max_id"] = self.max_id

            timeline = self.twi.cursor(self.twi.get_user_timeline,
                                       **params)
            try:
                for tweet in timeline:
                    self.note_tweet(tweet)
                    self.max_id = (tweet["id"] if self.max_id is None
                                   else min(self.max_id, tweet["id"]))
                    self.new_since_id = max(self.new_since_id, tweet["id"])

            finally:
                self.checkpoint()


class SnowballExtractor(Extractor):
    def __init__(self, args, db, twi):
        if not args.seed:
            fatal("{prog}: Must specify a Twitter handle from which to begin.")
        Extractor.__init__(self, args, db, twi)

class FrontierExtractor(Extractor):
    pass

class FirehoseStreamer(twython.TwythonStreamer):
    def __init__(self, app_key, app_secret, oauth_key, oauth_secret,
                 tweet_callback, stop_time):
        twython.TwythonStreamer.__init__(self, app_key, app_secret,
                                         oauth_key, oauth_secret)
        self.tweet_callback = tweet_callback
        self.stop_time = stop_time

    def on_success(self, message):
        # weed out non-tweet messages
        if 'id' in message and 'user' in message and 'entities' in message:
            self.tweet_callback(message)
        if time.time() > self.stop_time:
            self.disconnect()
            return False
        return True

class FirehoseExtractor(Extractor):
    def run(self):
        cred = pkgutil.get_data("url_sources", "twitter_credential.txt").strip()
        (app_key, app_secret, oauth_token, oauth_secret) = cred.split()

        start_time = 1395028799
        stop_time = 1395633600
        time.sleep(start_time - time.time())
        try:
            stream = FirehoseStreamer(app_key,
                                      app_secret,
                                      oauth_token,
                                      oauth_secret,
                                      tweet_callback=self.note_tweet,
                                      stop_time = stop_time)
            stream.statuses.sample() # does not return until interrupted
        finally:
            self.complete()

class UrlsOnlyExtractor(Extractor):
    pass
