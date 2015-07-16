#! /usr/bin/python3

import calendar
import email.utils
import pkgutil
import sys
import time
import twython
from lib.shared import url_database

class DbStub:
    def __init__(self):
        self.database = "tbbscraper_db"

def connect_to_twitter_api():
    cred = pkgutil.get_data("lib.url_sources", "twitter_credential.txt").strip()
    (app_key, app_secret, oauth_token, oauth_secret) = cred.split()
    return twython.Twython(app_key, app_secret, oauth_token, oauth_secret)

class RateLimitWrapper:
    def __init__(self, api, method, method_name, family):
        self.api = api
        self.method = method
        self.method_name = method_name
        self.family = family
        self.update()

    def update(self):
        data = self.api.get_application_rate_limit_status(resources=self.family)
        data = data["resources"][self.family][self.method_name]
        self.remaining = data["remaining"]
        self.reset_time = data["reset"]

        now = time.time()
        self.last_call_time = now
        if self.remaining > 0:
            self.interval = (self.reset_time - now) / self.remaining
        else:
            self.interval = self.reset_time - now
        sys.stderr.write("\n{}: {} calls before {}, interval={:.3}s\n"
                         .format(time.strftime("%H:%M:%S", time.gmtime(now)),
                                 self.remaining,
                                 time.strftime("%H:%M:%S",
                                               time.gmtime(self.reset_time)),
                                 self.interval))

    def __call__(self, **params):
        now = time.time()
        till_end_of_window = self.reset_time - now
        if self.remaining == 0 or till_end_of_window < 0:
            # If we are completely out of calls, wait out the entire
            # window and then query for a new window.
            if till_end_of_window > 0:
                sys.stderr.write("(waiting {:.3}s)".format(till_end_of_window))
                sys.stderr.flush()
                time.sleep(till_end_of_window)
            self.update()
        else:
            # Spread the method calls we're allowed to make over the
            # window between now and the reset time.
            delay = (self.last_call_time + self.interval) - now
            if delay > 0:
                sys.stderr.write("(waiting {:.3}s)".format(delay))
                sys.stderr.flush()
                time.sleep(delay)
            self.last_call_time = time.time()

        self.remaining -= 1
        return self.method(**params)

def next_ublock(lookup_user, block):
    uid_string = ",".join(str(r[0]) for r in block)
    try:
        return lookup_user(user_id=uid_string, include_entities=False)
    except twython.TwythonError:
        # "If none of your lookup criteria can be satisfied by
        # returning a user object, a HTTP 404 will be thrown."
        # I *think* in our case this means "if none of the users in
        # the block exist anymore" but let's log 'em just to be sure.
        sys.stderr.write("\n404 Not Found: " + uid_string + "\n")
        sys.stderr.flush()
        return []

def main():
    db = url_database.ensure_database(DbStub())
    twi = connect_to_twitter_api()
    lookup_user = RateLimitWrapper(twi, twi.lookup_user,
                                   "/users/lookup", "users")

    urls = []
    users = []

    with db, db.cursor() as cur:
        cur.execute("SELECT uid FROM twitter_users")
        while True:
            sys.stderr.write("\r{} users, {} urls...\033[K"
                             .format(len(users), len(urls)))
            sys.stderr.flush()
            block = cur.fetchmany(100)
            if not block: break
            ublock = next_ublock(lookup_user, block)
            for u in ublock:
                users.append(
                    { 'id': u['id'],
                      # no created_at_in_seconds for users :-(
                      'ca': calendar.timegm(
                                email.utils.parsedate(u['created_at'])),
                      'vr': int(u.get('verified', False)),
                      'pr': int(u.get('protected', False)),
                      'sn': u['screen_name'],
                      'nm': u.get('name', ""),
                      'la': u.get('lang', ""),
                      'lo': u.get('location', ""),
                      'ds': u.get('description', "") })
                for thing in u.get('entities', {}).values():
                    for url in thing.get('urls', []):
                        eu = url.get('expanded_url', None)
                        if eu:
                            urls.append((eu, u['id']))

    with db, db.cursor() as cur:
        sys.stderr.write("\nrecording url strings...")
        urls = list(set((url_database.add_url_string(cur, url[0])[0], url[1])
                        for url in urls))

        sys.stderr.write("\nupdating user table...")
        cur.executemany(
            "UPDATE twitter_users"
            "  SET  created_at  = %(ca)s,"
            "       verified    = %(vr)s,"
            "       protected   = %(pr)s,"
            "       screen_name = %(sn)s,"
            "       full_name   = %(nm)s,"
            "       lang        = %(la)s,"
            "       location    = %(lo)s,"
            "       description = %(ds)s"
            " WHERE uid = %(id)s",
            users)

        sys.stderr.write("\nupdating urls table...")
        cur.executemany(
            "INSERT INTO urls_twitter_user_profiles (url, uid) "
            "  VALUES (%s, %s)",
            urls)
        sys.stderr.write("\n")

main()
