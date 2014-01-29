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
import cStringIO
import csv
import os
import sys
import time
import twitter
import urlparse

def parse_args():
    parser = argparse.ArgumentParser(description=
                                     "Extract URLs from a Twitter stream.")
    parser.add_argument("--output", "-o", metavar="FILE",
                        help="write output to this file")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="disable progress messages")
    parser.add_argument("--creds", "-c", metavar="FILE",
                        help="read Twitter API credentials from this file "
                             "(four lines: consumer key, consumer secret, "
                             " access token, access secret)",
                        default="credential.txt")

    # PARSER is undocumented and maybe not exactly right, but it produces the
    # helptext I want.
    parser.add_argument("query", nargs=argparse.PARSER, metavar="QUERY",
                        help="Either a Twitter handle (beginning with an @), "
                             "or a search query.  This determines the stream "
                             "of tweets to extract URLs from.")

    return parser.parse_args()

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

# based on UnicodeWriter from http://docs.python.org/2/library/csv.html
class utf8_csv_writer(object):
    """A CSV writer which will write rows to CSV file "f", encoded in
       UTF-8, and without trailing CRs."""

    def __init__(self, f, dialect=csv.excel, **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f

    def writerow(self, row):
        # Ensure all column values are already UTF-8 before passing to
        # the underlying writerow().
        self.writer.writerow([str(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        # remove the CR and write to the real stream.
        if data and data[-1] == "\r":
            data = data[:-1]
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def emit_row(writer, t):
    lang = t.lang if t.lang else ""
    sensitive = "1" if t.possibly_sensitive else 0

    withheld = []
    if t.withheld_copyright:
        withheld.append("copy")
    if t.withheld_in_countries:
        withheld.extend(c.lower() for c in t.withheld_in_countries)
    withheld.sort()
    withheld = "|".join(withheld)

    hashtags = [h.text.replace("|", "_") for h in t.hashtags]
    hashtags.sort()
    hashtags = "|".join(hashtags)

    urls = [urlparse.urlparse(u.expanded_url).geturl().replace("|", "%7C")
            for u in t.urls]
    urls.sort()
    urls = "|".join(urls)

    writer.writerow([t.id,
                     t.user.id,
                     t.created_at_in_seconds,
                     t.retweet_count,
                     lang,
                     sensitive,
                     withheld,
                     hashtags,
                     urls])

def extract_urls(writer, sess, user, query, quiet):
    max_id = None
    if user:
        count = 200 # documented max for user timeline
    else:
        count = 100 # documented max for search timeline

    total = 0

    while True:
        if user:
            tweets = list(sess.GetUserTimeline(screen_name=user,
                                               max_id=max_id, count=count))
        else:
            tweets = list(sess.GetSearch(term=query,
                                         max_id=max_id, count=count))

        if not tweets: break

        for tweet in tweets:
            if tweet.urls:
                emit_row(writer, tweet)

        total += len(tweets)
        max_id = tweets[-1].id - 1
        if user:
            pause = sess.GetSleepTime("/statuses/user_timeline")
        else:
            pause = sess.GetSleepTime("/search/tweets")

        if not quiet:
            sys.stderr.write("\rprocessed: %d... sleeping for %ds ..."
                             % (total, pause))

        time.sleep(pause)

    if not quiet:
        sys.stderr.write("\n")


def main():
    args = parse_args()
    if len(args.query) == 1 and args.query[0][0] == '@':
        user  = args.query[0][1:]
        query = None
    else:
        user  = None
        query = " ".join(args.query)

    if hasattr(args, 'output'):
        output = open(args.output, "wb")
    else:
        output = sys.stdout

    with output:
        if user:
            output.write("# user: %s\n" % user)
        else:
            output.write("# query: %s\n" % query)
        writer = utf8_csv_writer(output)
        writer.writerow(["id", "user.id", "timestamp", "retweet.count", "lang",
                         "possibly.sensitive", "withheld", "hashtags", "urls"])

        with Session(args.creds) as sess:
            extract_urls(writer, sess, user, query, args.quiet)


if __name__ == "__main__": main()
