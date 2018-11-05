#! /usr/bin/python3

import csv
import itertools
import psycopg2
import sys

URL_SOURCE_GROUPS = {
    "Australia 2009 (ACMA; Wikileaks)" : "leak",
    "Germany 2014 (#BPjMleak)"         : "leak",
    "India 2012 (Anonymous)"           : "leak",
    "India 2012 (Assam riots)"         : "leak",
    "Italy 2009 (Wikileaks)"           : "leak",
    "Norway 2009 (Wikileaks)"          : "leak",
    "Russia 2014 (rublacklist.net)"    : "leak",
    "Thailand 2007 (Wikileaks)"        : "leak",
    "UK 2015 (blocked.org.uk)"         : "leak",
    "Syria 2015 (Bluesmote/Telecomix)" : "leak",
    "Turkey 2015 (Engelliweb)"         : "leak",
    "Denmark 2008 (Wikileaks)"         : "leak",
    "Finland 2009 (Wikileaks)"         : "leak",
    "Thailand 2008 (Wikileaks)"        : "leak",
    "Thailand 2009 (Wikileaks)"        : "leak",

    "CitizenLab 2014"                  : "sensitive",
    "Herdict 2014"                     : "sensitive",
    "Wikipedia controversies 2015"     : "sensitive",

    "Alexa 2014"                       : "control",
    "Pinboard 2014"                    : "control",
    "Tweets 2014"                      : "control",
    "Twitter user profiles 2014"       : "control",
}

def get_urls_can_analyze(db, archive):
    with db.cursor() as cur:
        cur.execute("""
            select url from collection.historical_page_availability
             where archive = %s
               and processed = true and cardinality(snapshots) > 0
        """, (archive,))
        return sorted(set(r[0] for r in cur))

def get_srcgroups_for_urls(db, urls):
    with db.cursor() as cur:
        cur.execute("""
            select u.url, array_agg(us.name)
              from collection.urls u, collection.url_sources us
             where u.src = us.id
               and u.url = any(%s)
          group by u.url
        """, (urls,))

        result = {}
        for url, srcs in cur:
            sgrps = frozenset(URL_SOURCE_GROUPS[s] for s in srcs)
            if "leak" in sgrps:
                result[url] = "leak"
            elif "sensitive" in sgrps:
                result[url] = "sensitive"
            else:
                assert "control" in sgrps
                result[url] = "control"

        return result

def write_events_for_urls(wr, db, archive, country, vantage, urls, srcgrps):
    with db.cursor() as cur:
        cur.execute("""
            with m(archive, country, vantage) as (values(%s, %s, %s)),
                 i(uid) as (select unnest(%s))
          select hp.url as uid, hp.archive_time as atime,
                 cr.result, hp.is_parked
            from m, i, collection.historical_pages hp,
                 collection.capture_result cr
                 where hp.url = i.uid and hp.archive = m.archive
                   and hp.result = cr.id
       union all
          select cp.url as uid, cp.access_time as atime,
                          cr.result, ch.is_parked
            from m, i, collection.captured_pages cp,
                 collection.capture_html_content ch,
                 collection.capture_result cr
           where cp.url = i.uid
             and cp.country = m.country
             and cp.vantage = m.vantage
             and cp.html_content = ch.id
             and cp.result = cr.id
        order by uid, atime
        """, (archive, country, vantage, urls))

        ONE_YEAR    = 60 * 60 * 24 * 365.2425
        prev_uid    = None
        first_atime = None
        skip        = False
        for uid, atime, result, is_parked in cur:
            if prev_uid != uid:
                prev_uid    = uid
                first_atime = atime
                # Only report on cases for which the earliest
                # available data is a successfully loaded,
                # non-parked page.
                skip = result != 'ok' or is_parked

            if skip: continue

            years = (atime - first_atime).total_seconds() / ONE_YEAR

            if result == 'ok':
                result = 'parked' if is_parked else 'live'
            elif result == 'host not found':
                result = 'dns error'
            else:
                result = 'http error'

            wr.writerow((uid, srcgrps[uid], years, result))

def main():
    db = psycopg2.connect(dbname=sys.argv[1])
    archive = sys.argv[2]
    country = sys.argv[3]
    if len(sys.argv) > 4:
        vantage = sys.argv[4]
    else:
        vantage = ''

    with db, sys.stdout:
        urls = get_urls_can_analyze(db, archive)
        srcgrps = get_srcgroups_for_urls(db, urls)

        wr = csv.writer(sys.stdout, dialect='unix', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(("url", "group", "years", "status"))
        write_events_for_urls(wr, db, archive, country, vantage,
                              urls, srcgrps)

main()
