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

def output_header(wr):
    wr.writerow(("uid","interval","change","group"))

def output_change_interval(wr, src, uid,
                           time1, time2, result1, result2, parked1, parked2):

    # Only report on cases for which the earliest available data is a
    # non-parked, successfully loaded page.
    if parked1 or result1 != 'ok':
        sys.stderr.write("SKIP {} {} {} {} {} {} {} {}\n"
                         .format(src, uid, time1, time2, parked1, parked2, result1, result2))
        return
    
    sys.stderr.write("REPT {} {} {} {} {} {} {} {}\n"
                     .format(src, uid, time1, time2, parked1, parked2, result1, result2))

    interval = (time2 - time1).total_seconds()
    if result2 != result1:
        if result2 == "host not found":
            change = "dns error"
        else:
            change = "http error"

    elif parked2:
        change = "parked"

    else:
        change = "unchanged"

    wr.writerow((uid, interval, change, URL_SOURCE_GROUPS[src]))

def compute_change_intervals(wr, db, archive, country, vantage):
    with db.cursor() as cur:
        cur.execute("""
           with m(archive, country, vantage) as (values(%s, %s, %s)),
                i(uid, lodate, hidate) as (
                    select hpa.url as uid,
                           hpa.earliest_date as lodate,
                           hpa.latest_date as hidate
                      from m, collection.historical_page_availability hpa
                     where hpa.archive = m.archive
                       and hpa.processed = true
                       and cardinality(hpa.snapshots) > 0),
                s(uid, src) as (
                    select i.uid, um.name as src
                      from i, collection.urls u, collection.url_sources um
                     where i.uid = u.url and u.src = um.id
                       and i.lodate = coalesce(
                               substring(u.meta->>'timestamp' for 10)::date,
                               (u.meta->>'date')::date,
                               um.last_updated)::timestamp),
                h(uid, atime, result, is_parked) as (
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
                      and cp.access_time = i.hidate
                      and cp.html_content = ch.id
                      and cp.result = cr.id)
         select s.src, h.uid, h.atime, h.result, h.is_parked
           from h, s
          where h.uid = s.uid
       order by h.uid, h.atime
        """, (archive, country, vantage))

        intervals = {}

        o_src    = None
        o_uid    = None
        o_time   = None
        p_time   = None
        o_result = None
        o_parked = None
        skipping = None

        for src, uid, time, result, parked in cur:
            if o_uid is None:
                o_src, o_uid, o_time, o_result, o_parked = \
                  src,   uid,   time,   result,   parked
                skipping = False
                p_time   = o_time
                continue

            if o_uid != uid:
                if not skipping:
                    # This means we got all the way to the end of the
                    # data for some URL without its status having
                    # changed at all.
                    output_change_interval(wr, o_src, o_uid,
                                           o_time, p_time,
                                           o_result, o_result,
                                           o_parked, o_parked)

                o_src, o_uid, o_time, o_result, o_parked = \
                  src,   uid,   time,   result,   parked
                skipping = False
                p_time   = o_time
                continue

            if not skipping:
                if result != o_result or parked != o_parked:
                    output_change_interval(wr, o_src, o_uid,
                                           o_time, time,
                                           o_result, result,
                                           o_parked, parked)
                    skipping = True

            p_time = time


def main():
    db = psycopg2.connect(dbname=sys.argv[1])
    archive = sys.argv[2]
    country = sys.argv[3]
    if len(sys.argv) > 4:
        vantage = sys.argv[4]
    else:
        vantage = ''

    with db, sys.stdout:
        wr = csv.writer(sys.stdout, dialect='unix', quoting=csv.QUOTE_MINIMAL)
        output_header(wr)
        compute_change_intervals(wr, db, archive, country, vantage)

main()
