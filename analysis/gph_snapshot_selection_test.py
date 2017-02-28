#! /usr/bin/python3

import bisect
import datetime
import psycopg2
import sys

THIRTY_DAYS = datetime.timedelta(days=30)
def select_snapshots(avail, lo, hi):
    """AVAIL is a list of datetime objects, and LO and HI are likewise
       datetime objects.  (Before anything else happens, AVAIL is
       sorted in place, and LO and HI are swapped if LO > HI.)

       Choose and return a subset of the datetimes in AVAIL, as
       follows:

          * the most recent datetime older than LO, or, if there is no such
            datetime, the oldest available datetime

          * a sequence of datetimes more recent than, or equal to, LO,
            but older than HI, separated by at least 30 days

          * the most recent datetime older than HI
    """
    if not avail: return []

    avail.sort()
    if lo > hi: lo, hi = hi, lo
    rv = []

    start = bisect.bisect_right(avail, lo)
    if start:
        start -= 1
    rv.append(avail[start])

    for i in range(start+1, len(avail)):
        if avail[i] >= hi:
            # Always take the most recent datetime older than HI, even if
            # that violates the thirty-day rule.
            if rv[-1] < avail[i-1]:
                rv.append(avail[i-1])
            return rv

        if avail[i] - rv[-1] >= THIRTY_DAYS:
            rv.append(avail[i])

    # If we get here, it means the WBM doesn't have anything _newer_
    # than 'hi', so take the last thing it does have.
    if rv[-1] < avail[-1]:
        rv.append(avail[-1])
    return rv

def test_select_snapshots(db, archive):
    cur = db.cursor()
    cur.execute("""
                SELECT earliest_date, latest_date, snapshots
                  FROM collection.historical_page_availability
                 WHERE archive = %s
                   AND processed = true
--                   AND cardinality(snapshots) >= 1
--                   AND cardinality(snapshots) <= 10
                 LIMIT 100
                """,
                (archive,))

    for lodate, hidate, snapshots in cur:

        selected = set(select_snapshots(snapshots, lodate, hidate))
        state = 0

        for s in snapshots:
            if s >= lodate and state == 0:
                sys.stdout.write(lodate.strftime("vvvv %Y-%m-%d\n"))
                state = 1
            if s >= hidate and state == 1:
                sys.stdout.write(hidate.strftime("^^^^ %Y-%m-%d\n"))
                state = 2
            sys.stdout.write(s.strftime("%Y-%m-%d "))
            if s in selected:
                sys.stdout.write("+")
            sys.stdout.write("\n")

        if state == 0:
            sys.stdout.write(lodate.strftime("vvvv %Y-%m-%d\n"))
            state = 1
        if state == 1:
            sys.stdout.write(hidate.strftime("^^^^ %Y-%m-%d\n"))

        sys.stdout.write("\n")

test_select_snapshots(psycopg2.connect(dbname=sys.argv[1]), sys.argv[2])
