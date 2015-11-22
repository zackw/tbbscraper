#! /usr/bin/python3

import difflib
import psycopg2
import sys

interesting = frozenset(('zh', 'zh-Hant', 'ja', 'vi', 'th',
                         'ar', 'fa', 'ku', 'ps', 'ur'))
def is_interesting(s, interesting=interesting):
    for chunk in s:
        if chunk['l'] in interesting:
            return True
    return False

def flatten_segmented(s):
    rv = []
    for chunk in s:
        l = chunk['l']
        rv.extend("{}  {}\n".format(l, w)
                  for w in chunk['t'])
    return rv

def compare_segmented(out, id, p, q):
    if is_interesting(p) or is_interesting(q):
        p = flatten_segmented(p)
        q = flatten_segmented(q)
        out.writelines(difflib.unified_diff(p, q, str(id), str(id)))

def main():
    out = sys.stdout
    db = psycopg2.connect(dbname=sys.argv[1])
    cur = db.cursor()
    cur.execute("SELECT p.id, p.segmented, q.segmented"
                "  FROM extracted_plaintext p, extracted_pt_resegment q"
                " WHERE p.id = q.id AND p.segmented IS NOT NULL"
                "   AND p.segmented <> q.segmented")
    for row in cur:
        compare_segmented(out, *row)

main()
