#! /usr/bin/python3

import csv
import glob
import html
import os
import sys

import psycopg2

def load_translation(cur, fname):
    lang = os.path.splitext(os.path.basename(fname))[0]

    with open(fname, 'r') as f:
        rd = csv.DictReader(f)
        insertion = set(
            cur.mogrify("(%s,%s,%s)",
                        (lang,
                         html.unescape(row[lang]),
                         html.unescape(row["en"])))
            for row in rd
        )
    insertion = sorted(insertion)
    sys.stderr.write("{}, {} words...".format(lang, len(insertion)))
    sys.stderr.flush()
    cur.execute(b"INSERT INTO ancillary.translations(lang, word, engl)"
                b"VALUES" + b",".join(insertion))
    sys.stderr.write("ok\n")

def main():
    db = psycopg2.connect("dbname="+sys.argv[1])
    cur = db.cursor()
    for fname in glob.glob(sys.argv[2] + "/*.csv"):
        load_translation(cur, fname)
        cur.execute("COMMIT")

main()
