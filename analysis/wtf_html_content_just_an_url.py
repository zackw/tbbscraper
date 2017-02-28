#! /usr/bin/python3

import psycopg2
import sys
import zlib
import word_seg

def find_questionable_pages(cur):
    cur.execute("""
        select id, content from capture_html_content
         where length(content) > 0 and length(content) < 512
    """)

    questionables = {}
    n = 0
    for id, content in cur:
        n += 1
        content = zlib.decompress(content).decode('utf-8')
        if word_seg.is_url(content):
            questionables[id] = content
    sys.stderr.write("n={} m={}\n".format(n, len(questionables)))

    cur.execute("""
        select cp.html_content, s.url from captured_pages cp, url_strings s
         where s.id = cp.url and cp.html_content = any(%s)
    """, (list(questionables.keys()),))

    for id, url in cur:
        sys.stdout.write("URL: {}\nCNT: {}\n\n"
                         .format(url, questionables[id]))


def main():
    db = psycopg2.connect(dbname=sys.argv[1])
    cur = db.cursor()
    find_questionable_pages(cur)

main()
