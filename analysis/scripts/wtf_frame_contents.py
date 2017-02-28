#! /usr/bin/python3

import psycopg2
import regex as re
import sys
import time
import zlib

start = None
def progress(msg):
    stop = time.monotonic()
    global start
    if start is None:
        start = stop
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    sys.stderr.write("{}:{:>02}:{:>05.2f}: {}\n".format(int(h), int(m), s, msg))

def main():
    has_frames = re.compile(r"<\s*i?frame\b", re.IGNORECASE)
    has_frame_contents = re.compile(r"""
        <\s*i?frame
        (?:\s+(?:\S+)=["']?(?:(?:.(?![\"']?\s+(?:\S+)=|[>"']))+.)["']?)*
        > \s* &lt;
    """, re.IGNORECASE|re.VERBOSE)

    db = psycopg2.connect(dbname=sys.argv[1])
    cur = db.cursor()

    total = 0
    has_frames = 0
    has_frame_contents = 0
    cur.execute("SELECT content FROM collection.capture_html_content")
    for (content,) in cur:
        total += 1
        if content:
            content = zlib.decompress(content).decode("utf-8")
            if has_frames.search(content):
                has_frames += 1
                if has_frame_contents.search(content):
                    has_frame_contents += 1

        if total % 1000 == 0:
            progress("{}/{}/{}".format(total, has_frames, has_frame_contents))

    progress("{}/{}/{}".format(total, has_frames, has_frame_contents))

main()
