#! /usr/bin/python3 -u

import sys
import os

def process_batch(fname):
    with open(fname, "rt", encoding="utf-8") as inf, \
         open(fname + ".result", "wt") as ouf:
        for n, _ in enumerate(inf):
            if n % 2:
                ouf.write("1\n")

def main():
    # feh
    unbuffered_stdin = os.fdopen(sys.stdin.fileno(), 'rb', buffering=0)
    while True:
        line = unbuffered_stdin.readline().decode('utf-8')
        if not line: break
        process_batch(line.strip())
        sys.stdout.write("\n")
        sys.stdout.flush()

main()
