#! /usr/bin/python3

import sys
import pagedb
import hashlib
import base64
import collections

class page_data:
    __slots__ = ('count', 'length', 'text', 'url')
    def __init__(self):
        self.count = 0
        self.length = 0
        self.text = None
        self.url = None

class agg_data:
    __slots__ = ('count', 'length', 'total_length')
    def __init__(self):
        self.count = 0
        self.length = 0
        self.total_length = 0

def main():
    db = pagedb.PageDB(sys.argv[1])

    stats = collections.defaultdict(page_data)
    for page in db.get_pages(limit=100000):
        #sys.stderr.write("{!r}\t{!r}\n"
        #                 .format(page.page_id, page.url))

        text = page.text_content.encode('utf-8')
        h = hashlib.sha256(text).digest()
        s = stats[h]
        s.count += 1
        if s.text is None:
            s.text = text
            s.length = len(text)
            s.url = page.url
        else:
            if s.text != text:
                sys.stderr.write("COLLISION: {}: {} != {}\n"
                                 .format(base64.b64encode(h),
                                         s.url, page.url))

    agg = collections.defaultdict(agg_data)
    for stat in stats.values():
        a = agg[stat.count]
        a.count += 1
        a.length += stat.length
        a.total_length += stat.count * stat.length

    sys.stdout.write(
        "n\tcount\tlen_uniq\tcumlen_uniq\tlen_total\tcumlen_total\n")
    cumlen_uniq = 0
    cumlen_total = 0
    for n, a in sorted(agg.items()):
        cumlen_uniq += a.length
        cumlen_total += a.total_length
        sys.stdout.write(
            "{n}\t{count}\t{len_uniq}\t{cumlen_uniq}\t{len_total}\t{cumlen_total}\n"
            .format(n=n,
                    count=a.count,
                    len_uniq=a.length,
                    cumlen_uniq=cumlen_uniq,
                    len_total=a.total_length,
                    cumlen_total=cumlen_total))

main()
