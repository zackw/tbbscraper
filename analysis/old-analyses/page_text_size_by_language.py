#! /usr/bin/python3

import sys
sys.path.append("/home/zack/langid.py")

import langid
import pagedb
import hashlib
import base64
import collections

class page_data:
    __slots__ = ('lang', 'count', 'length')
    def __init__(self):
        self.lang = None
        self.count = 0
        self.length = 0

def main():
    db = pagedb.PageDB(sys.argv[1])
    stats = collections.defaultdict(page_data)
    ident = langid.LanguageIdentifier.from_model(norm_probs=False)
    for page in db.get_pages(limit=int(sys.argv[2])):

        text = page.text_content.encode('utf-8')
        h = hashlib.sha256(text).digest()
        s = stats[h]
        s.count += 1
        if s.lang is None:
            s.length = len(text)
            s.lang = ident.classify(text)[0]

    agg = collections.Counter()
    for doc in stats.values():
        agg[doc.lang] += doc.length

    for lang, nbytes in sorted(agg.items(), key=lambda kv: -kv[1]):
        sys.stdout.write("{}\t{}\n".format(lang, nbytes))

main()
