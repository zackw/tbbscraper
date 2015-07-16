#! /usr/bin/python3

import collections
import csv
import hashlib
import lzma
import pagedb
import sys
import time
import zlib

class page_data:
    __slots__ = ('count', 'len_uc',
                 'len_zd', 'len_z9', 'len_xd', 'len_xf',
                 'time_zd', 'time_z9', 'time_xd', 'time_xf')
    def __init__(self):
        self.count  = 0
        self.len_uc = 0

        self.len_zd = 0
        self.len_z9 = 0
        self.len_xd = 0
        self.len_xf = 0

        self.time_zd = 0
        self.time_z9 = 0
        self.time_xd = 0
        self.time_xf = 0

    def asdict(self):
        return { k: getattr(self, k) for k in self.__slots__ }

    def _t(self, slot, fn):
        start = time.process_time()
        n = len(fn())
        d = time.process_time() - start

        setattr(self, 'len_' + slot, n)
        setattr(self, 'time_' + slot, d)

    def compression_trial(self, text):
        self.len_uc = len(text)
        self._t('zd', lambda: zlib.compress(text))
        self._t('z9', lambda: zlib.compress(text, 9))

        self._t('xd',
                lambda: lzma.compress(text,
                                      format=lzma.FORMAT_RAW,
                                      filters=[{"id": lzma.FILTER_LZMA2}]))
        self._t('xf',
                lambda: lzma.compress(text,
                                      format=lzma.FORMAT_RAW,
                                      filters=[{"id": lzma.FILTER_DELTA},
                                               {"id": lzma.FILTER_LZMA2}]))

def main():
    db = pagedb.PageDB(sys.argv[1])

    start = time.process_time()
    stats = collections.defaultdict(page_data)
    for n, page in enumerate(db.get_pages(limit=100000)):
        text = page.text_content.encode('utf-8')
        h = hashlib.sha256(text).digest()
        s = stats[h]
        s.count += 1
        if len(text) > 0 and s.len_zd == 0:
            s.compression_trial(text)

        if n and not n % 1000:
            sys.stderr.write("%d pages in %.4fs\n"
                             % (n, time.process_time()-start))

    out = csv.DictWriter(sys.stdout, page_data.__slots__,
                         dialect='unix', quoting=csv.QUOTE_MINIMAL)
    out.writeheader()
    for row in stats.values():
        out.writerow(row.asdict())

main()
