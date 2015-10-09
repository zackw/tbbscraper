#! /usr/bin/python3

import multiprocessing
import collections
import pagedb
import sys
import os
import csv
import time
import cld2
import re

##
## Selection of words to translate
##

# Common types of junk that can be weeded out a priori:
not_a_real_word_re = re.compile(r"""\A (?:
  [\W\d]+                         # entirely nonword characters and digits
| (?:[-_a-z0-9]+\.)+?[-_a-z0-9]+  # DNS name
  (?:/[-_a-z0-9/.:;?&=%#]+)?      # possibly followed by a path or query
  ) \Z""", re.VERBOSE | re.IGNORECASE)

def acceptable_word(lang, word):
    if not_a_real_word_re.match(word):
        return False
    return True
    #top3 = cld2.detect(word, lang_hint=lang)
    #if any(x.code == lang for x in top3):
    #    return True, top3
    #else:
    #    return False, top3

def lang_codes(top3):
    # discriminate words rejected by not_a_real_word_re
    if len(top3) == 0:
        return ["zxx", "zxx", "zxx"]

    rv = ["und", "und", "und"]
    for i, l in enumerate(top3):
        rv[i] = l.code
    return rv

def lang_scores(top3):
    rv = [0,0,0]
    for i, l in enumerate(top3):
        rv[i] = l.percent
    return rv

def find_words_for_language(db, lang):
    """Select the words in the corpus for language LANG which have
       the highest overall nfidf scores in the most documents, using
       raw cwf as a tiebreaker.  These are the words which should be
       most predictive of the content of the most documents.
    """
    _, cwf = db.get_corpus_statistic('cwf', lang, False)
    _, rdf = db.get_corpus_statistic('rdf', lang, False)

    max_nfidf = collections.defaultdict(lambda: 0)

    for pg in db.get_page_texts(where_clause=
                                "p.has_boilerplate=false and p.lang_code='{}'"
                                .format(lang),
                                load=['nfidf']):
        for word, nfidf in pg.nfidf.items():
            max_nfidf[word] = max(max_nfidf[word], nfidf)


    table = [ (nfidf, rdf[word], cwf[word], word)
              for word, nfidf in max_nfidf.items() ]
    table.sort(key=lambda row: (-row[0], -row[1], -row[2], row[3]))
    output_tables(lang, table)
    return lang

def output_tables(lang, table):
    with open("word_ranks/{}.csv".format(lang), "wt", newline="") as gf, \
         open("word_ranks/excluded/{}.csv".format(lang), "wt", newline="") as bf:
        gw = csv.writer(gf, dialect='unix', quoting = csv.QUOTE_MINIMAL)
        bw = csv.writer(bf, dialect='unix', quoting = csv.QUOTE_MINIMAL)
        gw.writerow(("nfidf", "rdf", "cwf", "word"))
        bw.writerow(("nfidf", "rdf", "cwf", "word"))
        for nfidf, rdf, cwf, word in table:
            acc = acceptable_word(lang, word)
            row = [nfidf, rdf, cwf, word]
            if acc:
                gw.writerow(row)
            else:
                bw.writerow(row)

    return lang

##
## Master control
##

DATABASE = None
def worker_init(dbname):
    global DATABASE
    DATABASE = pagedb.PageDB(dbname)

def fwfl_shim(lang):
    return find_words_for_language(DATABASE, lang)

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

def main():
    db = pagedb.PageDB(sys.argv[1])
    lang_codes = db.lang_codes
    del db

    pool = multiprocessing.Pool(initializer=worker_init,
                                initargs=(sys.argv[1],))
    start = time.time()
    sys.stderr.write("{}: processing {} languages...\n"
                     .format(fmt_interval(0), len(lang_codes)))
    for finished in pool.imap_unordered(fwfl_shim, lang_codes):
        sys.stderr.write("{}: {}\n".format(fmt_interval(time.time() - start),
                                           finished))

main()
