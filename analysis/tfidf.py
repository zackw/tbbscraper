#! /usr/bin/python3

import collections
import multiprocessing
import pagedb
import sys
import word_seg
import math
import time

# Each multiprocess worker needs its own connection to the database.
# The simplest way to accomplish this is with a global variable, which
# is set up in the pool initializer callback, and used by the map
# workers.  (Each process has its own copy of the global.)

DATABASE = None
def worker_init(dbname, runs):
    global DATABASE
    DATABASE = pagedb.PageDB(dbname, runs)

# worker functions
def corpus_wide_statistics(lang, db):
    """Compute corpus-wide frequency and raw document frequency per term,
       and count the number of documents."""

    corpus_word_freq = collections.Counter()
    raw_doc_freq     = collections.Counter()
    n_documents      = 0

    for text in db.get_page_texts(where_clause="lang_code='{}'"
                                  .format(lang)):

        n_documents += 1
        already_this_document = set()
        for word in word_seg.segment(lang, text.contents):
            corpus_word_freq[word] += 1
            if word not in already_this_document:
                raw_doc_freq[word] += 1
                already_this_document.add(word)

    idf = compute_idf(n_documents, raw_doc_freq)
    db.update_corpus_statistics(lang, n_documents,
                                [('cwf', corpus_word_freq),
                                 ('rdf', raw_doc_freq),
                                 ('idf', idf)])

    return idf

def compute_idf(n_documents, raw_doc_freq):
    """Compute inverse document frequencies:
           idf(t, D) = log |D|/|{d in D: t in d}|
       i.e. total number of documents over number of documents containing
       the term.  Since this is within-corpus IDF we know by construction
       that the denominator will never be zero."""

    log = math.log
    return { word: log(n_documents/doc_freq)
             for word, doc_freq in raw_doc_freq.items() }

def compute_tfidf(db, lang, text, idf):
    # This is baseline tf-idf: no corrections for document length or
    # anything like that.
    tf = collections.Counter()
    for word in word_seg.segment(lang, text.contents):
        tf[word] += 1

    for word in tf.keys():
        tf[word] *= idf[word]

    db.update_text_statistic('tfidf', text.origin, tf)

def compute_nfidf(db, lang, text, idf):
    # This is "augmented normalized" tf-idf: the term frequency within
    # each document is normalized by the maximum term frequency within
    # that document, so long documents cannot over-influence scoring
    # of the entire corpus.
    tf = collections.Counter()
    for word in word_seg.segment(lang, text.contents):
        tf[word] += 1

    try:
        max_tf = max(tf.values())
    except ValueError:
        max_tf = 1

    for word in tf.keys():
        tf[word] = (0.5 + (0.5 * tf[word])/max_tf) * idf[word]

    db.update_text_statistic('nfidf', text.origin, tf)

def process_language(lang):
    db = DATABASE

    idf = corpus_wide_statistics(lang, db)
    ndoc, idf = db.get_corpus_statistic('idf', lang)

    # Note: the entire get_page_texts() operation must be enclosed in a
    # single transaction; committing in the middle will invalidate the
    # server-side cursor it holds.
    with db:
        for text in db.get_page_texts(
                where_clause="lang_code='{}'"
                .format(lang)):
            compute_tfidf(db, lang, text, idf)
            compute_nfidf(db, lang, text, idf)

    return lang

def prep_database(dbname, runs):
    db = pagedb.PageDB(dbname, runs)
    langs  = db.prepare_text_statistic('tfidf')
    langs |= db.prepare_text_statistic('nfidf')
    return langs

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

def main():
    dbname = sys.argv[1]
    runs = sys.argv[2:]
    lang_codes = prep_database(dbname, runs)

    pool = multiprocessing.Pool(initializer=worker_init,
                                initargs=(dbname, runs))

    start = time.time()
    sys.stderr.write("{}: processing {} languages...\n"
                     .format(fmt_interval(0), len(lang_codes)))
    for finished in pool.imap_unordered(process_language, lang_codes):
        sys.stderr.write("{}: {}\n".format(fmt_interval(time.time() - start),
                                           finished))

main()
