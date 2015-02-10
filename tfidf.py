#! /usr/bin/python3

import collections
import multiprocessing
import pagedb
import sys
import word_seg
import math

# Each multiprocess worker needs its own connection to the database.
# The simplest way to accomplish this is with a global variable, which
# is set up in the pool initializer callback, and used by the map
# workers.  (Each process has its own copy of the global.)

DATABASE = None
def worker_init(dbname):
    global DATABASE
    DATABASE = pagedb.PageDB(dbname)

# worker functions
def corpus_wide_statistics(lang, db):
    """Compute corpus-wide frequency and raw document frequency per term,
       and count the number of documents."""

    corpus_word_freq = collections.Counter()
    raw_doc_freq     = collections.Counter()
    n_documents      = 0

    for text in db.get_page_texts(
            where_clause="p.has_boilerplate=false and p.lang_code='{}'"
            .format(lang)):

        n_documents += 1
        already_this_document = set()
        for word in word_seg.segment(lang, text.contents):
            corpus_word_freq[word] += 1
            if word not in already_this_document:
                raw_doc_freq[word] += 1
                already_this_document.add(word)

    idf = compute_idf(n_documents, raw_doc_freq)
    db.update_corpus_statistics(lang, False, n_documents,
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
    # There are a bunch of adjustments that one might want to apply to
    # tf, idf, or their combination in the literature (e.g. "cosine
    # normalization" or "augmented tf" to reduce the significance of
    # document length) but it's unclear that any of them are relevant
    # to this use scenario (all the literature I've found is about
    # document *retrieval*).
    tf = collections.Counter()
    for word in word_seg.segment(lang, text.contents):
        tf[word] += 1

    for word in tf.keys():
        tf[word] *= idf[word]

    db.update_text_statistic('tfidf', text.id, tf)

def process_language(lang):
    db = DATABASE

    idf = corpus_wide_statistics(lang, db)

    # Note: the entire get_page_texts() operation must be enclosed in a
    # single transaction; committing in the middle will invalidate the
    # server-side cursor it holds.
    with db:
        for text in db.get_page_texts(
                where_clause="p.has_boilerplate=false and p.lang_code='{}'"
                .format(lang)):
            compute_tfidf(db, lang, text, idf)

    return lang

def prep_database(dbname):
    db = pagedb.PageDB(dbname)
    db.prepare_text_statistic('tfidf')
    return set(db.lang_codes)

def main():
    lang_codes = prep_database(sys.argv[1])
    lang_codes.discard('und')
    lang_codes.discard('zxx')

    pool = multiprocessing.Pool(initializer=worker_init,
                                initargs=(sys.argv[1],))

    for finished in pool.imap_unordered(process_language, lang_codes):
        sys.stdout.write(finished)
        sys.stdout.write(' ')

    sys.stdout.write('\n')

main()
