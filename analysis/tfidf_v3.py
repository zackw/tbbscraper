#! /usr/bin/python3

import collections
import pagedb
import sys
import math
import time

def fmt_elapsed(start):
    interval = time.monotonic() - start
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

def compute_idf(n_documents, raw_doc_freq):
    """Compute inverse document frequencies:
           idf(t, D) = log |D|/|{d in D: t in d}|
       i.e. total number of documents over number of documents containing
       the term.  Since this is within-corpus IDF we know by construction
       that the denominator will never be zero."""

    log = math.log
    return { lang: { word: log(ndocs/doc_freq)
                     for word, doc_freq in raw_doc_freq[lang].items() }
             for lang, ndocs in n_documents.items() }

def corpus_wide_statistics(db, start):
    """Compute corpus-wide frequency and raw document frequency per term,
       and count the number of documents."""

    corpus_word_freq = collections.defaultdict(collections.Counter)
    raw_doc_freq     = collections.defaultdict(collections.Counter)
    n_documents      = collections.Counter()
    n_all_documents  = 0
    langs_in_block   = set()
    word_already_this_document = collections.defaultdict(set)
    lang_already_this_document = set()

    for text in db.get_page_texts(load = ["segmented"],
                                  where_clause =
                                  "p.segmented_text is not null"):
        n_all_documents += 1
        word_already_this_document.clear()
        lang_already_this_document.clear()
        for run in text.segmented:
            lang  = run["l"]
            words = run["t"]
            if lang not in lang_already_this_document:
                n_documents[lang] += 1
                langs_in_block.add(lang)
                lang_already_this_document.add(lang)
            for word in words:
                corpus_word_freq[lang][word] += 1
                if word not in word_already_this_document[lang]:
                    raw_doc_freq[lang][word] += 1
                    word_already_this_document[lang].add(word)

        if n_all_documents % 1000 == 0:
            sys.stderr.write("[{}] CS: {} docs - {}\n"
                             .format(fmt_elapsed(start),
                                     n_all_documents,
                                     " ".join(sorted(langs_in_block))))
            langs_in_block.clear()

    sys.stderr.write("[{}] CS: {} docs - {}\n"
                     .format(fmt_elapsed(start),
                             n_all_documents,
                             " ".join(sorted(langs_in_block))))
    idf = compute_idf(n_documents, raw_doc_freq)
    sys.stderr.write("[{}] CS: IDF computed.\n"
                     .format(fmt_elapsed(start)))
    for lang in n_documents.keys():
        db.update_corpus_statistics(lang, n_documents[lang],
                                    [('cwf', corpus_word_freq[lang]),
                                     ('rdf', raw_doc_freq[lang]),
                                     ('idf', idf[lang])])

    sys.stderr.write("[{}] CS: complete.\n"
                     .format(fmt_elapsed(start)))
    return idf


def compute_doc_statistics(db, text, idf, langs_in_block):
    # tf: baseline tfidf - no correction for document length.
    # nf: augmented normalized tfidf - use max term frequency within
    #     each document to normalize, so long documents cannot over-
    #     influence scoring of the entire corpus.
    # both are computed _across_ all languages within the doc.

    allwords = collections.Counter()
    for run in text.segmented:
        langs_in_block.add(run["l"])
        for word in run["t"]:
            allwords[word] += 1

    tf = {}
    nf = {}
    if allwords:
        max_tf = max(allwords.values())

        for run in text.segmented:
            lang  = run["l"]
            words = run["t"]

            for word in words:
                w_tf  = allwords[word]
                try:
                    w_idf = idf[lang][word]
                except KeyError:
                    sys.stderr.write("*** '{}' missing IDF in '{}'\n"
                                     .format(word, lang))
                    sys.stderr.write("*** seg dump: {!r}\n".format(text.segmented))
                    raise

                tf[word] = w_tf * w_idf
                nf[word] = (0.5 + (0.5 * w_tf)/max_tf) * w_idf

    db.update_text_statistic('tfidf', text, tf)
    db.update_text_statistic('nfidf', text, nf)

def per_document_statistics(db, idf, start):

    # Note: the entire get_page_texts() operation must be enclosed in a
    # single transaction; committing in the middle will invalidate the
    # server-side cursor it holds.

    processed = 0
    langs_in_block = set()
    with db:
        for text in db.get_page_texts(load = ["segmented"],
                                      where_clause =
                                      "p.segmented_text is not null"):
            compute_doc_statistics(db, text, idf, langs_in_block)
            processed += 1

            if processed % 1000 == 0:
                sys.stderr.write("[{}] DS: {} docs - {}\n"
                                 .format(fmt_elapsed(start),
                                         processed,
                                         " ".join(sorted(langs_in_block))))
                langs_in_block.clear()

        sys.stderr.write("[{}] DS: {} docs - {}\n"
                         .format(fmt_elapsed(start),
                                 processed,
                                 " ".join(sorted(langs_in_block))))
    sys.stderr.write("[{}] DS: complete.\n"
                     .format(fmt_elapsed(start)))


def prep_database(dbname, runs, start):
    db = pagedb.PageDB(dbname, runs)
    sys.stderr.write("[{}] preparation...\n".format(fmt_elapsed(start)))
    db.prepare_text_statistic('tfidf')
    db.prepare_text_statistic('nfidf')
    sys.stderr.write("[{}] preparation complete.\n"
                     .format(fmt_elapsed(start)))
    return db

def main():
    dbname = sys.argv[1]
    runs = sys.argv[2:]
    start = time.monotonic()

    db = prep_database(dbname, runs, start)
    idf = corpus_wide_statistics(db, start)
    per_document_statistics(db, idf, start)

main()
