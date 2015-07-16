#! /usr/bin/python3

import collections
import csv
import glob
import json
import multiprocessing
import os
import re
import sys
import time

from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError

def ensure_directory(d):
    try:
        os.mkdir(d)
    except FileExistsError:
        pass

# This can't be done with defaultdict, but __missing__ is a feature of
# dict in general!
class default_identity_dict(dict):
    def __missing__(self, key): return key

# Map CLD2's names for a few things to Google Translate's names.
REMAP_LCODE = default_identity_dict({
    "zh-Hant" : "zh-TW"
})

# Budget for API requests (in dollars)
BUDGET = 500

# Fee for the service, in characters per dollar.  Currently the official
# rate is 1,000,000 characters for $20.  Requests for the list of supported
# languages do not count toward the budget.
CHARS_PER_DOLLAR = 50000

# Maximum number of characters per POST request.  The documentation is
# a little vague about exactly how you structure this, but I *think*
# it means to say that if you use POST then you don't have to count
# the other parameters and repeated &q= constructs toward the limit.
CHARS_PER_POST = 5000

# There is also a completely undocumented limit of 128 q= segments per
# translation request.
WORDS_PER_POST = 128

##
## Modified version of
## http://thomassileo.com/blog/2012/03/26/using-google-translation-api-v2-with-python/
##

with open(os.path.join(os.environ["HOME"], ".google-api-key"), "rt") as f:
    API_KEY = f.read().strip()

TRANSLATE_URL = \
    "https://www.googleapis.com/language/translate/v2"
GET_LANGUAGES_URL = \
    "https://www.googleapis.com/language/translate/v2/languages"

def do_GET(url, params):
    url = url + '?' + urlencode(params)
    return json.loads(urlopen(url).read().decode('utf-8'))

def do_POST(url, postdata):
    req = Request(url, method='POST',
                  data=urlencode(postdata).encode('utf-8'),
                  headers={
                      'Content-Type':
                          'application/x-www-form-urlencoded;charset=utf-8',
                      'X-HTTP-Method-Override': 'GET'
                  })
    while True:
        try:
            return json.loads(urlopen(req).read().decode('utf-8'))
        except HTTPError as e:
            if (e.code == 403 and e.reason == "Forbidden" and
                json.loads(e.read().decode("utf-8"))["error"]["message"]
                == "User Rate Limit Exceeded"):
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(15)
                continue
        sys.stderr.write("{} {}\n".format(e.code, e.reason))
        sys.stderr.write(repr(e.read()))
        sys.stderr.write("\n")
        raise SystemExit(1)

def get_translations(source, target, words):
    params = [('key', API_KEY), ('source', source), ('target', target)]
    params.extend(('q', word) for word in words)
    blob = do_POST(TRANSLATE_URL, params)
    return list(zip(words,
                    (x["translatedText"]
                     for x in blob["data"]["translations"])))

def get_google_languages():
    blob = do_GET(GET_LANGUAGES_URL, [('key', API_KEY)])
    # Don't bother translating English into English.
    return frozenset(x["language"] for x in blob["data"]["languages"]
                     if x["language"] != "en")

def translate_block(lc, wordlist, translated):
    if not wordlist:
        return 0
    i = 0
    nwords = len(wordlist)
    nchars = 0
    this_block = []

    while i < nwords and i < WORDS_PER_POST:
        x = wordlist[i]
        l = len(x)
        if l > CHARS_PER_POST:
            sys.stdout.write("{}: word too long, skipping: {}\n"
                             .format(lc, x))
            i += 1
            continue
        if nchars + l > CHARS_PER_POST:
            break
        this_block.append(x)
        nchars += l
        i += 1

    sys.stdout.write("{}: translating {} words {} chars..."
                     .format(lc, len(this_block), nchars))
    sys.stdout.flush()

    translations = get_translations(REMAP_LCODE[lc], 'en', this_block)
    sys.stdout.write("ok\n")
    sys.stdout.flush()

    for wl, we in translations:
        translated[lc][wl] = we

    update_translated_on_disk(translated, lc)

    del wordlist[:i]
    return nchars

##
## Selection of words to translate
##

def update_translated_on_disk(translated, lc):
    new_f = "word_trans/{}.new.csv".format(lc)
    old_f = "word_trans/{}.csv".format(lc)
    with open(new_f, "wt", newline='') as f:
        wr = csv.DictWriter(f, (lc, "en"),
                            dialect='unix', quoting=csv.QUOTE_MINIMAL)
        wr.writeheader()
        for wl, we in sorted(translated[lc].items()):
            wr.writerow({ lc: wl, "en": we })

    os.rename(new_f, old_f)

def load_translated(google_langs):
    translated = {}
    for fname in glob.glob("word_trans/*.csv"):
        base = os.path.basename(fname)
        lc = os.path.splitext(base)[0]
        if lc not in google_langs:
            continue

        sys.stdout.write("Loading translated: {}...\n".format(lc))
        with open(fname, "rt", newline="") as f:
            rd = csv.DictReader(f)
            translated[lc] = { row[lc] : row["en"]
                               for row in rd }
    return translated

def load_todo(google_langs, translated):
    todo = {}
    for fname in glob.glob("word_ranks/*.csv"):
        base = os.path.basename(fname)
        lc = os.path.splitext(base)[0]
        if REMAP_LCODE[lc] not in google_langs:
            continue

        sys.stdout.write("Loading todo: {}...\n".format(lc))
        if lc in translated:
            already = translated[lc]
        else:
            already = {}
            translated[lc] = already

        with open(fname, "rt", newline="") as f:
            rd = csv.DictReader(f)
            todo[lc] = [row["word"] for row in rd
                        if row["word"] not in already]
            if not todo[lc]:
                del todo[lc]

    return todo

##
## Master control
##

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

def count_remaining(todo):
    words = 0
    chars = 0
    langs = 0
    for lang, wordlist in todo.items():
        if wordlist:
            langs += 1
            words += len(wordlist)
            chars += sum(len(w) for w in wordlist)

    return words, chars, langs

def main():
    ensure_directory("word_ranks")
    ensure_directory("word_trans")

    google_langs = get_google_languages()
    translated = load_translated(google_langs)
    todo = load_todo(google_langs, translated)

    character_budget = BUDGET * CHARS_PER_DOLLAR
    language_order = sorted(todo.keys(), key = lambda lc: len(todo[lc]))

    done = False
    while not done:
        done = True
        for lc in language_order:
            if character_budget <= 0:
                raise SystemExit("Budget exhausted! "
                                 "{} words, {} chars in {} languages remain"
                                 .format(*count_remaining(todo)))
            wordlist = todo[lc]
            if wordlist:
                done = False
                character_budget -= translate_block(lc, wordlist, translated)
                time.sleep(0.05)

main()
