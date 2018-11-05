#! /usr/bin/python3

import collections
import os
import sys
import time
import unicodedata

import psycopg2
import requests
import word_seg

##
## Utility
##

def fmt_interval(interval):
    m, s = divmod(interval, 60)
    h, m = divmod(m, 60)
    return "{}:{:>02}:{:>05.2f}".format(int(h), int(m), s)

start = None
def elapsed():
    stop = time.monotonic()
    global start
    if start is None:
        start = stop
    return fmt_interval(stop - start)

# This can't be done with defaultdict, but __missing__ is a feature of
# dict in general!
class default_identity_dict(dict):
    def __missing__(self, key): return key

# Map CLD2's names for a few things to Google Translate's names.
CLD2_TO_GOOGLE = default_identity_dict({
    "zh-Hant" : "zh-TW"
})
GOOGLE_TO_CLD2 = default_identity_dict({
    "zh-TW" : "zh-Hant"
})


##
## Modified version of
## http://thomassileo.com/blog/2012/03/26/using-google-translation-api-v2-with-python/
##

# Maximum number of characters per POST request.  The documentation is
# a little vague about exactly how you structure this, but I *think*
# it means to say that if you use POST then you don't have to count
# the other parameters and repeated &q= constructs toward the limit.
CHARS_PER_POST = 5000

# There is also a completely undocumented limit of 128 q= segments per
# translation request.
WORDS_PER_POST = 128

# Words longer than this are liable to (a) actually be some sort of
# HTML spew, and (b) cause Postgres to complain about not being able
# to index things larger than "1/3 of a buffer page".  Because of (a),
# we set the limit well below the threshold that triggers (b).
WORD_LENGTH_LIMIT = 750

with open(os.path.join(os.environ["HOME"], ".google-api-key"), "rt") as f:
    API_KEY = f.read().strip()

TRANSLATE_URL = \
    "https://www.googleapis.com/language/translate/v2"
GET_LANGUAGES_URL = \
    "https://www.googleapis.com/language/translate/v2/languages"

SESSION = requests.Session()

def do_GET(url, params):
    return SESSION.get(url, params=params).json()

def do_POST(url, postdata):
    while True:
        try:
            resp = SESSION.post(url, data=postdata, headers={
                'Content-Type':
                'application/x-www-form-urlencoded;charset=utf-8',
                'X-HTTP-Method-Override': 'GET'
            })
            resp.raise_for_status()
            return resp.json()
        except Exception:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(15)
            continue

def get_translations(source, target, words):
    blob = do_POST(TRANSLATE_URL, {
        'key': API_KEY,
        'source': source,
        'target': target,
        'q': words
    })
    return list(zip(words,
                    (unicodedata.normalize("NFKC", x["translatedText"])
                     .casefold()
                     for x in blob["data"]["translations"])))

def get_google_languages():
    blob = do_GET(GET_LANGUAGES_URL, {'key' : API_KEY})
    # Don't bother translating English into English.
    return frozenset(lc for lc in (
            GOOGLE_TO_CLD2[x["language"]] for x in blob["data"]["languages"]
        ) if lc != "en")

def translate_block(db, cur, lc, wordlist):
    if not wordlist:
        return

    translations = []
    skipped = 0
    nwords = len(wordlist)
    nchars = 0
    to_translate = []

    with db:
        while wordlist and len(to_translate) < WORDS_PER_POST:
            x = wordlist.pop()
            l = len(x)
            if l > WORD_LENGTH_LIMIT:
                sys.stdout.write("{}: word too long, skipping: {}\n"
                                 .format(lc, x))
                skipped += 1
                continue

            # load_todo isn't supposed to ever return words that are already
            # in the database, but sometimes it does anyway, and I've given
            # up trying to fix it.
            cur.execute("SELECT engl FROM translations"
                        " WHERE lang = %s AND word = %s",
                        (lc, x))
            if len(cur.fetchall()) > 0:
                sys.stdout.write("{}: word already in database, skipping: {}\n"
                                 .format(lc, x))
                skipped += 1
                continue
 
            if word_seg.is_nonword(x):
                translations.append((x, x))
                continue

            u = word_seg.is_url(x)
            if u:
                translations.append((x, u))
                continue

            if nchars + l > CHARS_PER_POST:
                wordlist.add(x)
                break

            to_translate.append(x)
            nchars += l

        sys.stdout.write("{} {}: translating {} words {} chars, "
                         "{} passthru, {} left..."
                         .format(elapsed(), lc, len(to_translate), nchars,
                                 len(translations), len(wordlist)))
        sys.stdout.flush()

        try:
            if to_translate:
                translations.extend(get_translations(CLD2_TO_GOOGLE[lc], 'en',
                                                     to_translate))
            sys.stdout.write("ok\n")
            sys.stdout.flush()

        except Exception as e:
            sys.stdout.write("error ({})\n".format(e))
            sys.stdout.flush()
            wordlist.update(to_translate)
            raise

        for word, engl in translations:
            try:
                cur.execute("SAVEPOINT insert_one_translation")
                cur.execute("INSERT INTO translations (lang, word, engl)"
                            "VALUES(%s,%s,%s)", (lc, word, engl))
                cur.execute("RELEASE SAVEPOINT insert_one_translation")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT insert_one_translation")
                sys.stdout.write("INSERT ({}, {}, {}) threw {}\n"
                                 .format(lc, word, engl, e))

def load_todo(cur, can_translate):
    # N.B. for the query planner to realize that this is an anti-join,
    # the WHERE and the ON must have matching conditions.
    cur.execute("""
        select w.lang, w.word from (
            select distinct
                l as lang, jsonb_array_elements_text(t) as word
            from (
                select a->>'l' as l, a->'t' as t from (
                    select jsonb_array_elements(segmented) as a
                      from extracted_plaintext
                     where segmented is not null
                ) _
            ) __ where l = any(%s)
        ) w
        left join translations t
          on w.lang = t.lang and w.word = t.word
       where t.lang is null and t.word is null
    """, (sorted(can_translate),))

    todo = collections.defaultdict(set)
    count = 0
    for lang, word in cur:
        todo[lang].add(word)
        count += 1
        if count % 10000 == 0:
            todo_report(todo)

    todo_report(todo)
    return todo

def todo_report(todo):
    words = 0
    chars = 0
    langs = 0
    for lang, wordlist in todo.items():
        if wordlist:
            langs += 1
            words += len(wordlist)
            chars += sum(len(w) for w in wordlist)

    sys.stdout.write("{}: todo {} words, {} chars in {} languages\n"
                     .format(elapsed(), words, chars, langs))

def main():
    sys.stdout.write("{}: getting translatable languages..."
                     .format(elapsed()))
    sys.stdout.flush()
    can_translate = get_google_languages()
    sys.stdout.write("ok, {}\n".format(len(can_translate)))

    if sys.argv[1] == '--dump-translatable':
        for lang in sorted(can_translate):
            sys.stdout.write(lang)
            sys.stdout.write("\n")
        sys.exit(0)

    db = psycopg2.connect(dbname=sys.argv[1])
    cur = db.cursor()
    sys.stdout.write("{}: getting words to translate...\n"
                     .format(elapsed()))
    todo = load_todo(cur, can_translate)
    done = False
    while not done:
        done = True
        language_order = sorted(todo.keys(), key=lambda lc: (len(todo[lc]),lc))
        todo_report(todo)
        for lc in language_order:
            wordlist = todo[lc]
            if wordlist:
                done = False
                translate_block(db, cur, lc, wordlist)
                time.sleep(0.05)
            else:
                del todo[lc]
        db.commit()

main()
