#! /usr/bin/python3

import csv
import glob
import os
import sys

def get_translation_sets(dirs):
    sets = {}
    for d in dirs:
        for f in glob.glob(os.path.join(d, "*.csv")):
            lang = os.path.splitext(os.path.basename(f))[0]
            if lang not in sets:
                sets[lang] = []
            sets[lang].append(f)
    return sets

def merge_translation_set(lang, files):
    words = {}
    for f in files:
        with open(f) as fp:
            rd = csv.reader(fp)
            row = next(rd)
            assert len(row) == 2
            assert row[0] == lang
            assert row[1] == 'en'
            for row in rd:
                assert len(row) == 2
                words[row[0]] = row[1]

    with open(lang + ".csv", "w") as w:
        wr = csv.DictWriter(w, (lang, "en"),
                            dialect='unix', quoting=csv.QUOTE_MINIMAL)
        wr.writeheader()
        for wl, we in sorted(words.items()):
            wr.writerow({ lang: wl, "en": we })

def main():
    sets = get_translation_sets(sys.argv[1:])
    for lang, files in sets.items():
        sys.stderr.write(lang + "\n")
        merge_translation_set(lang, files)

main()

