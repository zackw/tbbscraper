#! /usr/bin/python3

import regex as re
import sys
import unicodedata
import csv

# Segmentation formerly did not strip S* characters, leaving junk in the
# data set.
# The split RE deliberately doesn't split on whitespace or '-', because
# those can legitimately appear *inside* a word.

def _prep_cleanup_re():
    symbols = []
    digits  = []
    white   = []
    for c in range(0x10FFFF):
        x = chr(c)
        cat = unicodedata.category(x)
        # All punctuation, symbols, and whitespace, C0 and C1 controls,
        # and "format effectors" (e.g. ZWNJ, RLE).  Cn (unassigned),
        # Cs (surrogate), and Co (private use) are not stripped.
        if cat[0] in ('P', 'S'):
            # Don't strip leading and trailing hyphens and apostrophes.
            # FIXME: this really ought to be an exhaustive list of P-
            # and S-class characters that can be *part of* a word.
            if x in ('-', '‐', '\'', '’'): continue
            # These characters need to be escaped inside a character class.
            # '-' is not included because the preceding 'if' removed it.
            if (x in '\\', '[', ']'):
                symbols.append('\\' + x)
            else:
                symbols.append(x)
        elif cat[0] == 'N':
            digits.append(x)
        elif cat[0] == 'Z' or cat in ('Cc', 'Cf'):
            white.append(x)
    symbols = "".join(symbols)
    digits  = "".join(digits)
    white   = "".join(white)
    return (
        re.compile("^[" + symbols + white  + "]+"),
        re.compile("["  + symbols + white  + "]+$"),
        re.compile("^[" + symbols + white  + digits + "'’\\-‐" + "]+$"),
        re.compile("["  + symbols + digits + "]+")
    )

left_cleanup_re, right_cleanup_re, all_numeric_re, split_re = _prep_cleanup_re()

# modified version of the URL-detection regexp from
# https://gist.github.com/dperini/729294 - IP addresses are *not*
# validated.
url_re = re.compile(r"""
  ^
  # protocol identifier
  (?:(?:https?|ftp)://)?
  # user:pass authentication
  (?:\S+(?::\S*)?@)?
  (?:
    # IPv4 address
    \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} |
    # hostname
    (?:(?:[a-z0-9\u00a1-\U0010ffff]-*)*[a-z0-9\u00a1-\U0010ffff]+)
    # domain name
    (?:\.(?:[a-z0-9\u00a1-\U0010ffff]-*)*[a-z0-9\u00a1-\U0010ffff]+)*
    # TLD identifier
    (?:\.(?:[a-z\u00a1-\U0010ffff]{2,}))
    # TLD may end with dot
    \.?
  )
  # port number
  (?::\d{2,5})?
  # resource path
  (?:[/?#]\S*)?
  $
""", re.VERBOSE|re.IGNORECASE)

def iter_txt(fp):
    for line in fp:
        yield line.strip().split(",", 1)

def iter_csv(fp):
    rd = iter(csv.reader(fp))
    while True:
        try:
            line = next(rd)
        except csv.Error:
            pass
        except StopIteration:
            break

        yield line[0], line[1]

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--csv':
        raw = iter_csv(sys.stdin)
    else:
        raw = iter_txt(sys.stdin)

    with open("symbols.txt", "w") as symbols, \
         open("urls.txt", "w")    as urls, \
         open("numeric.txt", "w") as numeric, \
         open("words.txt", "w")   as words:
        for lang, token in raw:
            tk = left_cleanup_re.sub("", token)
            if not tk:
                symbols.write("{},{}\n".format(lang, token))
                continue
            tk = right_cleanup_re.sub("", tk)
            # If left_cleanup_re did not erase the whole token, neither will
            # right_cleanup_re.
            if url_re.match(tk):
                urls.write("{},{}\n".format(lang, tk))
                continue
            if all_numeric_re.match(tk):
                numeric.write("{},{}\n".format(lang, tk))
                continue

            for word in split_re.split(tk):
                word = word.strip()
                if word:
                    words.write("{},{}\n".format(lang, word))

main()
