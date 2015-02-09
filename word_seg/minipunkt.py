# Cut-down version of
# Natural Language Toolkit: Punkt sentence tokenizer
# containing only the generic *word* tokenizer.
#
# Copyright (C) 2001-2014 NLTK Project
# Algorithm: Kiss & Strunk (2006)
# Author: Willy <willy@csse.unimelb.edu.au> (original Python port)
#         Steven Bird <stevenbird1@gmail.com> (additions)
#         Edward Loper <edloper@gmail.com> (rewrite)
#         Joel Nothman <jnothman@student.usyd.edu.au> (almost rewrite)
#         Arthur Darcet <arthur@darcet.fr> (fixes)
# URL: <http://nltk.org/>
# For license information, see LICENSE.TXT

import re

class WordTokenizer:
    def __init__(self):

        _re_non_word_chars   = r"(?:[?!)\";}\]\*:@\'\({\[])"
        _re_multi_char_punct = r"(?:\-{2,}|\.{2,}|(?:\.\s){2,}\.)"
        _re_word_start    = r"[^\(\"\`{\[:;&\#\*@\)}\]\-,]"

        _word_tokenize_fmt = r'''(
        %(MultiChar)s
        |
        (?=%(WordStart)s)\S+?  # Accept word characters until end is found
        (?= # Sequences marking a word's end
            \s|                                 # White-space
            $|                                  # End-of-string
            %(NonWord)s|%(MultiChar)s|          # Punctuation
            ,(?=$|\s|%(NonWord)s|%(MultiChar)s) # Comma if at end of word
        )
        |
        \S
        )'''

        self._re_word_tokenizer = re.compile(
            _word_tokenize_fmt %
            {
                'NonWord':   _re_non_word_chars,
                'MultiChar': _re_multi_char_punct,
                'WordStart': _re_word_start,
            },
            re.UNICODE | re.VERBOSE
        )

    def tokenize(self, text):
        return self._re_word_tokenizer.findall(text)
