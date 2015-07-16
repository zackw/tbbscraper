# Test cases taken from the Rust library for grapheme segmentation:
# https://github.com/sbillig/rust-grapheme/

import unittest
import codecs
import re

from grapheme_counter import n_grapheme_clusters

def to_ascii_string_literal(s):
    if s == "": return "''"
    return "'" + s.encode("unicode_escape").decode("ascii") + "'"

class TestNGC(unittest.TestCase):
    def __init__(self, text="", exp_nclusters=0):
        unittest.TestCase.__init__(self)
        self._text = text
        self._exp_nclusters = exp_nclusters

    def runTest(self):
        nclusters = n_grapheme_clusters(self._text)
        self.assertEqual(nclusters, self._exp_nclusters,
                         msg="{}: exp {} got {} clusters"
                         .format(to_ascii_string_literal(self._text),
                                 self._exp_nclusters, nclusters))

def load_tests(loader, tests, pattern):

    tests = set()

    # Boundary cases not included in GraphemeBreakTest.txt.
    # The empty string contains zero clusters.
    tests.add(("", 0))

    # Any single-character string contains one cluster, no matter
    # what class the character is.
    for singleton in ["x",          # Other
                      "\u000D",     # CR
                      "\u000A",     # LF
                      "\u00AD",     # Control
                      "\u0300",     # Extend
                      "\u0903",     # SpacingMark
                      "\u1100",     # L
                      "\u1160",     # V
                      "\u11A8",     # T
                      "\uAC00",     # LV
                      "\uAC01",     # LVT
                      "\U0001F1E6", # Regional_Indicator
                      "\uD801",     # unpaired high surrogate
                      "\uDC01",     # unpaired low surrogate
                      "\uE001",     # BMP private use
                      "\U000F0001", # suppl private use
                      "\uFFFE",     # BMP noncharacter
                      "\U0001FFFE", # suppl noncharacter
                      ]:
        tests.add((singleton, 1))

    testf_re = re.compile(r"(÷|×) ([0-9A-F]+) ")

    with open("GraphemeBreakTest.txt") as testf:
        for line in testf:
            line = line.partition("#")[0].strip()
            if not line: continue
            assert line.endswith(' ÷')
            line = line[:-1]

            chars = []
            n = 0
            for m in testf_re.finditer(line):
                if m.group(1) == '÷':
                    n += 1
                chars.append(chr(int(m.group(2), 16)))

            tests.add(("".join(chars), n))

    suite = unittest.TestSuite()
    suite.addTests(TestNGC(*params)
                   for params in sorted(tests,
                                        key = lambda p: (p[1], p[0])))
    return suite

if __name__ == '__main__':
    unittest.main()
