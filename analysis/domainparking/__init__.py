#! /usr/bin/python3

import collections
import configparser
import os
import regex as re

__all__ = ('ParkingClassifier', 'ParkingClassification')

MODE_FILE = os.path.join(os.path.dirname(__file__), "modes.cf")
RULE_FILE = os.path.join(os.path.dirname(__file__), "rules.cf")

class Ruleset:
    """A set of tagged regular expressions."""
    def __init__(self, label, ruledict, only=None):
        self.label = label
        self.rules = [
            (tag, re.compile(rule, re.VERBOSE|re.IGNORECASE))
            for tag, rule in ruledict.items()
            if (only is None or tag in only)
        ]

    def test(self, text):
        """Match TEXT against all of the regular expressions in this
           set, and return the tags of those that matched."""
        return [
            tag for tag, rule in self.rules
            if rule.search(text)
        ]

ParkingClassification = collections.namedtuple(
    "ParkingClassification",
    ("is_parked", "rules_matched"))

class ParkingClassifier:
    """Classifier which decides whether a webpage is a domain-parking
       site.

       Methods:
           isParked(html)  - returns a named 2-tuple:
                             (is_parked, rules_matched)
                             is_parked is true or false, and rules is
                             the list of all rules that matched.

       Properties:
           mode            - The classification mode (see modes.cf)
           size_limit      - Pages larger than this are assumed not
                             to be parked.
    """

    def __init__(self, *,
                 mode='full',
                 size_limit=200000,
                 modefile=MODE_FILE,
                 rulefile=RULE_FILE):

        self.mode       = mode
        self.size_limit = size_limit

        mode_p = configparser.ConfigParser(interpolation=None,
                                           allow_no_value=True)
        with open(modefile) as m:
            mode_p.read_file(m, modefile)

        if mode == 'full':
            only = None
        elif mode in mode_p:
            only = frozenset(mode_p[mode].keys())
        else:
            raise ValueError("no mode definition for {!r} in {!r}"
                             .format(mode, modefile))

        rule_p = configparser.ConfigParser(interpolation=None)
        with open(rulefile) as r:
            rule_p.read_file(r, rulefile)

        for ruleset in ("strong", "weak1", "weak2"):
            if ruleset not in rule_p:
                raise ValueError("ruleset {!r} missing from {!r}"
                                 .format(ruleset, rulefile))

        self.strong_rules = Ruleset("strong", rule_p["strong"], only)
        self.weak_rules_1 = Ruleset("weak1",  rule_p["weak1"],  only)
        self.weak_rules_2 = Ruleset("weak2",  rule_p["weak2"],  only)

    def isParked(self, html):
        """Test whether HTML appears to be a webpage from a parked domain.
           Returns a 2-tuple (is_parked, rules_matched) where is_parked
           is a boolean and rules_matched is the list of all rules
           that matched.

           A page is considered to be parked if it matches at least
           one of the "strong" rules, or if it matches at least one of
           the "weak1" rules _and_ at least one of the "weak2" rules.
        """

        m_strong = self.strong_rules.test(html)
        m_weak1  = self.weak_rules_1.test(html)
        m_weak2  = self.weak_rules_2.test(html)

        is_parked = bool(m_strong) or (bool(m_weak1) and bool(m_weak2))
        rules_matched = m_strong + m_weak1 + m_weak2
        rules_matched.sort()
        return ParkingClassification(is_parked, rules_matched)

#
# Self-tests
#

def testParkedSample(content_dir, filename, classifier, outf):
    import time, datetime

    ok = 0
    errors = []
    start = time.monotonic()

    with open(filename) as f:
        for line in f:
            data = line[:-1].split(',')
            id = data[0]
            if data[6] == 'parked': cls = True
            elif data[6] == 'notparked': cls = False
            else:
                errors.append("{}: {!r} is not 'parked' or 'notparked'"
                              .format(id, data[6]))
                continue

            content_file = content_dir + '/' + id + '.html'
            with open(content_file, encoding='utf-8') as cf:
                content = cf.read()

            result = classifier.isParked(content)
            if(result.is_parked == cls):
                ok += 1
            else:
                errors.append("{}: exp {} got {} rm {}"
                              .format(id, cls, result.is_parked,
                                      result.rules_matched))

    interval = datetime.timedelta(seconds=time.monotonic() - start)

    errors.sort()
    outf.write("{} (mode {!r}): {}\n"
               "OK: {}\n"
               "Errors:\n  {}\n\n"
               .format(filename, classifier.mode, interval, ok,
                       "\n   ".join(errors)))

    return (not errors)

def testRules(mode, outf, samples):
    classifier = ParkingClassifier(mode=mode)
    success = True
    for i in range(0, len(samples), 2):
        success = testParkedSample(
            samples[i], samples[i+1], classifier, outf) and success
    return success

if __name__ == '__main__':
    import sys
    success = True
    for mode in ('full', 'balanced', 'min'):
        success = testRules(mode, sys.stdout, sys.argv[1:]) and success
    sys.exit(0 if success else 1)
