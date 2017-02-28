# Consolidated single-file version of
# "Extract data from websites using basic statistical magic"
# from https://github.com/datalib/libextract
# and https://github.com/datalib/StatsCounter

__all__ = ('extract_from_html',)

from   collections import Counter
from   functools   import partial
from   heapq       import nlargest
from   io          import BytesIO
from   lxml.html   import parse, HTMLParser
from   operator    import itemgetter
import statistics  as     stats

#
# StatsCounter
#

class StatsCounter(Counter):
        def mean(self):
                return stats.mean(self.values())

        def median(self):
                return stats.median(self.values())

        def median_low(self):
                return stats.median_low(self.values())

        def median_high(self):
                return stats.median_high(self.values())

        def median_grouped(self):
                return stats.median_grouped(self.values())

        def mode(self):
                return stats.mode(self.values())

        def variance(self):
                return stats.variance(self.values())

        def pvariance(self):
                return stats.pvariance(self.values())

        def stdev(self, ):
                return stats.stdev(self.values())

        def pstdev(self):
                return stats.pstdev(self.values())

        def best_pair(self):
                return self.most_common(1)[0]

        def argmax(self):
                key, _ = self.best_pair()
                return key

        def max(self):
                _, value = self.best_pair()
                return value

        def normalize(self):
                """
                Sum the values in a Counter, then create a new Counter
                where each new value (while keeping the original key)
                is equal to the original value divided by sum of all the
                original values (this is sometimes referred to as the
                normalization constant).
                https://en.wikipedia.org/wiki/Normalization_(statistics)
                """
                total = sum(self.values())
                stats = {k: (v / float(total)) for k, v in self.items()}
                return StatsCounter(stats)

        def get_weighted_random_value(self):
                """
                This will generate a value by creating a cumulative distribution,
                and a random number, and selecting the value who's cumulative
                distribution interval contains the generated random number.

                For example, if there's 0.7 chance of generating the letter "a"
                and 0.3 chance of generating the letter "b", then if you were to
                pick one letter 100 times over, the number of a's and b's you
                would have are likely to be around 70 and 30 respectively.

                The mechanics are known as "Cumulative distribution functions"
                (https://en.wikipedia.org/wiki/Cumulative_distribution_function)
                """
                from bisect import bisect
                from random import random
                #http://stackoverflow.com/questions/4437250/choose-list-variable-given-probability-of-each-variable

                total = sum(self.values())

                P = [(k, (v / float(total))) for k, v in self.items()]

                cdf = [P[0][1]]
                for i in range(1, len(P)):
                        cdf.append(cdf[-1] + P[i][1])

                return P[bisect(cdf, random())][0]


        def transform(self, key):
                dist = self
                newdist = StatsCounter()

                for k, v in dist.items():
                        newdist[key(k, v)] += v

                return newdist

#
# libextract.core
#

SELECT_PARENTS = '//body//*/..'

TOP_FIVE = 5

def parse_html(fileobj, encoding):
    """
    Given a file object *fileobj*, get an ElementTree instance.
    The *encoding* is assumed to be utf8.
    """
    parser = HTMLParser(encoding=encoding, remove_blank_text=True)
    return parse(fileobj, parser)


def pipeline(data, funcs):
    """
    Pipes *functions* onto a given *data*, where the result
    of the previous function is fed to the next function.
    """
    for func in funcs:
        data = func(data)
    return data


def select(etree, query=SELECT_PARENTS):
    return etree.xpath(query)


def measure(nodes):
    return [(node, StatsCounter([child.tag for child in node]))
            for node in nodes]

def rank(pairs, key=lambda x: x[1].most_common(1)[0][1],
         count=TOP_FIVE):
    return nlargest(count, pairs, key=key)


def finalise(ranked):
    for node, metric in ranked:
        yield node

def extract_from_html(document, encoding='utf-8', count=None):
    if isinstance(document, bytes):
        document = BytesIO(document)

    crank = partial(rank, count=count) if count else rank

    return pipeline(
        parse_html(document, encoding=encoding),
        (select, measure, crank, finalise)
        )
