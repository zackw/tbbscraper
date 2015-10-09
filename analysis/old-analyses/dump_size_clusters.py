import base64
import csv
import collections
import pagedb
import sys

def chunkstring(string, length):
    return (string[0+i:length+i] for i in range(0, len(string), length))

class Thing:
    def __init__(self, obs, depth):
        self.hhash    = bytes(obs.html_hash)
        self.hlen     = obs.html_len
        self.hdepth   = depth
        self.hcontent = obs.html_content
        self.obsvs    = collections.defaultdict(set)
        self.n_obsvs  = 0
        self.add_obs(obs)

    def add_obs(self, obs):
        self.obsvs[obs.url].add((obs.country, obs.result))
        self.n_obsvs += 1

    def sortkey(self):
        return (self.hdepth, self.hlen, self.hhash)

    def report(self, fp):
        fp.write("{}\n    HTML length {} DOM depth {}\n"
                 .format(base64.b16encode(self.hhash).decode('ascii'),
                         self.hlen, self.hdepth))

        for url, locales in sorted(self.obsvs.items()):
            fp.write("    {}\n".format(url))
            for loc, result in sorted(locales):
                fp.write("        {:<10} {}\n"
                         .format(loc[:10], result))

        for chunk in chunkstring(self.hcontent[:512], 64):
            fp.write("    |{}|\n".format(chunk))

        fp.write("\n")

def where_clause_for_clusters(all_clusters):
    where = []
    for loc, lengths in all_clusters.items():
        where.append("(o.locale = '{}' and o.html_length in ({}))"
                     .format(loc, ','.join(str(l) for l in lengths)))
    return " or ".join(where)

def process_clusters(db, all_clusters):
    interesting_pages = {}
    for obs in db.get_page_observations(where_clause=
                                        where_clause_for_clusters(all_clusters),
                                        load=['dom_stats']):
        maxdepth = max((int(x) for x in obs.dom_stats.tags_at_depth.keys()),
                default=0)
        if obs.html_hash not in interesting_pages:
            interesting_pages[obs.html_hash] = Thing(obs, maxdepth)
        else:
            interesting_pages[obs.html_hash].add_obs(obs)
    return interesting_pages

def load_clusters(fname):
    all_clusters = {}
    with open(fname, "rt", newline="") as f:
        rd = csv.reader(f)
        next(rd) # skip header
        for row in rd:
            country = row[0][:2]
            lengths = set(int(x.partition(':')[0]) for x in row[2].split(';'))
            lengths.discard(0)
            all_clusters[country] = sorted(lengths)
    return all_clusters

def main():
    db = pagedb.PageDB("ts_analysis")
    all_clusters = load_clusters(sys.argv[1])
    interesting_pages = process_clusters(db, all_clusters)

    for thing in sorted(interesting_pages.values(),
                        key = lambda x: x.sortkey()):
        thing.report(sys.stdout)


main()
