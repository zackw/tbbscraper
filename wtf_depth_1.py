import base64
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

        for chunk in chunkstring(self.hcontent, 64):
            fp.write("    |{}|\n".format(chunk))

        for url, locales in sorted(self.obsvs.items()):
            fp.write("        {}\n".format(url))
            for loc, result in sorted(locales):
                fp.write("            {:<10} {}\n"
                         .format(loc[:10], result))

        fp.write("\n")

def main():
    db = pagedb.PageDB("ts_analysis")

    interesting_pages = {}

    for obs in db.get_page_observations(load=['dom_stats']):
        if obs.result == 'crawler failure': continue
        maxdepth = max((int(x) for x in obs.dom_stats.tags_at_depth.keys()),
                default=0)
        #if maxdepth > 1 and obs.html_len > 512:
        #    continue
        if maxdepth < 95:
            continue

        if obs.html_hash not in interesting_pages:
            interesting_pages[obs.html_hash] = Thing(obs, maxdepth)
        else:
            interesting_pages[obs.html_hash].add_obs(obs)

    for thing in sorted(interesting_pages.values(),
                        key = lambda x: x.sortkey()):

        thing.report(sys.stdout)

main()
