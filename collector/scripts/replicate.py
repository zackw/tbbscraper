#! /usr/bin/python3

import sys
import psycopg2

def chunks(l,n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i+n]

def message(s):
    sys.stdout.write(s)
    sys.stdout.flush()

class Table:
    """One table to be replicated."""
    def __init__(self, name, keycols, restcols):
        self.name = name
        self.d_name = "d_"+name
        self.r_name = "r_"+name
        self.keycols = keycols
        self.restcols = restcols
        self.allcols = keycols + restcols

    def replicate(self, srcdb, dstdb):

        dbatch = []
        sbatch = []
        dpat = "(" + ",".join("%s" for _ in self.allcols) + ")"
        spat = "(" + ",".join("%s" for _ in self.keycols) + ")"
        slen = len(self.keycols)

        # Extract all the rows to be replicated from the old database.
        # Avoid any question of what happens to the d_ view if the r_
        # table gets updated under it, by doing this all at once
        # before any updates happen.
        message("{}: reading...        ".format(self.name))
        n = 0
        with srcdb, srcdb.cursor("f_"+name) as sc:
            sc.execute("SELECT " + ",".join('"%s"' % n
                                            for n in self.allcols) +
                       "  FROM " + self.d_name)
            for row in sc:
                dbatch.append(sc.mogrify(dpat, row))
                sbatch.append(sc.mogrify(spat, row[:slen]))
                n += 1
                message("\b\b\b\b\b\b\b{:>7}".format(n))
            message("\n")

        # Now, one chunk at a time, write back the dbatch to the
        # destination database and the sbatch to the source database.
        sc = srcdb.cursor()
        dc = dstdb.cursor()
        name = self.name.encode("ascii")
        r_name = self.r_name.encode("ascii")
        spat = spat.encode("ascii")
        dpat = dpat.encode("ascii")
        n = 0
        message("{}: writing...        ".format(self.name))
        for (schk, dchk) in zip(chunks(sbatch, 1000),
                                chunks(dbatch, 1000)):
            # Open and commit a transaction for each chunk.
            with srcdb, dstdb:
                dc.execute(b"INSERT INTO %s %s VALUES %s"
                           % (name, dpat, b",".join(dchk)))
                sc.execute(b"INSERT INTO %s %s VALUES %s"
                           % (r_name, spat, b",".join(schk)))

            n += len(dchk)
            message("\b\b\b\b\b\b\b{:>7}".format(n))
        message("\n")

tables_to_replicate = [
    # ancillary data
    Table("capture_detail",       ["id"],   ["detail"]),
    Table("clab_categories",      ["code"], ["description"]),
    Table("static_list_metadata", ["id"],   ["label", "url", "last_update"]),
    Table("url_strings",          ["id"],   ["url"]),
    Table("twitter_users",        ["uid"],  ["created_at",
                                             "verified",
                                             "protected",
                                             "highest_tweet_seen",
                                             "screen_name",
                                             "full_name",
                                             "lang",
                                             "location",
                                             "description"]),

    # url sources
    Table("urls_alexa",      ["retrieval_date", "url"],
                             ["rank"]),
    Table("urls_citizenlab", ["retrieval_date", "country", "url"],
                             ["category"]),
    Table("urls_herdict",    ["url", "timestamp"],
                             ["accessible", "country"]),
    Table("urls_staticlist", ["listid", "url"], []),
    Table("urls_tweeted",    ["uid", "url"],
                             ["timestamp", "retweets", "possibly_sensitive",
                              "lang", "withheld", "hashtags"]),
    Table("urls_twitter_user_profiles",
                             ["uid", "url"], []),
    Table("urls_pinboard",   ["username", "url"],
                             ["access_time", "title", "annotation", "tags"]),

    # capture results
    Table("captured_pages", ["locale", "url"],
          ["access_time", "result", "detail", "redir_url",
           "capture_log", "html_content", "screenshot"])
]

def main():
    with open(sys.argv[1]) as cfg:
        src = cfg.readline().strip()
        dst = cfg.readline().strip()

    srcdb = psycopg2.connect(src)
    dstdb = psycopg2.connect(dst)

    for tbl in tables_to_replicate:
        tbl.replicate(srcdb, dstdb)

if __name__ == '__main__':
    main()
