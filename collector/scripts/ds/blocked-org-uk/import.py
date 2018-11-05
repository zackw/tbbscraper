#! /usr/bin/python3

import csv
import json
import os
import psycopg2
import re
import sys
import time
import urllib.parse

def _urlsplit_forced_encoding(url):
    try:
        return urllib.parse.urlsplit(url)
    except UnicodeDecodeError:
        return urllib.parse.urlsplit(url.decode("utf-8", "surrogateescape"))

_enap_re = re.compile(br'[\x00-\x20\x7F-\xFF]|'
                      br'%(?!(?:[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}))')
def _encode_nonascii_and_percents(segment):
    segment = segment.encode("utf-8", "surrogateescape")
    return _enap_re.sub(
        lambda m: "%{:02X}".format(ord(m.group(0))).encode("ascii"),
        segment).decode("ascii")

def canon_url_syntax(url, *, want_splitresult=None):
    """Syntactically canonicalize a URL.  This makes the following
       transformations:
         - scheme and hostname are lowercased
         - hostname is punycoded if necessary
         - vacuous user, password, and port fields are stripped
         - ports redundant to the scheme are also stripped
         - path becomes '/' if empty
         - characters outside the printable ASCII range in path,
           query, fragment, user, and password are %-encoded, as are
           improperly used % signs

       You can provide either a string or a SplitResult, and you get
       back what you put in.  You can set the optional argument
       want_splitresult to True or False to force a particular
       type of output.
    """

    if isinstance(url, urllib.parse.SplitResult):
        if want_splitresult is None: want_splitresult = True
        exploded = url

    else:
        if want_splitresult is None: want_splitresult = False

        exploded = _urlsplit_forced_encoding(url)
        if not exploded.hostname:
            # Remove extra slashes after the scheme and retry.
            corrected = re.sub(r'(?i)^([a-z]+):///+', r'\1://', url)
            exploded = _urlsplit_forced_encoding(corrected)

    if not exploded.hostname:
        raise ValueError("url with no host - " + repr(url))

    scheme = exploded.scheme
    if scheme != "http" and scheme != "https":
        raise ValueError("url with non-http(s) scheme - " + repr(url))

    host   = exploded.hostname
    user   = _encode_nonascii_and_percents(exploded.username or "")
    passwd = _encode_nonascii_and_percents(exploded.password or "")
    port   = exploded.port
    path   = _encode_nonascii_and_percents(exploded.path)
    query  = _encode_nonascii_and_percents(exploded.query)
    frag   = _encode_nonascii_and_percents(exploded.fragment)

    if path == "":
        path = "/"

    # We do this even if there are no non-ASCII characters, because it
    # has the side-effect of throwing a UnicodeError if the hostname
    # is syntactically invalid (e.g. "foo..com").
    host = host.encode("idna").decode("ascii")

    if port is None:
        port = ""
    elif ((port == 80  and scheme == "http") or
          (port == 443 and scheme == "https")):
        port = ""
    else:
        port = ":{}".format(port)

    # We don't have to worry about ':' or '@' in the user and password
    # strings, because urllib.parse does not do %-decoding on them.
    if user == "" and passwd == "":
        auth = ""
    elif passwd == "":
        auth = "{}@".format(user)
    else:
        auth = "{}:{}@".format(user, passwd)
    netloc = auth + host + port

    result = urllib.parse.SplitResult(scheme, netloc, path, query, frag)
    if want_splitresult:
        return result
    else:
        return result.geturl()

# The tags on the UK blocklist need a certain amount of cleanup.
TAG_CORRECTIONS = {
    '':                                      [],
    '1337x':                                 ['filesharing', '1337x'],
    'anonymizers,drugs':                     ['anonymizers', 'drugs'],
    'anonymizers,phishing':                  ['anonymizers', 'phishing'],
    'anonymizers,pornography':               ['anonymizers', 'pornography'],
    'beemp3':                                ['filesharing', 'beemp3'],
    'bitsnoop':                              ['filesharing', 'bitsnoop'],
    'bitsoup':                               ['filesharing', 'bitsoup'],
    'bittorrentscene':                       ['filesharing', 'bittorrentscene'],
    'bomb-mp3':                              ['filesharing', 'bomb-mp3'],
    'btdigg':                                ['filesharing', 'btdigg'],
    'btloft':                                ['filesharing', 'btloft'],
    'chat & dating (adult)':                 ['dating', 'pornography'],
    'curica':                                ['drugs', 'curica'],
    'dating(g&l)':                           ['dating', 'glbt'],
    'dating,pornography':                    ['dating', 'pornography'],
    'demonoid':                              ['filesharing', 'demonoid'],
    'download':                              ['filesharing', 'download'],
    'drugs,pornography':                     ['drugs', 'pornography'],
    'emp3-world':                            ['filesharing', 'emp3-world'],
    'extratorrent':                          ['filesharing', 'extratorrent'],
    'eztv':                                  ['filesharing', 'eztv'],
    'fenopy block':                          ['filesharing', 'fenopy'],
    'filecrop':                              ['filesharing', 'filecrop'],
    'firstrow':                              ['filesharing', 'firstrow'],
    'general gambling':                      ['gambling'],
    'h33t block':                            ['filesharing', 'h33t'],
    'herotuko':                              ['filesharing', 'heroturko'],
    'icefilms':                              ['filesharing', 'icefilms'],
    'illegal drugs':                         ['drugs'],
    'iptorrents':                            ['filesharing', 'iptorrents'],
    'isohunt':                               ['filesharing', 'isohunt'],
    'iwanna':                                ['filesharing', 'iwanna'],
    'iwatchonline':                          ['filesharing', 'iwatchonline'],
    'kickass torrents':                      ['filesharing', 'kickass torrents'],
    'limetorrents':                          ['filesharing', 'limetorrents'],
    'lottery':                               ['gambling', 'lottery'],
    'megashare':                             ['filesharing', 'megashare'],
    'movie25':                               ['filesharing', 'movie25'],
    'movie2k':                               ['filesharing', 'movie2k'],
    'mp3juices':                             ['filesharing', 'mp3juices'],
    'mp3lemon':                              ['filesharing', 'mp3lemon'],
    'mp3raid':                               ['filesharing', 'mp3raid'],
    'mp3skull':                              ['filesharing', 'mp3skull'],
    'newalbumreleases':                      ['filesharing', 'newalbumreleases'],
    'nowtorrents':                           ['filesharing', 'nowtorrents'],
    'other - vulgar':                        ['vulgar'],
    'phishing,pornography':                  ['phishing', 'pornography'],
    'picktorrent':                           ['filesharing', 'picktorrent'],
    'pirate bay':                            ['filesharing', 'pirate bay'],
    'pornography & violence':                ['pornography', 'violence'],
    'primewire':                             ['filesharing', 'primewire'],
    'project_free-tv':                       ['filesharing', 'project_free-tv'],
    'rapid':                                 ['filesharing', 'rapid'],
    'rapidlibrary':                          ['filesharing', 'rapidlibrary'],
    'seedpeer':                              ['filesharing', 'seedpeer'],
    'solarmovie':                            ['filesharing', 'solarmovie'],
    'stream':                                ['filesharing', 'stream'],
    'sumotorrent':                           ['filesharing', 'sumotorrent'],
    'tehparadox':                            ['filesharing', 'tehparadox'],
    'torlock':                               ['filesharing', 'torlock'],
    'torrentbit':                            ['filesharing', 'torrentbit'],
    'torrentbutler':                         ['filesharing', 'torrentbutler'],
    'torrentbytes':                          ['filesharing', 'torrentbytes'],
    'torrentcd':                             ['filesharing', 'torrentcd'],
    'torrentday':                            ['filesharing', 'torrentday'],
    'torrentdb':                             ['filesharing', 'torrentdb'],
    'torrentdownload':                       ['filesharing', 'torrentdownload'],
    'torrentexpress':                        ['filesharing', 'torrentexpress'],
    'torrentfunk':                           ['filesharing', 'torrentfunk'],
    'torrenthound':                          ['filesharing', 'torrenthound'],
    'torrenting':                            ['filesharing', 'torrenting'],
    'torrentproject':                        ['filesharing', 'torrentproject'],
    'torrentreactor':                        ['filesharing', 'torrentreactor'],
    'torrentroom':                           ['filesharing', 'torrentroom'],
    'torrents':                              ['filesharing', 'torrents'],
    'torrentsfm':                            ['filesharing', 'torrentsfm'],
    'torrentus':                             ['filesharing', 'torrentus'],
    'torrentz':                              ['filesharing', 'torrentz'],
    'torrentzap':                            ['filesharing', 'torrentzap'],
    'torrentzpro':                           ['filesharing', 'torrentzpro'],
    'torrenzt':                              ['filesharing', 'torrenzt'],
    'tubeplus':                              ['filesharing', 'tubeplus'],
    'vertor':                                ['filesharing', 'vertor'],
    'viooz':                                 ['filesharing', 'viooz'],
    'vitorrent':                             ['filesharing', 'vitorrent'],
    'vodly':                                 ['filesharing', 'vodly'],
    'warez':                                 ['filesharing', 'warez'],
    'watch32':                               ['filesharing', 'watch32'],
    'watchfreemovies':                       ['filesharing', 'watchfreemovies'],
    'watchseries':                           ['filesharing', 'watchseries'],
    'watchseriesonline':                     ['filesharing', 'watchseriesonline'],
    'watchseriesto':                         ['filesharing', 'watchseriesto'],
    'weapon':                                ['weapons'],
    'weapons,anonymizers,drugs':             ['weapons', 'anonymizers', 'drugs'],
    'weapons,anonymizers,drugs,pornography': ['weapons', 'anonymizers', 'drugs', 'pornography'],
    'weapons,dating,pornography':            ['weapons', 'dating', 'pornography'],
    'weapons,drugs':                         ['weapons', 'drugs'],
    'weapons,pornography':                   ['weapons', 'pornography'],
    'weapons,suicide':                       ['weapons', 'suicide'],
    'yify_torrents':                         ['filesharing', 'yify_torrents'],
    'yourbittorrent':                        ['filesharing', 'yourbittorrent'],
    'zmovie':                                ['filesharing', 'zmovie'],
}
def canon_tags(tags):
    ntags = set()
    for t in tags.split("|"):
        ntags.update(TAG_CORRECTIONS.get(t, [t]))
    return sorted(ntags)

def add_url_string(db, url):
    """Add an URL to the url_strings table for DB, if it is not already there.
       Returns a pair (id, url) where ID is the table identifier, and URL
       is the URL as returned by canon_url_syntax()."""

    url = canon_url_syntax(url)

    # Accept either a database connection or a cursor.
    if hasattr(db, 'cursor'):
        cur = db.cursor()
    elif hasattr(db, 'execute'):
        cur = db
    else:
        raise TypeError("'db' argument must be a connection or cursor, not "
                        + type(db))

    # Wrap the operation below in a savepoint, so that if it aborts
    # (for instance, if the URL is too long) any outer transaction is
    # not ruined.
    try:
        cur.execute("SAVEPOINT url_string_insertion")

        # Theoretically this could be done in one query with WITH and
        # INSERT ... RETURNING, but it is convoluted enough that I don't
        # believe it will be faster.  Alas.
        cur.execute("SELECT id FROM url_strings WHERE url = %s", (url,))
        row = cur.fetchone()
        if row is not None:
            id = row[0]
        else:
            cur.execute("INSERT INTO url_strings(id, url) VALUES(DEFAULT, %s) "
                        "RETURNING id", (url,))
            id = cur.fetchone()[0]
        return (id, url)

    except:
        cur.execute("ROLLBACK TO SAVEPOINT url_string_insertion")
        raise

    finally:
        cur.execute("RELEASE SAVEPOINT url_string_insertion")


def process_import(db, rdr, datestamp):
    with db, db.cursor() as cur:
        cur.execute("SET search_path TO collection, public;")
        cur.execute("INSERT INTO url_sources (name, last_updated, meta)"
                    "VALUES (%s, %s, %s)"
                    "RETURNING id;",
                    ("UK "+datestamp[:4]+" (blocked.org.uk)",
                     datestamp,
                     json.dumps({"url":"https://api.blocked.org.uk/data/export.csv.gz"})))
        src = cur.fetchone()[0]
        values = []
        for url, tags in rdr:
            try: (uid, _) = add_url_string(cur, url)
            except ValueError: continue
            tags = canon_tags(tags)
            values.append(cur.mogrify("(%s,%s,%s)", (uid, src, json.dumps({
                "country": "UK",
                "tags": tags
            }))))
        cur.execute(b"INSERT INTO urls (url, src, meta) VALUES " +
                    b",".join(values) +
                    b";")

def main():
    _, dbname, raw_f, to_include_f = sys.argv
    with open(raw_f) as f:
        datestamp = time.strftime("%Y-%m-%d",
                                  time.gmtime(os.fstat(f.fileno()).st_mtime))
        rd = csv.reader(f)
        db = psycopg2.connect(dbname=dbname)
        process_import(db, rd, datestamp)

main()
