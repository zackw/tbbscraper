#! /usr/bin/env python3

import re
import sys
import urllib.parse

from reppy.robots import Robots
from collections import defaultdict

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

def canon_url_syntax(url):
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
    """

    exploded = _urlsplit_forced_encoding(url.strip())
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
    return result.geturl()


class DummyAgent:
    """Pseudo-REP ruleset which allows everything, with a crawl delay of
    one second.  Used when we can't retrieve or parse robots.txt."""
    def __init__(self):
        self.delay = 1
    def allowed(self, url):
        return True


class Site:
    """One site from the input list.  Sites are defined by their origin,
    and the origin is equated with the robots.txt URL.

    """
    def __init__(self):
        self.robots_url = None
        self.crawl_delay = None
        self.urls = set()

    def add(self, url):
        if self.robots_url:
            raise RuntimeError(".add called after .filter")
        self.urls.add(url)

    def filter(self, ua):
        """Remove all of the urls in URLS that UA is not allowed to crawl,
           and fill in the .crawl_delay and .robots_url properties."""

        rules = None
        for url in sorted(self.urls):
            robots_url = Robots.robots_url(url)
            if self.robots_url != robots_url:
                if self.robots_url is None:
                    try:
                        rules = Robots.fetch(robots_url, headers={
                            'User-Agent': ua
                        }).agent(ua)
                    except Exception as e:
                        sys.stderr.write(
                            "warning: failed to fetch and parse {}: {}\n"
                            .format(robots_url, e))
                        rules = DummyAgent()

                    self.robots_url = robots_url
                    self.crawl_delay = rules.delay or 1

                else:
                    raise ValueError(
                        "robots.txt for {} is {}, not {}"
                        .format(url, robots_url, self.robots_url))

            if not rules.allowed(url):
                self.urls.remove(url)

    def write_to(self, fp):
        """Write out a sorted list of urls that we are allowed to crawl,
           with a header giving the origin and crawl delay.
           Must be called after .filter()."""
        if not self.robots_url:
            raise RuntimeError(".write_to called before .filter")
        if not self.urls:
            return
        fp.write("Origin: {}\n".format(self.robots_url[:-len("robots.txt")]))
        fp.write("Crawl-Delay: {:2f}\n".format(self.crawl_delay))
        for url in sorted(self.urls):
            fp.write("  {}\n".format(url))
        fp.write("\n")

def filter_urls(urls, ua):
    """Partition URLS (an iterable) into sites, and then filter out all of
    the urls in each site that UA is not allowed to crawl.  Returns a list
    of Site objects."""

    sites = defaultdict(Site)
    for url in urls:
        url = canon_url_syntax(url)
        robots_url = Robots.robots_url(url)
        sites[robots_url].add(url)

    for site in sites.values(): site.filter(ua)
    return sorted(sites.values(), key = lambda s: s.robots_url)

if __name__ == '__main__':
    def main():
        ua = " ".join(sys.argv[1:])
        if not ua:
            ua = "generic-robot/0.0"
        sites = filter_urls(sys.stdin, ua)
        for site in sites:
            site.write_to(sys.stdout)

    main()
