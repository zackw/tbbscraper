# URL database management.  Shared among all the sources and the
# scraper controller.
#
# Copyright © 2014 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import psycopg2
import psycopg2.extras
import re
import time
import urllib.parse

def ensure_database(args):
    """Ensure that the database specified by args.database exists and has
       an up-to-date schema.  `args` would normally be an
       argparse.Namespace object, but we don't care as long as
       "database" and "schema" are attributes (nor do we care how the
       argument actually shows up on the command line).

       args.database is expected to be a "libpq connection string" or
       postgres:// URL.  If it appears to be neither of those, it is
       taken as just the name of the database.
    """

    dbstr = args.database
    if '=' not in dbstr and '://' not in dbstr:
        dbstr = "dbname="+dbstr

    db = psycopg2.connect(dbstr,
                          cursor_factory=psycopg2.extras.NamedTupleCursor)

    # Select the appropriate schema.
    with db, db.cursor() as c:
        c.execute("SET search_path TO " + args.schema)

    return db

#
# Utilities for working with the shared schema.
#

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

# Subroutines and REs for add_site:

def to_https(spliturl):
    return urllib.parse.SplitResult("https",
                                    spliturl.netloc,
                                    spliturl.path,
                                    spliturl.query,
                                    spliturl.fragment)

def to_siteroot(spliturl):
    return urllib.parse.SplitResult(spliturl.scheme,
                                    spliturl.netloc,
                                    "/",
                                    spliturl.query,
                                    spliturl.fragment)

def add_www(spliturl):
    if "@" in spliturl.netloc:
        (auth, rest) = spliturl.netloc.split("@", 1)
        netloc = auth + "@www." + rest
    else:
        netloc = "www." + spliturl.netloc

    return urllib.parse.SplitResult(spliturl.scheme,
                                    netloc,
                                    spliturl.path,
                                    spliturl.query,
                                    spliturl.fragment)

no_www_re = re.compile(r"^(?:\d+\.\d+\.\d+\.\d+$|\[[\dA-Fa-f:]+\]$|www\.)")

def add_site(db, site, http_only=False, www_only=False):
    """Add a site to the url_strings table for DB, if it is not already
       there.  Returns a list of pairs [(id1, url1), (id2, url2), ...]
       comprising all URLs chosen to represent the site.

       A "site" is a partial URL, from which the scheme and possibly a
       leading "www." have been stripped.  There may or may not be a
       path component. We reconstruct up to eight possible URLs from
       this partial URL:

         http://       site (/path)
         https://      site (/path)
         http://  www. site (/path)
         https:// www. site (/path)

       If there was a path component, we consider URLs both with and
       without that path.  If 'site' already starts with 'www.', or if
       it is an IP address, we do not prepend 'www.'

       This scheme won't do us any good if the actual content people
       are loading is neither at the name in the list nor at www. the
       name in the list; for instance, akamaihd.net appears highly in
       Alexa's site ranking, but neither akamaihd.net nor www.akamaihd.net
       has any A records, because, being a CDN, all of the actual
       content is on servers named SOMETHINGELSE.akamaihd.net, and
       you're not expected to notice that the domain even exists.
       But there's nothing we can do about that.

       If http_only is True, https urls are not added.
       If www_only is true, urls without 'www.' are not added.
    """

    parsed = canon_url_syntax("http://" + site, want_splitresult=True)

    assert parsed.path != ""
    if parsed.path != "/":
        root = to_siteroot(parsed)
        need_path = True
        with_path = parsed
    else:
        root = parsed
        need_path = False

    host = root.hostname
    if no_www_re.match(host):
        need_www = False
        with_www = root
        if need_path:
            with_www_path = with_path
    else:
        need_www = True
        with_www = add_www(root)
        if need_path:
            with_www_path = add_www(with_path)

    urls = [with_www.geturl()]
    if not http_only:
        urls.append(to_https(with_www).geturl())

    if need_www and not www_only:
        urls.append(root.geturl())
        if not http_only:
            urls.append(to_https(root).geturl())

    if need_path:
        urls.append(with_www_path.geturl())
        if not http_only:
            urls.append(to_https(with_www_path).geturl())

        if need_www and not www_only:
            urls.append(with_path.geturl())
            if not http_only:
                urls.append(to_https(with_path).geturl())

    return [ add_url_string(db, url) for url in urls ]


# see http://qt-project.org/doc/qt-5/qnetworkreply.html#NetworkError-enum
# codes not listed are mapped to "crawler failure" because they
# shouldn't be possible.
look_at_the_detail = object()
maybe_ok = object()
network_errors_by_code = {
    "N1":   "connection refused",
    "N2":   "connection interrupted",
    "N3":   "host not found",
    "N4":   "timeout",
    "N5":   "connection interrupted",
    "N6":   "TLS handshake failed",

    "N101": "proxy failure",   # All 1xx errors indicate something
    "N102": "proxy failure",   # wrong with the proxy.
    "N103": "proxy failure",
    "N104": "proxy failure",
    "N105": "proxy failure",
    "N199": "proxy failure",

    # Unlike all the other 2xx, 4xx QNetworkReply codes that
    # should be reported to us as proper HTTP status codes,
    # this one actually happens.  We're not sure why, but it's
    # quite rare and probably not worth digging into.
    "N205": "other network error",

    "N301": "invalid URL", # ProtocolUnknownError: unrecognized URL scheme.
    "N302": "other network error",
    "N399": "other network error",

    "N99":  look_at_the_detail
}
network_errors_by_detail = {
    "N99 Connection to proxy refused": "proxy failure",
    "N99 Host unreachable":            "server unreachable",
    "N99 Network unreachable":         "server unreachable",
    "N99 Unknown error":               "other network error"
}
misc_errors_by_status = {
    "hostname not found":   "host not found",
    "timeout":              "timeout",
}
http_statuses_by_code = {
    200: maybe_ok,

    301: "redirection loop",
    302: "redirection loop",
    303: "redirection loop",
    307: "redirection loop",
    308: "redirection loop",

    400: "bad request (400)",
    401: "authentication required (401)",
    403: "forbidden (403)",
    404: "page not found (404/410)",
    410: "page not found (404/410)",

    500: "server error (500)",
    503: "service unavailable (503)",

    502: "proxy error (502/504/52x)", # not our proxy, but a CDN's.
    504: "proxy error (502/504/52x)",
    520: "proxy error (502/504/52x)",
    521: "proxy error (502/504/52x)",
    522: "proxy error (502/504/52x)",
    523: "proxy error (502/504/52x)",
    524: "proxy error (502/504/52x)",
    525: "proxy error (502/504/52x)",
    526: "proxy error (502/504/52x)",
    527: "proxy error (502/504/52x)",
    528: "proxy error (502/504/52x)",
    529: "proxy error (502/504/52x)",
}

def categorize_result(status, detail, original_uid, canon_uid):

    if not isinstance(status, int):
        if status.startswith("N"):
            cat = network_errors_by_code.get(status, "crawler failure")
            if cat is look_at_the_detail:
                cat = network_errors_by_detail.get(detail, "crawler failure")

            return cat

        # I'm not sure if these can still happen, but best be safe.
        if status == "hostname not found":
            return "host not found"
        if status == "timeout":
            return "timeout"
        if status == "crawler failure":
            return "crawler failure"

        try:
            status = int(status)
        except ValueError:
            return "crawler failure"

    cat = http_statuses_by_code.get(status, None)

    if cat is maybe_ok:
        if canon_uid is None:
            return "invalid URL"
        if original_uid == canon_uid:
            return "ok"
        return "ok (redirected)"

    if cat is not None:
        return cat

    if canon_uid is None:
        return "invalid URL"

    return "other HTTP response"
