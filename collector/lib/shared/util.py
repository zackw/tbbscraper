# Utility functions.
#
# Copyright © 2014–2017 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import re
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

# see http://qt-project.org/doc/qt-5/qnetworkreply.html#NetworkError-enum
# codes not listed are mapped to "crawler failure" because they
# shouldn't be possible.
look_at_the_detail = object()
qt_network_errors_by_code = {
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
qt_network_errors_by_detail = {
    "N99 Connection to proxy refused": "proxy failure",
    "N99 Host unreachable":            "server unreachable",
    "N99 Network unreachable":         "server unreachable",
    "N99 Unknown error":               "other network error"
}

# same as above, but for errors reported by neterr-details.c
ned_network_errors_by_status = {
    "dns-notfound":    "host not found",

    "tcp-unreachable": "server unreachable",
    "tcp-refused":     "connection refused",
    "tcp-reset":       "connection interrupted",

    "tls-selfsigned":  "TLS handshake failed",
    "tls-untrusted":   "TLS handshake failed",
    "tls-invalid":     "TLS handshake failed",

    # If this happens, it means Firefox threw a network error but we
    # didn't have any trouble connecting to it from this, which is an
    # error condition.
    "success":         "crawler failure",
}

# http statuses are the same for both phantom and openwpm
http_statuses_by_code = {
    200: "ok",

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
def categorize_result_ph(status, detail):
    """Categorize a result produced by PhantomJS."""
    if not isinstance(status, int):
        if status.startswith("N"):
            cat = qt_network_errors_by_code.get(status, "crawler failure")
            if cat is look_at_the_detail:
                cat = qt_network_errors_by_detail.get(detail, "crawler failure")

            return cat

        if status == "crawler failure":
            return "crawler failure"
        if status == "timeout":
            return "timeout"

        # I'm not sure if this can still happen, but best be safe.
        if status == "hostname not found":
            return "host not found"

        try:
            status = int(status)
        except ValueError:
            return "crawler failure"

    return http_statuses_by_code.get(status, "other HTTP response")

def categorize_result_ff(status):
    """Categorize a result produced by Firefox/OpenWPM."""
    try:
        status = int(status)
    except ValueError:
        pass
    if isinstance(status, int):
        return http_statuses_by_code.get(status, "other HTTP response")

    return ned_network_errors_by_status.get(status, "other network error")
