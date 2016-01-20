#! /usr/bin/python3

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

header_line_re = re.compile(r"^# ([a-z]+): (.*)$")
def parse_file(fname):
    with open(fname) as f:
        head = {}
        for line in f:
            line = line.strip()
            m = header_line_re.match(line)
            if not m:
                break
            head[m.group(1)] = m.group(2)

        body = set((line,))
#        body.update(line.strip() for line in f)
        return head, sorted(body)

def process_one_import(db, cur, fname):
    head, body = parse_file(fname)
    assert frozenset(head.keys()) == frozenset(('country', 'date', 'url'))
    cc2        = head['country']
    datestamp  = head['date']
    listurl    = head['url']

    cur.execute("SELECT name FROM country_codes WHERE cc2 = %s", (cc2,))
    country  = cur.fetchone()[0]
    listname = country + " " + datestamp[:4] + " (Wikileaks)"

    with db:
        cur.execute("SELECT id FROM url_sources WHERE name = %s", (listname,))
        rv = cur.fetchall()
        if rv:
            assert len(rv) == 1
            src = rv[0][0]
        else:
            cur.execute("INSERT INTO url_sources (name, last_updated, meta)"
                        "VALUES (%s, %s, %s) RETURNING id",
                        (listname, datestamp, json.dumps({"url":listurl})))
            src = cur.fetchone()[0]

    with db:
        values = []
        meta = json.dumps({"country":cc2})
        for entry in body:
            if entry.startswith("http"):
                try:
                    (uid, _) = add_url_string(cur, entry)
                    values.append(cur.mogrify("(%s,%s,%s)", (uid, src, meta)))
                except ValueError:
                    pass

            else:
                try:
                    (uid, _) = add_url_string(cur, "http://" + entry)
                    values.append(cur.mogrify("(%s,%s,%s)", (uid, src, meta)))
                except ValueError:
                    pass

                if not entry.startswith("www."):
                    try:
                        (uid, _) = add_url_string(cur, "http://www." + entry)
                        values.append(cur.mogrify("(%s,%s,%s)",
                                                  (uid, src, meta)))
                    except ValueError:
                        pass

        cur.execute(b"INSERT INTO urls (url, src, meta) VALUES " +
                    b",".join(values) +
                    b";")

def main():
    dbname = sys.argv[1]
    sources = sys.argv[2:]
    db = psycopg2.connect(dbname=dbname)
    cur = db.cursor()
    cur.execute("SET search_path TO collection, public")
    for src in sources:
        sys.stderr.write(src +" ...\n")
        process_one_import(db, cur, src)

main()
