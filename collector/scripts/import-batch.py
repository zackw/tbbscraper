#! /usr/bin/python3

import collections
import datetime
import hashlib
import os
import psycopg2
import re
import sys
import time
import urllib.parse
import zlib

zlib_nothing = zlib.compress(b'')

class savepoint:
    def __init__(self, cur, name):
        self._cur  = cur
        self._name = name

    def __enter__(self):
        self._cur.execute('SAVEPOINT "' + self._name + '"')

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self._cur.execute('RELEASE SAVEPOINT "' + self._name + '"')
        else:
            self._cur.execute('ROLLBACK TO SAVEPOINT "' + self._name + '"')
        return False

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

       This version of this function tolerates a number of syntactic
       problems that other versions reject, e.g. "http:/host.dom.ain"
       and "itmss://whatever".
    """

    if isinstance(url, urllib.parse.SplitResult):
        if want_splitresult is None: want_splitresult = True
        exploded = url

    else:
        if want_splitresult is None: want_splitresult = False

        exploded = _urlsplit_forced_encoding(url)
        if not exploded.hostname:
            # Canonicalize the number of slashes after the scheme and retry.
            corrected = re.sub(r'(?i)^([a-z]+):/+', r'\1://', url)
            exploded = _urlsplit_forced_encoding(corrected)

    scheme = exploded.scheme
    host   = exploded.hostname or ""
    user   = _encode_nonascii_and_percents(exploded.username or "")
    passwd = _encode_nonascii_and_percents(exploded.password or "")
    port   = exploded.port
    path   = _encode_nonascii_and_percents(exploded.path or "/")
    query  = _encode_nonascii_and_percents(exploded.query)
    frag   = _encode_nonascii_and_percents(exploded.fragment)

    # It is easier to do this unconditionally than to try to detect
    # non-ASCII characters.  The split-join is to avoid barfing on
    # syntactically invalid hostnames (e.g. "foo..com").
    # encode('idna') is idempotent.
    host = ".".join((label.encode("idna").decode("ascii") if label else "")
                    for label in host.split("."))

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

def add_url_string(cur, url):
    """Add an URL to the url_strings table for DB, if it is not already there.
       Returns a pair (id, url) where ID is the table identifier, and URL
       is the URL as returned by canon_url_syntax()."""

    url = canon_url_syntax(url)

    # Wrap the operation below in a savepoint, so that if it aborts
    # (for instance, if the URL is too long) any outer transaction is
    # not ruined.
    with savepoint(cur, "url_string_insertion"):
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

def add_capture_result(cur, result, detail):
    # Wrap the operation below in a savepoint, so that if it aborts
    # (for instance, if the detail and result are inconsistent) any
    # outer transaction is not ruined.
    with savepoint(cur, "capture_result_insertion"):
        # Theoretically this could be done in one query with WITH and
        # INSERT ... RETURNING, but it is convoluted enough that I don't
        # believe it will be faster.  Alas.

        cur.execute("SELECT id FROM capture_coarse_result WHERE result = %s",
                    (result,))
        row = cur.fetchone()
        if row is not None:
            cid = row[0]
        else:
            cur.execute("INSERT INTO capture_coarse_result(id, result)"
                        "  VALUES(DEFAULT, %s)"
                        "  RETURNING id", (result,))
            cid = cur.fetchone()[0]

        cur.execute("SELECT id, result FROM capture_fine_result"
                    " WHERE detail = %s", (detail,))
        row = cur.fetchone()
        if row is not None:
            fid = row[0]
            if row[1] != cid:
                raise RuntimeError("{!r}: coarse result {!r} inconsistent "
                                   "with prior coarse result (id={!r})"
                                   .format(detail, result, cid))
        else:
            cur.execute("INSERT INTO capture_fine_result(id, result, detail)"
                        "  VALUES(DEFAULT, %s, %s)"
                        "  RETURNING id", (cid, detail))
            fid = cur.fetchone()[0]

        return fid

def add_capture_log_old(cur, log):
    # Wrap the operation below in a savepoint, so that if it aborts any
    # outer transaction is not ruined.
    with savepoint(cur, "capture_log_old_insertion"):
        # This definitely should not be done in one query, because we can
        # avoid pushing the actual data over the connection if it's a dupe.

        h = hashlib.sha256(log).digest()
        cur.execute("SELECT id FROM capture_logs_old WHERE hash = %s", (h,))
        row = cur.fetchone()
        if row is not None:
            return row[0]
        else:
            cur.execute("INSERT INTO capture_logs_old(id, hash, log)"
                        "  VALUES(DEFAULT, %s, %s)"
                        "  RETURNING id", (h, log))
            return cur.fetchone()[0]

def add_capture_html_content(cur, content):
    # Wrap the operation below in a savepoint, so that if it aborts any
    # outer transaction is not ruined.
    with savepoint(cur, "capture_html_content_insertion"):
        # This definitely should not be done in one query, because we can
        # avoid pushing the actual data over the connection if it's a dupe.

        h = hashlib.sha256(content).digest()
        cur.execute("SELECT id FROM capture_html_content WHERE hash = %s",
                    (h,))
        row = cur.fetchone()
        if row is not None:
            return row[0]
        else:
            cur.execute("INSERT INTO capture_html_content(id, hash, content)"
                        "  VALUES(DEFAULT, %s, %s)"
                        "  RETURNING id", (h, content))
            return cur.fetchone()[0]


CaptureResult = collections.namedtuple("CaptureResult", (
    "country", "vantage", "access_time", "elapsed",
    "orig_url", "redir_url", "status", "detail",
    "html_content", "capture_log_old"
))

def load_result_file(fname):
    _, _, loc = fname.partition('.')
    cc2, _, vantage = loc.partition('_')

    with open(fname, "rb") as fp:
        magic = fp.read(8)
        if magic != b"\x7fcap 00\n":
            raise RuntimeError(fname + ": not a capture file")

        data = memoryview(fp.read())
        atim = os.stat(fp.fileno()).st_mtime

    b1 = data.obj.find(b'\n')
    b2 = data.obj.find(b'\n', b1+1)
    b3 = data.obj.find(b'\n', b2+1)
    b4 = data.obj.find(b'\n', b3+1)
    b5 = data.obj.find(b'\n', b4+1)
    b6 = data.obj.find(b'\n', b5+1)

    ourl = data[     : b1].tobytes().decode("utf-8")
    rurl = data[b1+1 : b2].tobytes().decode("utf-8")
    stat = data[b2+1 : b3].tobytes().decode("utf-8")
    dtyl = data[b3+1 : b4].tobytes().decode("utf-8")
    elap = float(data[b4+1 : b5].tobytes().decode("utf-8"))
    lens = data[b5+1 : b6].tobytes().decode("ascii").split()

    # This can happen when the crawler crashed.
    if rurl == '':
        rurl = ourl

    clen = int(lens[0])
    llen = int(lens[1])
    cbeg = b6 + 1
    cend = cbeg + clen
    lbeg = cend
    lend = lbeg + llen

    if lend != len(data):
        raise RuntimeError(fname + ": ill-formed capture file (lend != eof)")

    hcon = data[cbeg:cend].tobytes()
    clog = data[lbeg:lend].tobytes()

    if hcon == b'': hcon = zlib_nothing
    if clog == b'': clog = zlib_nothing

    # Validate the compressed data.
    zlib.decompress(hcon)
    zlib.decompress(clog)

    return CaptureResult(cc2, vantage, atim, elap,
                         ourl, rurl, stat, dtyl,
                         hcon, clog)

def record_result(cur, result):
    (ouid, _) = add_url_string(cur, result.orig_url)
    (ruid, _) = add_url_string(cur, result.redir_url)
    fid       = add_capture_result(cur, result.status, result.detail)
    cid       = add_capture_html_content(cur, result.html_content)
    lid       = add_capture_log_old(cur, result.capture_log_old)



    with savepoint (cur, "captured_pages_insertion"):

        cur.execute ("SELECT id, access_time FROM captured_pages WHERE url = %s AND"
                     " country = %s AND vantage = %s AND"
                     " access_time = TIMESTAMP WITHOUT TIME ZONE 'epoch' + %s * INTERVAL '1 second'",
                    (ouid, result.country, result.vantage, result.access_time))

        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO captured_pages"
                    "  (id, url, country, vantage, access_time, elapsed_time,"
                    "   result, redir_url, capture_log, capture_log_old,"
                    "   html_content)"
                    " VALUES"
                    "   (DEFAULT, %s, %s, %s, "
                    "    TIMESTAMP WITHOUT TIME ZONE 'epoch' + "
                    "        %s * INTERVAL '1 second',"
                    "    %s, %s, %s, NULL, %s, %s)",
                    (ouid, result.country, result.vantage,
                    result.access_time,
                    result.elapsed,
                    fid, ruid, lid, cid))


class Cruncher:
    def __init__(self, dbname, dirs):
        self.dbname = dbname
        self.dirs   = dirs
        self.db     = psycopg2.connect(dbname=dbname)
        self.start  = time.monotonic()
        self.ndirs  = len(dirs)
        self.nfiles = 0
        self.pdirs  = None
        self.pfiles = None

    def run(self):
        self.count_files()
        self.import_files()

    def count_files(self):
        self.progress("counting files to be imported:")
        for d in self.dirs:
            self.progress("  " + d)
            for subdir, dirs, files in os.walk(d):
                self.nfiles += len(files)

        self.progress("total {} dirs {} files".format(self.ndirs, self.nfiles))

    def import_files(self):
        self.progress("importing...")
        self.pdirs = 0
        self.pfiles = 0
        cur = self.db.cursor()
        cur.execute("SET search_path TO collection, public")
        for d in self.dirs:
            for subdir, dirs, files in os.walk(d):
                if not files: continue
                self.progress(subdir)
                with self.db:
                    for fname in files:
                        pname = os.path.join(subdir, fname)
                        try:
                            record_result(cur, load_result_file(pname))
                        except Exception as e:
                            sys.stderr.write("{}: {}\n".format(pname, e))
                        self.pfiles += 1
            self.pdirs += 1
        self.progress("done")

    def progress(self, message):
        now = time.monotonic()
        delta = datetime.timedelta(seconds = now - self.start)

        if self.pdirs is not None:
            sys.stderr.write("[{}] processed {}/{}d {}/{}f | {}\n"
                             .format(delta, self.pdirs, self.ndirs,
                                     self.pfiles, self.nfiles, message))
        else:
            sys.stderr.write("[{}] {}\n".format(delta, message))


def main():
    dbname = sys.argv[1]
    dirs   = sys.argv[2:]
    Cruncher(dbname, dirs).run()

main()
