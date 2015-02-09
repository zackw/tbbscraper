#! /usr/bin/python3

# Extract captured pages from the database.

import os
import psycopg2
import zlib
import json
import random
from collections import defaultdict

__all__ = ['PageText', 'PageObservation', 'DOMStatistics', 'PageDB']

class PageText:
    """The text of at least one page.  Corresponds to one row of the
       ts_analysis.page_text table.  Must cross-reference to
       .page_observations to learn where it came from.

       Properties:
          id              - Serial number of this text blob.
          has_boilerplate - True if site boilerplate is included in the text.
          lang_code       - ISO 639 code: language as identified by CLD2.
          lang_name       - English name of the language.
          lang_conf       - % confidence in the language identification.
          contents        - The text itself (lazily uncompressed).
          observations    - Array of PageObservation objects: all the page
                            observations that have this text.
    """
    def __init__(self, db, id, has_boilerplate,
                 lang_code, lang_name, lang_conf, contents):
        self._db             = db
        self.id              = id
        self.has_boilerplate = has_boilerplate
        self.lang_code       = lang_code
        self.lang_name       = lang_name
        self.lang_conf       = lang_conf

        # For memory efficiency, contents are only uncompressed on
        # request, and may even be lazily loaded from the database.
        self._raw_contents      = contents
        self._unpacked_contents = None

        # Similarly, observations are lazily loaded.
        self._observations      = None

    @property
    def contents(self):
        if self._raw_contents is None:
            self._raw_contents = self._db.get_raw_page_contents(self.id)

        if self._unpacked_contents is None:
            self._unpacked_contents = \
                zlib.decompress(self._raw_contents).decode("utf-8")

        return self._unpacked_contents

    @property
    def observations(self):
        if self._observations is None:
            self._observations = \
                self._db.get_observations_for_text(self)
        return self._observations

class DOMStatistics:
    """Statistics about the DOM structure.  Has two attributes:

    tags - Dictionary of counters.  Each key is an HTML tag that
           appeared at least once in the document, with its spelling
           normalized.  The corresponding value is the number of times
           that tag appeared. Implicit tags are not counted.

    tags_at_depth - Dictionary of counters. Each key is a tree depth
                    in the document, and the corresponding value is
                    the number of times a tag appeared at that depth.
                    Depths containing only implicit tags are not counted.
    """

    def __init__(self, blob):
        if not blob:
            self.tags = {}
            self.tags_at_depth = {}
        else:
            self.tags          = blob["tags"]
            self.tags_at_depth = blob["tags_at_depth"]

class PageObservation:
    """A page as observed from a particular locale.  Corresponds to one
       row of the ts_analysis.page_observations table.  Many properties
       are lazily loaded.  Use (run, locale, url_id) for a unique key.

           run              - Which data collection run this is from.
           locale           - ISO 631 code of country where the page
                              was observed, possibly with a suffix.
           country          - English name corresponding to 'locale'.
           url_id           - Serial number of the page URL.
           url              - The page URL.
           access_time      - Date and time the page was accessed (UTC).
           result           - High-level result code.
           detail           - More detailed result code.
           redir_url        - URL of the page after following redirections.

           document         - PageText object: the text of the page,
                              boilerplate stripped.
           document_with_bp - PageText object: the text of the page,
                              boilerplate included.
           headings         - Array of strings: all text found within <hN> tags.
           links            - Array of strings: all outbound hyperlinks
                              from this page.
           resources        - Array of strings: all resources loaded by
                              this page.
           dom_stats        - DOMStatistics object counting tags and
                              tree depth.

       NOTE: retrieving the capture log is currently not implemented.

    """

    def __init__(self, db, run, locale, country, url_id, url,
                 access_time, result, detail, redir_url,
                 document_id, document_with_bp_id,
                 *,
                 document=None, document_with_bp=None,
                 headings=None, links=None, resources=None,
                 dom_stats=None, html_content=None):

        self._db                  = db

        self.run                  = run
        self.locale               = locale
        self.country              = country
        self.url_id               = url_id
        self.url                  = url
        self.access_time          = access_time
        self.result               = result
        self.detail               = detail
        self.redir_url            = redir_url

        self._document_id         = document_id
        self._document            = document
        self._document_with_bp_id = document_with_bp_id
        self._document_with_bp    = document_with_bp

        self._headings            = headings
        self._links               = links
        self._resources           = resources
        self._dom_stats           = dom_stats

        self._html_content        = html_content

    @property
    def document(self):
        if self._document is None:
            self._document = self._db.get_page_text(self._document_id)
        return self._document

    @property
    def document_with_bp(self):
        if self._document_with_bp is None:
            self._document_with_bp = \
                self._db.get_page_text(self._document_with_bp_id)
        return self._document

    @property
    def headings(self):
        if self._headings is None:
            self._headings = self._db.get_headings(self.run,
                                                   self.locale,
                                                   self.url_id)
        return self._headings

    @property
    def links(self):
        if self._links is None:
            self._links = self._db.get_links(self.run,
                                             self.locale,
                                             self.url_id)
        return self._links

    @property
    def resources(self):
        if self._resources is None:
            self._resources = self._db.get_resources(self.run,
                                                     self.locale,
                                                     self.url_id)
        return self._resources

    @property
    def dom_stats(self):
        if self._dom_stats is None:
            self._dom_stats = self._db.get_dom_stats(self.run,
                                                     self.locale,
                                                     self.url_id)
        return self._dom_stats

    @property
    def html_content(self):
        if self._html_content is None:
            self._html_content = self._db.get_html_content(self.run,
                                                           self.locale,
                                                           self.url_id)
        return self._html_content


class PageDB:
    """Wraps a database handle and knows how to extract pages or other
       interesting material (add queries as they become useful!)"""

    def __init__(self, connstr, exclude_partial_locales=True):
        """If 'exclude_partial_locales' is True, a hardcoded list of
           incompletely-scanned locales will be excluded from all
           processing."""

        self._locales    = None
        self._runs       = None
        self._cursor_tag = "pagedb_qtmp_{}_{}".format(os.getpid(), id(self))
        self._cursor_ctr = 0

        if exclude_partial_locales:
            self._exclude_partial_where = "o.locale NOT IN ('cn', 'jp_kobe')"
            self._exclude_partial_list  = frozenset(('cn', 'jp_kobe'))
        else:
            self._exclude_partial_where = ""
            self._exclude_partial_list  = frozenset()

        if "=" not in connstr:
            connstr = "dbname="+connstr
        self._db = psycopg2.connect(connstr)
        cur = self._db.cursor()

        # All tables are referenced with explicit schemas.
        cur.execute("SET search_path TO ''")

        # Dscourage the query planner from doing anything that will
        # involve sorting the entire page_text or page_observations
        # table before emitting a single row.
        #
        # This knob is documented as "the planner's estimate of the
        # fraction of a cursor's rows that will be retrieved" and
        # that's not _exactly_ the issue here, but it's close enough.
        # (We often _do_ retrieve all of the rows, but we only want a
        # tiny fraction of them held in RAM at once.)
        cur.execute("SET cursor_tuple_fraction TO 1e-6")

    @property
    def locales(self):
        """Retrieve a list of all available locales.  This involves a
           moderately expensive query, which has now been memoized on
           the server, but we memoize it again here just to be sure.
        """
        if self._locales is None:
            cur = self._db.cursor()
            cur.execute("SELECT locale FROM ts_analysis.captured_locales")
            self._locales = sorted(
                row[0] for row in cur
                if row[0] not in self._exclude_partial_list
            )
        return self._locales

    @property
    def runs(self):
        """Retrieve a list of all runs indexed in ts_analysis.url_strings.
           (Right now that means run 0 is not included.)
        """
        if self._runs is None:
            cur = self._db.cursor()
            cur.execute("SELECT CAST(n AS INTEGER) FROM ("
                        "SELECT SUBSTRING(column_name FROM 'r([0-9]+)id') AS n"
                        "  FROM information_schema.columns"
                        " WHERE table_schema = 'ts_analysis'"
                        "   AND table_name = 'url_strings') _"
                        " WHERE n <> ''")
            self._runs = sorted(row[0] for row in cur)
        return self._runs

    #
    # Methods for retrieving pages or observations in bulk.
    #
    def get_page_texts(self, *,
                       where_clause="",
                       ordered=None,
                       limit=None):
        """Retrieve page texts from the database matching the where_clause.
           This is a generator, which produces one PageText object per row.

           Useful 'where_clause' terms include

               has_boilerplate = [true | false]
               lang_code       [=, <>] <ISO 639 code>

           'ordered' may be either None or 'lang' to sort by language code.

           'limit' may be either None for no limit, or a positive integer;
           in the latter case at most that many page texts are produced.
        """

        query = ("SELECT p.id, p.has_boilerplate, p.lang_code,"
                 "       lc.name, p.lang_conf, p.contents"
                 "  FROM ts_analysis.page_text p"
                 "  JOIN ts_analysis.language_codes lc ON p.lang_code = lc.code")

        if where_clause:
            query += " WHERE ({})".format(where_clause)

        if ordered == "lang":
            query += " ORDER BY p.lang_code"
        else:
            assert ordered is None

        if limit:
            query += " LIMIT {}".format(limit)

        # This must be a named cursor, otherwise psycopg2 may attempt to
        # fetch all the rows at once and they won't fit in memory and
        # we'll crash.  (This is also necessary to make the planner
        # tuning in __init__ be effective.)
        # Note we are not using context-managed cursors because that's
        # not available in the version of psycopg2 on arima.
        cur = self._db.cursor(self._cursor_tag + "_" + str(self._cursor_ctr))
        self._cursor_ctr += 1
        cur.itersize = 5000
        cur.execute(query)
        try:
            for row in cur:
                yield PageText(self, *row)

        finally:
            cur.close()

    def get_random_page_texts(self, count, seed, where_clause="", **kwargs):

        cur = self._db.cursor()
        cur.execute("SELECT min(id), max(id) FROM ts_analysis.page_text")
        lo, hi = cur.fetchone()

        cur.execute("SELECT x.id FROM generate_series(%s, %s) as x(id)"
                    " LEFT JOIN ts_analysis.page_text p"
                    "        ON x.id = p.id WHERE p.id IS NULL",
                    (lo, hi))
        gaps = set(x[0] for x in cur.fetchall())

        rng = random.Random(seed)
        sample = []
        while len(sample) < count:
            block = set(rng.sample(range(lo, hi+1), count - len(sample))) - gaps
            sample.extend(block)
        sample.sort()
        selection = "p.id IN (" + ",".join(str(id) for id in sample) + ")"

        if where_clause:
            where_clause = "({}) AND ({})".format(where_clause, selection)
        else:
            where_clause = selection

        return self.get_page_texts(where_clause=where_clause, **kwargs)

    def get_page_observations(self, *,
                              where_clause="",
                              ordered='url',
                              limit=None,
                              load=[],
                              constructor_kwargs={}):
        """Retrieve page observations from the database matching the
           where_clause.  This is a generator, which produces one
           PageObservation object per row.

           Useful 'where_clause' terms include

               o.locale = <ISO 631 code>
               o.result = <high-level result>

           'limit' may be either None for no limit, or a positive integer;
           in the latter case at most that many page texts are produced.

           'ordered' may be None for unordered, 'url' to sort by URL id
           (_not_ actual URL text), or 'locale' to sort by locale.

           'load' is a list of PageObservation attributes to load
           eagerly from the database: zero or more of 'headings',
           'links', 'resources', 'dom_stats', and 'html_content'.
           If you know you're going to use these for every observation,
           requesting them up front is more efficient than allowing them
           to be loaded lazily.

           'constructor_kwargs' is for passing additional arguments to the
           PageObservation constructor; external code should not need it.
        """
        columns = { "run"                 : "o.run",
                    "locale"              : "o.locale",
                    "country"             : "cc.name",
                    "url_id"              : "o.url",
                    "url"                 : "u.url",
                    "access_time"         : "o.access_time",
                    "result"              : "o.result",
                    "detail"              : "o.detail",
                    "redir_url"           : "v.url",
                    "document_id"         : "o.document",
                    "document_with_bp_id" : "o.document_with_bp" }

        joins = [
            "FROM ts_analysis.page_observations o",
            "JOIN ts_analysis.capture_detail d ON o.detail = d.id",
            "JOIN ts_analysis.locale_data cc"
            "     ON SUBSTRING(o.locale FOR 2) = cc.cc2",
            "JOIN ts_analysis.url_strings u ON o.url = u.id",
            "LEFT JOIN ts_analysis.url_strings v ON o.redir_url = v.id",
        ]

        unpackers = defaultdict(lambda: (lambda x: x))

        if "headings" in load:
            columns["headings"] = "o.headings"
            unpackers["headings"] = \
                lambda x: (json.loads(zlib.decompress(x).decode("utf-8"))
                           if x else [])
        if "links" in load:
            columns["links"] = "o.links"
            unpackers["links"] = \
                lambda x: (json.loads(zlib.decompress(x).decode("utf-8"))
                           if x else [])
        if "resources" in load:
            columns["resources"] = "o.resources"
            unpackers["resources"] = \
                lambda x: (json.loads(zlib.decompress(x).decode("utf-8"))
                           if x else [])
        if "dom_stats" in load:
            columns["dom_stats"] = "o.dom_stats"
            unpackers["dom_stats"] = \
                lambda x: DOMStatistics(json.loads(zlib.decompress(x)
                                                   .decode("utf-8"))
                                        if x else {})
        if "html_content" in load:
            # This is extra-hairy because the HTML content is only
            # stored in the captured_pages table for the original run,
            # and that means we have to join _all of them_.
            # The access_time condition is necessary to handle cases where
            # the same locale visited the same URL more than once.
            coalesce = []
            for run in self.runs:
                coalesce.append("r{n}.html_content".format(n=run))
                joins.append(
                    "LEFT JOIN ts_run_{n}.captured_pages r{n}"
                    "       ON o.locale = r{n}.locale AND u.r{n}id = r{n}.url"
                    "      AND o.access_time = r{n}.access_time"
                    .format(n=run))
            columns["html_content"] = "COALESCE(" + ",".join(coalesce) + ")"
            unpackers["html_content"] = \
                lambda x: (zlib.decompress(x).decode("utf-8")
                           if x else "")

        # Fix a column ordering.
        column_order = list(enumerate(columns.keys()))

        query = "SELECT " + ",".join(columns[oc[1]] for oc in column_order)
        query += " "
        query += " ".join(joins)

        if where_clause and self._exclude_partial_where:
            query += " WHERE ({}) AND ({})".format(where_clause,
                                                   self._exclude_partial_where)
        elif where_clause:
            query += " WHERE ({})".format(where_clause)

        elif self._exclude_partial_where:
            query += " WHERE ({})".format(self._exclude_partial_where)

        if ordered == 'url':
            query += " ORDER BY o.url"
        elif ordered == 'locale':
            query += " ORDER BY o.locale, o.url"
        else:
            assert ordered is None

        if limit:
            query += " LIMIT {}".format(limit)

        # This must be a named cursor, otherwise psycopg2 attempts to
        # fetch all the rows at once and they won't fit in memory and
        # we'll crash.  (This is also necessary to make the planner
        # tuning in __init__ be effective.)
        # Note we are not using context-managed cursors because that's
        # not available in the version of psycopg2 on arima.
        cur = self._db.cursor(self._cursor_tag + "_" + str(self._cursor_ctr))
        self._cursor_ctr += 1
        cur.itersize = 5000
        cur.execute(query)
        try:
            for row in cur:
                data = constructor_kwargs.copy()
                for slot, label in column_order:
                    data[label] = unpackers[label](row[slot])

                yield PageObservation(self, **data)
        finally:
            cur.close()

    def get_random_page_observations(self, count, seed,
                                     where_clause="", **kwargs):

        cur = self._db.cursor()
        cur.execute("SELECT min(url), max(url)"
                    "  FROM ts_analysis.page_observations")
        lo, hi = cur.fetchone()

        cur.execute("SELECT x.url FROM generate_series(%s, %s) as x(url)"
                    " LEFT JOIN ts_analysis.page_observations p"
                    "        ON x.url = p.url WHERE p.url IS NULL",
                    (lo, hi))
        gaps = set(x[0] for x in cur.fetchall())

        rng = random.Random(seed)
        sample = []
        while len(sample) < count:
            block = set(rng.sample(range(lo, hi+1), count - len(sample))) - gaps
            sample.extend(block)
        sample.sort()
        selection = "o.url IN (" + ",".join(str(u) for u in sample) + ")"

        if where_clause:
            where_clause = "({}) AND ({})".format(where_clause, selection)
        else:
            where_clause = selection

        return self.get_page_observations(where_clause=where_clause, **kwargs)

    #
    # Methods primarily for internal use by PageText and PageObservation.
    #
    def get_observations_for_text(self, text):
        if text.has_boilerplate:
            where = "o.document_with_bp = {}".format(text.id)
            args = { 'document_with_bp': text }
        else:
            where = "o.document = {}".format(text.id)
            args = { 'document': text }

        return list(self.get_page_observations(where_clause=where,
                                               ordered=None,
                                               constructor_kwargs=args))

    def get_page_text(self, id):
        cur = self._db.cursor()
        cur.execute("SELECT p.id, p.has_boilerplate, p.lang_code,"
                    "       lc.name, p.lang_conf, p.contents"
                    "  FROM ts_analysis.page_text p"
                    "  JOIN ts_analysis.language_codes lc"
                    "    ON p.lang_code = lc.code"
                    " WHERE p.id = %s", (id,))
        return PageText(self, *cur.fetchone())

    def get_raw_page_contents(self, id):
        cur = self._db.cursor()
        cur.execute("SELECT contents FROM ts_analysis.page_text"
                    " WHERE id = %s", (id,))
        return cur.fetchone()[0]

    def get_headings(self, run, locale, url_id):
        cur = self._db.cursor()
        cur.execute("SELECT headings FROM ts_analysis.page_observations"
                    " WHERE run = %s AND locale = %s AND url = %s",
                    (run, locale, url_id))
        return json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8"))

    def get_links(self, run, locale, url_id):
        cur = self._db.cursor()
        cur.execute("SELECT links FROM ts_analysis.page_observations"
                    " WHERE run = %s AND locale = %s AND url = %s",
                    (run, locale, url_id))
        return json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8"))

    def get_resources(self, run, locale, url_id):
        cur = self._db.cursor()
        cur.execute("SELECT resources FROM ts_analysis.page_observations"
                    " WHERE run = %s AND locale = %s AND url = %s",
                    (run, locale, url_id))
        return json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8"))

    def get_dom_stats(self, run, locale, url_id):
        cur = self._db.cursor()
        cur.execute("SELECT dom_stats FROM ts_analysis.page_observations"
                    " WHERE run = %s AND locale = %s AND url = %s",
                    (run, locale, url_id))
        return DOMStatistics(
            json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8")))

    # This query is a little more complicated because the HTML content
    # is only stored in the captured_pages table for the original run.
    def get_html_content(self, run, locale, url_id):
        cur = self._db.cursor()
        cur.execute("SELECT c.html_content"
                    "  FROM ts_run_{n}.captured_pages c"
                    "  JOIN ts_analysis.url_strings u ON u.r{n}id = c.url"
                    " WHERE c.locale = %s AND u.id = %s".format(n=run),
                    (locale, url_id))
        # The blob in the database might be NULL or the empty string;
        # either makes zlib barf.
        blob = cur.fetchone()[0]
        if not blob:
            return ""
        return zlib.decompress(blob).decode("utf-8")
