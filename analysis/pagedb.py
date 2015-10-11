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
       analysis.capture_pruned_content table.  Must cross-reference to
       .page_observations to learn where it came from.

       Properties:
          origin          - Serial number of this text blob.
                            (.origin field in the database, *not* .id)
          lang_code       - ISO 639 code: language as identified by CLD2.
          lang_name       - English name of the language.
          lang_conf       - % confidence in the language identification.
          hash            - SHA256 hash of the *compressed* text.
          contents        - The text itself (lazily uncompressed).
          raw_hash        - SHA256 hash of the *compressed* unpruned text.
          raw_contents    - The text before pruning.
          observations    - Array of PageObservation objects: all the page
                            observations that have this text.
          tfidf           - Raw tf-idf scores for each word in the text.
          nfidf           - Augmented normalized tf-idf scores ditto.
          headings         - Array of strings: all text found within <hN> tags.
          links            - Array of strings: all outbound hyperlinks
                             from this page.
          resources        - Array of strings: all resources loaded by
                             this page.
          dom_stats        - DOMStatistics object counting tags and
                             tree depth.
    """
    def __init__(self, db, origin,
                 lang_code, lang_name, lang_conf, hash, raw_hash,
                 *,
                 contents=None, raw_contents=None, tfidf=None, nfidf=None,
                 headings=None, links=None, resources=None, dom_stats=None):
        self._db             = db
        self.origin          = origin
        self.lang_code       = lang_code
        self.lang_name       = lang_name
        self.lang_conf       = lang_conf
        self.hash            = hash
        self.raw_hash        = raw_hash

        # For memory efficiency, these are lazily loaded from the database.
        self._contents       = contents
        self._raw_contents   = raw_contents
        self._tfidf          = tfidf
        self._nfidf          = nfidf
        self._headings       = headings
        self._links          = links
        self._resources      = resources
        self._dom_stats      = dom_stats
        self._observations   = None

    def __hash__(self):
        return self.id

    @property
    def contents(self):
        if self._contents is None:
            self._contents = self._db.get_contents_for_text(self.origin)
        return self._contents

    @property
    def raw_contents(self):
        if self._raw_contents is None:
            self._raw_contents = self._db.get_raw_contents_for_text(self.origin)
        return self._raw_contents

    @property
    def tfidf(self):
        if self._tfidf is None:
            self._tfidf = self._db.get_text_statistic('tfidf', self.origin)
        return self._tfidf

    @property
    def nfidf(self):
        if self._nfidf is None:
            self._nfidf = self._db.get_text_statistic('nfidf', self.origin)
        return self._nfidf

    @property
    def headings(self):
        if self._headings is None:
            self._headings = \
                self._db.get_headings_for_text(self.origin)
        return self._headings

    @property
    def links(self):
        if self._links is None:
            self._links = \
                self._db.get_links_for_text(self.origin)
        return self._links

    @property
    def resources(self):
        if self._resources is None:
            self._resources = \
                self._db.get_resources_for_text(self.origin)
        return self._resources

    @property
    def dom_stats(self):
        if self._dom_stats is None:
            self._dom_stats = \
                self._db.get_dom_stats_for_text(self.origin)
        return self._dom_stats

    @property
    def observations(self):
        if self._observations is None:
            self._observations = \
                self._db.get_observations_for_text(self.origin)
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
       are lazily loaded.

           id               - Unique id for this observation.
           run              - Which data collection run this is from.
           locale           - ISO 631 code of country where the page
                              was observed.
           country          - English name corresponding to 'locale'.
           vantage          - Location within the country where the page
                              was observed; may be the empty string.
           url              - The page URL.
           access_time      - Date and time the page was accessed (UTC).
           elapsed_time     - Time taken to capture the page (seconds).
           result           - High-level result code.
           detail           - More detailed result code.
           redir_url        - URL of the page after following redirections.
           document_id      - Database ID of the text of the page.
           document         - PageText object: the text of the page.

       NOTE: retrieving the capture log is currently not implemented.

    """

    def __init__(self, db, id, run, locale, country, vantage,
                 access_time, elapsed_time, result, detail, redir_url,
                 document_id,
                 *,
                 document=None):

        self._db                  = db
        self.id                   = id
        self.run                  = run
        self.locale               = locale
        self.country              = country
        self.vantage              = vantage
        self.url                  = url
        self.access_time          = access_time
        self.elapsed_time         = elapsed_time
        self.result               = result
        self.detail               = detail
        self.redir_url            = redir_url
        self.document_id          = document_id
        self._document            = document

    def __hash__(self):
        return self.id

    @property
    def document(self):
        if self._document is None:
            self._document = self._db.get_page_text(self._document_id)
        return self._document



class PageDB:
    """Wraps a database handle and knows how to extract pages or other
       interesting material (add queries as they become useful!)"""

    def __init__(self, connstr, only_runs=[]):
        """If 'only_runs' is a list, only those runs will be examined."""

        self._locales    = None
        self._runs       = [int(x) for x in only_runs]
        self._lang_codes = None
        self._lang_names = None
        self._cursor_tag = "pagedb_qtmp_{}_{}".format(os.getpid(), id(self))
        self._cursor_ctr = 0

        if "=" not in connstr:
            connstr = "dbname="+connstr
        self._db = psycopg2.connect(connstr)
        cur = self._db.cursor()

        # All tables are referenced with explicit schemas.
        cur.execute("SET search_path TO ''")

        # Discourage the query planner from doing anything that will
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
           moderately expensive query, so we memoize it.
        """
        if self._locales is None:
            cur = self._db.cursor()
            if not self._runs:
                cur.execute("SELECT DISTINCT country"
                            "  FROM collection.captured_pages"
                            " ORDER BY country")
            else:
                cur.execute("SELECT DISTINCT country"
                            "  FROM collection.captured_pages"
                            " WHERE run = ANY(%s)"
                            " ORDER BY country", (self._runs,))
            self._locales = [row[0] for row in cur]
        return self._locales

    @property
    def lang_codes(self):
        """Retrieve a list of all available language codes.  Cost similar
           to locales.
        """
        if self._lang_codes is None:
            cur = self._db.cursor()
            cur.execute("SELECT DISTINCT lang_code"
                        "  FROM analysis.capture_pruned_content"
                        " ORDER BY lang_code")
            self._lang_codes = [row[0] for row in cur]
        return self._lang_codes

    @property
    def lang_names(self):
        """Retrieve a list of all available language names.  Note that the
           order of this list matches the order of language _codes_, i.e.
           zip(db.lang_codes, db.lang_names) produces a correct mapping.
        """
        if self._lang_names is None:
            cur = self._db.cursor()
            cur.execute("SELECT DISTINCT lc.name"
                        "  FROM analysis.capture_pruned_content cp"
                        "  JOIN collection.language_codes lc"
                        "    ON cp.lang_code = lc.code"
                        " ORDER BY cp.lang_code")
            self._lang_names = [row[0] for row in cur]
        return self._lang_names

    #
    # Methods for retrieving pages or observations in bulk.
    #
    def get_page_texts(self, *,
                       where_clause="",
                       ordered=None,
                       limit=None,
                       load=["contents"]):
        """Retrieve page texts from the database matching the where_clause.
           This is a generator, which produces one PageText object per row.

           Useful 'where_clause' terms include

               lang_code       [=, <>] <ISO 639 code>

           'ordered' may be either None or 'lang' to sort by language code.

           'limit' may be either None for no limit, or a positive integer;
           in the latter case at most that many page texts are produced.

           'load' is a list of PageText attributes to load eagerly from the
           database: zero or more of

               contents raw_contents tfidf nfidf headings links resources
               dom_stats

           If you know you're going to use these for every page, requesting
           them up front is more efficient than allowing them to be loaded
           lazily.
        """

        def up_iden(x):  return x
        def up_text(x):  return zlib.decompress(x).decode("utf-8")
        def up_ojson(x): return json.loads(zlib.decompress(x).decode("utf-8")) if x else {}
        def up_ajson(x): return json.loads(zlib.decompress(x).decode("utf-8")) if x else []
        def up_dstat(x): return DOMStatistics(json.loads(zlib.decompress(x).decode("utf-8")) if x else {})

        no_join    = []
        tfidf_join = ["LEFT JOIN analysis.page_text_stats st"
                      "       ON st.stat = 'tfidf' AND st.text_id = p.id"]
        nfidf_join = ["LEFT JOIN analysis.page_text_stats sn"
                      "       ON sn.stat = 'nfidf' AND sn.text_id = p.id"]

        columns = {
            "origin"          : "p.origin",
            "lang_code"       : "p.lang_code",
            "lang_name"       : "p.lang_name",
            "lang_conf"       : "p.lang_conf",
            "hash"            : "p.hash",
            "raw_hash"        : "p.raw_hash",
        }

        op_columns = {
            "contents":     ("p.contents",     up_text, no_join),
            "raw_contents": ("p.raw_contents", up_text, no_join),
            "tfidf":        ("st.data",        up_ojson, tfidf_join),
            "nfidf":        ("sn.data",        up_ojson, nfidf_join),
            "headings":     ("p.headings",     up_ajson, no_join),
            "links":        ("p.links",        up_ajson, no_join),
            "resources":    ("p.resources",    up_ajson, no_join),
            "dom_stats":    ("p.dom_stats",    up_dstat, no_join)
        }

        joins = ["  FROM analysis.page_text p"]
        unpackers = defaultdict(lambda: up_iden)

        for col in load:
            if col not in op_columns:
                raise ValueError("unknown optional column "+repr(col))
            dcol, unpack, join = op_columns[col]
            columns[col] = dcol
            unpackers[col] = unpack
            joins.extend(join)

        # Fix a column ordering.
        column_order = list(enumerate(columns.keys()))

        query = "SELECT " + ",".join(columns[oc[1]] for oc in column_order)
        query += " "
        query += " ".join(joins)

        if where_clause:
            query += " WHERE ({})".format(where_clause)

        if ordered == "lang":
            query += " ORDER BY p.lang_code"
        elif ordered is not None:
            raise ValueError("unknown ordering: " + ordered)

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
                data = { label: unpackers[label](row[slot])
                         for slot, label in column_order }
                yield PageText(self, **data)

        finally:
            cur.close()

    def get_random_page_texts(self, count, seed, where_clause="", **kwargs):

        cur = self._db.cursor()
        cur.execute("SELECT min(id), max(id)"
                    "  FROM collection.capture_html_content")
        lo, hi = cur.fetchone()

        cur.execute("SELECT x.id FROM generate_series(%s, %s) AS x(id)"
                    "  LEFT JOIN collection.capture_html_content p"
                    "         ON x.id = p.id WHERE p.id IS NULL", (lo, hi))
        gaps = set(x[0] for x in cur.fetchall())

        rng = random.Random(seed)
        sample = []
        while len(sample) < count:
            block = set(rng.sample(range(lo, hi+1), count - len(sample))) - gaps
            sample.extend(block)
        sample.sort()
        selection = "p.origin IN (" + ",".join(str(id) for id in sample) + ")"

        if where_clause:
            where_clause = "({}) AND ({})".format(where_clause, selection)
        else:
            where_clause = selection

        return self.get_page_texts(where_clause=where_clause, **kwargs)

    def get_page_observations(self, *,
                              where_clause="",
                              ordered='url',
                              limit=None,
                              constructor_kwargs={}):
        """Retrieve page observations from the database matching the
           where_clause.  This is a generator, which produces one
           PageObservation object per row.

           Useful 'where_clause' terms include

               country = <ISO 631 code>
               result = <high-level result>

           'limit' may be either None for no limit, or a positive integer;
           in the latter case at most that many page texts are produced.

           'ordered' may be None for unordered, 'url' to sort by URL id
           (_not_ actual URL text), or 'country' to sort by country code.

           'constructor_kwargs' is for passing additional arguments to the
           PageObservation constructor; external code should not need it.
        """

        query = ("SELECT id, run, country, country_name, vantage,"
                 "       orig_url, access_time, elapsed_time, result, detail,"
                 "       redir_url, document_id"
                 "  FROM analysis.page_observations")

        if where_clause and self._runs:
            query += " WHERE ({}) AND run IN ({})".format(
                where_clause, ",".join(str(r) for r in self._runs))
        elif where_clause:
            query += " WHERE ({})".format(where_clause)
        elif self._runs:
            query += " WHERE run IN ({})".format(
                ",".join(str(r) for r in self._runs))

        if ordered == 'url':
            query += " ORDER BY orig_url"
        elif ordered == 'country':
            query += " ORDER BY country, orig_url"
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
                yield PageObservation(self, *row, **constructor_kwargs)

        finally:
            cur.close()

    def get_random_page_observations(self, count, seed,
                                     where_clause="", **kwargs):

        cur = self._db.cursor()
        if self._runs:
            cur.execute("SELECT min(url), max(url)"
                        "  FROM collection.captured_pages"
                        " WHERE run = ANY(%s)", (self._runs,))
        else:
            cur.execute("SELECT min(url), max(url)"
                        "  FROM collection.captured_pages")

        lo, hi = cur.fetchone()

        if self._runs:
            cur.execute("SELECT x.url FROM generate_series(%s, %s) as x(url)"
                        " LEFT JOIN collection.captured_pages p"
                        "        ON x.url = p.url"
                        " WHERE p.url IS NULL AND p.run = ANY(%s)",
                        (lo, hi, self._runs))
        else:
            cur.execute("SELECT x.url FROM generate_series(%s, %s) as x(url)"
                        " LEFT JOIN collection.captured_pages p"
                        "        ON x.url = p.url WHERE p.url IS NULL",
                        (lo, hi))
        gaps = set(x[0] for x in cur.fetchall())

        rng = random.Random(seed)
        sample = []
        while len(sample) < count:
            block = set(rng.sample(range(lo, hi+1), count - len(sample))) - gaps
            sample.extend(block)
        sample.sort()
        selection = "url IN (" + ",".join(str(u) for u in sample) + ")"

        if where_clause:
            where_clause = "({}) AND ({})".format(where_clause, selection)
        else:
            where_clause = selection

        return self.get_page_observations(where_clause=where_clause, **kwargs)

    #
    # Methods primarily for internal use by PageText and PageObservation.
    #
    def get_observations_for_text(self, origin):
        return list(self.get_page_observations(
            where_clause       = "document_id = {}".format(origin),
            ordered            = None,
            constructor_kwargs = { "document": text }))

    def get_page_text(self, origin):
        cur = self._db.cursor()
        cur.execute("SELECT origin, lang_code, lang_name, lang_conf,"
                    "       hash, raw_hash"
                    "  FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return PageText(self, *cur.fetchone())

    def get_contents_for_text(self, origin):
        cur = self._db.cursor()
        cur.execute("SELECT contents FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return zlib.decompress(cur.fetchone()[0]).decode("utf-8")

    def get_raw_contents_for_text(self, origin):
        cur = self._db.cursor()
        cur.execute("SELECT raw_contents FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return zlib.decompress(cur.fetchone()[0]).decode("utf-8")

    def get_headings(self, origin):
        cur = self._db.cursor()
        cur.execute("SELECT headings FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8"))

    def get_links(self, origin):
        cur = self._db.cursor()
        cur.execute("SELECT links FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8"))

    def get_resources(self, origin):
        cur = self._db.cursor()
        cur.execute("SELECT resources FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8"))

    def get_dom_stats(self, run, locale, url_id):
        cur = self._db.cursor()
        cur.execute("SELECT dom_stats FROM analysis.page_text"
                    " WHERE origin = %s", (origin,))
        return DOMStatistics(
            json.loads(zlib.decompress(cur.fetchone()[0]).decode("utf-8")))

    #
    # Corpus-wide and per-document statistics.
    #
    def get_corpus_statistic(self, stat, lang):
        cur = self._db.cursor()
        cur.execute("SELECT n_documents, data FROM analysis.corpus_stats"
                    " WHERE stat = %s AND lang = %s AND runs = %s",
                    (stat, lang, self._runs))
        row = cur.fetchone()
        if row:
            return (row[0], json.loads(zlib.decompress(row[1]).decode('utf-8')))
        else:
            return (0, {})

    def update_corpus_statistics(self, lang, n_documents,
                                 statistics):
        cur = self._db.cursor()

        # This is the big-hammer exclusive-lockout approach to upsert.
        # It's possible that SHARE ROW EXCLUSIVE would be good enough,
        # but I don't really understand the difference between
        # EXCLUSIVE and SHARE ROW EXCLUSIVE, so I'm being conservative.
        try:
            cur.execute("BEGIN")
            cur.execute("LOCK analysis.corpus_stats IN EXCLUSIVE MODE")

            for stat, data in statistics:
                blob = zlib.compress(json.dumps(data, separators=(',',':'))
                                     .encode("utf-8"))

                # try UPDATE first, if it affects zero rows, then INSERT
                cur.execute("UPDATE analysis.corpus_stats"
                            "   SET n_documents = %s, data = %s"
                            " WHERE stat=%s AND lang=%s AND runs=%s",
                            (n_documents, blob, stat, lang, self._runs))

                if cur.rowcount == 0:
                    cur.execute("INSERT INTO analysis.corpus_stats"
                                " (stat, lang, runs, "
                                "  n_documents, data)"
                                " VALUES (%s, %s, %s, %s, %s)",
                                (stat, lang, self._runs,
                                 n_documents, blob))

            self._db.commit()

        except:
            self._db.rollback()
            raise

    def update_corpus_statistic(self, stat, lang, n_documents, data):
        self.update_corpus_statistics(lang, n_documents, [(stat, data)])

    def get_text_statistic(self, stat, text_id):
        cur = self._db.cursor()
        cur.execute("SELECT data FROM analysis.pruned_content_stats"
                    " WHERE stat = %s AND text_id = %s AND runs = %s",
                    (stat, text_id, self._runs))
        row = cur.fetchone()
        if row and row[0]:
            return json.loads(zlib.decompress(row[0]).decode('utf-8'))
        return {}

    def prepare_text_statistic(self, stat):
        cur = self._db.cursor()

        # For document statistics, we take a two-phase approach to the
        # upsert problem.  This function wields the big-lockout
        # hammer, but all it does is ensure that every document in
        # page_text has a row for this statistic in pruned_content_stats;
        # the actual data will be null.  This allows update_text_statistic
        # to just do a regular old UPDATE and not worry about missing rows.
        try:
            cur.execute("BEGIN")
            cur.execute("LOCK analysis.pruned_content_stats IN EXCLUSIVE MODE")
            cur.execute("INSERT INTO analysis.pruned_content_stats"
                        "  (stat, text_id, runs)"
                        "SELECT %s AS stat, p.origin AS text_id, %s AS runs"
                        "  FROM analysis.page_text p"
                        " WHERE NOT EXISTS ("
                        "  SELECT 1 FROM analysis.pruned_content_stats ps"
                        "   WHERE ps.stat = %s AND ps.text_id = p.id"
                        "     AND runs = %s)",
                        (stat, self._runs, stat, self._runs))
            cur.execute("SELECT lang_code FROM analysis.page_text p"
                        "   LEFT JOIN analysis.pruned_content_stats ps"
                        "   ON ps.stat = %s AND ps.text_id = p.id AND runs = %s"
                        "   WHERE ps.data IS NULL",
                        (stat, self._runs))
            rv = set(row[0] for row in cur)
            rv.discard("und")
            rv.discard("zxx")
            self._db.commit()
            return rv

        except:
            self._db.rollback()
            raise

    def update_text_statistic(self, stat, text_id, data):
        cur = self._db.cursor()
        blob = zlib.compress(json.dumps(data, separators=(',',':'))
                             .encode("utf-8"))
        cur.execute("UPDATE analysis.pruned_content_stats"
                    "   SET data = %s"
                    " WHERE stat = %s AND text_id = %s AND runs = %s",
                    (blob, stat, text_id, self._runs))
        if cur.rowcount == 0:
            raise RuntimeError("%s/%s/%r: no row in pruned_content_stats"
                               % (stat, text_id, self._runs))

    # Transaction manager issues a regular database transaction.
    def __enter__(self):
        cur = self._db.cursor()
        cur.execute("BEGIN")
        cur.close()
        return self

    def __exit__(self, ty, vl, tb):
        if ty is None:
            self._db.commit()
        else:
            self._db.rollback()
        return False
