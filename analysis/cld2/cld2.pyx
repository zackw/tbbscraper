# Wrap Google's CLD2 into a Python module.
# Note that we do not bother exposing some of the fine details of the
# C++-level API, in particular the encoding and language hints and the
# flags, which are not especially useful in this application.

from libcpp cimport bool as bool_t

cimport compact_lang_det
from compact_lang_det cimport UNKNOWN_ENCODING, UNKNOWN_LANGUAGE, \
                              NUM_LANGUAGES, LanguageCode, LanguageName, \
                              ResultChunkVector

cdef bytes _as_utf8(s):
    if isinstance(s, unicode):
        s = (<unicode>s).encode('utf8')
    return s

cdef class Language:
    """Wrapper around the Language enumeration that CLD2 uses to report
       results.  Stringifies to the ISO 639 code for the language.
       Also has the following properties:

           code    - The ISO 639 code for the language.
           name    - The common name for the language in English.
           score   - The score assigned to this language for the text.
           percent - Probability of this language for the text, as a
                     percentage
    """
    cdef compact_lang_det.Language _lang
    cdef str _lcode
    cdef str _lname

    property code:
        "The ISO 639 code for this language."
        def __get__(self):
            if not self._lcode:
                self._lcode = LanguageCode(self._lang).decode('ascii')
            return self._lcode
    property name:
        "The common name for this language in English."
        def __get__(self):
            if not self._lname:
                self._lname = LanguageName(self._lang).decode('ascii')
            return self._lname

    cdef readonly double score
    cdef readonly int    percent

    def __cinit__(self, compact_lang_det.Language lang,
                  double score, int pct):
        self._lcode  = ''
        self._lname  = ''
        self._lang   = lang
        self.score   = score
        self.percent = pct

    def __str__(self):
        return self.code

cdef class DetectedLanguages:
    """The object returned by `detect`.  Has the following properties:

        - text       The original text.
        - scores     A list of 1 to 3 Language objects, giving the
                     overall detection result for the text.
                     If UNKNOWN_LANGUAGE appears in the list, then
                     it will be the only entry in the list.
        - chunks     List of tuples (language, run), dividing the
                     text into runs of a single language.  Scoring
                     information is not available for chunks.
    """
    cdef readonly str     text
    cdef readonly list    scores
    cdef readonly list    chunks

    def __cinit__(self, str text, list scores, list chunks):
        self.text   = text
        self.scores = scores
        self.chunks = chunks

cpdef detect(text, lang_hint=None, tld_hint=None, want_chunks=False):
    cdef compact_lang_det.CLDHints hints
    if lang_hint is not None:
        # reuse lang_hint as an owning reference to the bytes object
        # returned by _as_utf8
        lang_hint = _as_utf8(lang_hint)
        hints.content_language_hint = lang_hint
    else:
        hints.content_language_hint = NULL
    if tld_hint is not None:
        # ditto for tld_hint
        tld_hint = _as_utf8(tld_hint)
        hints.tld_hint = tld_hint
    else:
        hints.tld_hint = NULL

    # Caller isn't allowed to provide these.
    hints.encoding_hint = UNKNOWN_ENCODING
    hints.language_hint = UNKNOWN_LANGUAGE

    # Must precalculate these before dropping the GIL.
    cdef bytes u8text = _as_utf8(text)
    cdef const char *u8text_raw = u8text
    cdef int u8text_len = len(u8text)
    cdef bool_t want_chunks_raw = want_chunks

    # hoo boy, ExtDetectLanguageSummary has a lot of out-parameters, eh?
    cdef compact_lang_det.Language top3[3]
    cdef int                       pct3[3]
    cdef double                    score3[3]
    cdef ResultChunkVector         raw_chunks
    cdef int                       text_bytes
    cdef bool_t                    reliable

    with nogil:
        chosen_lang = compact_lang_det.ExtDetectLanguageSummary(
            u8text_raw, u8text_len, True, &hints, 0,
            top3, pct3, score3,
            &raw_chunks if want_chunks_raw else NULL,
            &text_bytes, &reliable)

    cdef list scores = []
    # This typecast seems to be required only in -3 mode :-(
    if chosen_lang == <int>UNKNOWN_LANGUAGE or not reliable:
        scores.append(Language(UNKNOWN_LANGUAGE, 0, 0))
    else:
        # If chosen_lang isn't UNKNOWN_LANGUAGE, it will be one of the top3.
        # Sort that to the beginning.
        if chosen_lang == top3[0]:   a,b,c = 0,1,2
        elif chosen_lang == top3[1]: a,b,c = 1,0,2
        elif chosen_lang == top3[2]: a,b,c = 2,0,1
        else:
            raise AssertionError("chosen_lang not found in top3")

        assert top3[a] != <int>UNKNOWN_LANGUAGE
        scores.append(Language(top3[a], score3[a], pct3[a]))

        if top3[b] != <int>UNKNOWN_LANGUAGE:
            scores.append(Language(top3[b], score3[b], pct3[b]))
        if top3[c] != <int>UNKNOWN_LANGUAGE:
            scores.append(Language(top3[c], score3[c], pct3[c]))

    cdef list chunks
    if want_chunks:
        chunks = [
            (Language(<compact_lang_det.Language>x.lang1, -1, -1),
             u8text[x.offset : (x.offset + x.bytes)].decode('utf-8'))
            for x in raw_chunks
        ]
    else:
        chunks = []

    return DetectedLanguages(text, scores, chunks)

cpdef get_all_languages():
    """Returns a dictionary mapping language codes to language names for
    all languages supported by this version of cld2."""
    rv = {}
    for i in range(NUM_LANGUAGES):
        lname = LanguageName(<compact_lang_det.Language>i).decode("ascii")
        lcode = LanguageCode(<compact_lang_det.Language>i).decode("ascii")
        if lcode != "":
            rv[lcode] = lname

    return rv
