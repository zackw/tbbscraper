# Wrap Google's CLD2 into a Python module.
# Note that we do not bother exposing some of the fine details of the
# C++-level API, in particular the encoding and language hints and the
# flags, which are not especially useful in this application.

from libcpp cimport bool as bool_t

cimport compact_lang_det
from compact_lang_det cimport UNKNOWN_ENCODING, UNKNOWN_LANGUAGE, \
                              LanguageCode, LanguageName

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
    property code:
        "The ISO 639 code for this language."
        def __get__(self):
            return LanguageCode(self._lang).decode('ascii')
    property name:
        "The common name for this language in English."
        def __get__(self):
            return LanguageName(self._lang).decode('ascii')

    cdef readonly double score
    cdef readonly int    percent

    def __cinit__(self, compact_lang_det.Language lang,
                  double score, int pct):
        self._lang = lang
        self.score = score
        self.percent = pct

    def __str__(self):
        return self.code

cdef bytes _as_utf8(s):
    if isinstance(s, unicode):
        s = (<unicode>s).encode('utf8')
    return s

cpdef detect(text, lang_hint=None, tld_hint=None):
    cdef compact_lang_det.CLDHints hints
    if lang_hint is not None:
        lang_hint = _as_utf8(lang_hint)
        hints.content_language_hint = lang_hint
    else:
        hints.content_language_hint = NULL
    if tld_hint is not None:
        tld_hint = _as_utf8(tld_hint)
        hints.tld_hint = tld_hint
    else:
        hints.tld_hint = NULL

    # Caller isn't allowed to provide these.
    hints.encoding_hint = UNKNOWN_ENCODING
    hints.language_hint = UNKNOWN_LANGUAGE

    text = _as_utf8(text)

    cdef compact_lang_det.Language top3[3]
    cdef int pct3[3]
    cdef double score3[3]
    cdef bool_t reliable
    chosen_lang = compact_lang_det.ExtDetectLanguageSummary(
        text, len(text), &hints, 0, top3, pct3, score3, &reliable)

    # This typecast seems to be required only in -3 mode :-(
    if chosen_lang == <int>UNKNOWN_LANGUAGE or not reliable:
        return [Language(UNKNOWN_LANGUAGE, 0, 0)]

    # If chosen_lang isn't UNKNOWN_LANGUAGE, it will be one of the top3.
    # Sort that to the beginning.
    if chosen_lang == top3[0]:   a,b,c = 0,1,2
    elif chosen_lang == top3[1]: a,b,c = 1,0,2
    elif chosen_lang == top3[2]: a,b,c = 2,0,1
    else:
        raise AssertionError("chosen_lang not found in top3")

    assert top3[a] != <int>UNKNOWN_LANGUAGE
    rv = [Language(top3[a], score3[a], pct3[a])]

    if top3[b] != <int>UNKNOWN_LANGUAGE:
        rv.append(Language(top3[b], score3[b], pct3[b]))
    if top3[c] != <int>UNKNOWN_LANGUAGE:
        rv.append(Language(top3[c], score3[c], pct3[c]))

    return rv
