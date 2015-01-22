# Cython declaration glue for cld2.

from libcpp cimport bool

cdef extern from "compact_lang_det.h" namespace "CLD2":
    ctypedef enum Language:
        UNKNOWN_LANGUAGE

    ctypedef enum Encoding:
        UNKNOWN_ENCODING

    ctypedef struct CLDHints:
        const char *content_language_hint
        const char *tld_hint
        Encoding    encoding_hint
        Language    language_hint

    # hint values
    cdef enum:
        kCLDFlagScoreAsQuads
        kCLDFlagBestEffort

    Language ExtDetectLanguageSummary(
        const char     *buffer,
        size_t          buffer_length,
        const CLDHints *cld_hints,
        int             flags,
        Language       *language3,
        int            *percent3,
        double         *normalized_score3,
        bool           *is_reliable) except +

    const char *LanguageName(Language)
    const char *LanguageCode(Language)
