# Cython declaration glue for cld2.

from libcpp cimport bool
from libcpp.vector cimport vector

cdef extern from "cld2/public/encodings.h" namespace "CLD2":
    ctypedef enum Encoding:
        UNKNOWN_ENCODING

cdef extern from "cld2/public/compact_lang_det.h" namespace "CLD2":
    ctypedef enum Language:
        UNKNOWN_LANGUAGE
        NUM_LANGUAGES

    ctypedef struct CLDHints:
        const char *content_language_hint
        const char *tld_hint
        Encoding    encoding_hint
        Language    language_hint

    ctypedef struct ResultChunk:
        int           offset # should be size_t
        int           bytes  # should be size_t; int32 in header
        unsigned int  lang1  # should be Language; uint16 in header

    ctypedef vector[ResultChunk] ResultChunkVector

    # hint values
    cdef enum:
        kCLDFlagScoreAsQuads
        kCLDFlagBestEffort

    Language ExtDetectLanguageSummary(
        const char        *buffer,
        int                buffer_length,
        bool               is_plain_text,
        const CLDHints    *cld_hints,
        int                flags,
        Language          *language3,
        int               *percent3,
        double            *normalized_score3,
        ResultChunkVector *resultchunkvector,
        int               *text_bytes,
        bool              *is_reliable) nogil except +

    const char *LanguageName(Language) nogil
    const char *LanguageCode(Language) nogil
