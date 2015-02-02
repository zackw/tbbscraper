# Text crunching utilities used by both boilerplate_removal and _extractor.

__all__ = ["split_ascii_space",
           "strip_ascii_space",
           "normalize_text",
           "n_grapheme_clusters"]

# Input normalization per http://docs.cython.org/src/tutorial/strings.html
cdef inline unicode _ustring(s):
    if isinstance(s, unicode):
        return <unicode>s
    elif isinstance(s, bytes):
        # silently decode
        return (<bytes>s).decode('utf-8')
    else:
        raise TypeError("expected a string")

cdef inline unicode _ustringv(s):
    if isinstance(s, unicode):
        return <unicode>s
    elif isinstance(s, bytes):
        # silently decode
        return (<bytes>s).decode('utf-8')
    elif isinstance(s, list):
        return "".join(s)
    else:
        raise TypeError("expected a string")

# All text chunks are fed through a normalizer before processing.
# We apply NFKC despite its occasionally destructive consequences,
# because downstream processing really needs some of the transforms it
# does (full/halfwidth CJK, removal of ligatures) and shouldn't be
# sensitive to things like losing mathematical semantics.  We also
# compress all runs of whitespace to a single U+0020, and strip
# leading and trailing spaces; this is even more aggressive than NFKC,
# which, for instance, converts U+2000 through U+200A into U+0020, but
# leaves U+0009 and U+000A alone, and doesn't collapse runs.

from re import compile as _Regex
from re import DOTALL  as _Re_DOTALL
from unicodedata import normalize as unicode_norm

WSRE = _Regex("\\s+")
cpdef unicode normalize_text(text):
    cdef unicode utext = _ustringv(text)
    return WSRE.sub(" ", unicode_norm("NFKC", utext)).strip()

NAWSRE = _Regex("\\S")
cpdef bint not_all_whitespace(unicode text):
    return (NAWSRE.search(text) is not None)

# The HTML spec makes a distinction between "space characters", which
# are exclusively ASCII, and "White_Space characters", which include
# all of Unicode's whitespace characters.  Only "space characters" are
# to be stripped from URL attributes.

SRE  = _Regex("[ \t\r\n\f]+")
SSRE = _Regex("^[ \t\r\n\f]*(.*?)[ \t\r\n\f]*$", _Re_DOTALL)
cpdef unicode strip_ascii_space(unicode s):
        return SSRE.sub("\\1", s)

cpdef list split_ascii_space(unicode s):
        rv = SRE.split(s)

        # Remove leading and trailing empty strings if necessary.
        # Note that [''][1:-2] cheerfully returns [], so we don't have
        # to special-case that.
        if not rv:
            return rv
        if rv[0] != '' and rv[-1] != '':
            return rv

        left = 0
        right = -1
        if rv[0] == '': left += 1
        if rv[-1] == '': right -= 1
        return rv[left:right]

# This code inspired by the Rust library for grapheme segmentation:
# https://github.com/sbillig/rust-grapheme/  Unfortunately, Python's
# unicodedata module does not include the Grapheme_Cluster_Break character
# property, so we have to define it by hand; that's done in the generated
# files gbp_tbl.*.

from gbp_tbl cimport *

cpdef Py_ssize_t n_grapheme_clusters(text) except -1:
    cdef unicode utext = _ustring(text)

    # Setting the "previous character"'s class to GBP_Control at the
    # beginning of the string implements UAX#29 rule GB1, because
    # GBP_Control invariably has a cluster break after it.
    cdef unsigned long n = 0
    cdef GBP_Class prev = GBP_Control
    cdef GBP_Class cur
    for cpoint in utext:
        cur = GBP_GetClass(cpoint)
        if GBP_CLUSTER_BOUNDARY[<unsigned>prev][<unsigned>cur]:
            n += 1

        prev = cur

    # It might seem that we should add one more here because of rule GB2,
    # but that would be wrong.  Consider: "ab" -> (NUL, a) + (a, b) -> 2
    return n
