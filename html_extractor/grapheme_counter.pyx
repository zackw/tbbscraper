# This code inspired by the Rust library for grapheme segmentation:
# https://github.com/sbillig/rust-grapheme/  Unfortunately, Python's
# unicodedata module does not include the Grapheme_Cluster_Break character
# property, so we have to define it by hand; that's done in the generated
# files gbp_tbl.*.

from gbp_tbl cimport *

# Input normalization per http://docs.cython.org/src/tutorial/strings.html
cdef unicode _ustring(s):
    if isinstance(s, unicode):
        return <unicode>s
    elif isinstance(s, bytes):
        # silently decode
        return (<bytes>s).decode('utf-8')
    else:
        raise TypeError("expected a string")

cpdef n_grapheme_clusters(text):
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
