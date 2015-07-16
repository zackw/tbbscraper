# Interface definitions for unicode_utils.

cpdef Py_ssize_t n_grapheme_clusters(text) except -1
cpdef unicode normalize_text(text)
cpdef unicode strip_ascii_space(unicode text)
cpdef list split_ascii_space(unicode text)
cpdef bint not_all_whitespace(unicode text)
