# This allows other Cython modules to call n_grapheme_clusters more
# efficiently (they do not have to box and unbox the return value).
cpdef Py_ssize_t n_grapheme_clusters(text) except -1
