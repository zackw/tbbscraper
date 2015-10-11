# Cython declaration glue for prescan.c.

cdef extern from "prescan.h":
    const char *canonical_encoding_for_label(const char *encoding)

    const char *prescan_a_byte_stream_to_determine_its_encoding(const char *b,
                                                                size_t len)
