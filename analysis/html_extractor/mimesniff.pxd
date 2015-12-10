# Cython declaration glue for mimesniff.c.

cdef extern from "mimesniff.h":
    const char *get_computed_mimetype(const char *mimetype,
                                      const char *charset,
                                      const unsigned char *buffer,
                                      size_t nbytes)
