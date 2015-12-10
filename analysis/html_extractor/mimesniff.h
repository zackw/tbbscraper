#ifndef _MIMESNIFF_H
#define _MIMESNIFF_H

#include <stddef.h> /* size_t */

/**
 * Examine the first NBYTES bytes of BUFFER, plus the MIMETYPE and
 * CHARSET provided as HTTP metadata, and decide what MIME type
 * BUFFER actually contains.
 */
extern const char *get_computed_mimetype(const char *mimetype,
                                         const char *charset,
                                         const unsigned char *buffer,
                                         size_t nbytes);

#endif
