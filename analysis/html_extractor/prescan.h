#ifndef _META_SCANNER_H
#define _META_SCANNER_H

#include <stddef.h> /* size_t */

extern const char *
canonical_encoding_for_label(const char *encoding);

extern const char *
prescan_a_byte_stream_to_determine_its_encoding(const char *buffer,
                                                size_t nbytes);

#endif
