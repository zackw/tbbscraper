#ifndef _PRESCAN_H
#define _PRESCAN_H

#include <stddef.h> /* size_t */

/**
 * Given an encoding label, return the canonical form of that label as
 * defined by <http://encoding.spec.whatwg.org/#encodings>, or NULL if
 * the label is not recognized.  Note that this performs a *case-sensitive*
 * comparison; if you want case-insensitivity as specified by most
 * encoding-related standards, ASCII-lowercase LABEL yourself first.
 */
extern const char *
canonical_encoding_for_label(const char *label);

/**
 * Examine the first NBYTES bytes of BUFFER to determine its character
 * encoding, on the assumption that BUFFER contains (the top of) an
 * HTML document.  This implements the algorithm specified at
 * https://html.spec.whatwg.org/multipage/syntax.html#prescan-a-byte-stream-to-determine-its-encoding
 * Generally NBYTES should be no more than 1024.
 *
 * The return value is either a canonical encoding label, or NULL if
 * no character encoding was detected.  utf-16{,le,be} *are* mapped to
 * utf-8 (unlike the behavior of the function above).
 *
 * Note that this function *only* implements the "prescan a byte
 * stream..." algorithm, *not* the full "determining the character
 * encoding" algorithm
 * (https://html.spec.whatwg.org/multipage/syntax.html#determining-the-character-encoding).
 * In particular, it does NOT look for UTF-16/8 byte order marks,
 * and it is unaware of any external encoding annotations
 * (e.g. from the HTTP Content-Type header).
 */
extern const char *
prescan_a_byte_stream_to_determine_its_encoding(const char *buffer,
                                                size_t nbytes);

#endif
