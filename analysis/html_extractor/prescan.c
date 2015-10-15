/* HTML5 mandated algorithm for character set sniffing.
 * Spec: https://html.spec.whatwg.org/multipage/syntax.html#prescan-a-byte-stream-to-determine-its-encoding
 * Original Java version by Henri Sivonen: https://hg.mozilla.org/projects/htmlparser/raw-file/889da0df0868/src/nu/validator/htmlparser/impl/MetaScanner.java
 *
 * Copyright (c) 2007 Henri Sivonen
 * Copyright (c) 2008-2015 Mozilla Foundation
 * Copyright (c) 2015 Zack Weinberg
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include "prescan.h"
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* Significant string literals. */
static const char CHARSET[]      = "harset";
static const char CONTENT[]      = "ontent";
static const char HTTP_EQUIV[]   = "ttp-equiv";
static const char CONTENT_TYPE[] = "content-type";

#define CHARSET_LEN      (sizeof CHARSET      - 1)
#define CONTENT_LEN      (sizeof CONTENT      - 1)
#define HTTP_EQUIV_LEN   (sizeof HTTP_EQUIV   - 1)
#define CONTENT_TYPE_LEN (sizeof CONTENT_TYPE - 1)

/* Constants for the state machine that recognizes the tag name "meta". */
enum {
  NO = 0, M, E, T, A
};

/* Constants for handling http-equiv. */
enum {
  HTTP_EQUIV_NOT_SEEN = 0,
  HTTP_EQUIV_CONTENT_TYPE,
  HTTP_EQUIV_OTHER
};

#define isAsciiWhitespace(c) \
  ((c) == ' ' || (c) == '\t' || (c) == '\r' || (c) == '\n' || (c) == '\f')

#define toAsciiLowerCase(c) (((c) >= 'A' && (c) <= 'Z') ? (c) + 0x20 : (c))

struct canonical_encoding
{
  const char *encoding;
  const char *canonical;
};

/* The canonical list of encodings ... */
static const struct canonical_encoding canonical_encodings[] = {
#define E(l,e) { l, e },
#include "encodings.inc"
#undef E
};

/* ... and the minimum and maximum number of bytes required to hold a
   not-yet-canonicalized encoding label.  */
#include "uncanon-max.inc"

static int
canonical_encoding_compar(const void *k, const void *v)
{
  const char *key = k;
  const struct canonical_encoding *val = v;
  return strcmp(key, val->encoding);
}

const char *
canonical_encoding_for_label(const char *label)
{
  struct canonical_encoding *match =
    bsearch(label, canonical_encodings,
            sizeof(canonical_encodings) / sizeof(struct canonical_encoding),
            sizeof(struct canonical_encoding),
            canonical_encoding_compar);

  if (match)
    return match->canonical;
  return 0;
}

static const char *
validate_charset(const char *charset, size_t charset_len)
{
  while (charset_len && isAsciiWhitespace(*charset)) {
    charset++;
    charset_len--;
  }
  while (charset_len && isAsciiWhitespace(charset[charset_len-1])) {
    charset_len--;
  }

  if (charset_len < UNCANON_LABEL_MIN || charset_len > UNCANON_LABEL_MAX)
    /* The label is too short or too long, it cannot possibly be in
       the table. */
    return 0;

  char label[UNCANON_LABEL_MAX + 1];
  for (size_t i = 0; i < charset_len; i++) {
    label[i] = toAsciiLowerCase(charset[i]);
  }
  label[charset_len] = '\0';

  return canonical_encoding_for_label(label);
}

static const char *
validate_content(const char *content, size_t content_len)
{
  const char *charset = 0, *p, *limit;
  size_t charset_len = 0;
  size_t charset_idx = SIZE_MAX;
  char c;
  char quote = 0;
  bool saw_equals = false;
  bool saw_closequote = false;

  for (p = content, limit = p + content_len; p < limit; p++) {
    c = *p;
    if (quote) {
      if (c == quote ||
          (quote == ' ' &&
           (c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ';'))) {
        saw_closequote = true;
        break;
      }

    } else if (saw_equals) {
      if (c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r')
        continue;

      if (c == '"' || c == '\'') {
        quote = c;
        charset = p+1;
        continue;
      }

      quote = ' ';
      charset = p;
      continue;

    } else if (charset_idx == CHARSET_LEN) {
      if (c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r')
        continue;
      else if (c == '=')
        saw_equals = true;
      else
        return 0;

    } else if (charset_idx < CHARSET_LEN) {
      if (toAsciiLowerCase(c) == CHARSET[charset_idx])
        charset_idx++;
      else
        charset_idx = SIZE_MAX;

    } else if (*p == 'c' || *p == 'C') {
      charset_idx = 0;
    }
  }
  if (quote && (quote == ' ' || saw_closequote))
    charset_len = p - charset;
  if (charset && charset_len)
    return validate_charset(charset, charset_len);
  return 0;
}


const char *
prescan_a_byte_stream_to_determine_its_encoding(const char *buffer,
                                                size_t nbytes)
{
  /* The data source. */
  const char *p     = buffer;
  const char *limit = buffer + nbytes;
  int c             = -1;
#undef  READ
#define READ() (p >= limit ? -1 : *p++)

  /* Whether we are within a tag named "meta". */
  unsigned int metaState;

  /* Whether we have encountered "http-equiv". */
  unsigned int httpEquivState;

  /* The current position in recognizing the attribute name "content". */
  size_t contentIndex;

  /* The current position in recognizing the attribute name "charset". */
  size_t charsetIndex;

  /* The current position in recognizing the attribute name "http-equiv". */
  size_t httpEquivIndex;

  /* The current position in recognizing the attribute value "content-type". */
  size_t contentTypeIndex;

  /* The character that terminates the current attribute value: ', ",
     or white. */
  char valueQuote;

  /* The current attribute value, itself. */
  const char *attrValue;

  /* Values that might get returned.  */
  const char *content;
  size_t content_len;
  const char *charset;
  size_t charset_len;
  const char *rv;

#define HANDLE_EMPTY_ATTRIBUTE_VALUE() do {                     \
  if (metaState == A) {                                         \
    if (contentIndex == CONTENT_LEN && !content) {              \
      content = p; content_len = 0;                             \
    } else if (charsetIndex == CHARSET_LEN && !charset) {       \
      charset = p; charset_len = 0;                             \
    } else if (httpEquivIndex == HTTP_EQUIV_LEN                 \
               && httpEquivState == HTTP_EQUIV_NOT_SEEN) {      \
      httpEquivState = HTTP_EQUIV_OTHER;                        \
    }                                                           \
  }                                                             \
} while (0)

  /* State machine begins here */
 DATA:
  /* Every time we come back to this point, forget everything we knew
     about previous tags (if any). */
  metaState        = NO;
  httpEquivState   = HTTP_EQUIV_NOT_SEEN;
  contentIndex     = SIZE_MAX;
  charsetIndex     = SIZE_MAX;
  httpEquivIndex   = SIZE_MAX;
  contentTypeIndex = SIZE_MAX;
  valueQuote       = '\0';
  attrValue        = 0;
  content          = 0;
  content_len      = 0;
  charset          = 0;
  charset_len      = 0;
  rv               = 0;

  for (;;) {
    c = READ();
    if (c == -1)
      goto END_OF_FILE;
    if (c == '<')
      goto TAG_OPEN;
  }

 TAG_OPEN:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;

    case 'm':
    case 'M':
      metaState = M;
      goto TAG_NAME;

    case '!':
      goto MARKUP_DECLARATION_OPEN;

    case '?':
    case '/':
      goto SCAN_UNTIL_GT;

    case '>':
      goto DATA;

    case '<':
      continue;

    default:
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
        metaState = NO;
        goto TAG_NAME;
      }
      goto DATA;
    }
  }

 TAG_NAME:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;

    case ' ':
    case '\t':
    case '\n':
    case '\r':
    case '\f':
    case '/':
      goto BEFORE_ATTRIBUTE_NAME;

    case '>':
      goto DATA;

    case 'e':
    case 'E':
      metaState = (metaState == M) ? E : NO;
      continue;
    case 't':
    case 'T':
      metaState = (metaState == E) ? T : NO;
      continue;
    case 'a':
    case 'A':
      metaState = (metaState == T) ? A : NO;
      continue;
    default:
      metaState = NO;
      continue;
    }
  }

 BEFORE_ATTRIBUTE_NAME:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;

    case ' ':
    case '\t':
    case '\n':
    case '\r':
    case '\f':
    case '/':
      continue;

    case '>':
      goto TAG_COMPLETE;

    case 'c':
    case 'C':
      contentIndex = 0;
      charsetIndex = 0;
      httpEquivIndex = SIZE_MAX;
      goto ATTRIBUTE_NAME;

    case 'h':
    case 'H':
      contentIndex = SIZE_MAX;
      charsetIndex = SIZE_MAX;
      httpEquivIndex = 0;
      goto ATTRIBUTE_NAME;

    default:
      contentIndex = SIZE_MAX;
      charsetIndex = SIZE_MAX;
      httpEquivIndex = SIZE_MAX;
      goto ATTRIBUTE_NAME;
    }
  }

 ATTRIBUTE_NAME:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;

    case ' ':
    case '\t':
    case '\n':
    case '\r':
    case '\f':
      goto AFTER_ATTRIBUTE_NAME;

    case '/':
      goto BEFORE_ATTRIBUTE_NAME;

    case '=':
      goto BEFORE_ATTRIBUTE_VALUE;

    case '>':
      goto TAG_COMPLETE;

    default:
      if (metaState == A) {
        c = toAsciiLowerCase(c);
        if (contentIndex < CONTENT_LEN && c == CONTENT[contentIndex]) {
          ++contentIndex;
        } else {
          contentIndex = SIZE_MAX;
        }
        if (charsetIndex < CHARSET_LEN && c == CHARSET[charsetIndex]) {
          ++charsetIndex;
        } else {
          charsetIndex = SIZE_MAX;
        }
        if (httpEquivIndex < HTTP_EQUIV_LEN && c == HTTP_EQUIV[httpEquivIndex]) {
          ++httpEquivIndex;
        } else {
          httpEquivIndex = SIZE_MAX;
        }
      }
      continue;
    }
  }

 AFTER_ATTRIBUTE_NAME:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;

    case ' ':
    case '\t':
    case '\n':
    case '\r':
    case '\f':
      continue;

    case '=':
      goto BEFORE_ATTRIBUTE_VALUE;

    case '>':
      goto TAG_COMPLETE;

    default:
      HANDLE_EMPTY_ATTRIBUTE_VALUE();
      p--;
      goto BEFORE_ATTRIBUTE_NAME;
    }
  }

 BEFORE_ATTRIBUTE_VALUE:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;

    case ' ':
    case '\t':
    case '\n':
    case '\r':
    case '\f':
      continue;

    case '"':
    case '\'':
      valueQuote = c;
      goto ATTRIBUTE_VALUE;

    case '>':
      HANDLE_EMPTY_ATTRIBUTE_VALUE();
      goto TAG_COMPLETE;

    default:
      valueQuote = ' ';
      p--;
      goto ATTRIBUTE_VALUE;
    }
  }

 ATTRIBUTE_VALUE:
  attrValue = p;
  contentTypeIndex = 0;
  for (;;) {
    c = READ();
    if (c == -1)
      goto END_OF_FILE;

    if (c == valueQuote ||
        (valueQuote == ' ' &&
         (c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '>'))) {

      if (metaState == A) {
        if (contentIndex == CONTENT_LEN && !content) {
          content     = attrValue;
          content_len = (p-1) - attrValue;
        } else if (charsetIndex == CHARSET_LEN && !charset) {
          charset     = attrValue;
          charset_len = (p-1) - attrValue;
        } else if (httpEquivIndex == HTTP_EQUIV_LEN
                   && httpEquivState == HTTP_EQUIV_NOT_SEEN) {
          httpEquivState = (contentTypeIndex == CONTENT_TYPE_LEN)
            ? HTTP_EQUIV_CONTENT_TYPE
            : HTTP_EQUIV_OTHER;
        }
      }
      if (c == '>')
        goto TAG_COMPLETE;
      goto BEFORE_ATTRIBUTE_NAME;
    }
    if (metaState == A && httpEquivIndex == HTTP_EQUIV_LEN) {
      if (contentTypeIndex < CONTENT_TYPE_LEN &&
          toAsciiLowerCase(c) == CONTENT_TYPE[contentTypeIndex]) {
        ++contentTypeIndex;
      } else {
        contentTypeIndex = SIZE_MAX;
      }
    }
  }

 MARKUP_DECLARATION_OPEN:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '-':
      goto MARKUP_DECLARATION_HYPHEN;
    case '>':
      goto DATA;
    default:
      goto SCAN_UNTIL_GT;
    }
  }

 MARKUP_DECLARATION_HYPHEN:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '-':
      goto COMMENT_START;
    case '>':
      goto DATA;
    default:
      goto SCAN_UNTIL_GT;
    }
  }

 COMMENT_START:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '-':
      goto COMMENT_START_DASH;
    case '>':
      goto DATA;
    default:
      goto COMMENT;
    }
  }

 COMMENT:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '-':
      goto COMMENT_END_DASH;
    default:
      continue;
    }
  }

 COMMENT_END_DASH:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '-':
      goto COMMENT_END;
    default:
      goto COMMENT;
    }
  }

 COMMENT_END:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '>':
      goto DATA;
    case '-':
      continue;
    default:
      goto COMMENT;
    }
  }

 COMMENT_START_DASH:
  c = READ();
  switch (c) {
  case -1:
    goto END_OF_FILE;
  case '-':
    goto COMMENT_END;
  case '>':
    goto DATA;
  default:
    goto COMMENT;
  }

 SCAN_UNTIL_GT:
  for (;;) {
    c = READ();
    switch (c) {
    case -1:
      goto END_OF_FILE;
    case '>':
      goto DATA;
    default:
      continue;
    }
  }

 TAG_COMPLETE:
  if (charset && charset_len) {
    rv = validate_charset(charset, charset_len);
    if (rv) return rv;
  }
  if (content && content_len && httpEquivState == HTTP_EQUIV_CONTENT_TYPE) {
    rv = validate_content(content, content_len);
    if (rv) return rv;
  }
  goto DATA;

 END_OF_FILE:
  return 0;
}
