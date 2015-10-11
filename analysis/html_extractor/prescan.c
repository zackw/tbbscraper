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
#include <strings.h>

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
#define NO 0
#define M  1
#define E  2
#define T  3
#define A  4

/* Constants for handling http-equiv. */
#define HTTP_EQUIV_NOT_SEEN     0
#define HTTP_EQUIV_CONTENT_TYPE 1
#define HTTP_EQUIV_OTHER        2

#define toAsciiLowerCase(c) (((c) >= 'A' && (c) <= 'Z') ? (c) + 0x20 : (c))

struct canonical_encoding
{
  const char *encoding;
  const char *canonical;
};

/* This is the list of encodings that browsers are required to
   recognize, from http://encoding.spec.whatwg.org/#encodings.  It has
   been sorted by field 0 to permit binary search, and the
   substitution of "windows-1252" for "x-user-defined", mandated by
   the prescan algorithm's step 2.14, has been applied to the table.
   (The substitution of utf-8 for utf-16, mandated by step 2.13, is
   handled below, because we *don't* want to do it in
   canonical_encoding_for_label.)  The distinction between iso-8859-8
   and iso-8859-8-i has been elided, because Python doesn't know what
   iso-8859-8-i is, and downstream processing has no chance of
   handling visual-order Hebrew correctly anyway, so we're better off
   pretending Hebrew is always logical-order.  */

static const struct canonical_encoding canonical_encodings[] = {
  { "866",                 "ibm866"         },
  { "ansi_x3.4-1968",      "windows-1252"   },
  { "arabic",              "iso-8859-6"     },
  { "ascii",               "windows-1252"   },
  { "asmo-708",            "iso-8859-6"     },
  { "big5",                "big5"           },
  { "big5-hkscs",          "big5"           },
  { "chinese",             "gbk"            },
  { "cn-big5",             "big5"           },
  { "cp1250",              "windows-1250"   },
  { "cp1251",              "windows-1251"   },
  { "cp1252",              "windows-1252"   },
  { "cp1253",              "windows-1253"   },
  { "cp1254",              "windows-1254"   },
  { "cp1255",              "windows-1255"   },
  { "cp1256",              "windows-1256"   },
  { "cp1257",              "windows-1257"   },
  { "cp1258",              "windows-1258"   },
  { "cp819",               "windows-1252"   },
  { "cp866",               "ibm866"         },
  { "csbig5",              "big5"           },
  { "cseuckr",             "euc-kr"         },
  { "cseucpkdfmtjapanese", "euc-jp"         },
  { "csgb2312",            "gbk"            },
  { "csibm866",            "ibm866"         },
  { "csiso2022jp",         "iso-2022-jp"    },
  { "csiso2022kr",         "replacement"    },
  { "csiso58gb231280",     "gbk"            },
  { "csiso88596e",         "iso-8859-6"     },
  { "csiso88596i",         "iso-8859-6"     },
  { "csiso88598e",         "iso-8859-8"     },
  { "csiso88598i",         "iso-8859-8"   },
  { "csisolatin1",         "windows-1252"   },
  { "csisolatin2",         "iso-8859-2"     },
  { "csisolatin3",         "iso-8859-3"     },
  { "csisolatin4",         "iso-8859-4"     },
  { "csisolatin5",         "windows-1254"   },
  { "csisolatin6",         "iso-8859-10"    },
  { "csisolatin9",         "iso-8859-15"    },
  { "csisolatinarabic",    "iso-8859-6"     },
  { "csisolatincyrillic",  "iso-8859-5"     },
  { "csisolatingreek",     "iso-8859-7"     },
  { "csisolatinhebrew",    "iso-8859-8"     },
  { "cskoi8r",             "koi8-r"         },
  { "csksc56011987",       "euc-kr"         },
  { "csmacintosh",         "macintosh"      },
  { "csshiftjis",          "shift_jis"      },
  { "cyrillic",            "iso-8859-5"     },
  { "dos-874",             "windows-874"    },
  { "ecma-114",            "iso-8859-6"     },
  { "ecma-118",            "iso-8859-7"     },
  { "elot_928",            "iso-8859-7"     },
  { "euc-jp",              "euc-jp"         },
  { "euc-kr",              "euc-kr"         },
  { "gb18030",             "gb18030"        },
  { "gb2312",              "gbk"            },
  { "gb_2312",             "gbk"            },
  { "gb_2312-80",          "gbk"            },
  { "gbk",                 "gbk"            },
  { "greek",               "iso-8859-7"     },
  { "greek8",              "iso-8859-7"     },
  { "hebrew",              "iso-8859-8"     },
  { "hz-gb-2312",          "replacement"    },
  { "ibm819",              "windows-1252"   },
  { "ibm866",              "ibm866"         },
  { "iso-2022-cn",         "replacement"    },
  { "iso-2022-cn-ext",     "replacement"    },
  { "iso-2022-jp",         "iso-2022-jp"    },
  { "iso-2022-kr",         "replacement"    },
  { "iso-8859-1",          "windows-1252"   },
  { "iso-8859-10",         "iso-8859-10"    },
  { "iso-8859-11",         "windows-874"    },
  { "iso-8859-13",         "iso-8859-13"    },
  { "iso-8859-14",         "iso-8859-14"    },
  { "iso-8859-15",         "iso-8859-15"    },
  { "iso-8859-16",         "iso-8859-16"    },
  { "iso-8859-2",          "iso-8859-2"     },
  { "iso-8859-3",          "iso-8859-3"     },
  { "iso-8859-4",          "iso-8859-4"     },
  { "iso-8859-5",          "iso-8859-5"     },
  { "iso-8859-6",          "iso-8859-6"     },
  { "iso-8859-6-e",        "iso-8859-6"     },
  { "iso-8859-6-i",        "iso-8859-6"     },
  { "iso-8859-7",          "iso-8859-7"     },
  { "iso-8859-8",          "iso-8859-8"     },
  { "iso-8859-8-e",        "iso-8859-8"     },
  { "iso-8859-8-i",        "iso-8859-8"   },
  { "iso-8859-9",          "windows-1254"   },
  { "iso-ir-100",          "windows-1252"   },
  { "iso-ir-101",          "iso-8859-2"     },
  { "iso-ir-109",          "iso-8859-3"     },
  { "iso-ir-110",          "iso-8859-4"     },
  { "iso-ir-126",          "iso-8859-7"     },
  { "iso-ir-127",          "iso-8859-6"     },
  { "iso-ir-138",          "iso-8859-8"     },
  { "iso-ir-144",          "iso-8859-5"     },
  { "iso-ir-148",          "windows-1254"   },
  { "iso-ir-149",          "euc-kr"         },
  { "iso-ir-157",          "iso-8859-10"    },
  { "iso-ir-58",           "gbk"            },
  { "iso8859-1",           "windows-1252"   },
  { "iso8859-10",          "iso-8859-10"    },
  { "iso8859-11",          "windows-874"    },
  { "iso8859-13",          "iso-8859-13"    },
  { "iso8859-14",          "iso-8859-14"    },
  { "iso8859-15",          "iso-8859-15"    },
  { "iso8859-2",           "iso-8859-2"     },
  { "iso8859-3",           "iso-8859-3"     },
  { "iso8859-4",           "iso-8859-4"     },
  { "iso8859-5",           "iso-8859-5"     },
  { "iso8859-6",           "iso-8859-6"     },
  { "iso8859-7",           "iso-8859-7"     },
  { "iso8859-8",           "iso-8859-8"     },
  { "iso8859-9",           "windows-1254"   },
  { "iso88591",            "windows-1252"   },
  { "iso885910",           "iso-8859-10"    },
  { "iso885911",           "windows-874"    },
  { "iso885913",           "iso-8859-13"    },
  { "iso885914",           "iso-8859-14"    },
  { "iso885915",           "iso-8859-15"    },
  { "iso88592",            "iso-8859-2"     },
  { "iso88593",            "iso-8859-3"     },
  { "iso88594",            "iso-8859-4"     },
  { "iso88595",            "iso-8859-5"     },
  { "iso88596",            "iso-8859-6"     },
  { "iso88597",            "iso-8859-7"     },
  { "iso88598",            "iso-8859-8"     },
  { "iso88599",            "windows-1254"   },
  { "iso_8859-1",          "windows-1252"   },
  { "iso_8859-15",         "iso-8859-15"    },
  { "iso_8859-1:1987",     "windows-1252"   },
  { "iso_8859-2",          "iso-8859-2"     },
  { "iso_8859-2:1987",     "iso-8859-2"     },
  { "iso_8859-3",          "iso-8859-3"     },
  { "iso_8859-3:1988",     "iso-8859-3"     },
  { "iso_8859-4",          "iso-8859-4"     },
  { "iso_8859-4:1988",     "iso-8859-4"     },
  { "iso_8859-5",          "iso-8859-5"     },
  { "iso_8859-5:1988",     "iso-8859-5"     },
  { "iso_8859-6",          "iso-8859-6"     },
  { "iso_8859-6:1987",     "iso-8859-6"     },
  { "iso_8859-7",          "iso-8859-7"     },
  { "iso_8859-7:1987",     "iso-8859-7"     },
  { "iso_8859-8",          "iso-8859-8"     },
  { "iso_8859-8:1988",     "iso-8859-8"     },
  { "iso_8859-9",          "windows-1254"   },
  { "iso_8859-9:1989",     "windows-1254"   },
  { "koi",                 "koi8-r"         },
  { "koi8",                "koi8-r"         },
  { "koi8-r",              "koi8-r"         },
  { "koi8-u",              "koi8-u"         },
  { "koi8_r",              "koi8-r"         },
  { "korean",              "euc-kr"         },
  { "ks_c_5601-1987",      "euc-kr"         },
  { "ks_c_5601-1989",      "euc-kr"         },
  { "ksc5601",             "euc-kr"         },
  { "ksc_5601",            "euc-kr"         },
  { "l1",                  "windows-1252"   },
  { "l2",                  "iso-8859-2"     },
  { "l3",                  "iso-8859-3"     },
  { "l4",                  "iso-8859-4"     },
  { "l5",                  "windows-1254"   },
  { "l6",                  "iso-8859-10"    },
  { "l9",                  "iso-8859-15"    },
  { "latin1",              "windows-1252"   },
  { "latin2",              "iso-8859-2"     },
  { "latin3",              "iso-8859-3"     },
  { "latin4",              "iso-8859-4"     },
  { "latin5",              "windows-1254"   },
  { "latin6",              "iso-8859-10"    },
  { "logical",             "iso-8859-8"   },
  { "mac",                 "macintosh"      },
  { "macintosh",           "macintosh"      },
  { "ms_kanji",            "shift_jis"      },
  { "shift-jis",           "shift_jis"      },
  { "shift_jis",           "shift_jis"      },
  { "sjis",                "shift_jis"      },
  { "sun_eu_greek",        "iso-8859-7"     },
  { "tis-620",             "windows-874"    },
  { "unicode-1-1-utf-8",   "utf-8"          },
  { "us-ascii",            "windows-1252"   },
  { "utf-16",              "utf-16le"       },
  { "utf-16be",            "utf-16be"       },
  { "utf-16le",            "utf-16le"       },
  { "utf-8",               "utf-8"          },
  { "utf8",                "utf-8"          },
  { "visual",              "iso-8859-8"     },
  { "windows-1250",        "windows-1250"   },
  { "windows-1251",        "windows-1251"   },
  { "windows-1252",        "windows-1252"   },
  { "windows-1253",        "windows-1253"   },
  { "windows-1254",        "windows-1254"   },
  { "windows-1255",        "windows-1255"   },
  { "windows-1256",        "windows-1256"   },
  { "windows-1257",        "windows-1257"   },
  { "windows-1258",        "windows-1258"   },
  { "windows-31j",         "shift_jis"      },
  { "windows-874",         "windows-874"    },
  { "windows-949",         "euc-kr"         },
  { "x-cp1250",            "windows-1250"   },
  { "x-cp1251",            "windows-1251"   },
  { "x-cp1252",            "windows-1252"   },
  { "x-cp1253",            "windows-1253"   },
  { "x-cp1254",            "windows-1254"   },
  { "x-cp1255",            "windows-1255"   },
  { "x-cp1256",            "windows-1256"   },
  { "x-cp1257",            "windows-1257"   },
  { "x-cp1258",            "windows-1258"   },
  { "x-euc-jp",            "euc-jp"         },
  { "x-gbk",               "gbk"            },
  { "x-mac-cyrillic",      "x-mac-cyrillic" },
  { "x-mac-roman",         "macintosh"      },
  { "x-mac-ukrainian",     "x-mac-cyrillic" },
  { "x-sjis",              "shift_jis"      },
  { "x-user-defined",      "windows-1252"   },
  { "x-x-big5",            "big5"           },
};

static int
canonical_encoding_compar(const void *k, const void *v)
{
  const char *key = k;
  const struct canonical_encoding *val = v;
  return strcasecmp(key, val->encoding);
}

const char *
canonical_encoding_for_label(const char *encoding)
{
  struct canonical_encoding *match =
    bsearch(encoding, canonical_encodings,
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
  char buf[charset_len + 1];
  memcpy(buf, charset, charset_len);
  buf[charset_len] = '\0';

  const char *canonical = canonical_encoding_for_label(buf);
  if (canonical) {
    /* */
    if (!strncmp(buf, "utf-16", sizeof "utf-16"-1))
      return "utf-8";
    return canonical;
  }
  return 0;
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

  for (p = content, limit = p + content_len; p < limit; p++) {
    c = *p;
    if (quote) {
      if (c == quote ||
          (quote == ' ' &&
           (c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ';')))
        break;

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
  if (quote)
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
