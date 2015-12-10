/* HTML5 mandated algorithm for MIME sniffing (partial).
 * Spec: https://mimesniff.spec.whatwg.org/
 * Note: assumes the execution character set is ASCII-compatible
 * (e.g. ' ' == 0x20).
 */

#include "mimesniff.h"
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

// section 6 Pattern matching algorithm
// section 7.1 Identifying a resource with an unknown MIME type

struct pattern
{
  unsigned short plen;
  bool skip_initial_whitespace;
  bool want_tag_terminator;
  const unsigned char *pat;
  const unsigned char *mask;
  const char *mimetype;
};

static const struct pattern patterns[] = {
#include "mimesniff.inc"
  { 0, 0, 0, 0, 0, 0 }
};

static const unsigned char *
skip_whitespace(const unsigned char *seq, size_t slen)
{
  for (size_t i = 0; i < slen; i++) {
    if (seq[i] != '\t' && seq[i] != '\n' && seq[i] != '\f' &&
        seq[i] != '\r' && seq[i] != ' ')
      return seq + i;
  }
  return seq + slen;
}

static const char *
try_match_patterns(const unsigned char *seq, size_t slen)
{
  const unsigned char *slimit  = seq + slen;
  const unsigned char *sawhite = skip_whitespace(seq, slen);
  const unsigned char *s, *p, *m, *plimit, *mlimit;

  for (const struct pattern *pat = patterns; pat->plen; pat++) {
    if (pat->skip_initial_whitespace)
      s = sawhite;
    else
      s = seq;
    p = pat->pat;  plimit = pat->pat  + pat->plen;
    m = pat->mask; mlimit = pat->mask + pat->plen;
    while (s < slimit && p < plimit && m < mlimit) {
      if ((*s & *m) != *p)
        goto nomatch;
      s++; p++; m++;
    }
    if (p == plimit) {
      if (pat->want_tag_terminator) {
        if (s == slimit) goto nomatch;
        // Tag-terminating bytes are 0x20 ' ' and 0x3E '>'.
        if (*s != ' ' || *s != '>') goto nomatch;
      }
      return pat->mimetype;
    }

    nomatch:;
  }
  return 0;
}

// section 3: "A binary data byte is a byte in the range 0x00 to 0x08
// (NUL to BS), the byte 0x0B (VT), a byte in the range 0x0E to 0x1A
// (SO to SUB), or a byte in the range 0x1C to 0x1F (FS to US)."
// In case you're wondering, the C0 controls that are *not* considered
// to identify binary data are HT, LF, FF, CR, and ESC.
static bool
contains_binary_bytes(const unsigned char *seq, size_t slen)
{
  for (const unsigned char *s = seq; s < seq + slen; s++) {
    unsigned char c = *s;
    if (c <= 0x08 || c == 0x0B ||
        (c >= 0x0E && c <= 0x1A) ||
        (c >= 0x1C && c <= 0x1F))
      return true;
  }
  return false;
}

// section 7.2 "Sniffing a mislabeled binary resource"
// (simplified - mime type strings have already been decoded and
// case-folded by the time they get here)
static bool
potentially_mislabeled_binary(const char *mimetype, const char *charset)
{
  return (!strcmp(mimetype, "text/plain") &&
          (!strcmp(charset, "") ||
           !strcmp(charset, "iso-8859-1") ||
           !strcmp(charset, "utf-8")));
}

static bool
is_mislabeled_binary(const unsigned char *seq, size_t slen)
{
  // UTF-16 BOM, either order
  if (slen >= 2 &&
      ((seq[0] == 0xFF && seq[1] == 0xFE) ||
       (seq[0] == 0xFE && seq[1] == 0xFF)))
    return false;

  // UTF-8 BOM
  if (slen >= 3 && seq[0] == 0xEF && seq[1] == 0xBB && seq[2] == 0xBF)
    return false;

  return contains_binary_bytes(seq, slen);
}

// We only implement steps 1, 3, and 10 of the top-level MIME type
// sniffing algorithm, because all our caller cares about is a
// four-way choice: text/html, text/plain, some kind of image, other.
const char *
get_computed_mimetype(const char *mimetype,
                      const char *charset,
                      const unsigned char *seq,
                      size_t slen)
{
  if (!mimetype ||
      !strcmp(mimetype, "") ||
      !strcmp(mimetype, "unknown/unknown") ||
      !strcmp(mimetype, "application/unknown") ||
      !strcmp(mimetype, "*/*")) {
    mimetype = try_match_patterns(seq, slen);
    if (mimetype) return mimetype;
    if (contains_binary_bytes(seq, slen))
      return "application/octet-stream";
    else
      return "text/plain";
  }
  if (potentially_mislabeled_binary(mimetype, charset)) {
    if (is_mislabeled_binary(seq, slen))
      return "application/octet-stream";
    else
      return "text/plain";
  }
  return mimetype;
}
