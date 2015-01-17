// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// Author: dsites@google.com (Dick Sites)
//

#include "getonescriptspan.h"

#include <stdint.h>
#include <string.h>

#include "generated_language.h"
#include "generated_ulscript.h"
#include "stringpiece.h"
#include "unaligned_access.h"
#include "utf8prop_lettermarkscriptnum.h"
#include "utf8repl_lettermarklower.h"
#include "utf8scannot_lettermarkspecial.h"
#include "utf8statetable.h"

namespace CLD2 {

static const int kMaxScriptBuffer = 40960;
static const int kMaxScriptLowerBuffer = (kMaxScriptBuffer * 3) / 2;
static const int kMaxScriptBytes = kMaxScriptBuffer - 32;   // Leave some room
static const int kWithinScriptTail = 32;    // Stop at word space in last
                                            // N bytes of script buffer
static const int kMaxUpToWordBoundary = 50;       // span < this make longer,
                                                  // else make shorter
static const int kMaxAdvanceToWordBoundary = 10;  // +/- this many bytes
                                                  // to round to word boundary,
                                                  // direction above

#define LT 0      // <
#define GT 1      // >
#define EX 2      // !
#define HY 3      // -
#define QU 4      // "
#define AP 5      // '
#define SL 6      // /
#define S_ 7
#define C_ 8
#define R_ 9
#define I_ 10
#define P_ 11
#define T_ 12
#define Y_ 13
#define L_ 14
#define E_ 15
#define CR 16     // <cr> or <lf>
#define NL 17     // non-letter: ASCII whitespace, digit, punctuation
#define PL 18     // possible letter, incl. &
#define xx 19     // <unused>

// Map byte to one of ~20 interesting categories for cheap tag parsing
static const uint8_t kCharToSub[256] = {
  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,CR,NL, NL,CR,NL,NL,
  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL,
  NL,EX,QU,NL, NL,NL,PL,AP, NL,NL,NL,NL, NL,HY,NL,SL,
  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL, LT,NL,GT,NL,

  PL,PL,PL,C_, PL,E_,PL,PL, PL,I_,PL,PL, L_,PL,PL,PL,
  P_,PL,R_,S_, T_,PL,PL,PL, PL,Y_,PL,NL, NL,NL,NL,NL,
  PL,PL,PL,C_, PL,E_,PL,PL, PL,I_,PL,PL, L_,PL,PL,PL,
  P_,PL,R_,S_, T_,PL,PL,PL, PL,Y_,PL,NL, NL,NL,NL,NL,

  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL,
  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL,
  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL,
  NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL, NL,NL,NL,NL,

  PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL,
  PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL,
  PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL,
  PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL, PL,PL,PL,PL,
};

#undef LT
#undef GT
#undef EX
#undef HY
#undef QU
#undef AP
#undef SL
#undef S_
#undef C_
#undef R_
#undef I_
#undef P_
#undef T_
#undef Y_
#undef L_
#undef E_
#undef CR
#undef NL
#undef PL
#undef xx


#define OK 0
#define X_ 1


static const int kMaxExitStateLettersMarksOnly = 1;
static const int kMaxExitStateAllText = 2;


// State machine to do cheap parse of non-letter strings incl. tags
// advances <tag>
//          |    |
// advances <tag> ... </tag>  for <script> <style>
//          |               |
// advances <!-- ... <tag> ... -->
//          |                     |
// advances <tag
//          ||  (0)
// advances <tag <tag2>
//          ||  (0)
//
// We start in state [0] at a non-letter and make at least one transition
// When scanning for just letters, arriving back at state [0] or [1] exits
//   the state machine.
// When scanning for any non-tag text, arriving at state [2] also exits
static const uint8_t kTagParseTbl_0[] = {
// <  >  !  -   "  '  /  S   C  R  I  P   T  Y  L  E  CR NL PL xx
   3, 2, 2, 2,  2, 2, 2,OK, OK,OK,OK,OK, OK,OK,OK,OK,  2, 2,OK,X_, // [0] OK    exit state
  X_,X_,X_,X_, X_,X_,X_,X_, X_,X_,X_,X_, X_,X_,X_,X_, X_,X_,X_,X_, // [1] error exit state
   3, 2, 2, 2,  2, 2, 2,OK, OK,OK,OK,OK, OK,OK,OK,OK,  2, 2,OK,X_, // [2] NL*   [exit state]
  X_, 2, 4, 9, 10,11, 9,13,  9, 9, 9, 9,  9, 9, 9, 9,  9, 9, 9,X_, // [3] <
  X_, 2, 9, 5, 10,11, 9, 9,  9, 9, 9, 9,  9, 9, 9, 9,  9, 9, 9,X_, // [4] <!
  X_, 2, 9, 6, 10,11, 9, 9,  9, 9, 9, 9,  9, 9, 9, 9,  9, 9, 9,X_, // [5] <!-
   6, 6, 6, 7,  6, 6, 6, 6,  6, 6, 6, 6,  6, 6, 6, 6,  6, 6, 6,X_, // [6] <!--.*
   6, 6, 6, 8,  6, 6, 6, 6,  6, 6, 6, 6,  6, 6, 6, 6,  6, 6, 6,X_, // [7] <!--.*-
   6, 2, 6, 8,  6, 6, 6, 6,  6, 6, 6, 6,  6, 6, 6, 6,  6, 6, 6,X_, // [8] <!--.*--
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9, 9, 9,  9, 9, 9, 9,  9, 9, 9,X_, // [9] <.*
  10,10,10,10,  9,10,10,10, 10,10,10,10, 10,10,10,10, 12,10,10,X_, // [10] <.*"
  11,11,11,11, 11, 9,11,11, 11,11,11,11, 11,11,11,11, 12,11,11,X_, // [11] <.*'
  X_, 2,12,12, 12,12,12,12, 12,12,12,12, 12,12,12,12, 12,12,12,X_, // [12] <.* no " '

// <  >  !  -   "  '  /  S   C  R  I  P   T  Y  L  E  CR NL PL xx
  X_, 2, 9, 9, 10,11, 9, 9, 14, 9, 9, 9, 28, 9, 9, 9,  9, 9, 9,X_, // [13] <S
  X_, 2, 9, 9, 10,11, 9, 9,  9,15, 9, 9,  9, 9, 9, 9,  9, 9, 9,X_, // [14] <SC
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9,16, 9,  9, 9, 9, 9,  9, 9, 9,X_, // [15] <SCR
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9, 9,17,  9, 9, 9, 9,  9, 9, 9,X_, // [16] <SCRI
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9, 9, 9, 18, 9, 9, 9,  9, 9, 9,X_, // [17] <SCRIP
  X_,19, 9, 9, 10,11, 9, 9,  9, 9, 9, 9,  9, 9, 9, 9, 19,19, 9,X_, // [18] <SCRIPT
  20,19,19,19, 19,19,19,19, 19,19,19,19, 19,19,19,19, 19,19,19,X_, // [19] <SCRIPT .*
  19,19,19,19, 19,19,21,19, 19,19,19,19, 19,19,19,19, 19,19,19,X_, // [20] <SCRIPT .*<
  19,19,19,19, 19,19,19,22, 19,19,19,19, 19,19,19,19, 21,21,19,X_, // [21] <SCRIPT .*</ allow SP CR LF
  19,19,19,19, 19,19,19,19, 23,19,19,19, 19,19,19,19, 19,19,19,X_, // [22] <SCRIPT .*</S
  19,19,19,19, 19,19,19,19, 19,24,19,19, 19,19,19,19, 19,19,19,X_, // [23] <SCRIPT .*</SC
  19,19,19,19, 19,19,19,19, 19,19,25,19, 19,19,19,19, 19,19,19,X_, // [24] <SCRIPT .*</SCR
  19,19,19,19, 19,19,19,19, 19,19,19,26, 19,19,19,19, 19,19,19,X_, // [25] <SCRIPT .*</SCRI
  19,19,19,19, 19,19,19,19, 19,19,19,19, 27,19,19,19, 19,19,19,X_, // [26] <SCRIPT .*</SCRIP
  19, 2,19,19, 19,19,19,19, 19,19,19,19, 19,19,19,19, 19,19,19,X_, // [27] <SCRIPT .*</SCRIPT

// <  >  !  -   "  '  /  S   C  R  I  P   T  Y  L  E  CR NL PL xx
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9, 9, 9,  9,29, 9, 9,  9, 9, 9,X_, // [28] <ST
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9, 9, 9,  9, 9,30, 9,  9, 9, 9,X_, // [29] <STY
  X_, 2, 9, 9, 10,11, 9, 9,  9, 9, 9, 9,  9, 9, 9,31,  9, 9, 9,X_, // [30] <STYL
  X_,32, 9, 9, 10,11, 9, 9,  9, 9, 9, 9,  9, 9, 9, 9, 32,32, 9,X_, // [31] <STYLE
  33,32,32,32, 32,32,32,32, 32,32,32,32, 32,32,32,32, 32,32,32,X_, // [32] <STYLE .*
  32,32,32,32, 32,32,34,32, 32,32,32,32, 32,32,32,32, 32,32,32,X_, // [33] <STYLE .*<
  32,32,32,32, 32,32,32,35, 32,32,32,32, 32,32,32,32, 34,34,32,X_, // [34] <STYLE .*</ allow SP CR LF
  32,32,32,32, 32,32,32,32, 32,32,32,32, 36,32,32,32, 32,32,32,X_, // [35] <STYLE .*</S
  32,32,32,32, 32,32,32,32, 32,32,32,32, 32,37,32,32, 32,32,32,X_, // [36] <STYLE .*</ST
  32,32,32,32, 32,32,32,32, 32,32,32,32, 32,32,38,32, 32,32,32,X_, // [37] <STYLE .*</STY
  32,32,32,32, 32,32,32,32, 32,32,32,32, 32,32,32,39, 32,32,32,X_, // [38] <STYLE .*</STYL
  32, 2,32,32, 32,32,32,32, 32,32,32,32, 32,32,32,32, 32,32,32,X_, // [39] <STYLE .*</STYLE
};

#undef OK
#undef X_

enum
{
  UTFmax        = 4,            // maximum bytes per rune
  Runesync      = 0x80,         // cannot represent part of a UTF sequence (<)
  Runeself      = 0x80,         // rune and UTF sequences are the same (<)
  Runeerror     = 0xFFFD,       // decoding error in UTF
  Runemax       = 0x10FFFF,     // maximum rune value
};


// Quick Skip to next letter or < > & or to end of string (eos)
// Always return is_letter for eos
static int ScanToLetter(const char* src, int len) {
  int bytes_consumed;
  StringPiece str(src, len);
  UTF8GenericScan(&utf8scannot_lettermarkspecial_obj, str, &bytes_consumed);
  return bytes_consumed;
}

// Gets lscript number for letters; always returns
//   0 (common script) for non-letters
static int GetUTF8LetterScriptNum(const char* src) {
  int srclen = UTF8OneCharLen(src);
  const uint8_t* usrc = reinterpret_cast<const uint8_t*>(src);
  return UTF8GenericPropertyTwoByte(&utf8prop_lettermarkscriptnum_obj,
                                    &usrc, &srclen);
}

ScriptScanner::ScriptScanner(const char* buffer,
                             std::size_t buffer_length)
  : start_byte_(buffer),
  next_byte_(buffer),
  next_byte_limit_(buffer + buffer_length),
  byte_length_(buffer_length),
  letters_marks_only_(true),
  one_script_only_(true),
  exit_state_(kMaxExitStateLettersMarksOnly) {
    script_buffer_ = new char[kMaxScriptBuffer];
    script_buffer_lower_ = new char[kMaxScriptLowerBuffer];
}

// Extended version to allow spans of any non-tag text and spans of mixed script
ScriptScanner::ScriptScanner(const char* buffer,
                             std::size_t buffer_length,
                             bool any_text,
                             bool any_script)
  : start_byte_(buffer),
  next_byte_(buffer),
  next_byte_limit_(buffer + buffer_length),
  byte_length_(buffer_length),
  letters_marks_only_(!any_text),
  one_script_only_(!any_script),
  exit_state_(any_text ? kMaxExitStateAllText : kMaxExitStateLettersMarksOnly) {
    script_buffer_ = new char[kMaxScriptBuffer];
    script_buffer_lower_ = new char[kMaxScriptLowerBuffer];
}


ScriptScanner::~ScriptScanner() {
  delete[] script_buffer_;
  delete[] script_buffer_lower_;
}

// Get to the first real letter
// Sets script of that letter
// Return len if no more letters
int ScriptScanner::SkipToFrontOfSpan(const char* src, int len, int* script) {
  int sc = UNKNOWN_ULSCRIPT;
  int skip = 0;
  int tlen;

  // Do run of non-letters
  tlen = 0;
  while (skip < len) {
    // Do fast scan to next interesting byte
    // int oldskip = skip;
    skip += ScanToLetter(src + skip, len - skip);

    // Check for no more letters
    if (skip >= len) {
      // All done
      *script = sc;
      return len;
    }

    // Update 1..4 bytes
    tlen = UTF8OneCharLen(src + skip);
    sc = GetUTF8LetterScriptNum(src + skip);
    if (sc != 0) {break;}           // Letter found
    skip += tlen;                   // Else advance
  }

  *script = sc;
  return skip;
}


// These are for ASCII-only tag names
// Compare one letter uplow to c, ignoring case of uplowp
inline bool EqCase(char uplow, char c) {
  return (uplow | 0x20) == c;
}

// These are for ASCII-only tag names
// Return true for space / < > etc. all less than 0x40
inline bool NeqLetter(char c) {
  return c < 0x40;
}

// These are for ASCII-only tag names
// Return true for space \n false for \r
inline bool WS(char c) {
  return (c == ' ') || (c == '\n');
}

// Canonical CR or LF
static const char LF = '\n';


// The naive loop scans from next_byte_ to script_buffer_ until full.
// But this can leave an awkward hard-to-identify short fragment at the
// end of the input. We would prefer to make the next-to-last fragment
// shorter and the last fragment longer.

// Copy next run of non-tag characters to buffer [NUL terminated]
// This just replaces tags with space or \n and removes entities.
// Tags <br> <p> and <tr> are replaced with \n. Non-letter sequences
// including \r or \n are replaced by \n. All other tags and skipped text
// are replaced with ASCII space.
//
// Buffer ALWAYS has leading space and trailing space space space NUL
bool ScriptScanner::GetOneTextSpan(LangSpan* span) {
  span->text = script_buffer_;
  span->text_bytes = 0;
  span->offset = next_byte_ - start_byte_;
  span->ulscript = UNKNOWN_ULSCRIPT;
  span->lang = UNKNOWN_LANGUAGE;
  span->truncated = false;

  int put_soft_limit = kMaxScriptBytes - kWithinScriptTail;
  if ((kMaxScriptBytes <= byte_length_) &&
      (byte_length_ < (2 * kMaxScriptBytes))) {
    // Try to split the last two fragments in half
    put_soft_limit = byte_length_ / 2;
  }

  script_buffer_[0] = ' ';  // Always a space at front of output
  script_buffer_[1] = '\0';
  std::size_t take = 0;
  int put = 1;              // Start after the initial space

  if (byte_length_ <= 0) {
    return false;          // No more text to be found
  }

  // Go over alternating spans of text and tags,
  // copying letters to buffer with single spaces for each run of non-letters
  bool last_byte_was_space = false;
  while (take < byte_length_) {
    char c = next_byte_[take];
    if (c == '\r') {c = LF;}      // Canonical CR or LF
    if (c == '\n') {c = LF;}      // Canonical CR or LF

    // Copy one byte, compressing spaces
    if (!last_byte_was_space || !WS(c)) {
      script_buffer_[put++] = c;      // Advance dest
      last_byte_was_space = WS(c);
    }
    ++take;                         // Advance source

    if (WS(c) &&
        (put >= put_soft_limit)) {
      // Buffer is almost full
      span->truncated = true;
      break;
    }
    if (put >= kMaxScriptBytes) {
      // Buffer is completely full
      span->truncated = true;
      break;
    }
  }

  // Almost done. Back up to a character boundary if needed
  while ((0 < take) && ((next_byte_[take] & 0xc0) == 0x80)) {
    // Back up over continuation byte
    --take;
    --put;
  }

  // Update input position
  next_byte_ += take;
  byte_length_ -= take;

  // Put four more spaces/NUL. Worst case is abcd _ _ _ \0
  //                          kMaxScriptBytes |   | put
  script_buffer_[put + 0] = ' ';
  script_buffer_[put + 1] = ' ';
  script_buffer_[put + 2] = ' ';
  script_buffer_[put + 3] = '\0';

  span->text_bytes = put;       // Does not include the last four chars above
  return true;
}


// Copy next run of same-script non-tag letters to buffer [NUL terminated]
// Buffer ALWAYS has leading space and trailing space space space NUL
bool ScriptScanner::GetOneScriptSpan(LangSpan* span) {
  if (!letters_marks_only_) {
    // Return non-tag text, including punctuation and digits
    return GetOneTextSpan(span);
  }

  span->text = script_buffer_;
  span->text_bytes = 0;
  span->offset = next_byte_ - start_byte_;
  span->ulscript = UNKNOWN_ULSCRIPT;
  span->lang = UNKNOWN_LANGUAGE;
  span->truncated = false;

  // struct timeval script_start, script_mid, script_end;

  int put_soft_limit = kMaxScriptBytes - kWithinScriptTail;
  if ((kMaxScriptBytes <= byte_length_) &&
      (byte_length_ < (2 * kMaxScriptBytes))) {
    // Try to split the last two fragments in half
    put_soft_limit = byte_length_ / 2;
  }


  int spanscript;           // The script of this span
  int sc = UNKNOWN_ULSCRIPT;  // The script of next character
  int tlen = 0;
  int plen = 0;

  script_buffer_[0] = ' ';  // Always a space at front of output
  script_buffer_[1] = '\0';
  std::size_t take = 0;
  int put = 1;              // Start after the initial space

  // Get to the first real non-tag letter or entity that is a letter
  int skip = SkipToFrontOfSpan(next_byte_, byte_length_, &spanscript);
  next_byte_ += skip;
  byte_length_ -= skip;

  if (byte_length_ <= 0) {
    return false;               // No more letters to be found
  }

  // There is at least one letter, so we know the script for this span
  span->ulscript = (ULScript)spanscript;


  // Go over alternating spans of same-script letters and non-letters,
  // copying letters to buffer with single spaces for each run of non-letters
  while (take < byte_length_) {
    // Copy run of letters in same script (&LS | LS)*
    int letter_count = 0;              // Keep track of word length
    bool need_break = false;

    while (take < byte_length_) {
      // Real letter, safely copy up to 4 bytes, increment by 1..4
      // Will update by 1..4 bytes at Advance, below
      tlen = plen = UTF8OneCharLen(next_byte_ + take);
      if (take < (byte_length_ - 3)) {
        // X86 fast case, does unaligned load/store
        UNALIGNED_STORE32(script_buffer_ + put,
                          UNALIGNED_LOAD32(next_byte_ + take));

      } else {
        // Slow case, happens 1-3 times per input document
        memcpy(script_buffer_ + put, next_byte_ + take, plen);
      }
      sc = GetUTF8LetterScriptNum(next_byte_ + take);

      // Allow continue across a single letter in a different script:
      // A B D = three scripts, c = common script, i = inherited script,
      // - = don't care, ( = take position before the += below
      //  AAA(A-    continue
      //
      //  AAA(BA    continue
      //  AAA(BB    break
      //  AAA(Bc    continue (breaks after B)
      //  AAA(BD    break
      //  AAA(Bi    break
      //
      //  AAA(c-    break
      //
      //  AAA(i-    continue
      //

      if ((sc != spanscript) && (sc != ULScript_Inherited)) {
        // Might need to break this script span
        if (sc == ULScript_Common) {
          need_break = true;
        } else {
          // Look at next following character, ignoring entity as Common
          int sc2 = GetUTF8LetterScriptNum(next_byte_ + take + tlen);
          if ((sc2 != ULScript_Common) && (sc2 != spanscript)) {
            // We found a non-trivial change of script
            if (one_script_only_) {
              need_break = true;
            }
          }
        }
      }
      if (need_break) {break;}  // Non-letter or letter in wrong script

      take += tlen;                   // Advance
      put += plen;                    // Advance

      ++letter_count;
      if (put >= kMaxScriptBytes) {
        // Buffer is full
        span->truncated = true;
        break;
      }
    }     // End while letters

    // Do run of non-letters (tag | &NL | NL)*
    while (take < byte_length_) {
      // Do fast scan to next interesting byte
      tlen = ScanToLetter(next_byte_ + take, byte_length_ - take);
      take += tlen;
      if (take >= byte_length_) {break;}    // Might have scanned to end

      // Update 1..4
      tlen = UTF8OneCharLen(next_byte_ + take);
      sc = GetUTF8LetterScriptNum(next_byte_ + take);
      if (sc != 0) {break;}           // Letter found
      take += tlen;                   // Else advance
    }     // End while not-letters

    script_buffer_[put++] = ' ';

    // Letter in wrong script ?
    if ((sc != spanscript) && (sc != ULScript_Inherited)) {break;}
    if (put >= put_soft_limit) {
      // Buffer is almost full
      span->truncated = true;
      break;
    }
  }

  // Almost done. Back up to a character boundary if needed
  while ((0 < take) && (take < byte_length_) &&
         ((next_byte_[take] & 0xc0) == 0x80)) {
    // Back up over continuation byte
    --take;
    --put;
  }

  // Update input position
  next_byte_ += take;
  byte_length_ -= take;

  // Put four more spaces/NUL. Worst case is abcd _ _ _ \0
  //                          kMaxScriptBytes |   | put
  script_buffer_[put + 0] = ' ';
  script_buffer_[put + 1] = ' ';
  script_buffer_[put + 2] = ' ';
  script_buffer_[put + 3] = '\0';

  span->text_bytes = put;       // Does not include the last four chars above
  return true;
}

// Force Latin, Cyrillic, Armenian, Greek scripts to be lowercase
// List changes with each version of Unicode, so just always lowercase
// Unicode 6.2.0:
//   ARMENIAN COPTIC CYRILLIC DESERET GEORGIAN GLAGOLITIC GREEK LATIN
void ScriptScanner::LowerScriptSpan(LangSpan* span) {
  // If needed, lowercase all the text. If we do it sooner, might miss
  // lowercasing an entity such as &Aacute;
  // We only need to do this for Latn and Cyrl scripts
  // Full Unicode lowercase of the entire buffer, including
  // four pad bytes off the end.
  // Ahhh. But the last byte 0x00 is not interchange-valid, so we do 3 pad
  // bytes and put the 0x00 in explicitly.
  // Build an offset map from script_buffer_lower_ back to script_buffer_
  int consumed, filled, changed;
  StringPiece istr(span->text, span->text_bytes + 3);
  StringPiece ostr(script_buffer_lower_, kMaxScriptLowerBuffer);

  UTF8GenericReplace(&utf8repl_lettermarklower_obj,
                     istr, ostr,
                     &consumed, &filled, &changed);
  script_buffer_lower_[filled] = '\0';
  span->text = script_buffer_lower_;
  span->text_bytes = filled - 3;
}

// Copy next run of same-script non-tag letters to buffer [NUL terminated]
// Force Latin, Cyrillic, Greek scripts to be lowercase
// Buffer ALWAYS has leading space and trailing space space space NUL
bool ScriptScanner::GetOneScriptSpanLower(LangSpan* span) {
  bool ok = GetOneScriptSpan(span);
  if (ok) {
    LowerScriptSpan(span);
  }
  return ok;
}

}  // namespace CLD2
