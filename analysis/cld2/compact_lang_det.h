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

// Scan interchange-valid UTF-8 bytes and detect most likely language,
// or set of languages.
//
// Design goals:
//   Relatively small tables and relatively fast processing
//   Thread safe
//
// Inputs: text and text_length
// Outputs:
//  language3 is an array of the top 3 languages or UNKNOWN_LANGUAGE
//  percent3 is an array of the text percentages 0..100 of the top 3 languages
//  is_reliable set true if the returned Language is some amount more
//   probable then the second-best Language. Calculation is a complex function
//   of the length of the text and the different-script runs of text.
// Return value: the most likely Language for the majority of the input text
//  Length 0 input returns UNKNOWN_LANGUAGE. Very short indeterminate text
//  defaults to ENGLISH.

// NOTE:
// Baybayin (ancient script of the Philippines) is detected as TAGALOG.
// Chu Nom (Vietnamese ancient Han characters) is detected as VIETNAMESE.
// HAITIAN_CREOLE is detected as such.
// NORWEGIAN and NORWEGIAN_N are detected separately (but not robustly)
// PORTUGUESE, PORTUGUESE_P, and PORTUGUESE_B are all detected as PORTUGUESE.
// ROMANIAN-Latin is detected as ROMANIAN; ROMANIAN-Cyrillic as ROMANIAN.
// BOSNIAN is not detected as such, but likely scores as Croatian or Serbian.
// MONTENEGRIN is not detected as such, but likely scores as Serbian.
// CROATIAN is detected in the Latin script
// SERBIAN is detected in the Cyrililc and Latin scripts
// Zhuang is detected in the Latin script only.
//
// The languages X_PIG_LATIN and X_KLINGON are detected in the
//  extended calls ExtDetectLanguageSummary().
//
// UNKNOWN_LANGUAGE is returned if no language's internal reliablity measure
//  is high enough. This happens with non-text input such as the bytes of a
//  JPEG, and also with text in languages outside training set.
//
// The following languages are to be detected in multiple scripts:
//  AZERBAIJANI (Latin, Cyrillic*, Arabic*)
//  BURMESE (Latin, Myanmar)
//  HAUSA (Latin, Arabic)
//  KASHMIRI (Arabic, Devanagari)
//  KAZAKH (Latin, Cyrillic, Arabic)
//  KURDISH (Latin, Arabic)
//  KYRGYZ (Cyrillic, Arabic)
//  LIMBU (Devanagari, Limbu)
//  MONGOLIAN (Cyrillic, Mongolian)
//  SANSKRIT (Latin, Devanagari)
//  SINDHI (Arabic, Devanagari)
//  TAGALOG (Latin, Tagalog)
//  TAJIK (Cyrillic, Arabic*)
//  TATAR (Latin, Cyrillic, Arabic)
//  TURKMEN (Latin, Cyrillic, Arabic)
//  UIGHUR (Latin, Cyrillic, Arabic)
//  UZBEK (Latin, Cyrillic, Arabic)
//
// * Due to a shortage of training text, AZERBAIJANI is not currently detected
//   in Arabic or Cyrillic scripts, nor TAJIK in Arabic script.
//

#ifndef I18N_ENCODINGS_CLD2_PUBLIC_COMPACT_LANG_DET_H_
#define I18N_ENCODINGS_CLD2_PUBLIC_COMPACT_LANG_DET_H_

#include <cstddef>
#include "lang_script.h"
#include "encodings.h"

namespace CLD2 {

  // Pass in hints whenever possible; doing so improves detection accuracy. The
  // set of passed-in hints are all information that is external to the text
  // itself.
  // Init to {NULL, NULL, UNKNOWN_ENCODING, UNKNOWN_LANGUAGE} if not known.
  //
  // The content_language_hint is intended to come from an HTTP header
  // Content-Language: field, the tld_hint from the hostname of a URL, the
  // encoding-hint from an encoding detector applied to the input
  // document, and the language hint from any other context you might have.

  typedef struct {
    const char* content_language_hint;      // "mi,en" boosts Maori and English
    const char* tld_hint;                   // "id" boosts Indonesian
    Encoding encoding_hint;                 // SJS boosts Japanese
    Language language_hint;                 // ITALIAN boosts it
  } CLDHints;

  // Public use flags, debug output controls
  static const unsigned int kCLDFlagScoreAsQuads = 0x0100;
  static const unsigned int kCLDFlagBestEffort =   0x4000;

/***

Flag meanings:
 kCLDFlagScoreAsQuads
   Normally, several languages are detected solely by their Unicode script.
   Combined with appropritate lookup tables, this flag forces them instead
   to be detected via quadgrams. This can be a useful refinement when looking
   for meaningful text in these languages, instead of just character sets.
   The default tables do not support this use.
 kCLDFlagBestEffort
  Give best-effort answer, instead of UNKNOWN_LANGUAGE. May be useful for
  short text if the caller prefers an approximate answer over none.

***/

  // Also returns 3 internal language scores as a ratio to
  // normal score for real text in that language. Scores close to 1.0 indicate
  // normal text, while scores far away from 1.0 indicate badly-skewed text or
  // gibberish.
  // The data in 'buffer' is expected to be valid UTF-8.
  Language ExtDetectLanguageSummary(const char* buffer,
                                    std::size_t buffer_length,
                                    const CLDHints* cld_hints,
                                    int flags,
                                    Language* language3,
                                    int* percent3,
                                    double* normalized_score3,
                                    bool* is_reliable);

};      // End namespace CLD2

#endif  // I18N_ENCODINGS_CLD2_PUBLIC_COMPACT_LANG_DET_H_
