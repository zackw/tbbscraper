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

#include "compact_lang_det.h"
#include "compact_lang_det_impl.h"

namespace CLD2 {

// Use this one ONLY if you can prove the the input text is valid UTF-8 by
// design because it went through a known-good conversion program.
//
// Hints are collected into a struct.
// Flags are passed in (normally zero).
//
// Also returns 3 internal language scores as a ratio to
// normal score for real text in that language. Scores close to 1.0 indicate
// normal text, while scores far away from 1.0 indicate badly-skewed text or
// gibberish
//
// Returns a vector of chunks in different languages, so that caller may
// spell-check, translate, or otherwaise process different parts of the input
// buffer in language-dependant ways.
//
Language ExtDetectLanguageSummary(
                        const char* buffer,
                        std::size_t buffer_length,
                        const CLDHints* cld_hints,
                        int flags,
                        Language* language3,
                        int* percent3,
                        double* normalized_score3,
                        bool* is_reliable) {

  CLDHints dummy_hints = { NULL, NULL, UNKNOWN_ENCODING, UNKNOWN_LANGUAGE };
  Language dummy_lang3[3];
  int dummy_pct3[3];
  double dummy_nscore3[3];

  if (!cld_hints) cld_hints = &dummy_hints;
  if (!language3) language3 = dummy_lang3;
  if (!percent3) percent3 = dummy_pct3;
  if (!normalized_score3) normalized_score3 = dummy_nscore3;

  return DetectLanguageSummaryV2(
                          buffer,
                          buffer_length,
                          cld_hints,
                          true,
                          flags,
                          UNKNOWN_LANGUAGE,
                          language3,
                          percent3,
                          normalized_score3,
                          is_reliable);
}

}       // End namespace CLD2
