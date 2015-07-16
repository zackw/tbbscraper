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
// Stuff used only by online detector, not used offline
//

#ifndef I18N_ENCODINGS_CLD2_INTERNAL_NEW_CLDUTIL_H__
#define I18N_ENCODINGS_CLD2_INTERNAL_NEW_CLDUTIL_H__

#include <stdint.h>
#include "generated_language.h"

namespace CLD2 {

struct ScoringContext;
struct ScoringHitBuffer;
class Tote;

// Score up to 64KB of a single script span in one pass
// Make a dummy entry off the end to calc length of last span
// Return offset of first unused input byte
int GetUniHits(const char* text,
                     int letter_offset, int letter_limit,
                     ScoringContext* scoringcontext,
                     ScoringHitBuffer* hitbuffer);

// Score up to 64KB of a single script span, doing both delta-bi and
// distinct bis in one pass
void GetBiHits(const char* text,
                     int letter_offset, int letter_limit,
                     ScoringContext* scoringcontext,
                     ScoringHitBuffer* hitbuffer);

// Score up to 64KB of a single script span in one pass
// Make a dummy entry off the end to calc length of last span
// Return offset of first unused input byte
int GetQuadHits(const char* text,
                     int letter_offset, int letter_limit,
                     ScoringContext* scoringcontext,
                     ScoringHitBuffer* hitbuffer);

// Score up to 64KB of a single script span, doing both delta-octa and
// distinct words in one pass
void GetOctaHits(const char* text,
                     int letter_offset, int letter_limit,
                     ScoringContext* scoringcontext,
                     ScoringHitBuffer* hitbuffer);

// Not sure if these belong here or in scoreonescriptspan.cc
int ReliabilityDelta(int value1, int value2, int gramcount);
int ReliabilityExpected(int actual_score_1kb, int expected_score_1kb);

// Create a langprob packed value from its parts.
uint32_t MakeLangProb(Language lang, int qprob);


void ProcessProbV2Tote(uint32_t probs, Tote* tote);

// Return score for a particular per-script language, or zero
int GetLangScore(uint32_t probs, uint8_t pslang);

static inline int minint(int a, int b) {return (a < b) ? a: b;}
static inline int maxint(int a, int b) {return (a > b) ? a: b;}

}       // End namespace CLD2

#endif  // I18N_ENCODINGS_CLD2_INTERNAL_NEW_CLDUTIL_H__


