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
// Updated 2014.01 for dual table lookup
//

#include <stdint.h>
#include <string.h>
#include <vector>

#include "compact_lang_det_impl.h"

#include "cld2tablesummary.h"
#include "cldutil.h"
#include "compact_lang_det_hint_code.h"
#include "encodings.h"
#include "generated_ulscript.h"
#include "getonescriptspan.h"
#include "lang_script.h"
#include "langspan.h"
#include "scoreonescriptspan.h"
#include "tote.h"
#include "utf8statetable.h"

namespace CLD2 {

using namespace std;

// Linker supplies the right tables, From files
// cld_generated_cjk_uni_prop_80.cc  cld2_generated_cjk_compatible.cc
// cld_generated_cjk_delta_bi_32.cc  generated_distinct_bi_0.cc
// cld2_generated_quad*.cc  cld2_generated_deltaocta*.cc
// cld2_generated_distinctocta*.cc
// cld_generated_score_quad_octa_1024_256.cc

// 2014.01 Now implementing quadgram dual lookup tables, to allow main table
//   sizes that are 1/3/5 times a power of two, instead of just powers of two.
//   Gives more flexibility of total footprint for CLD2.

extern const int kLanguageToPLangSize;
extern const int kCloseSetSize;

extern const UTF8PropObj cld_generated_CjkUni_obj;
extern const CLD2TableSummary kCjkCompat_obj;
extern const CLD2TableSummary kCjkDeltaBi_obj;
extern const CLD2TableSummary kDistinctBiTable_obj;
extern const CLD2TableSummary kQuad_obj;
extern const CLD2TableSummary kQuad_obj2;     // Dual lookup tables
extern const CLD2TableSummary kDeltaOcta_obj;
extern const CLD2TableSummary kDistinctOcta_obj;
extern const short kAvgDeltaOctaScore[];

static const bool FLAGS_cld_no_minimum_bytes = false;
static const bool FLAGS_cld_forcewords = true;
static const bool FLAGS_cld_showme = false;
static const bool FLAGS_cld_echotext = true;
static const int32_t FLAGS_cld_textlimit = 160;
static const int32_t FLAGS_cld_smoothwidth = 20;
static const bool FLAGS_cld_2011_hints = true;
static const int32_t FLAGS_cld_max_lang_tag_scan_kb = 8;

static const bool FLAGS_dbgscore = false;


static const int kLangHintInitial = 12;  // Boost language by N initially
static const int kLangHintBoost = 12;    // Boost language by N/16 per quadgram

static const int kShortSpanThresh = 32;       // Bytes
static const int kMaxSecondChanceLen = 1024;  // Look at first 1K of short spans

static const int kCheapSqueezeTestThresh = 4096;  // Only look for squeezing
                                                  // after this many text bytes
static const int kCheapSqueezeTestLen = 256;  // Bytes to test to trigger sqz
static const int kSpacesTriggerPercent = 25;  // Trigger sqz if >=25% spaces
static const int kPredictTriggerPercent = 67; // Trigger sqz if >=67% predicted

static const int kChunksizeDefault = 48;      // Squeeze 48-byte chunks
static const int kSpacesThreshPercent = 25;   // Squeeze if >=25% spaces
static const int kPredictThreshPercent = 40;  // Squeeze if >=40% predicted

static const int kMaxSpaceScan = 32;          // Bytes

static const int kGoodLang1Percent = 70;
static const int kGoodLang1and2Percent = 93;
static const int kShortTextThresh = 256;      // Bytes

static const int kMinChunkSizeQuads = 4;      // Chunk is at least four quads
static const int kMaxChunkSizeQuads = 1024;   // Chunk is at most 1K quads

static const int kDefaultWordSpan = 256;      // Scan at least this many initial
                                              // bytes with word scoring
static const int kReallyBigWordSpan = 9999999;  // Forces word scoring all text

static const int kMinReliableSeq = 50;      // Record in seq if >= 50% reliable

static const int kPredictionTableSize = 4096;   // Must be exactly 4096 for
                                                // cheap compressor

static const int kNonEnBoilerplateMinPercent = 17;    // <this => no second
static const int kNonFIGSBoilerplateMinPercent = 20;  // <this => no second
static const int kGoodFirstMinPercent = 26;           // <this => UNK
static const int kGoodFirstReliableMinPercent = 51;   // <this => unreli
static const int kIgnoreMaxPercent = 20;              // >this => unreli
static const int kKeepMinPercent = 2;                 // <this => unreli



// Statistically closest language, based on quadgram table
// Those that are far from other languges map to UNKNOWN_LANGUAGE
// Subscripted by Language
//
// From lang_correlation.txt and hand-edits
// sed 's/^\([^ ]*\) \([^ ]*\) coef=0\.\(..\).*$/
//   (\3 >= kMinCorrPercent) ? \2 : UNKNOWN_LANGUAGE,
//   \/\/ \1/' lang_correlation.txt >/tmp/closest_lang_decl.txt
//
static const int kMinCorrPercent = 24;        // Pick off how close you want
                                              // 24 catches PERSIAN <== ARABIC
                                              // but not SPANISH <== PORTUGESE
static Language Unknown = UNKNOWN_LANGUAGE;

// Suspect idea
// Subscripted by Language
static const Language kClosestAltLanguage[] = {
  (28 >= kMinCorrPercent) ? SCOTS : UNKNOWN_LANGUAGE,  // ENGLISH
  (36 >= kMinCorrPercent) ? NORWEGIAN : UNKNOWN_LANGUAGE,  // DANISH
  (31 >= kMinCorrPercent) ? AFRIKAANS : UNKNOWN_LANGUAGE,  // DUTCH
  (15 >= kMinCorrPercent) ? ESTONIAN : UNKNOWN_LANGUAGE,  // FINNISH
  (11 >= kMinCorrPercent) ? OCCITAN : UNKNOWN_LANGUAGE,  // FRENCH
  (17 >= kMinCorrPercent) ? LUXEMBOURGISH : UNKNOWN_LANGUAGE,  // GERMAN
  (27 >= kMinCorrPercent) ? YIDDISH : UNKNOWN_LANGUAGE,  // HEBREW
  (16 >= kMinCorrPercent) ? CORSICAN : UNKNOWN_LANGUAGE,  // ITALIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // Japanese
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // Korean
  (41 >= kMinCorrPercent) ? NORWEGIAN_N : UNKNOWN_LANGUAGE,  // NORWEGIAN
  ( 5 >= kMinCorrPercent) ? SLOVAK : UNKNOWN_LANGUAGE,  // POLISH
  (23 >= kMinCorrPercent) ? SPANISH : UNKNOWN_LANGUAGE,  // PORTUGUESE
  (33 >= kMinCorrPercent) ? BULGARIAN : UNKNOWN_LANGUAGE,  // RUSSIAN
  (28 >= kMinCorrPercent) ? GALICIAN : UNKNOWN_LANGUAGE,  // SPANISH
  (17 >= kMinCorrPercent) ? NORWEGIAN : UNKNOWN_LANGUAGE,  // SWEDISH
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // Chinese
  (42 >= kMinCorrPercent) ? SLOVAK : UNKNOWN_LANGUAGE,  // CZECH
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // GREEK
  (35 >= kMinCorrPercent) ? FAROESE : UNKNOWN_LANGUAGE,  // ICELANDIC
  ( 7 >= kMinCorrPercent) ? LITHUANIAN : UNKNOWN_LANGUAGE,  // LATVIAN
  ( 7 >= kMinCorrPercent) ? LATVIAN : UNKNOWN_LANGUAGE,  // LITHUANIAN
  ( 4 >= kMinCorrPercent) ? LATIN : UNKNOWN_LANGUAGE,  // ROMANIAN
  ( 4 >= kMinCorrPercent) ? SLOVAK : UNKNOWN_LANGUAGE,  // HUNGARIAN
  (15 >= kMinCorrPercent) ? FINNISH : UNKNOWN_LANGUAGE,  // ESTONIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // Ignore
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // Unknown
  (33 >= kMinCorrPercent) ? RUSSIAN : UNKNOWN_LANGUAGE,  // BULGARIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // CROATIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // SERBIAN
  (24 >= kMinCorrPercent) ? SCOTS_GAELIC : UNKNOWN_LANGUAGE,  // IRISH
  (28 >= kMinCorrPercent) ? SPANISH : UNKNOWN_LANGUAGE,  // GALICIAN
  ( 8 >= kMinCorrPercent) ? INDONESIAN : UNKNOWN_LANGUAGE,  // TAGALOG
  (29 >= kMinCorrPercent) ? AZERBAIJANI : UNKNOWN_LANGUAGE,  // TURKISH
  (28 >= kMinCorrPercent) ? RUSSIAN : UNKNOWN_LANGUAGE,  // UKRAINIAN
  (37 >= kMinCorrPercent) ? MARATHI : UNKNOWN_LANGUAGE,  // HINDI
  (29 >= kMinCorrPercent) ? BULGARIAN : UNKNOWN_LANGUAGE,  // MACEDONIAN
  (14 >= kMinCorrPercent) ? ASSAMESE : UNKNOWN_LANGUAGE,  // BENGALI
  (46 >= kMinCorrPercent) ? MALAY : UNKNOWN_LANGUAGE,  // INDONESIAN
  ( 9 >= kMinCorrPercent) ? INTERLINGUA : UNKNOWN_LANGUAGE,  // LATIN
  (46 >= kMinCorrPercent) ? INDONESIAN : UNKNOWN_LANGUAGE,  // MALAY
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // MALAYALAM
  ( 4 >= kMinCorrPercent) ? BRETON : UNKNOWN_LANGUAGE,  // WELSH
  ( 8 >= kMinCorrPercent) ? HINDI : UNKNOWN_LANGUAGE,  // NEPALI
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // TELUGU
  ( 3 >= kMinCorrPercent) ? ESPERANTO : UNKNOWN_LANGUAGE,  // ALBANIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // TAMIL
  (22 >= kMinCorrPercent) ? UKRAINIAN : UNKNOWN_LANGUAGE,  // BELARUSIAN
  (15 >= kMinCorrPercent) ? SUNDANESE : UNKNOWN_LANGUAGE,  // JAVANESE
  (19 >= kMinCorrPercent) ? CATALAN : UNKNOWN_LANGUAGE,  // OCCITAN
  (27 >= kMinCorrPercent) ? PERSIAN : UNKNOWN_LANGUAGE,  // URDU
  (36 >= kMinCorrPercent) ? HINDI : UNKNOWN_LANGUAGE,  // BIHARI
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // GUJARATI
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // THAI
  (24 >= kMinCorrPercent) ? PERSIAN : UNKNOWN_LANGUAGE,  // ARABIC
  (19 >= kMinCorrPercent) ? OCCITAN : UNKNOWN_LANGUAGE,  // CATALAN
  ( 4 >= kMinCorrPercent) ? LATIN : UNKNOWN_LANGUAGE,  // ESPERANTO
  ( 3 >= kMinCorrPercent) ? GERMAN : UNKNOWN_LANGUAGE,  // BASQUE
  ( 9 >= kMinCorrPercent) ? LATIN : UNKNOWN_LANGUAGE,  // INTERLINGUA
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // KANNADA
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // PUNJABI
  (24 >= kMinCorrPercent) ? IRISH : UNKNOWN_LANGUAGE,  // SCOTS_GAELIC
  ( 7 >= kMinCorrPercent) ? KINYARWANDA : UNKNOWN_LANGUAGE,  // SWAHILI
  (28 >= kMinCorrPercent) ? SERBIAN : UNKNOWN_LANGUAGE,  // SLOVENIAN
  (37 >= kMinCorrPercent) ? HINDI : UNKNOWN_LANGUAGE,  // MARATHI
  ( 3 >= kMinCorrPercent) ? ITALIAN : UNKNOWN_LANGUAGE,  // MALTESE
  ( 1 >= kMinCorrPercent) ? YORUBA : UNKNOWN_LANGUAGE,  // VIETNAMESE
  (15 >= kMinCorrPercent) ? DUTCH : UNKNOWN_LANGUAGE,  // FRISIAN
  (42 >= kMinCorrPercent) ? CZECH : UNKNOWN_LANGUAGE,  // SLOVAK
  // Original ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // ChineseT
  (24 >= kMinCorrPercent) ? CHINESE : UNKNOWN_LANGUAGE,  // ChineseT
  (35 >= kMinCorrPercent) ? ICELANDIC : UNKNOWN_LANGUAGE,  // FAROESE
  (15 >= kMinCorrPercent) ? JAVANESE : UNKNOWN_LANGUAGE,  // SUNDANESE
  (17 >= kMinCorrPercent) ? TAJIK : UNKNOWN_LANGUAGE,  // UZBEK
  ( 7 >= kMinCorrPercent) ? TIGRINYA : UNKNOWN_LANGUAGE,  // AMHARIC
  (29 >= kMinCorrPercent) ? TURKISH : UNKNOWN_LANGUAGE,  // AZERBAIJANI
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // GEORGIAN
  ( 7 >= kMinCorrPercent) ? AMHARIC : UNKNOWN_LANGUAGE,  // TIGRINYA
  (27 >= kMinCorrPercent) ? URDU : UNKNOWN_LANGUAGE,  // PERSIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // BOSNIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // SINHALESE
  (41 >= kMinCorrPercent) ? NORWEGIAN : UNKNOWN_LANGUAGE,  // NORWEGIAN_N
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // PORTUGUESE_P
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // PORTUGUESE_B
  (37 >= kMinCorrPercent) ? ZULU : UNKNOWN_LANGUAGE,  // XHOSA
  (37 >= kMinCorrPercent) ? XHOSA : UNKNOWN_LANGUAGE,  // ZULU
  ( 2 >= kMinCorrPercent) ? SPANISH : UNKNOWN_LANGUAGE,  // GUARANI
  (29 >= kMinCorrPercent) ? TSWANA : UNKNOWN_LANGUAGE,  // SESOTHO
  ( 7 >= kMinCorrPercent) ? TURKISH : UNKNOWN_LANGUAGE,  // TURKMEN
  ( 8 >= kMinCorrPercent) ? KAZAKH : UNKNOWN_LANGUAGE,  // KYRGYZ
  ( 5 >= kMinCorrPercent) ? FRENCH : UNKNOWN_LANGUAGE,  // BRETON
  ( 3 >= kMinCorrPercent) ? GANDA : UNKNOWN_LANGUAGE,  // TWI
  (27 >= kMinCorrPercent) ? HEBREW : UNKNOWN_LANGUAGE,  // YIDDISH
  (28 >= kMinCorrPercent) ? SLOVENIAN : UNKNOWN_LANGUAGE,  // SERBO_CROATIAN
  (12 >= kMinCorrPercent) ? OROMO : UNKNOWN_LANGUAGE,  // SOMALI
  ( 9 >= kMinCorrPercent) ? UZBEK : UNKNOWN_LANGUAGE,  // UIGHUR
  (15 >= kMinCorrPercent) ? PERSIAN : UNKNOWN_LANGUAGE,  // KURDISH
  ( 6 >= kMinCorrPercent) ? KYRGYZ : UNKNOWN_LANGUAGE,  // MONGOLIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // ARMENIAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // LAOTHIAN
  ( 8 >= kMinCorrPercent) ? URDU : UNKNOWN_LANGUAGE,  // SINDHI
  (10 >= kMinCorrPercent) ? ITALIAN : UNKNOWN_LANGUAGE,  // RHAETO_ROMANCE
  (31 >= kMinCorrPercent) ? DUTCH : UNKNOWN_LANGUAGE,  // AFRIKAANS
  (17 >= kMinCorrPercent) ? GERMAN : UNKNOWN_LANGUAGE,  // LUXEMBOURGISH
  ( 2 >= kMinCorrPercent) ? SCOTS : UNKNOWN_LANGUAGE,  // BURMESE
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // KHMER
  (45 >= kMinCorrPercent) ? DZONGKHA : UNKNOWN_LANGUAGE,  // TIBETAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // DHIVEHI
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // CHEROKEE
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // SYRIAC
  ( 8 >= kMinCorrPercent) ? DUTCH : UNKNOWN_LANGUAGE,  // LIMBU
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // ORIYA
  (14 >= kMinCorrPercent) ? BENGALI : UNKNOWN_LANGUAGE,  // ASSAMESE
  (16 >= kMinCorrPercent) ? ITALIAN : UNKNOWN_LANGUAGE,  // CORSICAN
  ( 5 >= kMinCorrPercent) ? INTERLINGUA : UNKNOWN_LANGUAGE,  // INTERLINGUE
  ( 8 >= kMinCorrPercent) ? KYRGYZ : UNKNOWN_LANGUAGE,  // KAZAKH
  ( 4 >= kMinCorrPercent) ? SWAHILI : UNKNOWN_LANGUAGE,  // LINGALA
  (11 >= kMinCorrPercent) ? RUSSIAN : UNKNOWN_LANGUAGE,  // MOLDAVIAN
  (19 >= kMinCorrPercent) ? PERSIAN : UNKNOWN_LANGUAGE,  // PASHTO
  ( 5 >= kMinCorrPercent) ? AYMARA : UNKNOWN_LANGUAGE,  // QUECHUA
  ( 5 >= kMinCorrPercent) ? KINYARWANDA : UNKNOWN_LANGUAGE,  // SHONA
  (17 >= kMinCorrPercent) ? UZBEK : UNKNOWN_LANGUAGE,  // TAJIK
  (13 >= kMinCorrPercent) ? BASHKIR : UNKNOWN_LANGUAGE,  // TATAR
  (11 >= kMinCorrPercent) ? SAMOAN : UNKNOWN_LANGUAGE,  // TONGA
  ( 2 >= kMinCorrPercent) ? TWI : UNKNOWN_LANGUAGE,  // YORUBA
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // CREOLES_AND_PIDGINS_ENGLISH_BASED
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // CREOLES_AND_PIDGINS_FRENCH_BASED
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // CREOLES_AND_PIDGINS_PORTUGUESE_BASED
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // CREOLES_AND_PIDGINS_OTHER
  ( 6 >= kMinCorrPercent) ? TONGA : UNKNOWN_LANGUAGE,  // MAORI
  ( 3 >= kMinCorrPercent) ? OROMO : UNKNOWN_LANGUAGE,  // WOLOF
  ( 1 >= kMinCorrPercent) ? MONGOLIAN : UNKNOWN_LANGUAGE,  // ABKHAZIAN
  ( 8 >= kMinCorrPercent) ? SOMALI : UNKNOWN_LANGUAGE,  // AFAR
  ( 5 >= kMinCorrPercent) ? QUECHUA : UNKNOWN_LANGUAGE,  // AYMARA
  (13 >= kMinCorrPercent) ? TATAR : UNKNOWN_LANGUAGE,  // BASHKIR
  ( 3 >= kMinCorrPercent) ? ENGLISH : UNKNOWN_LANGUAGE,  // BISLAMA
  (45 >= kMinCorrPercent) ? TIBETAN : UNKNOWN_LANGUAGE,  // DZONGKHA
  ( 4 >= kMinCorrPercent) ? TONGA : UNKNOWN_LANGUAGE,  // FIJIAN
  ( 7 >= kMinCorrPercent) ? INUPIAK : UNKNOWN_LANGUAGE,  // GREENLANDIC
  ( 3 >= kMinCorrPercent) ? AFAR : UNKNOWN_LANGUAGE,  // HAUSA
  ( 3 >= kMinCorrPercent) ? OCCITAN : UNKNOWN_LANGUAGE,  // HAITIAN_CREOLE
  ( 7 >= kMinCorrPercent) ? GREENLANDIC : UNKNOWN_LANGUAGE,  // INUPIAK
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // INUKTITUT
  ( 4 >= kMinCorrPercent) ? HINDI : UNKNOWN_LANGUAGE,  // KASHMIRI
  (30 >= kMinCorrPercent) ? RUNDI : UNKNOWN_LANGUAGE,  // KINYARWANDA
  ( 2 >= kMinCorrPercent) ? TAGALOG : UNKNOWN_LANGUAGE,  // MALAGASY
  (17 >= kMinCorrPercent) ? GERMAN : UNKNOWN_LANGUAGE,  // NAURU
  (12 >= kMinCorrPercent) ? SOMALI : UNKNOWN_LANGUAGE,  // OROMO
  (30 >= kMinCorrPercent) ? KINYARWANDA : UNKNOWN_LANGUAGE,  // RUNDI
  (11 >= kMinCorrPercent) ? TONGA : UNKNOWN_LANGUAGE,  // SAMOAN
  ( 1 >= kMinCorrPercent) ? LINGALA : UNKNOWN_LANGUAGE,  // SANGO
  (32 >= kMinCorrPercent) ? MARATHI : UNKNOWN_LANGUAGE,  // SANSKRIT
  (16 >= kMinCorrPercent) ? ZULU : UNKNOWN_LANGUAGE,  // SISWANT
  ( 5 >= kMinCorrPercent) ? SISWANT : UNKNOWN_LANGUAGE,  // TSONGA
  (29 >= kMinCorrPercent) ? SESOTHO : UNKNOWN_LANGUAGE,  // TSWANA
  ( 2 >= kMinCorrPercent) ? ESTONIAN : UNKNOWN_LANGUAGE,  // VOLAPUK
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // ZHUANG
  ( 1 >= kMinCorrPercent) ? MALAY : UNKNOWN_LANGUAGE,  // KHASI
  (28 >= kMinCorrPercent) ? ENGLISH : UNKNOWN_LANGUAGE,  // SCOTS
  (15 >= kMinCorrPercent) ? KINYARWANDA : UNKNOWN_LANGUAGE,  // GANDA
  ( 7 >= kMinCorrPercent) ? ENGLISH : UNKNOWN_LANGUAGE,  // MANX
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // MONTENEGRIN

  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // AKAN
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // IGBO
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // MAURITIAN_CREOLE
  ( 0 >= kMinCorrPercent) ? Unknown : UNKNOWN_LANGUAGE,  // HAWAIIAN
};

// COMPILE_ASSERT(arraysize(kClosestAltLanguage) == NUM_LANGUAGES,
//                kClosestAltLanguage_has_incorrect_size);

static const ScoringTables kScoringtables = {
    &cld_generated_CjkUni_obj,
    &kCjkCompat_obj,
    &kCjkDeltaBi_obj,
    &kDistinctBiTable_obj,

    &kQuad_obj,
    &kQuad_obj2,                              // Dual lookup tables
    &kDeltaOcta_obj,
    &kDistinctOcta_obj,

    kAvgDeltaOctaScore,
};


inline bool FlagFinish(int flags) {return (flags & kCLDFlagFinish) != 0;}
inline bool FlagSqueeze(int flags) {return (flags & kCLDFlagSqueeze) != 0;}
inline bool FlagRepeats(int flags) {return (flags & kCLDFlagRepeats) != 0;}
inline bool FlagTop40(int flags) {return (flags & kCLDFlagTop40) != 0;}
inline bool FlagShort(int flags) {return (flags & kCLDFlagShort) != 0;}
inline bool FlagHint(int flags) {return (flags & kCLDFlagHint) != 0;}
inline bool FlagUseWords(int flags) {return (flags & kCLDFlagUseWords) != 0;}
inline bool FlagBestEffort(int flags) {
  return (flags & kCLDFlagBestEffort) != 0;
}


  // Defines Top40 packed languages

  // Google top 40 languages
  //
  // Tier 0/1 Language enum list (16)
  //   ENGLISH, /*no en_GB,*/ FRENCH, ITALIAN, GERMAN, SPANISH,    // E - FIGS
  //   DUTCH, CHINESE, CHINESE_T, JAPANESE, KOREAN,
  //   PORTUGUESE, RUSSIAN, POLISH, TURKISH, THAI,
  //   ARABIC,
  //
  // Tier 2 Language enum list (22)
  //   SWEDISH, FINNISH, DANISH, /*no pt-PT,*/ ROMANIAN, HUNGARIAN,
  //   HEBREW, INDONESIAN, CZECH, GREEK, NORWEGIAN,
  //   VIETNAMESE, BULGARIAN, CROATIAN, LITHUANIAN, SLOVAK,
  //   TAGALOG, SLOVENIAN, SERBIAN, CATALAN, LATVIAN,
  //   UKRAINIAN, HINDI,
  //
  //   use SERBO_CROATIAN instead of BOSNIAN, SERBIAN, CROATIAN, MONTENEGRIN(21)
  //
  // Include IgnoreMe (TG_UNKNOWN_LANGUAGE, 25+1) as a top 40

// Backscan to word boundary, returning how many bytes n to go back
// so that src - n is non-space ans src - n - 1 is space.
// If not found in kMaxSpaceScan bytes, return 0..3 to a clean UTF-8 boundary
int BackscanToSpace(const char* src, int limit) {
  int n = 0;
  limit = minint(limit, kMaxSpaceScan);
  while (n < limit) {
    if (src[-n - 1] == ' ') {return n;}    // We are at _X
    ++n;
  }
  n = 0;
  while (n < limit) {
    if ((src[-n] & 0xc0) != 0x80) {return n;}    // We are at char begin
    ++n;
  }
  return 0;
}

// Forwardscan to word boundary, returning how many bytes n to go forward
// so that src + n is non-space ans src + n - 1 is space.
// If not found in kMaxSpaceScan bytes, return 0..3 to a clean UTF-8 boundary
int ForwardscanToSpace(const char* src, int limit) {
  int n = 0;
  limit = minint(limit, kMaxSpaceScan);
  while (n < limit) {
    if (src[n] == ' ') {return n + 1;}    // We are at _X
    ++n;
  }
  n = 0;
  while (n < limit) {
    if ((src[n] & 0xc0) != 0x80) {return n;}    // We are at char begin
    ++n;
  }
  return 0;
}


// This uses a cheap predictor to get a measure of compression, and
// hence a measure of repetitiveness. It works on complete UTF-8 characters
// instead of bytes, because three-byte UTF-8 Indic, etc. text compress highly
// all the time when done with a byte-based count. Sigh.
//
// To allow running prediction across multiple chunks, caller passes in current
// 12-bit hash value and int[4096] prediction table. Caller inits these to 0.
//
// Returns the number of *bytes* correctly predicted, increments by 1..4 for
// each correctly-predicted character.
//
// NOTE: Overruns by up to three bytes. Not a problem with valid UTF-8 text
//

// TODO(dsites) make this use just one byte per UTF-8 char and incr by charlen

int CountPredictedBytes(const char* isrc, int src_len, int* hash, int* tbl) {
  int p_count = 0;
  const uint8_t* src = reinterpret_cast<const uint8_t*>(isrc);
  const uint8_t* srclimit = src + src_len;
  int local_hash = *hash;

  while (src < srclimit) {
    int c = src[0];
    int incr = 1;

    // Pick up one char and length
    if (c < 0xc0) {
      // One-byte or continuation byte: 00xxxxxx 01xxxxxx 10xxxxxx
      // Do nothing more
    } else if ((c & 0xe0) == 0xc0) {
      // Two-byte
      c = (c << 8) | src[1];
      incr = 2;
    } else if ((c & 0xf0) == 0xe0) {
      // Three-byte
      c = (c << 16) | (src[1] << 8) | src[2];
      incr = 3;
    } else {
      // Four-byte
      c = (c << 24) | (src[1] << 16) | (src[2] << 8) | src[3];
      incr = 4;
    }
    src += incr;

    int p = tbl[local_hash];            // Prediction
    tbl[local_hash] = c;                // Update prediction
    if (c == p) {
      p_count += incr;                  // Count bytes of good predictions
    }

    local_hash = ((local_hash << 4) ^ c) & 0xfff;
  }
  *hash = local_hash;
  return p_count;
}



// Counts number of spaces; a little faster than one-at-a-time
// Doesn't count odd bytes at end
int CountSpaces4(const char* src, int src_len) {
  int s_count = 0;
  for (int i = 0; i < (src_len & ~3); i += 4) {
    s_count += (src[i] == ' ');
    s_count += (src[i+1] == ' ');
    s_count += (src[i+2] == ' ');
    s_count += (src[i+3] == ' ');
  }
  return s_count;
}


// Remove words of text that have more than half their letters predicted
// correctly by our cheap predictor, moving the remaining words in-place
// to the front of the input buffer.
//
// To allow running prediction across multiple chunks, caller passes in current
// 12-bit hash value and int[4096] prediction table. Caller inits these to 0.
//
// Return the new, possibly-shorter length
//
// Result Buffer ALWAYS has leading space and trailing space space space NUL,
// if input does
//
int CheapRepWordsInplace(char* isrc, int src_len, int* hash, int* tbl) {
  const uint8_t* src = reinterpret_cast<const uint8_t*>(isrc);
  const uint8_t* srclimit = src + src_len;
  char* dst = isrc;
  int local_hash = *hash;
  char* word_dst = dst;           // Start of next word
  int good_predict_bytes = 0;
  int word_length_bytes = 0;

  while (src < srclimit) {
    int c = src[0];
    int incr = 1;
    *dst++ = c;

    if (c == ' ') {
      if ((good_predict_bytes * 2) > word_length_bytes) {
        // Word is well-predicted: backup to start of this word
        dst = word_dst;
        if (FLAGS_cld_showme) {
          // Mark the deletion point with period
          // Don't repeat multiple periods
          // Cannot mark with more bytes or may overwrite unseen input
          if ((isrc < (dst - 2)) && (dst[-2] != '.')) {
            *dst++ = '.';
            *dst++ = ' ';
          }
        }
      }
      word_dst = dst;              // Start of next word
      good_predict_bytes = 0;
      word_length_bytes = 0;
    }

    // Pick up one char and length
    if (c < 0xc0) {
      // One-byte or continuation byte: 00xxxxxx 01xxxxxx 10xxxxxx
      // Do nothing more
    } else if ((c & 0xe0) == 0xc0) {
      // Two-byte
      *dst++ = src[1];
      c = (c << 8) | src[1];
      incr = 2;
    } else if ((c & 0xf0) == 0xe0) {
      // Three-byte
      *dst++ = src[1];
      *dst++ = src[2];
      c = (c << 16) | (src[1] << 8) | src[2];
      incr = 3;
    } else {
      // Four-byte
      *dst++ = src[1];
      *dst++ = src[2];
      *dst++ = src[3];
      c = (c << 24) | (src[1] << 16) | (src[2] << 8) | src[3];
      incr = 4;
    }
    src += incr;
    word_length_bytes += incr;

    int p = tbl[local_hash];            // Prediction
    tbl[local_hash] = c;                // Update prediction
    if (c == p) {
      good_predict_bytes += incr;       // Count good predictions
    }

    local_hash = ((local_hash << 4) ^ c) & 0xfff;
  }

  *hash = local_hash;

  if ((dst - isrc) < (src_len - 3)) {
    // Pad and make last char clean UTF-8 by putting following spaces
    dst[0] = ' ';
    dst[1] = ' ';
    dst[2] = ' ';
    dst[3] = '\0';
  } else  if ((dst - isrc) < src_len) {
    // Make last char clean UTF-8 by putting following space off the end
    dst[0] = ' ';
  }

  return static_cast<int>(dst - isrc);
}


// This alternate form overwrites redundant words, thus avoiding corrupting the
// backmap for generating a vector of original-text ranges.
int CheapRepWordsInplaceOverwrite(char* isrc, int src_len, int* hash, int* tbl) {
  const uint8_t* src = reinterpret_cast<const uint8_t*>(isrc);
  const uint8_t* srclimit = src + src_len;
  char* dst = isrc;
  int local_hash = *hash;
  char* word_dst = dst;           // Start of next word
  int good_predict_bytes = 0;
  int word_length_bytes = 0;

  while (src < srclimit) {
    int c = src[0];
    int incr = 1;
    *dst++ = c;

    if (c == ' ') {
      if ((good_predict_bytes * 2) > word_length_bytes) {
        // Word [word_dst..dst-1) is well-predicted: overwrite
        for (char* p = word_dst; p < dst - 1; ++p) {*p = '.';}
      }
      word_dst = dst;              // Start of next word
      good_predict_bytes = 0;
      word_length_bytes = 0;
    }

    // Pick up one char and length
    if (c < 0xc0) {
      // One-byte or continuation byte: 00xxxxxx 01xxxxxx 10xxxxxx
      // Do nothing more
    } else if ((c & 0xe0) == 0xc0) {
      // Two-byte
      *dst++ = src[1];
      c = (c << 8) | src[1];
      incr = 2;
    } else if ((c & 0xf0) == 0xe0) {
      // Three-byte
      *dst++ = src[1];
      *dst++ = src[2];
      c = (c << 16) | (src[1] << 8) | src[2];
      incr = 3;
    } else {
      // Four-byte
      *dst++ = src[1];
      *dst++ = src[2];
      *dst++ = src[3];
      c = (c << 24) | (src[1] << 16) | (src[2] << 8) | src[3];
      incr = 4;
    }
    src += incr;
    word_length_bytes += incr;

    int p = tbl[local_hash];            // Prediction
    tbl[local_hash] = c;                // Update prediction
    if (c == p) {
      good_predict_bytes += incr;       // Count good predictions
    }

    local_hash = ((local_hash << 4) ^ c) & 0xfff;
  }

  *hash = local_hash;

  if ((dst - isrc) < (src_len - 3)) {
    // Pad and make last char clean UTF-8 by putting following spaces
    dst[0] = ' ';
    dst[1] = ' ';
    dst[2] = ' ';
    dst[3] = '\0';
  } else  if ((dst - isrc) < src_len) {
    // Make last char clean UTF-8 by putting following space off the end
    dst[0] = ' ';
  }

  return static_cast<int>(dst - isrc);
}


// Remove portions of text that have a high density of spaces, or that are
// overly repetitive, squeezing the remaining text in-place to the front of the
// input buffer.
//
// Squeezing looks at density of space/prediced chars in fixed-size chunks,
// specified by chunksize. A chunksize <= 0 uses the default size of 48 bytes.
//
// Return the new, possibly-shorter length
//
// Result Buffer ALWAYS has leading space and trailing space space space NUL,
// if input does
//
int CheapSqueezeInplace(char* isrc,
                                            int src_len,
                                            int ichunksize) {
  char* src = isrc;
  char* dst = src;
  char* srclimit = src + src_len;
  bool skipping = false;

  int hash = 0;
  // Allocate local prediction table.
  int* predict_tbl = new int[kPredictionTableSize];
  memset(predict_tbl, 0, kPredictionTableSize * sizeof(predict_tbl[0]));

  int chunksize = ichunksize;
  if (chunksize == 0) {chunksize = kChunksizeDefault;}
  int space_thresh = (chunksize * kSpacesThreshPercent) / 100;
  int predict_thresh = (chunksize * kPredictThreshPercent) / 100;

  while (src < srclimit) {
    int remaining_bytes = srclimit - src;
    int len = minint(chunksize, remaining_bytes);
    // Make len land us on a UTF-8 character boundary.
    // Ah. Also fixes mispredict because we could get out of phase
    // Loop always terminates at trailing space in buffer
    while ((src[len] & 0xc0) == 0x80) {++len;}  // Move past continuation bytes

    int space_n = CountSpaces4(src, len);
    int predb_n = CountPredictedBytes(src, len, &hash, predict_tbl);
    if ((space_n >= space_thresh) || (predb_n >= predict_thresh)) {
      // Skip the text
      if (!skipping) {
        // Keeping-to-skipping transition; do it at a space
        int n = BackscanToSpace(dst, static_cast<int>(dst - isrc));
        dst -= n;
        if (dst == isrc) {
          // Force a leading space if the first chunk is deleted
          *dst++ = ' ';
        }
        if (FLAGS_cld_showme) {
          // Mark the deletion point with black square U+25A0
          *dst++ = static_cast<unsigned char>(0xe2);
          *dst++ = static_cast<unsigned char>(0x96);
          *dst++ = static_cast<unsigned char>(0xa0);
          *dst++ = ' ';
        }
        skipping = true;
      }
    } else {
      // Keep the text
      if (skipping) {
        // Skipping-to-keeping transition; do it at a space
        int n = ForwardscanToSpace(src, len);
        src += n;
        remaining_bytes -= n;   // Shrink remaining length
        len -= n;
        skipping = false;
      }
      // "len" can be negative in some cases
      if (len > 0) {
        memmove(dst, src, len);
        dst += len;
      }
    }
    src += len;
  }

  if ((dst - isrc) < (src_len - 3)) {
    // Pad and make last char clean UTF-8 by putting following spaces
    dst[0] = ' ';
    dst[1] = ' ';
    dst[2] = ' ';
    dst[3] = '\0';
  } else   if ((dst - isrc) < src_len) {
    // Make last char clean UTF-8 by putting following space off the end
    dst[0] = ' ';
  }

  // Deallocate local prediction table
  delete[] predict_tbl;
  return static_cast<int>(dst - isrc);
}

// This alternate form overwrites redundant words, thus avoiding corrupting the
// backmap for generating a vector of original-text ranges.
int CheapSqueezeInplaceOverwrite(char* isrc,
                                            int src_len,
                                            int ichunksize) {
  char* src = isrc;
  char* dst = src;
  char* srclimit = src + src_len;
  bool skipping = false;

  int hash = 0;
  // Allocate local prediction table.
  int* predict_tbl = new int[kPredictionTableSize];
  memset(predict_tbl, 0, kPredictionTableSize * sizeof(predict_tbl[0]));

  int chunksize = ichunksize;
  if (chunksize == 0) {chunksize = kChunksizeDefault;}
  int space_thresh = (chunksize * kSpacesThreshPercent) / 100;
  int predict_thresh = (chunksize * kPredictThreshPercent) / 100;

  // Always keep first byte (space)
  ++src;
  ++dst;
  while (src < srclimit) {
    int remaining_bytes = srclimit - src;
    int len = minint(chunksize, remaining_bytes);
    // Make len land us on a UTF-8 character boundary.
    // Ah. Also fixes mispredict because we could get out of phase
    // Loop always terminates at trailing space in buffer
    while ((src[len] & 0xc0) == 0x80) {++len;}  // Move past continuation bytes

    int space_n = CountSpaces4(src, len);
    int predb_n = CountPredictedBytes(src, len, &hash, predict_tbl);
    if ((space_n >= space_thresh) || (predb_n >= predict_thresh)) {
      // Overwrite the text [dst-n..dst)
      if (!skipping) {
        // Keeping-to-skipping transition; do it at a space
        int n = BackscanToSpace(dst, static_cast<int>(dst - isrc));
        // Text [word_dst..dst) is well-predicted: overwrite
        for (char* p = dst - n; p < dst; ++p) {*p = '.';}
        skipping = true;
      }
      // Overwrite the text [dst..dst+len)
      for (char* p = dst; p < dst + len; ++p) {*p = '.';}
      dst[len - 1] = ' ';    // Space at end so we can see what is happening
    } else {
      // Keep the text
      if (skipping) {
        // Skipping-to-keeping transition; do it at a space
        int n = ForwardscanToSpace(src, len);
        // Text [dst..dst+n) is well-predicted: overwrite
        for (char* p = dst; p < dst + n - 1; ++p) {*p = '.';}
        skipping = false;
      }
    }
    dst += len;
    src += len;
  }

  if ((dst - isrc) < (src_len - 3)) {
    // Pad and make last char clean UTF-8 by putting following spaces
    dst[0] = ' ';
    dst[1] = ' ';
    dst[2] = ' ';
    dst[3] = '\0';
  } else   if ((dst - isrc) < src_len) {
    // Make last char clean UTF-8 by putting following space off the end
    dst[0] = ' ';
  }

  // Deallocate local prediction table
  delete[] predict_tbl;
  return static_cast<int>(dst - isrc);
}

// Timing 2.8GHz P4 (dsites 2008.03.20) with 170KB input
//  About 90 MB/sec, with or without memcpy, chunksize 48 or 4096
//  Just CountSpaces is about 340 MB/sec
//  Byte-only CountPredictedBytes is about 150 MB/sec
//  Byte-only CountPredictedBytes, conditional tbl[] = is about 85! MB/sec
//  Byte-only CountPredictedBytes is about 180 MB/sec, byte tbl, byte/int c
//  Unjammed byte-only both = 170 MB/sec
//  Jammed byte-only both = 120 MB/sec
//  Back to original w/slight updates, 110 MB/sec
//
bool CheapSqueezeTriggerTest(const char* src, int src_len, int testsize) {
  // Don't trigger at all on short text
  if (src_len < testsize) {return false;}
  int space_thresh = (testsize * kSpacesTriggerPercent) / 100;
  int predict_thresh = (testsize * kPredictTriggerPercent) / 100;
  int hash = 0;
  // Allocate local prediction table.
  int* predict_tbl = new int[kPredictionTableSize];
  memset(predict_tbl, 0, kPredictionTableSize * sizeof(predict_tbl[0]));

  bool retval = false;
  if ((CountSpaces4(src, testsize) >= space_thresh) ||
      (CountPredictedBytes(src, testsize, &hash, predict_tbl) >=
       predict_thresh)) {
    retval = true;
  }
  // Deallocate local prediction table
  delete[] predict_tbl;
  return retval;
}

static const int kMinReliableKeepPercent = 41;  // Remove lang if reli < this

// For Tier3 languages, require a minimum number of bytes to be first-place lang
static const int kGoodFirstT3MinBytes = 24;         // <this => no first

// Move bytes for unreliable langs to another lang or UNKNOWN
// doc_tote is sorted, so cannot Add
//
// If both CHINESE and CHINESET are present and unreliable, do not delete both;
// merge both into CHINESE.
//
//dsites 2009.03.19
// we also want to remove Tier3 languages as the first lang if there is very
// little text like ej1 ej2 ej3 ej4
// maybe fold this back in earlier
//
void RemoveUnreliableLanguages(DocTote* doc_tote) {
  // Prepass to merge some low-reliablility languages
  // TODO: this shouldn't really reach in to the internal structure of doc_tote
  int total_bytes = 0;
  for (int sub = 0; sub < doc_tote->MaxSize(); ++sub) {
    int plang = doc_tote->Key(sub);
    if (plang == DocTote::kUnusedKey) {continue;}               // Empty slot

    Language lang = static_cast<Language>(plang);
    int bytes = doc_tote->Value(sub);
    int reli = doc_tote->Reliability(sub);
    if (bytes == 0) {continue;}                     // Zero bytes
    total_bytes += bytes;

    // Reliable percent = stored reliable score over stored bytecount
    int reliable_percent = reli / bytes;
    if (reliable_percent >= kMinReliableKeepPercent) {continue;}   // Keeper

    // This language is too unreliable to keep, but we might merge it.
    Language altlang = UNKNOWN_LANGUAGE;
    if (lang <= HAWAIIAN) {altlang = kClosestAltLanguage[lang];}
    if (altlang == UNKNOWN_LANGUAGE) {continue;}    // No alternative

    // Look for alternative in doc_tote
    int altsub = doc_tote->Find(altlang);
    if (altsub < 0) {continue;}                     // No alternative text

    int bytes2 = doc_tote->Value(altsub);
    int reli2 = doc_tote->Reliability(altsub);
    if (bytes2 == 0) {continue;}                    // Zero bytes

    // Reliable percent is stored reliable score over stored bytecount
    int reliable_percent2 = reli2 / bytes2;

    // Merge one language into the other. Break ties toward lower lang #
    int tosub = altsub;
    int fromsub = sub;
    if ((reliable_percent2 < reliable_percent) ||
        ((reliable_percent2 == reliable_percent) && (lang < altlang))) {
      tosub = sub;
      fromsub = altsub;
    }

    // Make sure merged reliability doesn't drop and is enough to avoid delete
    int newpercent = maxint(reliable_percent, reliable_percent2);
    newpercent = maxint(newpercent, kMinReliableKeepPercent);
    int newbytes = bytes + bytes2;
    int newreli = newpercent * newbytes;

    doc_tote->SetKey(fromsub, DocTote::kUnusedKey);
    doc_tote->SetScore(fromsub, 0);
    doc_tote->SetReliability(fromsub, 0);
    doc_tote->SetScore(tosub, newbytes);
    doc_tote->SetReliability(tosub, newreli);
  }


  // Pass to delete any remaining unreliable languages
  for (int sub = 0; sub < doc_tote->MaxSize(); ++sub) {
    int plang = doc_tote->Key(sub);
    if (plang == DocTote::kUnusedKey) {continue;}               // Empty slot

    int bytes = doc_tote->Value(sub);
    int reli = doc_tote->Reliability(sub);
    if (bytes == 0) {continue;}                     // Zero bytes

    // Reliable percent is stored as reliable score over stored bytecount
    int reliable_percent = reli / bytes;
    if (reliable_percent >= kMinReliableKeepPercent) {  // Keeper?
       continue;                                        // yes
    }

    // Delete unreliable entry
    doc_tote->SetKey(sub, DocTote::kUnusedKey);
    doc_tote->SetScore(sub, 0);
    doc_tote->SetReliability(sub, 0);
  }
}


// Move all the text bytes from lower byte-count to higher one
void MoveLang1ToLang2(int lang1_sub, int lang2_sub, DocTote* doc_tote) {
  // In doc_tote, move all the bytes lang1 => lang2
  int sum = doc_tote->Value(lang2_sub) + doc_tote->Value(lang1_sub);
  doc_tote->SetValue(lang2_sub, sum);
  sum = doc_tote->Score(lang2_sub) + doc_tote->Score(lang1_sub);
  doc_tote->SetScore(lang2_sub, sum);
  sum = doc_tote->Reliability(lang2_sub) + doc_tote->Reliability(lang1_sub);
  doc_tote->SetReliability(lang2_sub, sum);

  // Delete old entry
  doc_tote->SetKey(lang1_sub, DocTote::kUnusedKey);
  doc_tote->SetScore(lang1_sub, 0);
  doc_tote->SetReliability(lang1_sub, 0);
}

// Move less likely byte count to more likely for close pairs of languages
// If given, also update resultchunkvector
void RefineScoredClosePairs(DocTote* doc_tote) {
  for (int sub = 0; sub < doc_tote->MaxSize(); ++sub) {
    int close_packedlang = doc_tote->Key(sub);
    int subscr = LanguageCloseSet(static_cast<Language>(close_packedlang));
    if (subscr == 0) {continue;}

    // We have a close pair language -- if the other one is also scored and the
    // longword score differs enough, put all our eggs into one basket

    // Nonzero longword score: Go look for the other of this pair
    for (int sub2 = sub + 1; sub2 < doc_tote->MaxSize(); ++sub2) {
      if (LanguageCloseSet(static_cast<Language>(doc_tote->Key(sub2))) == subscr) {
        // We have a matching pair
        // Move all the text bytes from lower byte-count to higher one
        int from_sub, to_sub;
        if (doc_tote->Value(sub) < doc_tote->Value(sub2)) {
          from_sub = sub;
          to_sub = sub2;
        } else {
          from_sub = sub2;
          to_sub = sub;
        }

        MoveLang1ToLang2(from_sub, to_sub, doc_tote);
        break;    // Exit inner for sub2 loop
      }
    }     // End for sub2
  }   // End for sub
}

// Return internal probability score (sum) per 1024 bytes
double GetNormalizedScore(int bytecount, int score) {
  if (bytecount <= 0) {return 0.0;}
  return (score << 10) / double(bytecount);
}

// Extract return values before fixups
void ExtractLangEtc(DocTote* doc_tote, int total_text_bytes,
                    int* reliable_percent3, Language* language3, int* percent3,
                    double*  normalized_score3,
                    int* text_bytes, bool* is_reliable) {
  reliable_percent3[0] = 0;
  reliable_percent3[1] = 0;
  reliable_percent3[2] = 0;
  language3[0] = UNKNOWN_LANGUAGE;
  language3[1] = UNKNOWN_LANGUAGE;
  language3[2] = UNKNOWN_LANGUAGE;
  percent3[0] = 0;
  percent3[1] = 0;
  percent3[2] = 0;
  normalized_score3[0] = 0.0;
  normalized_score3[1] = 0.0;
  normalized_score3[2] = 0.0;

  *text_bytes = total_text_bytes;
  *is_reliable = false;

  int bytecount1 = 0;
  int bytecount2 = 0;
  int bytecount3 = 0;

  int lang1 = doc_tote->Key(0);
  if ((lang1 != DocTote::kUnusedKey) && (lang1 != UNKNOWN_LANGUAGE)) {
    // We have a top language
    language3[0] = static_cast<Language>(lang1);
    bytecount1 = doc_tote->Value(0);
    int reli1 = doc_tote->Reliability(0);
    reliable_percent3[0] = reli1 / (bytecount1 ? bytecount1 : 1);  // avoid zdiv
    normalized_score3[0] = GetNormalizedScore(bytecount1, doc_tote->Score(0));
  }

  int lang2 = doc_tote->Key(1);
  if ((lang2 != DocTote::kUnusedKey) && (lang2 != UNKNOWN_LANGUAGE)) {
    language3[1] = static_cast<Language>(lang2);
    bytecount2 = doc_tote->Value(1);
    int reli2 = doc_tote->Reliability(1);
    reliable_percent3[1] = reli2 / (bytecount2 ? bytecount2 : 1);  // avoid zdiv
    normalized_score3[1] = GetNormalizedScore(bytecount2, doc_tote->Score(1));
  }

  int lang3 = doc_tote->Key(2);
  if ((lang3 != DocTote::kUnusedKey) && (lang3 != UNKNOWN_LANGUAGE)) {
    language3[2] = static_cast<Language>(lang3);
    bytecount3 = doc_tote->Value(2);
    int reli3 = doc_tote->Reliability(2);
    reliable_percent3[2] = reli3 / (bytecount3 ? bytecount3 : 1);  // avoid zdiv
    normalized_score3[2] = GetNormalizedScore(bytecount3, doc_tote->Score(2));
  }

  // Increase total bytes to sum (top 3) if low for some reason
  int total_bytecount12 = bytecount1 + bytecount2;
  int total_bytecount123 = total_bytecount12 + bytecount3;
  if (total_text_bytes < total_bytecount123) {
    total_text_bytes = total_bytecount123;
    *text_bytes = total_text_bytes;
  }

  // Sum minus previous % gives better roundoff behavior than bytecount/total
  int total_text_bytes_div = maxint(1, total_text_bytes);    // Avoid zdiv
  percent3[0] = (bytecount1 * 100) / total_text_bytes_div;
  percent3[1] = (total_bytecount12 * 100) / total_text_bytes_div;
  percent3[2] = (total_bytecount123 * 100) / total_text_bytes_div;
  percent3[2] -= percent3[1];
  percent3[1] -= percent3[0];

  // Roundoff, say 96% 1.6% 1.4%, will produce non-obvious 96% 1% 2%
  // Fix this explicitly
  if (percent3[1] < percent3[2]) {
    ++percent3[1];
    --percent3[2];
  }
  if (percent3[0] < percent3[1]) {
    ++percent3[0];
    --percent3[1];
  }

  *text_bytes = total_text_bytes;

  if ((lang1 != DocTote::kUnusedKey) && (lang1 != UNKNOWN_LANGUAGE)) {
    // We have a top language
    // Its reliability is overall result reliability
    int bytecount = doc_tote->Value(0);
    int reli = doc_tote->Reliability(0);
    int reliable_percent = reli / (bytecount ? bytecount : 1);  // avoid zdiv
    *is_reliable = (reliable_percent >= kMinReliableKeepPercent);
  } else {
    // No top language at all. This can happen with zero text or 100% Klingon
    // if extended=false. Just return all UNKNOWN_LANGUAGE, unreliable.
    *is_reliable = false;
  }

  // If ignore percent is too large, set unreliable.
  int ignore_percent = 100 - (percent3[0] + percent3[1] + percent3[2]);
  if ((ignore_percent > kIgnoreMaxPercent)) {
    *is_reliable = false;
  }
}

bool IsFIGS(Language lang) {
  if (lang == FRENCH) {return true;}
  if (lang == ITALIAN) {return true;}
  if (lang == GERMAN) {return true;}
  if (lang == SPANISH) {return true;}
  return false;
}

bool IsEFIGS(Language lang) {
  if (lang == ENGLISH) {return true;}
  if (lang == FRENCH) {return true;}
  if (lang == ITALIAN) {return true;}
  if (lang == GERMAN) {return true;}
  if (lang == SPANISH) {return true;}
  return false;
}

// For Tier3 languages, require more bytes of text to override
// the first-place language
static const int kGoodSecondT1T2MinBytes = 15;        // <this => no second
static const int kGoodSecondT3MinBytes = 128;         // <this => no second

// Calculate a single summary language for the document, and its reliability.
// Returns language3[0] or language3[1] or ENGLISH or UNKNOWN_LANGUAGE
// This is the heart of matching human-rater perception.
// reliable_percent3[] is currently unused
//
// Do not return Tier3 second language unless there are at least 128 bytes
void CalcSummaryLang(int total_text_bytes,
                     const Language* language3,
                     const int* percent3,
                     Language* summary_lang, bool* is_reliable,
                     int flags) {
  // Vector of active languages; changes if we delete some
  int slot_count = 3;
  int active_slot[3] = {0, 1, 2};

  int ignore_percent = 0;
  int return_percent = percent3[0];   // Default to top lang
  *summary_lang = language3[0];
  *is_reliable = true;
  if (percent3[0] < kKeepMinPercent) {*is_reliable = false;}

  // If any of top 3 is IGNORE, remove it and increment ignore_percent
  for (int i = 0; i < 3; ++i) {
    if (language3[i] == TG_UNKNOWN_LANGUAGE) {
      ignore_percent += percent3[i];
      // Move the rest up, leaving input vectors unchanged
      for (int j=i+1; j < 3; ++j) {
        active_slot[j - 1] = active_slot[j];
      }
      -- slot_count;
      // Logically remove Ignore from percentage-text calculation
      // (extra 1 in 101 avoids zdiv, biases slightly small)
      return_percent = (percent3[0] * 100) / (101 - ignore_percent);
      *summary_lang = language3[active_slot[0]];
      if (percent3[active_slot[0]] < kKeepMinPercent) {*is_reliable = false;}
    }
  }


  // If English and X, where X (not UNK) is big enough,
  // assume the English is boilerplate and return X.
  // Logically remove English from percentage-text calculation
  int second_bytes = (total_text_bytes * percent3[active_slot[1]]) / 100;
  // Require more bytes of text for Tier3 languages
  int minbytesneeded = kGoodSecondT1T2MinBytes;

  if ((language3[active_slot[0]] == ENGLISH) &&
      (language3[active_slot[1]] != ENGLISH) &&
      (language3[active_slot[1]] != UNKNOWN_LANGUAGE) &&
      (percent3[active_slot[1]] >= kNonEnBoilerplateMinPercent) &&
      (second_bytes >= minbytesneeded)) {
    ignore_percent += percent3[active_slot[0]];
    return_percent = (percent3[active_slot[1]] * 100) / (101 - ignore_percent);
    *summary_lang = language3[active_slot[1]];
    if (percent3[active_slot[1]] < kKeepMinPercent) {*is_reliable = false;}

  // Else If FIGS and X, where X (not UNK, EFIGS) is big enough,
  // assume the FIGS is boilerplate and return X.
  // Logically remove FIGS from percentage-text calculation
  } else if (IsFIGS(language3[active_slot[0]]) &&
             !IsEFIGS(language3[active_slot[1]]) &&
             (language3[active_slot[1]] != UNKNOWN_LANGUAGE) &&
             (percent3[active_slot[1]] >= kNonFIGSBoilerplateMinPercent) &&
             (second_bytes >= minbytesneeded)) {
    ignore_percent += percent3[active_slot[0]];
    return_percent = (percent3[active_slot[1]] * 100) / (101 - ignore_percent);
    *summary_lang = language3[active_slot[1]];
    if (percent3[active_slot[1]] < kKeepMinPercent) {*is_reliable = false;}

  // Else we are returning the first language, but want to improve its
  // return_percent if the second language should be ignored
  } else  if ((language3[active_slot[1]] == ENGLISH) &&
              (language3[active_slot[0]] != ENGLISH)) {
    ignore_percent += percent3[active_slot[1]];
    return_percent = (percent3[active_slot[0]] * 100) / (101 - ignore_percent);
  } else  if (IsFIGS(language3[active_slot[1]]) &&
              !IsEFIGS(language3[active_slot[0]])) {
    ignore_percent += percent3[active_slot[1]];
    return_percent = (percent3[active_slot[0]] * 100) / (101 - ignore_percent);
  }

  // If return percent is too small (too many languages), return UNKNOWN
  if ((return_percent < kGoodFirstMinPercent) && !FlagBestEffort(flags)) {
    *summary_lang = UNKNOWN_LANGUAGE;
    *is_reliable = false;
  }

  // If return percent is small, return language but set unreliable.
  if ((return_percent < kGoodFirstReliableMinPercent)) {
    *is_reliable = false;
  }

  // If ignore percent is too large, set unreliable.
  ignore_percent = 100 - (percent3[0] + percent3[1] + percent3[2]);
  if ((ignore_percent > kIgnoreMaxPercent)) {
    *is_reliable = false;
  }

  // If we removed all the active languages, return UNKNOWN
  if (slot_count == 0) {
    *summary_lang = UNKNOWN_LANGUAGE;
    *is_reliable = false;
  }
}

void AddLangPriorBoost(Language lang, uint32_t langprob,
                       ScoringContext* scoringcontext) {
  // This is called 0..n times with language hints
  // but we don't know the script -- so boost either or both Latn, Othr.

  if (IsLatnLanguage(lang)) {
    LangBoosts* langprior_boost = &scoringcontext->langprior_boost.latn;
    int n = langprior_boost->n;
    langprior_boost->langprob[n] = langprob;
    langprior_boost->n = langprior_boost->wrap(n + 1);
  }

  if (IsOthrLanguage(lang)) {
    LangBoosts* langprior_boost = &scoringcontext->langprior_boost.othr;
    int n = langprior_boost->n;
    langprior_boost->langprob[n] = langprob;
    langprior_boost->n = langprior_boost->wrap(n + 1);
  }

}

void AddOneWhack(Language whacker_lang, Language whackee_lang,
                 ScoringContext* scoringcontext) {
  uint32_t langprob = MakeLangProb(whackee_lang, 1);
  // This logic avoids hr-Latn whacking sr-Cyrl, but still whacks sr-Latn
  if (IsLatnLanguage(whacker_lang) && IsLatnLanguage(whackee_lang)) {
    LangBoosts* langprior_whack = &scoringcontext->langprior_whack.latn;
    int n = langprior_whack->n;
    langprior_whack->langprob[n] = langprob;
    langprior_whack->n = langprior_whack->wrap(n + 1);
  }
  if (IsOthrLanguage(whacker_lang) && IsOthrLanguage(whackee_lang)) {
    LangBoosts* langprior_whack = &scoringcontext->langprior_whack.othr;
    int n = langprior_whack->n;
    langprior_whack->langprob[n] = langprob;
    langprior_whack->n = langprior_whack->wrap(n + 1);
 }
}

void AddCloseLangWhack(Language lang, ScoringContext* scoringcontext) {
  // We do not in general want zh-Hans and zh-Hant to be close pairs,
  // but we do here.
  if (lang == CLD2::CHINESE) {
    AddOneWhack(lang, CLD2::CHINESE_T, scoringcontext);
    return;
  }
  if (lang == CLD2::CHINESE_T) {
    AddOneWhack(lang, CLD2::CHINESE, scoringcontext);
    return;
  }

  int base_lang_set = LanguageCloseSet(lang);
  if (base_lang_set == 0) {return;}
  // TODO: add an explicit list of each set to avoid this 512-times loop
  for (int i = 0; i < kLanguageToPLangSize; ++i) {
    Language lang2 = static_cast<Language>(i);
    if ((base_lang_set == LanguageCloseSet(lang2)) && (lang != lang2)) {
      AddOneWhack(lang, lang2, scoringcontext);
    }
  }
}


void ApplyHints(const CLDHints* cld_hints,
                ScoringContext* scoringcontext) {
  CLDLangPriors lang_priors;
  InitCLDLangPriors(&lang_priors);

  if (cld_hints != NULL) {
    if ((cld_hints->content_language_hint != NULL) &&
        (cld_hints->content_language_hint[0] != '\0')) {
      SetCLDContentLangHint(cld_hints->content_language_hint, &lang_priors);
    }

    // Input is from GetTLD(), already lowercased
    if ((cld_hints->tld_hint != NULL) && (cld_hints->tld_hint[0] != '\0')) {
      SetCLDTLDHint(cld_hints->tld_hint, &lang_priors);
    }

    if (cld_hints->encoding_hint != UNKNOWN_ENCODING) {
      Encoding enc = static_cast<Encoding>(cld_hints->encoding_hint);
      SetCLDEncodingHint(enc, &lang_priors);
    }

    if (cld_hints->language_hint != UNKNOWN_LANGUAGE) {
      SetCLDLanguageHint(cld_hints->language_hint, &lang_priors);
    }
  }

  // Keep no more than four different languages with hints
  TrimCLDLangPriors(4, &lang_priors);

  // Put boosts into ScoringContext
  for (int i = 0; i < GetCLDLangPriorCount(&lang_priors); ++i) {
    Language lang = GetCLDPriorLang(lang_priors.prior[i]);
    int qprob = GetCLDPriorWeight(lang_priors.prior[i]);
    if (qprob > 0) {
      uint32_t langprob = MakeLangProb(lang, qprob);
      AddLangPriorBoost(lang, langprob, scoringcontext);
    }
  }

  // Put whacks into scoring context
  // We do not in general want zh-Hans and zh-Hant to be close pairs,
  // but we do here. Use close_set_count[kCloseSetSize] to count zh, zh-Hant
  std::vector<int> close_set_count(kCloseSetSize + 1, 0);

  for (int i = 0; i < GetCLDLangPriorCount(&lang_priors); ++i) {
    Language lang = GetCLDPriorLang(lang_priors.prior[i]);
    ++close_set_count[LanguageCloseSet(lang)];
    if (lang == CLD2::CHINESE) {++close_set_count[kCloseSetSize];}
    if (lang == CLD2::CHINESE_T) {++close_set_count[kCloseSetSize];}
  }

  // If a boost language is in a close set, force suppressing the others in
  // that set, if exactly one of the set is present
  for (int i = 0; i < GetCLDLangPriorCount(&lang_priors); ++i) {
    Language lang = GetCLDPriorLang(lang_priors.prior[i]);
    int qprob = GetCLDPriorWeight(lang_priors.prior[i]);
    if (qprob > 0) {
      int close_set = LanguageCloseSet(lang);
      if ((close_set > 0) && (close_set_count[close_set] == 1)) {
        AddCloseLangWhack(lang, scoringcontext);
      }
      if (((lang == CLD2::CHINESE) || (lang == CLD2::CHINESE_T)) &&
          (close_set_count[kCloseSetSize] == 1)) {
        AddCloseLangWhack(lang, scoringcontext);
      }
    }
  }
}


// Results language3/percent3/normalized_score3 must be exactly three items
Language DetectLanguageSummaryV2(
                        const char* buffer,
                        std::size_t buffer_length,
                        const CLDHints* cld_hints,
                        bool allow_extended_lang,
                        int flags,
                        Language plus_one,
                        Language* language3,
                        int* percent3,
                        double* normalized_score3,
                        bool* is_reliable) {
  language3[0] = UNKNOWN_LANGUAGE;
  language3[1] = UNKNOWN_LANGUAGE;
  language3[2] = UNKNOWN_LANGUAGE;
  percent3[0] = 0;
  percent3[1] = 0;
  percent3[2] = 0;
  normalized_score3[0] = 0.0;
  normalized_score3[1] = 0.0;
  normalized_score3[2] = 0.0;
  *is_reliable = false;

  // Exit now if no text
  if (buffer_length == 0) {return UNKNOWN_LANGUAGE;}
  if (kScoringtables.quadgram_obj == NULL) {return UNKNOWN_LANGUAGE;}

  // Document totals
  DocTote doc_tote;   // Reliability = 0..100

  // ScoringContext carries state across scriptspans
  ScoringContext scoringcontext;
  scoringcontext.flags_cld2_score_as_quads =
    ((flags & kCLDFlagScoreAsQuads) != 0);
  scoringcontext.prior_chunk_lang = UNKNOWN_LANGUAGE;
  scoringcontext.ulscript = ULScript_Common;
  scoringcontext.scoringtables = &kScoringtables;
  scoringcontext.scanner = NULL;
  scoringcontext.init();            // Clear the internal memory arrays

  ApplyHints(cld_hints, &scoringcontext);

  // Loop through text spans in a single script
  ScriptScanner ss(buffer, buffer_length);
  LangSpan scriptspan;

  scoringcontext.scanner = &ss;

  scriptspan.text = NULL;
  scriptspan.text_bytes = 0;
  scriptspan.offset = 0;
  scriptspan.ulscript = ULScript_Common;
  scriptspan.lang = UNKNOWN_LANGUAGE;

  int total_text_bytes = 0;
  int textlimit = FLAGS_cld_textlimit << 10;    // in KB
  if (textlimit == 0) {textlimit = 0x7fffffff;}

  // Pick up chunk sizes
  // Smoothwidth is units of quadgrams, about 2.5 chars (unigrams) each
  // Sanity check -- force into a reasonable range
  int chunksizequads = FLAGS_cld_smoothwidth;
  chunksizequads = minint(maxint(chunksizequads, kMinChunkSizeQuads),
                               kMaxChunkSizeQuads);

  // Allocate full-document prediction table for finding repeating words
  int hash = 0;
  int* predict_tbl = new int[kPredictionTableSize];
  if (FlagRepeats(flags)) {
    memset(predict_tbl, 0, kPredictionTableSize * sizeof(predict_tbl[0]));
  }



  // Loop through scriptspans accumulating number of text bytes in each language
  while (ss.GetOneScriptSpanLower(&scriptspan)) {
    // Squeeze out big chunks of text span if asked to
    if (FlagSqueeze(flags)) {
      // Remove repetitive or mostly-spaces chunks
      int newlen;
      int chunksize = 0;    // Use the default
      newlen = CheapSqueezeInplace(scriptspan.text, scriptspan.text_bytes,
                                   chunksize);
      scriptspan.text_bytes = newlen;
    } else {
      // Check now and then to see if we should be squeezing
      if (((kCheapSqueezeTestThresh >> 1) < scriptspan.text_bytes) &&
          !FlagFinish(flags)) {

        if (CheapSqueezeTriggerTest(scriptspan.text,
                                      scriptspan.text_bytes,
                                      kCheapSqueezeTestLen)) {
          // Deallocate full-document prediction table
          delete[] predict_tbl;

          return DetectLanguageSummaryV2(
                            buffer,
                            buffer_length,
                            cld_hints,
                            allow_extended_lang,
                            flags | kCLDFlagSqueeze,
                            plus_one,
                            language3,
                            percent3,
                            normalized_score3,
                            is_reliable);
        }
      }
    }

    // Remove repetitive words if asked to
    if (FlagRepeats(flags)) {
      // Remove repetitive words
      int newlen;
      newlen = CheapRepWordsInplace(scriptspan.text, scriptspan.text_bytes,
                                    &hash, predict_tbl);
      scriptspan.text_bytes = newlen;
    }

    // Scoring depends on scriptspan buffer ALWAYS having
    // leading space and off-the-end space space space NUL,
    // DCHECK(scriptspan.text[0] == ' ');
    // DCHECK(scriptspan.text[scriptspan.text_bytes + 0] == ' ');
    // DCHECK(scriptspan.text[scriptspan.text_bytes + 1] == ' ');
    // DCHECK(scriptspan.text[scriptspan.text_bytes + 2] == ' ');
    // DCHECK(scriptspan.text[scriptspan.text_bytes + 3] == '\0');

    // The real scoring
    // Accumulate directly into the document total, or accmulate in one of four
    // chunk totals. The purpose of the multiple chunk totals is to piece
    // together short choppy pieces of text in alternating scripts. One total is
    // dedicated to Latin text, one to Han text, and the other two are dynamicly
    // assigned.

    scoringcontext.ulscript = scriptspan.ulscript;
    // FLAGS_cld2_html = scoringcontext.flags_cld2_html;

    ScoreOneScriptSpan(scriptspan,
                       &scoringcontext,
                       &doc_tote);

    total_text_bytes += scriptspan.text_bytes;
  }     // End while (ss.GetOneScriptSpanLower())

  // Deallocate full-document prediction table
  delete[] predict_tbl;

  // Force close pairs to one or the other
  // If given, also update resultchunkvector
  RefineScoredClosePairs(&doc_tote);

  // Calculate return results
  // Find top three byte counts in tote heap
  int reliable_percent3[3];

  // Cannot use Add, etc. after sorting
  doc_tote.Sort(3);

  int dummy_text_bytes;
  ExtractLangEtc(&doc_tote, total_text_bytes,
                 reliable_percent3, language3, percent3, normalized_score3,
                 &dummy_text_bytes, is_reliable);

  bool have_good_answer = false;
  if (FlagFinish(flags)) {
    // Force a result
    have_good_answer = true;
  } else if (total_text_bytes <= kShortTextThresh) {
    // Don't recurse on short text -- we already did word scores
    have_good_answer = true;
  } else if (*is_reliable &&
             (percent3[0] >= kGoodLang1Percent)) {
    have_good_answer = true;
  } else if (*is_reliable &&
             ((percent3[0] + percent3[1]) >= kGoodLang1and2Percent)) {
    have_good_answer = true;
  }


  if (have_good_answer) {
    // This is the real, non-recursive return

    // Move bytes for unreliable langs to another lang or UNKNOWN
    if (!FlagBestEffort(flags)) {
      RemoveUnreliableLanguages(&doc_tote);
    }

    // Redo the result extraction after the removal above
    doc_tote.Sort(3);
    ExtractLangEtc(&doc_tote, total_text_bytes,
                   reliable_percent3, language3, percent3, normalized_score3,
                   &dummy_text_bytes, is_reliable);

    Language summary_lang;
    CalcSummaryLang(total_text_bytes,
                    language3, percent3,
                    &summary_lang, is_reliable, flags);

    return summary_lang;
  }

  // For restriction to Top40 + one, the one is 1st/2nd lang that is not Top40
  // For this purpose, we treate "Ignore" as top40
  Language new_plus_one = UNKNOWN_LANGUAGE;

  if (total_text_bytes < kShortTextThresh) {
      // Short text: Recursive call with top40 and short set
      return DetectLanguageSummaryV2(
                        buffer,
                        buffer_length,
                        cld_hints,
                        allow_extended_lang,
                        flags | kCLDFlagTop40 | kCLDFlagRepeats |
                          kCLDFlagShort | kCLDFlagUseWords | kCLDFlagFinish,
                        new_plus_one,
                        language3,
                        percent3,
                        normalized_score3,
                        is_reliable);
  }

  return DetectLanguageSummaryV2(
                        buffer,
                        buffer_length,
                        cld_hints,
                        allow_extended_lang,
                        flags | kCLDFlagTop40 | kCLDFlagRepeats |
                          kCLDFlagFinish,
                        new_plus_one,
                        language3,
                        percent3,
                        normalized_score3,
                        is_reliable);
}

}       // End namespace CLD2
