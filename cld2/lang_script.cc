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
// File: lang_script.cc
// ================
//
// Author: dsites@google.com (Dick Sites)
//
// This file declares language and script numbers and names for CLD2
//

#include "lang_script.h"

#include <stdlib.h>
#include <string.h>

namespace CLD2 {

//
// File: lang_script.h
// ================
//
// Author: dsites@google.com (Dick Sites)
//
// This file declares language and script numbers and names for CLD2
//

// NOTE: These tables are defined in generated_language.cc and
// generated_ulscript.cc.  They are not declared in their respective
// .h files because only this file needs to know about them.

// Language tables
// Subscripted by enum Language
extern const int kLanguageToNameSize;
extern const char* const kLanguageToName[];
extern const int kLanguageToCodeSize;
extern const char* const kLanguageToCode[];

// Subscripted by Language
extern const int kLanguageToPLangSize;
extern const uint8_t kLanguageToPLang[];
// Subscripted by per-script language
extern const uint16_t kPLangToLanguageLatn[];
extern const uint16_t kPLangToLanguageOthr[];

// ULScript tables
// Subscripted by enum ULScript
extern const int kULScriptToRtypeSize;
extern const ULScriptRType kULScriptToRtype[];
extern const int kULScriptToDefaultLangSize;
extern const Language kULScriptToDefaultLang[];

// NOTE: The script numbers and language numbers here are not guaranteed to be
// stable. If you want to record a result for posterity, save the ISO codes
// as character strings.
//
//
// The Unicode scripts recognized by CLD2 are numbered almost arbitrarily,
// specified in an enum. Each script has a recognition type
//  r_type: 0 script-only, 1 nilgrams, 2 quadgrams, 3 CJK
//
// The declarations for a particular version of Unicode are machine-generated in
//   cld2_generated_scripts.h
//
// This file includes that one and declares the access routines. The type
// involved is called "ULScript" to signify Unicode Letters-Marks Scripts,
// which are not quite Unicode Scripts. In particular, the CJK scripts are
// merged into a single number because CLD2 recognizes the CJK languages from
// four scripts intermixed: Hani (both Hans  and Hant), Hangul, Hiragana, and
// Katakana.

// Each script has one of these four recognition types.
// RTypeNone: There is no language associated with this script. In extended
//  language recognition calls, return a fake language number that maps to
//  xx-Cham, with literally "xx" for the language code,and with the script
//  code instead of "Cham". In non-extended calls, return UNKNOWN_LANGUAGE.
// RTypeOne: The script maps 1:1 to a single language. No letters are examined
//  during recognition and no lookups done.
// RTypeMany: The usual quadgram + delta-octagram + distinctive-words scoring
//  is done to determine the languages involved.
// RTypeCJK: The CJK unigram + delta-bigram scoring is done to determine the
//  languages involved.
//
// Note that the choice of recognition type is a function of script, not
// language. In particular, some languges are recognized in multiple scripts
// and those have different recognition types (Mongolian mn-Latn vs. mn-Mong
// for example).

//----------------------------------------------------------------------------//
// Functions of ULScript                                                      //
//----------------------------------------------------------------------------//

// If the input is out of range or otherwise unrecognized, it is treated
// as UNKNOWN_ULSCRIPT (which never participates in language recognition)

ULScriptRType ULScriptRecognitionType(ULScript ulscript) {
  int i_ulscript = ulscript;
  if (i_ulscript < 0) {i_ulscript = UNKNOWN_ULSCRIPT;}
  if (i_ulscript >= NUM_ULSCRIPTS) {i_ulscript = UNKNOWN_ULSCRIPT;}
  return kULScriptToRtype[i_ulscript];
}


// The languages recognized by CLD2 are numbered almost arbitrarily,
// specified in an enum. Each language has human-readable language
// name and a 2- or 3-letter ISO 639 language code.  Each has a list
// of up to four scripts in which it is currently recognized.
//
// The declarations for a particular set of recognized languages are
// machine-generated in
//   cld2_generated_languages.h
//
// The Language enum is intended to match the internal Google Language enum
// in i18n/languages/proto/languages.proto up to NUM_LANGUAGES, with additional
// languages assigned above that. Over time, some languages may be renumbered
// if they are moved into the Language enum.
//
// The Language enum includes the fake language numbers for RTypeNone above.
//
// In an open-source environment, the Google-specific Language enum is not
// available. Language decouples the two environments while maintaining
// internal compatibility.


// If the input is out of range or otherwise unrecognized, it is treated
// as UNKNOWN_LANGUAGE
//
// LanguageCode
// ------------
// Given the Language, return the language code, e.g. "ko"
// This is determined by
// the following (in order of preference):
// - ISO-639-1 two-letter language code
//   (all except those mentioned below)
// - ISO-639-2 three-letter bibliographic language code
//   (Tibetan, Dhivehi, Cherokee, Syriac)
// - Google-specific language code
//   (ChineseT ("zh-TW"), Teragram Unknown, Unknown,
//   Portuguese-Portugal, Portuguese-Brazil, Limbu)
// - Fake RTypeNone names.

//----------------------------------------------------------------------------//
// Functions of Language                                                      //
//----------------------------------------------------------------------------//

const char* LanguageName(Language lang) {
  int i_lang = lang;
  if (i_lang < 0) {i_lang = UNKNOWN_LANGUAGE;}
  if (i_lang >= NUM_LANGUAGES) {i_lang = UNKNOWN_LANGUAGE;}
  return kLanguageToName[i_lang];
}
const char* LanguageCode(Language lang) {
  int i_lang = lang;
  if (i_lang < 0) {i_lang = UNKNOWN_LANGUAGE;}
  if (i_lang >= NUM_LANGUAGES) {i_lang = UNKNOWN_LANGUAGE;}
  return kLanguageToCode[i_lang];
}

extern const int kCloseSetSize = 10;

// Returns which set of statistically-close languages lang is in. 0 means none.
int LanguageCloseSet(Language lang) {
  // Scaffolding
  // id ms         # INDONESIAN MALAY coef=0.4698    Problematic w/o extra words
  // bo dz         # TIBETAN DZONGKHA coef=0.4571
  // cs sk         # CZECH SLOVAK coef=0.4273
  // zu xh         # ZULU XHOSA coef=0.3716
  //
  // bs hr sr srm  # BOSNIAN CROATIAN SERBIAN MONTENEGRIN
  // hi mr bh ne   # HINDI MARATHI BIHARI NEPALI
  // no nn da      # NORWEGIAN NORWEGIAN_N DANISH
  // gl es pt      # GALICIAN SPANISH PORTUGUESE
  // rw rn         # KINYARWANDA RUNDI

  if (lang == INDONESIAN) {return 1;}
  if (lang == MALAY) {return 1;}

  if (lang == TIBETAN) {return 2;}
  if (lang == DZONGKHA) {return 2;}

  if (lang == CZECH) {return 3;}
  if (lang == SLOVAK) {return 3;}

  if (lang == ZULU) {return 4;}
  if (lang == XHOSA) {return 4;}

  if (lang == BOSNIAN) {return 5;}
  if (lang == CROATIAN) {return 5;}
  if (lang == SERBIAN) {return 5;}
  if (lang == MONTENEGRIN) {return 5;}

  if (lang == HINDI) {return 6;}
  if (lang == MARATHI) {return 6;}
  if (lang == BIHARI) {return 6;}
  if (lang == NEPALI) {return 6;}

  if (lang == NORWEGIAN) {return 7;}
  if (lang == NORWEGIAN_N) {return 7;}
  if (lang == DANISH) {return 7;}

  if (lang == GALICIAN) {return 8;}
  if (lang == SPANISH) {return 8;}
  if (lang == PORTUGUESE) {return 8;}

  if (lang == KINYARWANDA) {return 9;}
  if (lang == RUNDI) {return 9;}

  return 0;
}

//----------------------------------------------------------------------------//
// Functions of ULScript and Language                                         //
//----------------------------------------------------------------------------//

Language DefaultLanguage(ULScript ulscript) {
  if (ulscript < 0) {return UNKNOWN_LANGUAGE;}
  if (ulscript >= NUM_ULSCRIPTS) {return UNKNOWN_LANGUAGE;}
  return kULScriptToDefaultLang[ulscript];
}

uint8_t PerScriptNumber(ULScript ulscript, Language lang) {
  if (ulscript < 0) {return 0;}
  if (ulscript >= NUM_ULSCRIPTS) {return 0;}
  if (kULScriptToRtype[ulscript] == RTypeNone) {return 1;}
  if (lang >= kLanguageToPLangSize) {return 0;}
  return kLanguageToPLang[lang];
}

Language FromPerScriptNumber(ULScript ulscript, uint8_t perscript_number) {
  if (ulscript < 0) {return UNKNOWN_LANGUAGE;}
  if (ulscript >= NUM_ULSCRIPTS) {return UNKNOWN_LANGUAGE;}
  if ((kULScriptToRtype[ulscript] == RTypeNone) ||
      (kULScriptToRtype[ulscript] == RTypeOne)) {
    return kULScriptToDefaultLang[ulscript];
  }

  if (ulscript == ULScript_Latin) {
     return static_cast<Language>(kPLangToLanguageLatn[perscript_number]);
  } else {
     return static_cast<Language>(kPLangToLanguageOthr[perscript_number]);
  }
}

// Return true if language can be in the Latin script
bool IsLatnLanguage(Language lang) {
  if (lang >= kLanguageToPLangSize) {return false;}
  return (lang == kPLangToLanguageLatn[kLanguageToPLang[lang]]);
}

// Return true if language can be in a non-Latin script
bool IsOthrLanguage(Language lang) {
  if (lang >= kLanguageToPLangSize) {return false;}
  return (lang == kPLangToLanguageOthr[kLanguageToPLang[lang]]);
}

// Map script into Latin, Cyrillic, Arabic, Other
int LScript4(ULScript ulscript) {
  if (ulscript == ULScript_Latin) {return 0;}
  if (ulscript == ULScript_Cyrillic) {return 1;}
  if (ulscript == ULScript_Arabic) {return 2;}
  return 3;
}

}  // namespace CLD2
