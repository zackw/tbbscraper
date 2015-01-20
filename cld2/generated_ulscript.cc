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

// generated_ulscript.cc
// Machine generated. Do Not Edit.
//
// Declarations for scripts recognized by CLD2
//

#include "generated_ulscript.h"
#include "generated_language.h"

namespace CLD2 {

// Subscripted by enum ULScript
static const int kULScriptToRtypeSize = 102;
extern const ULScriptRType kULScriptToRtype[kULScriptToRtypeSize] = {
  RTypeNone,   // 0 Zyyy
  RTypeMany,   // 1 Latn
  RTypeOne,    // 2 Grek
  RTypeMany,   // 3 Cyrl
  RTypeOne,    // 4 Armn
  RTypeMany,   // 5 Hebr
  RTypeMany,   // 6 Arab
  RTypeOne,    // 7 Syrc
  RTypeOne,    // 8 Thaa
  RTypeMany,   // 9 Deva
  RTypeMany,   // 10 Beng
  RTypeOne,    // 11 Guru
  RTypeOne,    // 12 Gujr
  RTypeOne,    // 13 Orya
  RTypeOne,    // 14 Taml
  RTypeOne,    // 15 Telu
  RTypeOne,    // 16 Knda
  RTypeOne,    // 17 Mlym
  RTypeOne,    // 18 Sinh
  RTypeOne,    // 19 Thai
  RTypeOne,    // 20 Laoo
  RTypeMany,   // 21 Tibt
  RTypeOne,    // 22 Mymr
  RTypeOne,    // 23 Geor
  RTypeCJK,    // 24 Hani
  RTypeMany,   // 25 Ethi
  RTypeOne,    // 26 Cher
  RTypeOne,    // 27 Cans
  RTypeNone,   // 28 Ogam
  RTypeNone,   // 29 Runr
  RTypeOne,    // 30 Khmr
  RTypeOne,    // 31 Mong
  RTypeNone,   // 32
  RTypeNone,   // 33
  RTypeNone,   // 34 Bopo
  RTypeNone,   // 35
  RTypeNone,   // 36 Yiii
  RTypeNone,   // 37 Ital
  RTypeNone,   // 38 Goth
  RTypeNone,   // 39 Dsrt
  RTypeNone,   // 40 Zinh
  RTypeOne,    // 41 Tglg
  RTypeNone,   // 42 Hano
  RTypeNone,   // 43 Buhd
  RTypeNone,   // 44 Tagb
  RTypeOne,    // 45 Limb
  RTypeNone,   // 46 Tale
  RTypeNone,   // 47 Linb
  RTypeNone,   // 48 Ugar
  RTypeNone,   // 49 Shaw
  RTypeNone,   // 50 Osma
  RTypeNone,   // 51 Cprt
  RTypeNone,   // 52 Brai
  RTypeNone,   // 53 Bugi
  RTypeNone,   // 54 Copt
  RTypeNone,   // 55 Talu
  RTypeNone,   // 56 Glag
  RTypeNone,   // 57 Tfng
  RTypeNone,   // 58 Sylo
  RTypeNone,   // 59 Xpeo
  RTypeNone,   // 60 Khar
  RTypeNone,   // 61 Bali
  RTypeNone,   // 62 Xsux
  RTypeNone,   // 63 Phnx
  RTypeNone,   // 64 Phag
  RTypeNone,   // 65 Nkoo
  RTypeNone,   // 66 Sund
  RTypeNone,   // 67 Lepc
  RTypeNone,   // 68 Olck
  RTypeNone,   // 69 Vaii
  RTypeNone,   // 70 Saur
  RTypeNone,   // 71 Kali
  RTypeNone,   // 72 Rjng
  RTypeNone,   // 73 Lyci
  RTypeNone,   // 74 Cari
  RTypeNone,   // 75 Lydi
  RTypeNone,   // 76 Cham
  RTypeNone,   // 77 Lana
  RTypeNone,   // 78 Tavt
  RTypeNone,   // 79 Avst
  RTypeNone,   // 80 Egyp
  RTypeNone,   // 81 Samr
  RTypeNone,   // 82 Lisu
  RTypeNone,   // 83 Bamu
  RTypeNone,   // 84 Java
  RTypeNone,   // 85 Mtei
  RTypeNone,   // 86 Armi
  RTypeNone,   // 87 Sarb
  RTypeNone,   // 88 Prti
  RTypeNone,   // 89 Phli
  RTypeNone,   // 90 Orkh
  RTypeNone,   // 91 Kthi
  RTypeNone,   // 92 Batk
  RTypeNone,   // 93 Brah
  RTypeNone,   // 94 Mand
  RTypeNone,   // 95 Cakm
  RTypeNone,   // 96 Merc
  RTypeNone,   // 97 Mero
  RTypeNone,   // 98 Plrd
  RTypeNone,   // 99 Shrd
  RTypeNone,   // 100 Sora
  RTypeNone,   // 101 Takr
};

// Subscripted by enum ULScript
static const int kULScriptToDefaultLangSize = 102;
extern const Language kULScriptToDefaultLang[kULScriptToDefaultLangSize] = {
  X_Common,              // 0 Zyyy RTypeNone
  ENGLISH,               // 1 Latn RTypeMany
  GREEK,                 // 2 Grek RTypeOne
  RUSSIAN,               // 3 Cyrl RTypeMany
  ARMENIAN,              // 4 Armn RTypeOne
  HEBREW,                // 5 Hebr RTypeMany
  ARABIC,                // 6 Arab RTypeMany
  SYRIAC,                // 7 Syrc RTypeOne
  DHIVEHI,               // 8 Thaa RTypeOne
  HINDI,                 // 9 Deva RTypeMany
  BENGALI,               // 10 Beng RTypeMany
  PUNJABI,               // 11 Guru RTypeOne
  GUJARATI,              // 12 Gujr RTypeOne
  ORIYA,                 // 13 Orya RTypeOne
  TAMIL,                 // 14 Taml RTypeOne
  TELUGU,                // 15 Telu RTypeOne
  KANNADA,               // 16 Knda RTypeOne
  MALAYALAM,             // 17 Mlym RTypeOne
  SINHALESE,             // 18 Sinh RTypeOne
  THAI,                  // 19 Thai RTypeOne
  LAOTHIAN,              // 20 Laoo RTypeOne
  TIBETAN,               // 21 Tibt RTypeMany
  BURMESE,               // 22 Mymr RTypeOne
  GEORGIAN,              // 23 Geor RTypeOne
  JAPANESE,              // 24 Hani RTypeCJK
  AMHARIC,               // 25 Ethi RTypeMany
  CHEROKEE,              // 26 Cher RTypeOne
  INUKTITUT,             // 27 Cans RTypeOne
  X_Ogham,               // 28 Ogam RTypeNone
  X_Runic,               // 29 Runr RTypeNone
  KHMER,                 // 30 Khmr RTypeOne
  MONGOLIAN,             // 31 Mong RTypeOne
  UNKNOWN_LANGUAGE,      // 32  RTypeNone
  UNKNOWN_LANGUAGE,      // 33  RTypeNone
  X_Bopomofo,            // 34 Bopo RTypeNone
  UNKNOWN_LANGUAGE,      // 35  RTypeNone
  X_Yi,                  // 36 Yiii RTypeNone
  X_Old_Italic,          // 37 Ital RTypeNone
  X_Gothic,              // 38 Goth RTypeNone
  X_Deseret,             // 39 Dsrt RTypeNone
  X_Inherited,           // 40 Zinh RTypeNone
  TAGALOG,               // 41 Tglg RTypeOne
  X_Hanunoo,             // 42 Hano RTypeNone
  X_Buhid,               // 43 Buhd RTypeNone
  X_Tagbanwa,            // 44 Tagb RTypeNone
  LIMBU,                 // 45 Limb RTypeOne
  X_Tai_Le,              // 46 Tale RTypeNone
  X_Linear_B,            // 47 Linb RTypeNone
  X_Ugaritic,            // 48 Ugar RTypeNone
  X_Shavian,             // 49 Shaw RTypeNone
  X_Osmanya,             // 50 Osma RTypeNone
  X_Cypriot,             // 51 Cprt RTypeNone
  X_Braille,             // 52 Brai RTypeNone
  X_Buginese,            // 53 Bugi RTypeNone
  X_Coptic,              // 54 Copt RTypeNone
  X_New_Tai_Lue,         // 55 Talu RTypeNone
  X_Glagolitic,          // 56 Glag RTypeNone
  X_Tifinagh,            // 57 Tfng RTypeNone
  X_Syloti_Nagri,        // 58 Sylo RTypeNone
  X_Old_Persian,         // 59 Xpeo RTypeNone
  X_Kharoshthi,          // 60 Khar RTypeNone
  X_Balinese,            // 61 Bali RTypeNone
  X_Cuneiform,           // 62 Xsux RTypeNone
  X_Phoenician,          // 63 Phnx RTypeNone
  X_Phags_Pa,            // 64 Phag RTypeNone
  X_Nko,                 // 65 Nkoo RTypeNone
  X_Sundanese,           // 66 Sund RTypeNone
  X_Lepcha,              // 67 Lepc RTypeNone
  X_Ol_Chiki,            // 68 Olck RTypeNone
  X_Vai,                 // 69 Vaii RTypeNone
  X_Saurashtra,          // 70 Saur RTypeNone
  X_Kayah_Li,            // 71 Kali RTypeNone
  X_Rejang,              // 72 Rjng RTypeNone
  X_Lycian,              // 73 Lyci RTypeNone
  X_Carian,              // 74 Cari RTypeNone
  X_Lydian,              // 75 Lydi RTypeNone
  X_Cham,                // 76 Cham RTypeNone
  X_Tai_Tham,            // 77 Lana RTypeNone
  X_Tai_Viet,            // 78 Tavt RTypeNone
  X_Avestan,             // 79 Avst RTypeNone
  X_Egyptian_Hieroglyphs,  // 80 Egyp RTypeNone
  X_Samaritan,           // 81 Samr RTypeNone
  X_Lisu,                // 82 Lisu RTypeNone
  X_Bamum,               // 83 Bamu RTypeNone
  X_Javanese,            // 84 Java RTypeNone
  X_Meetei_Mayek,        // 85 Mtei RTypeNone
  X_Imperial_Aramaic,    // 86 Armi RTypeNone
  X_Old_South_Arabian,   // 87 Sarb RTypeNone
  X_Inscriptional_Parthian,  // 88 Prti RTypeNone
  X_Inscriptional_Pahlavi,  // 89 Phli RTypeNone
  X_Old_Turkic,          // 90 Orkh RTypeNone
  X_Kaithi,              // 91 Kthi RTypeNone
  X_Batak,               // 92 Batk RTypeNone
  X_Brahmi,              // 93 Brah RTypeNone
  X_Mandaic,             // 94 Mand RTypeNone
  X_Chakma,              // 95 Cakm RTypeNone
  X_Meroitic_Cursive,    // 96 Merc RTypeNone
  X_Meroitic_Hieroglyphs,  // 97 Mero RTypeNone
  X_Miao,                // 98 Plrd RTypeNone
  X_Sharada,             // 99 Shrd RTypeNone
  X_Sora_Sompeng,        // 100 Sora RTypeNone
  X_Takri,               // 101 Takr RTypeNone
};

}  // namespace CLD2
