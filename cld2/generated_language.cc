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

// generated_language.cc
// Machine generated. Do Not Edit.
//
// Declarations for languages recognized by CLD2
//

#include "generated_language.h"
#include "generated_ulscript.h"

namespace CLD2 {

// Subscripted by enum Language
static const int kLanguageToNameSize = 614;
extern const char* const kLanguageToName[kLanguageToNameSize] = {
  "ENGLISH",               // 0 en
  "DANISH",                // 1 da
  "DUTCH",                 // 2 nl
  "FINNISH",               // 3 fi
  "FRENCH",                // 4 fr
  "GERMAN",                // 5 de
  "HEBREW",                // 6 iw
  "ITALIAN",               // 7 it
  "Japanese",              // 8 ja
  "Korean",                // 9 ko
  "NORWEGIAN",             // 10 no
  "POLISH",                // 11 pl
  "PORTUGUESE",            // 12 pt
  "RUSSIAN",               // 13 ru
  "SPANISH",               // 14 es
  "SWEDISH",               // 15 sv
  "Chinese",               // 16 zh
  "CZECH",                 // 17 cs
  "GREEK",                 // 18 el
  "ICELANDIC",             // 19 is
  "LATVIAN",               // 20 lv
  "LITHUANIAN",            // 21 lt
  "ROMANIAN",              // 22 ro
  "HUNGARIAN",             // 23 hu
  "ESTONIAN",              // 24 et
  "Ignore",                // 25 xxx
  "Unknown",               // 26 un
  "BULGARIAN",             // 27 bg
  "CROATIAN",              // 28 hr
  "SERBIAN",               // 29 sr
  "IRISH",                 // 30 ga
  "GALICIAN",              // 31 gl
  "TAGALOG",               // 32 tl
  "TURKISH",               // 33 tr
  "UKRAINIAN",             // 34 uk
  "HINDI",                 // 35 hi
  "MACEDONIAN",            // 36 mk
  "BENGALI",               // 37 bn
  "INDONESIAN",            // 38 id
  "LATIN",                 // 39 la
  "MALAY",                 // 40 ms
  "MALAYALAM",             // 41 ml
  "WELSH",                 // 42 cy
  "NEPALI",                // 43 ne
  "TELUGU",                // 44 te
  "ALBANIAN",              // 45 sq
  "TAMIL",                 // 46 ta
  "BELARUSIAN",            // 47 be
  "JAVANESE",              // 48 jw
  "OCCITAN",               // 49 oc
  "URDU",                  // 50 ur
  "BIHARI",                // 51 bh
  "GUJARATI",              // 52 gu
  "THAI",                  // 53 th
  "ARABIC",                // 54 ar
  "CATALAN",               // 55 ca
  "ESPERANTO",             // 56 eo
  "BASQUE",                // 57 eu
  "INTERLINGUA",           // 58 ia
  "KANNADA",               // 59 kn
  "PUNJABI",               // 60 pa
  "SCOTS_GAELIC",          // 61 gd
  "SWAHILI",               // 62 sw
  "SLOVENIAN",             // 63 sl
  "MARATHI",               // 64 mr
  "MALTESE",               // 65 mt
  "VIETNAMESE",            // 66 vi
  "FRISIAN",               // 67 fy
  "SLOVAK",                // 68 sk
  "ChineseT",              // 69 zh-Hant
  "FAROESE",               // 70 fo
  "SUNDANESE",             // 71 su
  "UZBEK",                 // 72 uz
  "AMHARIC",               // 73 am
  "AZERBAIJANI",           // 74 az
  "GEORGIAN",              // 75 ka
  "TIGRINYA",              // 76 ti
  "PERSIAN",               // 77 fa
  "BOSNIAN",               // 78 bs
  "SINHALESE",             // 79 si
  "NORWEGIAN_N",           // 80 nn
  "81",                    // 81
  "82",                    // 82
  "XHOSA",                 // 83 xh
  "ZULU",                  // 84 zu
  "GUARANI",               // 85 gn
  "SESOTHO",               // 86 st
  "TURKMEN",               // 87 tk
  "KYRGYZ",                // 88 ky
  "BRETON",                // 89 br
  "TWI",                   // 90 tw
  "YIDDISH",               // 91 yi
  "92",                    // 92
  "SOMALI",                // 93 so
  "UIGHUR",                // 94 ug
  "KURDISH",               // 95 ku
  "MONGOLIAN",             // 96 mn
  "ARMENIAN",              // 97 hy
  "LAOTHIAN",              // 98 lo
  "SINDHI",                // 99 sd
  "RHAETO_ROMANCE",        // 100 rm
  "AFRIKAANS",             // 101 af
  "LUXEMBOURGISH",         // 102 lb
  "BURMESE",               // 103 my
  "KHMER",                 // 104 km
  "TIBETAN",               // 105 bo
  "DHIVEHI",               // 106 dv
  "CHEROKEE",              // 107 chr
  "SYRIAC",                // 108 syr
  "LIMBU",                 // 109 lif
  "ORIYA",                 // 110 or
  "ASSAMESE",              // 111 as
  "CORSICAN",              // 112 co
  "INTERLINGUE",           // 113 ie
  "KAZAKH",                // 114 kk
  "LINGALA",               // 115 ln
  "116",                   // 116
  "PASHTO",                // 117 ps
  "QUECHUA",               // 118 qu
  "SHONA",                 // 119 sn
  "TAJIK",                 // 120 tg
  "TATAR",                 // 121 tt
  "TONGA",                 // 122 to
  "YORUBA",                // 123 yo
  "124",                   // 124
  "125",                   // 125
  "126",                   // 126
  "127",                   // 127
  "MAORI",                 // 128 mi
  "WOLOF",                 // 129 wo
  "ABKHAZIAN",             // 130 ab
  "AFAR",                  // 131 aa
  "AYMARA",                // 132 ay
  "BASHKIR",               // 133 ba
  "BISLAMA",               // 134 bi
  "DZONGKHA",              // 135 dz
  "FIJIAN",                // 136 fj
  "GREENLANDIC",           // 137 kl
  "HAUSA",                 // 138 ha
  "HAITIAN_CREOLE",        // 139 ht
  "INUPIAK",               // 140 ik
  "INUKTITUT",             // 141 iu
  "KASHMIRI",              // 142 ks
  "KINYARWANDA",           // 143 rw
  "MALAGASY",              // 144 mg
  "NAURU",                 // 145 na
  "OROMO",                 // 146 om
  "RUNDI",                 // 147 rn
  "SAMOAN",                // 148 sm
  "SANGO",                 // 149 sg
  "SANSKRIT",              // 150 sa
  "SISWANT",               // 151 ss
  "TSONGA",                // 152 ts
  "TSWANA",                // 153 tn
  "VOLAPUK",               // 154 vo
  "ZHUANG",                // 155 za
  "KHASI",                 // 156 kha
  "SCOTS",                 // 157 sco
  "GANDA",                 // 158 lg
  "MANX",                  // 159 gv
  "MONTENEGRIN",           // 160 sr-ME
  "AKAN",                  // 161 ak
  "IGBO",                  // 162 ig
  "MAURITIAN_CREOLE",      // 163 mfe
  "HAWAIIAN",              // 164 haw
  "CEBUANO",               // 165 ceb
  "EWE",                   // 166 ee
  "GA",                    // 167 gaa
  "HMONG",                 // 168 hmn
  "KRIO",                  // 169 kri
  "LOZI",                  // 170 loz
  "LUBA_LULUA",            // 171 lua
  "LUO_KENYA_AND_TANZANIA",  // 172 luo
  "NEWARI",                // 173 new
  "NYANJA",                // 174 ny
  "OSSETIAN",              // 175 os
  "PAMPANGA",              // 176 pam
  "PEDI",                  // 177 nso
  "RAJASTHANI",            // 178 raj
  "SESELWA",               // 179 crs
  "TUMBUKA",               // 180 tum
  "VENDA",                 // 181 ve
  "WARAY_PHILIPPINES",     // 182 war
  "183",                   // 183
  "184",                   // 184
  "185",                   // 185
  "186",                   // 186
  "187",                   // 187
  "188",                   // 188
  "189",                   // 189
  "190",                   // 190
  "191",                   // 191
  "192",                   // 192
  "193",                   // 193
  "194",                   // 194
  "195",                   // 195
  "196",                   // 196
  "197",                   // 197
  "198",                   // 198
  "199",                   // 199
  "200",                   // 200
  "201",                   // 201
  "202",                   // 202
  "203",                   // 203
  "204",                   // 204
  "205",                   // 205
  "206",                   // 206
  "207",                   // 207
  "208",                   // 208
  "209",                   // 209
  "210",                   // 210
  "211",                   // 211
  "212",                   // 212
  "213",                   // 213
  "214",                   // 214
  "215",                   // 215
  "216",                   // 216
  "217",                   // 217
  "218",                   // 218
  "219",                   // 219
  "220",                   // 220
  "221",                   // 221
  "222",                   // 222
  "223",                   // 223
  "224",                   // 224
  "225",                   // 225
  "226",                   // 226
  "227",                   // 227
  "228",                   // 228
  "229",                   // 229
  "230",                   // 230
  "231",                   // 231
  "232",                   // 232
  "233",                   // 233
  "234",                   // 234
  "235",                   // 235
  "236",                   // 236
  "237",                   // 237
  "238",                   // 238
  "239",                   // 239
  "240",                   // 240
  "241",                   // 241
  "242",                   // 242
  "243",                   // 243
  "244",                   // 244
  "245",                   // 245
  "246",                   // 246
  "247",                   // 247
  "248",                   // 248
  "249",                   // 249
  "250",                   // 250
  "251",                   // 251
  "252",                   // 252
  "253",                   // 253
  "254",                   // 254
  "255",                   // 255
  "256",                   // 256
  "257",                   // 257
  "258",                   // 258
  "259",                   // 259
  "260",                   // 260
  "261",                   // 261
  "262",                   // 262
  "263",                   // 263
  "264",                   // 264
  "265",                   // 265
  "266",                   // 266
  "267",                   // 267
  "268",                   // 268
  "269",                   // 269
  "270",                   // 270
  "271",                   // 271
  "272",                   // 272
  "273",                   // 273
  "274",                   // 274
  "275",                   // 275
  "276",                   // 276
  "277",                   // 277
  "278",                   // 278
  "279",                   // 279
  "280",                   // 280
  "281",                   // 281
  "282",                   // 282
  "283",                   // 283
  "284",                   // 284
  "285",                   // 285
  "286",                   // 286
  "287",                   // 287
  "288",                   // 288
  "289",                   // 289
  "290",                   // 290
  "291",                   // 291
  "292",                   // 292
  "293",                   // 293
  "294",                   // 294
  "295",                   // 295
  "296",                   // 296
  "297",                   // 297
  "298",                   // 298
  "299",                   // 299
  "300",                   // 300
  "301",                   // 301
  "302",                   // 302
  "303",                   // 303
  "304",                   // 304
  "305",                   // 305
  "306",                   // 306
  "307",                   // 307
  "308",                   // 308
  "309",                   // 309
  "310",                   // 310
  "311",                   // 311
  "312",                   // 312
  "313",                   // 313
  "314",                   // 314
  "315",                   // 315
  "316",                   // 316
  "317",                   // 317
  "318",                   // 318
  "319",                   // 319
  "320",                   // 320
  "321",                   // 321
  "322",                   // 322
  "323",                   // 323
  "324",                   // 324
  "325",                   // 325
  "326",                   // 326
  "327",                   // 327
  "328",                   // 328
  "329",                   // 329
  "330",                   // 330
  "331",                   // 331
  "332",                   // 332
  "333",                   // 333
  "334",                   // 334
  "335",                   // 335
  "336",                   // 336
  "337",                   // 337
  "338",                   // 338
  "339",                   // 339
  "340",                   // 340
  "341",                   // 341
  "342",                   // 342
  "343",                   // 343
  "344",                   // 344
  "345",                   // 345
  "346",                   // 346
  "347",                   // 347
  "348",                   // 348
  "349",                   // 349
  "350",                   // 350
  "351",                   // 351
  "352",                   // 352
  "353",                   // 353
  "354",                   // 354
  "355",                   // 355
  "356",                   // 356
  "357",                   // 357
  "358",                   // 358
  "359",                   // 359
  "360",                   // 360
  "361",                   // 361
  "362",                   // 362
  "363",                   // 363
  "364",                   // 364
  "365",                   // 365
  "366",                   // 366
  "367",                   // 367
  "368",                   // 368
  "369",                   // 369
  "370",                   // 370
  "371",                   // 371
  "372",                   // 372
  "373",                   // 373
  "374",                   // 374
  "375",                   // 375
  "376",                   // 376
  "377",                   // 377
  "378",                   // 378
  "379",                   // 379
  "380",                   // 380
  "381",                   // 381
  "382",                   // 382
  "383",                   // 383
  "384",                   // 384
  "385",                   // 385
  "386",                   // 386
  "387",                   // 387
  "388",                   // 388
  "389",                   // 389
  "390",                   // 390
  "391",                   // 391
  "392",                   // 392
  "393",                   // 393
  "394",                   // 394
  "395",                   // 395
  "396",                   // 396
  "397",                   // 397
  "398",                   // 398
  "399",                   // 399
  "400",                   // 400
  "401",                   // 401
  "402",                   // 402
  "403",                   // 403
  "404",                   // 404
  "405",                   // 405
  "406",                   // 406
  "407",                   // 407
  "408",                   // 408
  "409",                   // 409
  "410",                   // 410
  "411",                   // 411
  "412",                   // 412
  "413",                   // 413
  "414",                   // 414
  "415",                   // 415
  "416",                   // 416
  "417",                   // 417
  "418",                   // 418
  "419",                   // 419
  "420",                   // 420
  "421",                   // 421
  "422",                   // 422
  "423",                   // 423
  "424",                   // 424
  "425",                   // 425
  "426",                   // 426
  "427",                   // 427
  "428",                   // 428
  "429",                   // 429
  "430",                   // 430
  "431",                   // 431
  "432",                   // 432
  "433",                   // 433
  "434",                   // 434
  "435",                   // 435
  "436",                   // 436
  "437",                   // 437
  "438",                   // 438
  "439",                   // 439
  "440",                   // 440
  "441",                   // 441
  "442",                   // 442
  "443",                   // 443
  "444",                   // 444
  "445",                   // 445
  "446",                   // 446
  "447",                   // 447
  "448",                   // 448
  "449",                   // 449
  "450",                   // 450
  "451",                   // 451
  "452",                   // 452
  "453",                   // 453
  "454",                   // 454
  "455",                   // 455
  "456",                   // 456
  "457",                   // 457
  "458",                   // 458
  "459",                   // 459
  "460",                   // 460
  "461",                   // 461
  "462",                   // 462
  "463",                   // 463
  "464",                   // 464
  "465",                   // 465
  "466",                   // 466
  "467",                   // 467
  "468",                   // 468
  "469",                   // 469
  "470",                   // 470
  "471",                   // 471
  "472",                   // 472
  "473",                   // 473
  "474",                   // 474
  "475",                   // 475
  "476",                   // 476
  "477",                   // 477
  "478",                   // 478
  "479",                   // 479
  "480",                   // 480
  "481",                   // 481
  "482",                   // 482
  "483",                   // 483
  "484",                   // 484
  "485",                   // 485
  "486",                   // 486
  "487",                   // 487
  "488",                   // 488
  "489",                   // 489
  "490",                   // 490
  "491",                   // 491
  "492",                   // 492
  "493",                   // 493
  "494",                   // 494
  "495",                   // 495
  "496",                   // 496
  "497",                   // 497
  "498",                   // 498
  "499",                   // 499
  "500",                   // 500
  "501",                   // 501
  "502",                   // 502
  "503",                   // 503
  "504",                   // 504
  "505",                   // 505
  "NDEBELE",               // 506 nr
  "X_BORK_BORK_BORK",      // 507 zzb
  "X_PIG_LATIN",           // 508 zzp
  "X_HACKER",              // 509 zzh
  "X_KLINGON",             // 510 tlh
  "X_ELMER_FUDD",          // 511 zze
  "X_Common",              // 512 xx-Zyyy
  "X_Latin",               // 513 xx-Latn
  "X_Greek",               // 514 xx-Grek
  "X_Cyrillic",            // 515 xx-Cyrl
  "X_Armenian",            // 516 xx-Armn
  "X_Hebrew",              // 517 xx-Hebr
  "X_Arabic",              // 518 xx-Arab
  "X_Syriac",              // 519 xx-Syrc
  "X_Thaana",              // 520 xx-Thaa
  "X_Devanagari",          // 521 xx-Deva
  "X_Bengali",             // 522 xx-Beng
  "X_Gurmukhi",            // 523 xx-Guru
  "X_Gujarati",            // 524 xx-Gujr
  "X_Oriya",               // 525 xx-Orya
  "X_Tamil",               // 526 xx-Taml
  "X_Telugu",              // 527 xx-Telu
  "X_Kannada",             // 528 xx-Knda
  "X_Malayalam",           // 529 xx-Mlym
  "X_Sinhala",             // 530 xx-Sinh
  "X_Thai",                // 531 xx-Thai
  "X_Lao",                 // 532 xx-Laoo
  "X_Tibetan",             // 533 xx-Tibt
  "X_Myanmar",             // 534 xx-Mymr
  "X_Georgian",            // 535 xx-Geor
  "X_Hangul",              // 536 xx-Hang
  "X_Ethiopic",            // 537 xx-Ethi
  "X_Cherokee",            // 538 xx-Cher
  "X_Canadian_Aboriginal",  // 539 xx-Cans
  "X_Ogham",               // 540 xx-Ogam
  "X_Runic",               // 541 xx-Runr
  "X_Khmer",               // 542 xx-Khmr
  "X_Mongolian",           // 543 xx-Mong
  "X_Hiragana",            // 544 xx-Hira
  "X_Katakana",            // 545 xx-Kana
  "X_Bopomofo",            // 546 xx-Bopo
  "X_Han",                 // 547 xx-Hani
  "X_Yi",                  // 548 xx-Yiii
  "X_Old_Italic",          // 549 xx-Ital
  "X_Gothic",              // 550 xx-Goth
  "X_Deseret",             // 551 xx-Dsrt
  "X_Inherited",           // 552 xx-Qaai
  "X_Tagalog",             // 553 xx-Tglg
  "X_Hanunoo",             // 554 xx-Hano
  "X_Buhid",               // 555 xx-Buhd
  "X_Tagbanwa",            // 556 xx-Tagb
  "X_Limbu",               // 557 xx-Limb
  "X_Tai_Le",              // 558 xx-Tale
  "X_Linear_B",            // 559 xx-Linb
  "X_Ugaritic",            // 560 xx-Ugar
  "X_Shavian",             // 561 xx-Shaw
  "X_Osmanya",             // 562 xx-Osma
  "X_Cypriot",             // 563 xx-Cprt
  "X_Braille",             // 564 xx-Brai
  "X_Buginese",            // 565 xx-Bugi
  "X_Coptic",              // 566 xx-Copt
  "X_New_Tai_Lue",         // 567 xx-Talu
  "X_Glagolitic",          // 568 xx-Glag
  "X_Tifinagh",            // 569 xx-Tfng
  "X_Syloti_Nagri",        // 570 xx-Sylo
  "X_Old_Persian",         // 571 xx-Xpeo
  "X_Kharoshthi",          // 572 xx-Khar
  "X_Balinese",            // 573 xx-Bali
  "X_Cuneiform",           // 574 xx-Xsux
  "X_Phoenician",          // 575 xx-Phnx
  "X_Phags_Pa",            // 576 xx-Phag
  "X_Nko",                 // 577 xx-Nkoo
  "X_Sundanese",           // 578 xx-Sund
  "X_Lepcha",              // 579 xx-Lepc
  "X_Ol_Chiki",            // 580 xx-Olck
  "X_Vai",                 // 581 xx-Vaii
  "X_Saurashtra",          // 582 xx-Saur
  "X_Kayah_Li",            // 583 xx-Kali
  "X_Rejang",              // 584 xx-Rjng
  "X_Lycian",              // 585 xx-Lyci
  "X_Carian",              // 586 xx-Cari
  "X_Lydian",              // 587 xx-Lydi
  "X_Cham",                // 588 xx-Cham
  "X_Tai_Tham",            // 589 xx-Lana
  "X_Tai_Viet",            // 590 xx-Tavt
  "X_Avestan",             // 591 xx-Avst
  "X_Egyptian_Hieroglyphs",  // 592 xx-Egyp
  "X_Samaritan",           // 593 xx-Samr
  "X_Lisu",                // 594 xx-Lisu
  "X_Bamum",               // 595 xx-Bamu
  "X_Javanese",            // 596 xx-Java
  "X_Meetei_Mayek",        // 597 xx-Mtei
  "X_Imperial_Aramaic",    // 598 xx-Armi
  "X_Old_South_Arabian",   // 599 xx-Sarb
  "X_Inscriptional_Parthian",  // 600 xx-Prti
  "X_Inscriptional_Pahlavi",  // 601 xx-Phli
  "X_Old_Turkic",          // 602 xx-Orkh
  "X_Kaithi",              // 603 xx-Kthi
  "X_Batak",               // 604 xx-Batk
  "X_Brahmi",              // 605 xx-Brah
  "X_Mandaic",             // 606 xx-Mand
  "X_Chakma",              // 607 xx-Cakm
  "X_Meroitic_Cursive",    // 608 xx-Merc
  "X_Meroitic_Hieroglyphs",  // 609 xx-Mero
  "X_Miao",                // 610 xx-Plrd
  "X_Sharada",             // 611 xx-Shrd
  "X_Sora_Sompeng",        // 612 xx-Sora
  "X_Takri",               // 613 xx-Takr
};

// Subscripted by enum Language
static const int kLanguageToCodeSize = 614;
extern const char* const kLanguageToCode[kLanguageToCodeSize] = {
  "en",    // 0 ENGLISH
  "da",    // 1 DANISH
  "nl",    // 2 DUTCH
  "fi",    // 3 FINNISH
  "fr",    // 4 FRENCH
  "de",    // 5 GERMAN
  "iw",    // 6 HEBREW
  "it",    // 7 ITALIAN
  "ja",    // 8 Japanese
  "ko",    // 9 Korean
  "no",    // 10 NORWEGIAN
  "pl",    // 11 POLISH
  "pt",    // 12 PORTUGUESE
  "ru",    // 13 RUSSIAN
  "es",    // 14 SPANISH
  "sv",    // 15 SWEDISH
  "zh",    // 16 Chinese
  "cs",    // 17 CZECH
  "el",    // 18 GREEK
  "is",    // 19 ICELANDIC
  "lv",    // 20 LATVIAN
  "lt",    // 21 LITHUANIAN
  "ro",    // 22 ROMANIAN
  "hu",    // 23 HUNGARIAN
  "et",    // 24 ESTONIAN
  "xxx",   // 25 Ignore
  "un",    // 26 Unknown
  "bg",    // 27 BULGARIAN
  "hr",    // 28 CROATIAN
  "sr",    // 29 SERBIAN
  "ga",    // 30 IRISH
  "gl",    // 31 GALICIAN
  "tl",    // 32 TAGALOG
  "tr",    // 33 TURKISH
  "uk",    // 34 UKRAINIAN
  "hi",    // 35 HINDI
  "mk",    // 36 MACEDONIAN
  "bn",    // 37 BENGALI
  "id",    // 38 INDONESIAN
  "la",    // 39 LATIN
  "ms",    // 40 MALAY
  "ml",    // 41 MALAYALAM
  "cy",    // 42 WELSH
  "ne",    // 43 NEPALI
  "te",    // 44 TELUGU
  "sq",    // 45 ALBANIAN
  "ta",    // 46 TAMIL
  "be",    // 47 BELARUSIAN
  "jw",    // 48 JAVANESE
  "oc",    // 49 OCCITAN
  "ur",    // 50 URDU
  "bh",    // 51 BIHARI
  "gu",    // 52 GUJARATI
  "th",    // 53 THAI
  "ar",    // 54 ARABIC
  "ca",    // 55 CATALAN
  "eo",    // 56 ESPERANTO
  "eu",    // 57 BASQUE
  "ia",    // 58 INTERLINGUA
  "kn",    // 59 KANNADA
  "pa",    // 60 PUNJABI
  "gd",    // 61 SCOTS_GAELIC
  "sw",    // 62 SWAHILI
  "sl",    // 63 SLOVENIAN
  "mr",    // 64 MARATHI
  "mt",    // 65 MALTESE
  "vi",    // 66 VIETNAMESE
  "fy",    // 67 FRISIAN
  "sk",    // 68 SLOVAK
  "zh-Hant",  // 69 ChineseT
  "fo",    // 70 FAROESE
  "su",    // 71 SUNDANESE
  "uz",    // 72 UZBEK
  "am",    // 73 AMHARIC
  "az",    // 74 AZERBAIJANI
  "ka",    // 75 GEORGIAN
  "ti",    // 76 TIGRINYA
  "fa",    // 77 PERSIAN
  "bs",    // 78 BOSNIAN
  "si",    // 79 SINHALESE
  "nn",    // 80 NORWEGIAN_N
  "",      // 81 81
  "",      // 82 82
  "xh",    // 83 XHOSA
  "zu",    // 84 ZULU
  "gn",    // 85 GUARANI
  "st",    // 86 SESOTHO
  "tk",    // 87 TURKMEN
  "ky",    // 88 KYRGYZ
  "br",    // 89 BRETON
  "tw",    // 90 TWI
  "yi",    // 91 YIDDISH
  "",      // 92 92
  "so",    // 93 SOMALI
  "ug",    // 94 UIGHUR
  "ku",    // 95 KURDISH
  "mn",    // 96 MONGOLIAN
  "hy",    // 97 ARMENIAN
  "lo",    // 98 LAOTHIAN
  "sd",    // 99 SINDHI
  "rm",    // 100 RHAETO_ROMANCE
  "af",    // 101 AFRIKAANS
  "lb",    // 102 LUXEMBOURGISH
  "my",    // 103 BURMESE
  "km",    // 104 KHMER
  "bo",    // 105 TIBETAN
  "dv",    // 106 DHIVEHI
  "chr",   // 107 CHEROKEE
  "syr",   // 108 SYRIAC
  "lif",   // 109 LIMBU
  "or",    // 110 ORIYA
  "as",    // 111 ASSAMESE
  "co",    // 112 CORSICAN
  "ie",    // 113 INTERLINGUE
  "kk",    // 114 KAZAKH
  "ln",    // 115 LINGALA
  "",      // 116 116
  "ps",    // 117 PASHTO
  "qu",    // 118 QUECHUA
  "sn",    // 119 SHONA
  "tg",    // 120 TAJIK
  "tt",    // 121 TATAR
  "to",    // 122 TONGA
  "yo",    // 123 YORUBA
  "",      // 124 124
  "",      // 125 125
  "",      // 126 126
  "",      // 127 127
  "mi",    // 128 MAORI
  "wo",    // 129 WOLOF
  "ab",    // 130 ABKHAZIAN
  "aa",    // 131 AFAR
  "ay",    // 132 AYMARA
  "ba",    // 133 BASHKIR
  "bi",    // 134 BISLAMA
  "dz",    // 135 DZONGKHA
  "fj",    // 136 FIJIAN
  "kl",    // 137 GREENLANDIC
  "ha",    // 138 HAUSA
  "ht",    // 139 HAITIAN_CREOLE
  "ik",    // 140 INUPIAK
  "iu",    // 141 INUKTITUT
  "ks",    // 142 KASHMIRI
  "rw",    // 143 KINYARWANDA
  "mg",    // 144 MALAGASY
  "na",    // 145 NAURU
  "om",    // 146 OROMO
  "rn",    // 147 RUNDI
  "sm",    // 148 SAMOAN
  "sg",    // 149 SANGO
  "sa",    // 150 SANSKRIT
  "ss",    // 151 SISWANT
  "ts",    // 152 TSONGA
  "tn",    // 153 TSWANA
  "vo",    // 154 VOLAPUK
  "za",    // 155 ZHUANG
  "kha",   // 156 KHASI
  "sco",   // 157 SCOTS
  "lg",    // 158 GANDA
  "gv",    // 159 MANX
  "sr-ME",  // 160 MONTENEGRIN
  "ak",    // 161 AKAN
  "ig",    // 162 IGBO
  "mfe",   // 163 MAURITIAN_CREOLE
  "haw",   // 164 HAWAIIAN
  "ceb",   // 165 CEBUANO
  "ee",    // 166 EWE
  "gaa",   // 167 GA
  "hmn",   // 168 HMONG
  "kri",   // 169 KRIO
  "loz",   // 170 LOZI
  "lua",   // 171 LUBA_LULUA
  "luo",   // 172 LUO_KENYA_AND_TANZANIA
  "new",   // 173 NEWARI
  "ny",    // 174 NYANJA
  "os",    // 175 OSSETIAN
  "pam",   // 176 PAMPANGA
  "nso",   // 177 PEDI
  "raj",   // 178 RAJASTHANI
  "crs",   // 179 SESELWA
  "tum",   // 180 TUMBUKA
  "ve",    // 181 VENDA
  "war",   // 182 WARAY_PHILIPPINES
  "",      // 183 183
  "",      // 184 184
  "",      // 185 185
  "",      // 186 186
  "",      // 187 187
  "",      // 188 188
  "",      // 189 189
  "",      // 190 190
  "",      // 191 191
  "",      // 192 192
  "",      // 193 193
  "",      // 194 194
  "",      // 195 195
  "",      // 196 196
  "",      // 197 197
  "",      // 198 198
  "",      // 199 199
  "",      // 200 200
  "",      // 201 201
  "",      // 202 202
  "",      // 203 203
  "",      // 204 204
  "",      // 205 205
  "",      // 206 206
  "",      // 207 207
  "",      // 208 208
  "",      // 209 209
  "",      // 210 210
  "",      // 211 211
  "",      // 212 212
  "",      // 213 213
  "",      // 214 214
  "",      // 215 215
  "",      // 216 216
  "",      // 217 217
  "",      // 218 218
  "",      // 219 219
  "",      // 220 220
  "",      // 221 221
  "",      // 222 222
  "",      // 223 223
  "",      // 224 224
  "",      // 225 225
  "",      // 226 226
  "",      // 227 227
  "",      // 228 228
  "",      // 229 229
  "",      // 230 230
  "",      // 231 231
  "",      // 232 232
  "",      // 233 233
  "",      // 234 234
  "",      // 235 235
  "",      // 236 236
  "",      // 237 237
  "",      // 238 238
  "",      // 239 239
  "",      // 240 240
  "",      // 241 241
  "",      // 242 242
  "",      // 243 243
  "",      // 244 244
  "",      // 245 245
  "",      // 246 246
  "",      // 247 247
  "",      // 248 248
  "",      // 249 249
  "",      // 250 250
  "",      // 251 251
  "",      // 252 252
  "",      // 253 253
  "",      // 254 254
  "",      // 255 255
  "",      // 256 256
  "",      // 257 257
  "",      // 258 258
  "",      // 259 259
  "",      // 260 260
  "",      // 261 261
  "",      // 262 262
  "",      // 263 263
  "",      // 264 264
  "",      // 265 265
  "",      // 266 266
  "",      // 267 267
  "",      // 268 268
  "",      // 269 269
  "",      // 270 270
  "",      // 271 271
  "",      // 272 272
  "",      // 273 273
  "",      // 274 274
  "",      // 275 275
  "",      // 276 276
  "",      // 277 277
  "",      // 278 278
  "",      // 279 279
  "",      // 280 280
  "",      // 281 281
  "",      // 282 282
  "",      // 283 283
  "",      // 284 284
  "",      // 285 285
  "",      // 286 286
  "",      // 287 287
  "",      // 288 288
  "",      // 289 289
  "",      // 290 290
  "",      // 291 291
  "",      // 292 292
  "",      // 293 293
  "",      // 294 294
  "",      // 295 295
  "",      // 296 296
  "",      // 297 297
  "",      // 298 298
  "",      // 299 299
  "",      // 300 300
  "",      // 301 301
  "",      // 302 302
  "",      // 303 303
  "",      // 304 304
  "",      // 305 305
  "",      // 306 306
  "",      // 307 307
  "",      // 308 308
  "",      // 309 309
  "",      // 310 310
  "",      // 311 311
  "",      // 312 312
  "",      // 313 313
  "",      // 314 314
  "",      // 315 315
  "",      // 316 316
  "",      // 317 317
  "",      // 318 318
  "",      // 319 319
  "",      // 320 320
  "",      // 321 321
  "",      // 322 322
  "",      // 323 323
  "",      // 324 324
  "",      // 325 325
  "",      // 326 326
  "",      // 327 327
  "",      // 328 328
  "",      // 329 329
  "",      // 330 330
  "",      // 331 331
  "",      // 332 332
  "",      // 333 333
  "",      // 334 334
  "",      // 335 335
  "",      // 336 336
  "",      // 337 337
  "",      // 338 338
  "",      // 339 339
  "",      // 340 340
  "",      // 341 341
  "",      // 342 342
  "",      // 343 343
  "",      // 344 344
  "",      // 345 345
  "",      // 346 346
  "",      // 347 347
  "",      // 348 348
  "",      // 349 349
  "",      // 350 350
  "",      // 351 351
  "",      // 352 352
  "",      // 353 353
  "",      // 354 354
  "",      // 355 355
  "",      // 356 356
  "",      // 357 357
  "",      // 358 358
  "",      // 359 359
  "",      // 360 360
  "",      // 361 361
  "",      // 362 362
  "",      // 363 363
  "",      // 364 364
  "",      // 365 365
  "",      // 366 366
  "",      // 367 367
  "",      // 368 368
  "",      // 369 369
  "",      // 370 370
  "",      // 371 371
  "",      // 372 372
  "",      // 373 373
  "",      // 374 374
  "",      // 375 375
  "",      // 376 376
  "",      // 377 377
  "",      // 378 378
  "",      // 379 379
  "",      // 380 380
  "",      // 381 381
  "",      // 382 382
  "",      // 383 383
  "",      // 384 384
  "",      // 385 385
  "",      // 386 386
  "",      // 387 387
  "",      // 388 388
  "",      // 389 389
  "",      // 390 390
  "",      // 391 391
  "",      // 392 392
  "",      // 393 393
  "",      // 394 394
  "",      // 395 395
  "",      // 396 396
  "",      // 397 397
  "",      // 398 398
  "",      // 399 399
  "",      // 400 400
  "",      // 401 401
  "",      // 402 402
  "",      // 403 403
  "",      // 404 404
  "",      // 405 405
  "",      // 406 406
  "",      // 407 407
  "",      // 408 408
  "",      // 409 409
  "",      // 410 410
  "",      // 411 411
  "",      // 412 412
  "",      // 413 413
  "",      // 414 414
  "",      // 415 415
  "",      // 416 416
  "",      // 417 417
  "",      // 418 418
  "",      // 419 419
  "",      // 420 420
  "",      // 421 421
  "",      // 422 422
  "",      // 423 423
  "",      // 424 424
  "",      // 425 425
  "",      // 426 426
  "",      // 427 427
  "",      // 428 428
  "",      // 429 429
  "",      // 430 430
  "",      // 431 431
  "",      // 432 432
  "",      // 433 433
  "",      // 434 434
  "",      // 435 435
  "",      // 436 436
  "",      // 437 437
  "",      // 438 438
  "",      // 439 439
  "",      // 440 440
  "",      // 441 441
  "",      // 442 442
  "",      // 443 443
  "",      // 444 444
  "",      // 445 445
  "",      // 446 446
  "",      // 447 447
  "",      // 448 448
  "",      // 449 449
  "",      // 450 450
  "",      // 451 451
  "",      // 452 452
  "",      // 453 453
  "",      // 454 454
  "",      // 455 455
  "",      // 456 456
  "",      // 457 457
  "",      // 458 458
  "",      // 459 459
  "",      // 460 460
  "",      // 461 461
  "",      // 462 462
  "",      // 463 463
  "",      // 464 464
  "",      // 465 465
  "",      // 466 466
  "",      // 467 467
  "",      // 468 468
  "",      // 469 469
  "",      // 470 470
  "",      // 471 471
  "",      // 472 472
  "",      // 473 473
  "",      // 474 474
  "",      // 475 475
  "",      // 476 476
  "",      // 477 477
  "",      // 478 478
  "",      // 479 479
  "",      // 480 480
  "",      // 481 481
  "",      // 482 482
  "",      // 483 483
  "",      // 484 484
  "",      // 485 485
  "",      // 486 486
  "",      // 487 487
  "",      // 488 488
  "",      // 489 489
  "",      // 490 490
  "",      // 491 491
  "",      // 492 492
  "",      // 493 493
  "",      // 494 494
  "",      // 495 495
  "",      // 496 496
  "",      // 497 497
  "",      // 498 498
  "",      // 499 499
  "",      // 500 500
  "",      // 501 501
  "",      // 502 502
  "",      // 503 503
  "",      // 504 504
  "",      // 505 505
  "nr",    // 506 NDEBELE
  "zzb",   // 507 X_BORK_BORK_BORK
  "zzp",   // 508 X_PIG_LATIN
  "zzh",   // 509 X_HACKER
  "tlh",   // 510 X_KLINGON
  "zze",   // 511 X_ELMER_FUDD
  "xx-Zyyy",  // 512 X_Common
  "xx-Latn",  // 513 X_Latin
  "xx-Grek",  // 514 X_Greek
  "xx-Cyrl",  // 515 X_Cyrillic
  "xx-Armn",  // 516 X_Armenian
  "xx-Hebr",  // 517 X_Hebrew
  "xx-Arab",  // 518 X_Arabic
  "xx-Syrc",  // 519 X_Syriac
  "xx-Thaa",  // 520 X_Thaana
  "xx-Deva",  // 521 X_Devanagari
  "xx-Beng",  // 522 X_Bengali
  "xx-Guru",  // 523 X_Gurmukhi
  "xx-Gujr",  // 524 X_Gujarati
  "xx-Orya",  // 525 X_Oriya
  "xx-Taml",  // 526 X_Tamil
  "xx-Telu",  // 527 X_Telugu
  "xx-Knda",  // 528 X_Kannada
  "xx-Mlym",  // 529 X_Malayalam
  "xx-Sinh",  // 530 X_Sinhala
  "xx-Thai",  // 531 X_Thai
  "xx-Laoo",  // 532 X_Lao
  "xx-Tibt",  // 533 X_Tibetan
  "xx-Mymr",  // 534 X_Myanmar
  "xx-Geor",  // 535 X_Georgian
  "xx-Hang",  // 536 X_Hangul
  "xx-Ethi",  // 537 X_Ethiopic
  "xx-Cher",  // 538 X_Cherokee
  "xx-Cans",  // 539 X_Canadian_Aboriginal
  "xx-Ogam",  // 540 X_Ogham
  "xx-Runr",  // 541 X_Runic
  "xx-Khmr",  // 542 X_Khmer
  "xx-Mong",  // 543 X_Mongolian
  "xx-Hira",  // 544 X_Hiragana
  "xx-Kana",  // 545 X_Katakana
  "xx-Bopo",  // 546 X_Bopomofo
  "xx-Hani",  // 547 X_Han
  "xx-Yiii",  // 548 X_Yi
  "xx-Ital",  // 549 X_Old_Italic
  "xx-Goth",  // 550 X_Gothic
  "xx-Dsrt",  // 551 X_Deseret
  "xx-Qaai",  // 552 X_Inherited
  "xx-Tglg",  // 553 X_Tagalog
  "xx-Hano",  // 554 X_Hanunoo
  "xx-Buhd",  // 555 X_Buhid
  "xx-Tagb",  // 556 X_Tagbanwa
  "xx-Limb",  // 557 X_Limbu
  "xx-Tale",  // 558 X_Tai_Le
  "xx-Linb",  // 559 X_Linear_B
  "xx-Ugar",  // 560 X_Ugaritic
  "xx-Shaw",  // 561 X_Shavian
  "xx-Osma",  // 562 X_Osmanya
  "xx-Cprt",  // 563 X_Cypriot
  "xx-Brai",  // 564 X_Braille
  "xx-Bugi",  // 565 X_Buginese
  "xx-Copt",  // 566 X_Coptic
  "xx-Talu",  // 567 X_New_Tai_Lue
  "xx-Glag",  // 568 X_Glagolitic
  "xx-Tfng",  // 569 X_Tifinagh
  "xx-Sylo",  // 570 X_Syloti_Nagri
  "xx-Xpeo",  // 571 X_Old_Persian
  "xx-Khar",  // 572 X_Kharoshthi
  "xx-Bali",  // 573 X_Balinese
  "xx-Xsux",  // 574 X_Cuneiform
  "xx-Phnx",  // 575 X_Phoenician
  "xx-Phag",  // 576 X_Phags_Pa
  "xx-Nkoo",  // 577 X_Nko
  "xx-Sund",  // 578 X_Sundanese
  "xx-Lepc",  // 579 X_Lepcha
  "xx-Olck",  // 580 X_Ol_Chiki
  "xx-Vaii",  // 581 X_Vai
  "xx-Saur",  // 582 X_Saurashtra
  "xx-Kali",  // 583 X_Kayah_Li
  "xx-Rjng",  // 584 X_Rejang
  "xx-Lyci",  // 585 X_Lycian
  "xx-Cari",  // 586 X_Carian
  "xx-Lydi",  // 587 X_Lydian
  "xx-Cham",  // 588 X_Cham
  "xx-Lana",  // 589 X_Tai_Tham
  "xx-Tavt",  // 590 X_Tai_Viet
  "xx-Avst",  // 591 X_Avestan
  "xx-Egyp",  // 592 X_Egyptian_Hieroglyphs
  "xx-Samr",  // 593 X_Samaritan
  "xx-Lisu",  // 594 X_Lisu
  "xx-Bamu",  // 595 X_Bamum
  "xx-Java",  // 596 X_Javanese
  "xx-Mtei",  // 597 X_Meetei_Mayek
  "xx-Armi",  // 598 X_Imperial_Aramaic
  "xx-Sarb",  // 599 X_Old_South_Arabian
  "xx-Prti",  // 600 X_Inscriptional_Parthian
  "xx-Phli",  // 601 X_Inscriptional_Pahlavi
  "xx-Orkh",  // 602 X_Old_Turkic
  "xx-Kthi",  // 603 X_Kaithi
  "xx-Batk",  // 604 X_Batak
  "xx-Brah",  // 605 X_Brahmi
  "xx-Mand",  // 606 X_Mandaic
  "xx-Cakm",  // 607 X_Chakma
  "xx-Merc",  // 608 X_Meroitic_Cursive
  "xx-Mero",  // 609 X_Meroitic_Hieroglyphs
  "xx-Plrd",  // 610 X_Miao
  "xx-Shrd",  // 611 X_Sharada
  "xx-Sora",  // 612 X_Sora_Sompeng
  "xx-Takr",  // 613 X_Takri
};

// Subscripted by enum Language
extern const int kLanguageToPLangSize = 512;
extern const uint8_t kLanguageToPLang[kLanguageToPLangSize] = {
    1,  // 0 en
    2,  // 1 da
    3,  // 2 nl
    4,  // 3 fi
    5,  // 4 fr
    6,  // 5 de
    1,  // 6 iw
    7,  // 7 it
    2,  // 8 ja
    3,  // 9 ko
    8,  // 10 no
    9,  // 11 pl
   10,  // 12 pt
    4,  // 13 ru
   11,  // 14 es
   12,  // 15 sv
    5,  // 16 zh
   13,  // 17 cs
    6,  // 18 el
   14,  // 19 is
   15,  // 20 lv
   16,  // 21 lt
   17,  // 22 ro
   18,  // 23 hu
   19,  // 24 et
   20,  // 25 xxx
   21,  // 26 un
    7,  // 27 bg
   22,  // 28 hr
   23,  // 29 sr
   24,  // 30 ga
   25,  // 31 gl
   26,  // 32 tl
   27,  // 33 tr
    8,  // 34 uk
    9,  // 35 hi
   10,  // 36 mk
   11,  // 37 bn
   28,  // 38 id
   29,  // 39 la
   30,  // 40 ms
   12,  // 41 ml
   31,  // 42 cy
   13,  // 43 ne
   14,  // 44 te
   32,  // 45 sq
   15,  // 46 ta
   16,  // 47 be
   33,  // 48 jw
   34,  // 49 oc
   18,  // 50 ur
   19,  // 51 bh
   21,  // 52 gu
   22,  // 53 th
   24,  // 54 ar
   35,  // 55 ca
   36,  // 56 eo
   37,  // 57 eu
   38,  // 58 ia
   25,  // 59 kn
   27,  // 60 pa
   39,  // 61 gd
   40,  // 62 sw
   41,  // 63 sl
   28,  // 64 mr
   42,  // 65 mt
   43,  // 66 vi
   44,  // 67 fy
   45,  // 68 sk
   29,  // 69 zh-Hant
   46,  // 70 fo
   47,  // 71 su
   48,  // 72 uz
   30,  // 73 am
   49,  // 74 az
   31,  // 75 ka
   32,  // 76 ti
   33,  // 77 fa
   50,  // 78 bs
   34,  // 79 si
   51,  // 80 nn
    0,  // 81
    0,  // 82
   52,  // 83 xh
   53,  // 84 zu
   54,  // 85 gn
   55,  // 86 st
   56,  // 87 tk
   35,  // 88 ky
   57,  // 89 br
   58,  // 90 tw
   36,  // 91 yi
    0,  // 92
   59,  // 93 so
   60,  // 94 ug
   61,  // 95 ku
   37,  // 96 mn
   38,  // 97 hy
   39,  // 98 lo
   40,  // 99 sd
   62,  // 100 rm
   63,  // 101 af
   64,  // 102 lb
   65,  // 103 my
   41,  // 104 km
   42,  // 105 bo
   43,  // 106 dv
   44,  // 107 chr
   45,  // 108 syr
   46,  // 109 lif
   47,  // 110 or
   51,  // 111 as
   66,  // 112 co
   67,  // 113 ie
   68,  // 114 kk
   69,  // 115 ln
    0,  // 116
   52,  // 117 ps
   70,  // 118 qu
   71,  // 119 sn
   53,  // 120 tg
   72,  // 121 tt
   73,  // 122 to
   74,  // 123 yo
    0,  // 124
    0,  // 125
    0,  // 126
    0,  // 127
   75,  // 128 mi
   76,  // 129 wo
   54,  // 130 ab
   77,  // 131 aa
   78,  // 132 ay
   55,  // 133 ba
   79,  // 134 bi
   57,  // 135 dz
   80,  // 136 fj
   81,  // 137 kl
   82,  // 138 ha
   83,  // 139 ht
   84,  // 140 ik
   58,  // 141 iu
   59,  // 142 ks
   85,  // 143 rw
   86,  // 144 mg
   87,  // 145 na
   88,  // 146 om
   89,  // 147 rn
   90,  // 148 sm
   91,  // 149 sg
   92,  // 150 sa
   93,  // 151 ss
   94,  // 152 ts
   95,  // 153 tn
   96,  // 154 vo
   97,  // 155 za
   98,  // 156 kha
   99,  // 157 sco
  100,  // 158 lg
  101,  // 159 gv
  102,  // 160 sr-ME
  103,  // 161 ak
  104,  // 162 ig
  105,  // 163 mfe
  106,  // 164 haw
  107,  // 165 ceb
  108,  // 166 ee
  109,  // 167 gaa
  110,  // 168 hmn
  111,  // 169 kri
  112,  // 170 loz
  113,  // 171 lua
  114,  // 172 luo
   62,  // 173 new
  115,  // 174 ny
   63,  // 175 os
  116,  // 176 pam
  117,  // 177 nso
   64,  // 178 raj
  118,  // 179 crs
  119,  // 180 tum
  120,  // 181 ve
  121,  // 182 war
    0,  // 183
    0,  // 184
    0,  // 185
    0,  // 186
    0,  // 187
    0,  // 188
    0,  // 189
    0,  // 190
    0,  // 191
    0,  // 192
    0,  // 193
    0,  // 194
    0,  // 195
    0,  // 196
    0,  // 197
    0,  // 198
    0,  // 199
    0,  // 200
    0,  // 201
    0,  // 202
    0,  // 203
    0,  // 204
    0,  // 205
    0,  // 206
    0,  // 207
    0,  // 208
    0,  // 209
    0,  // 210
    0,  // 211
    0,  // 212
    0,  // 213
    0,  // 214
    0,  // 215
    0,  // 216
    0,  // 217
    0,  // 218
    0,  // 219
    0,  // 220
    0,  // 221
    0,  // 222
    0,  // 223
    0,  // 224
    0,  // 225
    0,  // 226
    0,  // 227
    0,  // 228
    0,  // 229
    0,  // 230
    0,  // 231
    0,  // 232
    0,  // 233
    0,  // 234
    0,  // 235
    0,  // 236
    0,  // 237
    0,  // 238
    0,  // 239
    0,  // 240
    0,  // 241
    0,  // 242
    0,  // 243
    0,  // 244
    0,  // 245
    0,  // 246
    0,  // 247
    0,  // 248
    0,  // 249
    0,  // 250
    0,  // 251
    0,  // 252
    0,  // 253
    0,  // 254
    0,  // 255
    0,  // 256
    0,  // 257
    0,  // 258
    0,  // 259
    0,  // 260
    0,  // 261
    0,  // 262
    0,  // 263
    0,  // 264
    0,  // 265
    0,  // 266
    0,  // 267
    0,  // 268
    0,  // 269
    0,  // 270
    0,  // 271
    0,  // 272
    0,  // 273
    0,  // 274
    0,  // 275
    0,  // 276
    0,  // 277
    0,  // 278
    0,  // 279
    0,  // 280
    0,  // 281
    0,  // 282
    0,  // 283
    0,  // 284
    0,  // 285
    0,  // 286
    0,  // 287
    0,  // 288
    0,  // 289
    0,  // 290
    0,  // 291
    0,  // 292
    0,  // 293
    0,  // 294
    0,  // 295
    0,  // 296
    0,  // 297
    0,  // 298
    0,  // 299
    0,  // 300
    0,  // 301
    0,  // 302
    0,  // 303
    0,  // 304
    0,  // 305
    0,  // 306
    0,  // 307
    0,  // 308
    0,  // 309
    0,  // 310
    0,  // 311
    0,  // 312
    0,  // 313
    0,  // 314
    0,  // 315
    0,  // 316
    0,  // 317
    0,  // 318
    0,  // 319
    0,  // 320
    0,  // 321
    0,  // 322
    0,  // 323
    0,  // 324
    0,  // 325
    0,  // 326
    0,  // 327
    0,  // 328
    0,  // 329
    0,  // 330
    0,  // 331
    0,  // 332
    0,  // 333
    0,  // 334
    0,  // 335
    0,  // 336
    0,  // 337
    0,  // 338
    0,  // 339
    0,  // 340
    0,  // 341
    0,  // 342
    0,  // 343
    0,  // 344
    0,  // 345
    0,  // 346
    0,  // 347
    0,  // 348
    0,  // 349
    0,  // 350
    0,  // 351
    0,  // 352
    0,  // 353
    0,  // 354
    0,  // 355
    0,  // 356
    0,  // 357
    0,  // 358
    0,  // 359
    0,  // 360
    0,  // 361
    0,  // 362
    0,  // 363
    0,  // 364
    0,  // 365
    0,  // 366
    0,  // 367
    0,  // 368
    0,  // 369
    0,  // 370
    0,  // 371
    0,  // 372
    0,  // 373
    0,  // 374
    0,  // 375
    0,  // 376
    0,  // 377
    0,  // 378
    0,  // 379
    0,  // 380
    0,  // 381
    0,  // 382
    0,  // 383
    0,  // 384
    0,  // 385
    0,  // 386
    0,  // 387
    0,  // 388
    0,  // 389
    0,  // 390
    0,  // 391
    0,  // 392
    0,  // 393
    0,  // 394
    0,  // 395
    0,  // 396
    0,  // 397
    0,  // 398
    0,  // 399
    0,  // 400
    0,  // 401
    0,  // 402
    0,  // 403
    0,  // 404
    0,  // 405
    0,  // 406
    0,  // 407
    0,  // 408
    0,  // 409
    0,  // 410
    0,  // 411
    0,  // 412
    0,  // 413
    0,  // 414
    0,  // 415
    0,  // 416
    0,  // 417
    0,  // 418
    0,  // 419
    0,  // 420
    0,  // 421
    0,  // 422
    0,  // 423
    0,  // 424
    0,  // 425
    0,  // 426
    0,  // 427
    0,  // 428
    0,  // 429
    0,  // 430
    0,  // 431
    0,  // 432
    0,  // 433
    0,  // 434
    0,  // 435
    0,  // 436
    0,  // 437
    0,  // 438
    0,  // 439
    0,  // 440
    0,  // 441
    0,  // 442
    0,  // 443
    0,  // 444
    0,  // 445
    0,  // 446
    0,  // 447
    0,  // 448
    0,  // 449
    0,  // 450
    0,  // 451
    0,  // 452
    0,  // 453
    0,  // 454
    0,  // 455
    0,  // 456
    0,  // 457
    0,  // 458
    0,  // 459
    0,  // 460
    0,  // 461
    0,  // 462
    0,  // 463
    0,  // 464
    0,  // 465
    0,  // 466
    0,  // 467
    0,  // 468
    0,  // 469
    0,  // 470
    0,  // 471
    0,  // 472
    0,  // 473
    0,  // 474
    0,  // 475
    0,  // 476
    0,  // 477
    0,  // 478
    0,  // 479
    0,  // 480
    0,  // 481
    0,  // 482
    0,  // 483
    0,  // 484
    0,  // 485
    0,  // 486
    0,  // 487
    0,  // 488
    0,  // 489
    0,  // 490
    0,  // 491
    0,  // 492
    0,  // 493
    0,  // 494
    0,  // 495
    0,  // 496
    0,  // 497
    0,  // 498
    0,  // 499
    0,  // 500
    0,  // 501
    0,  // 502
    0,  // 503
    0,  // 504
    0,  // 505
  250,  // 506 nr
  251,  // 507 zzb
  252,  // 508 zzp
  253,  // 509 zzh
  254,  // 510 tlh
  255,  // 511 zze
};

// Subscripted by PLang, for ULScript = Latn
extern const uint16_t kPLangToLanguageLatn[256] = {
  UNKNOWN_LANGUAGE,      // 0
  ENGLISH,               // 1
  DANISH,                // 2
  DUTCH,                 // 3
  FINNISH,               // 4
  FRENCH,                // 5
  GERMAN,                // 6
  ITALIAN,               // 7
  NORWEGIAN,             // 8
  POLISH,                // 9
  PORTUGUESE,            // 10
  SPANISH,               // 11
  SWEDISH,               // 12
  CZECH,                 // 13
  ICELANDIC,             // 14
  LATVIAN,               // 15
  LITHUANIAN,            // 16
  ROMANIAN,              // 17
  HUNGARIAN,             // 18
  ESTONIAN,              // 19
  TG_UNKNOWN_LANGUAGE,   // 20
  UNKNOWN_LANGUAGE,      // 21
  CROATIAN,              // 22
  SERBIAN,               // 23
  IRISH,                 // 24
  GALICIAN,              // 25
  TAGALOG,               // 26
  TURKISH,               // 27
  INDONESIAN,            // 28
  LATIN,                 // 29
  MALAY,                 // 30
  WELSH,                 // 31
  ALBANIAN,              // 32
  JAVANESE,              // 33
  OCCITAN,               // 34
  CATALAN,               // 35
  ESPERANTO,             // 36
  BASQUE,                // 37
  INTERLINGUA,           // 38
  SCOTS_GAELIC,          // 39
  SWAHILI,               // 40
  SLOVENIAN,             // 41
  MALTESE,               // 42
  VIETNAMESE,            // 43
  FRISIAN,               // 44
  SLOVAK,                // 45
  FAROESE,               // 46
  SUNDANESE,             // 47
  UZBEK,                 // 48
  AZERBAIJANI,           // 49
  BOSNIAN,               // 50
  NORWEGIAN_N,           // 51
  XHOSA,                 // 52
  ZULU,                  // 53
  GUARANI,               // 54
  SESOTHO,               // 55
  TURKMEN,               // 56
  BRETON,                // 57
  TWI,                   // 58
  SOMALI,                // 59
  UIGHUR,                // 60
  KURDISH,               // 61
  RHAETO_ROMANCE,        // 62
  AFRIKAANS,             // 63
  LUXEMBOURGISH,         // 64
  BURMESE,               // 65
  CORSICAN,              // 66
  INTERLINGUE,           // 67
  KAZAKH,                // 68
  LINGALA,               // 69
  QUECHUA,               // 70
  SHONA,                 // 71
  TATAR,                 // 72
  TONGA,                 // 73
  YORUBA,                // 74
  MAORI,                 // 75
  WOLOF,                 // 76
  AFAR,                  // 77
  AYMARA,                // 78
  BISLAMA,               // 79
  FIJIAN,                // 80
  GREENLANDIC,           // 81
  HAUSA,                 // 82
  HAITIAN_CREOLE,        // 83
  INUPIAK,               // 84
  KINYARWANDA,           // 85
  MALAGASY,              // 86
  NAURU,                 // 87
  OROMO,                 // 88
  RUNDI,                 // 89
  SAMOAN,                // 90
  SANGO,                 // 91
  SANSKRIT,              // 92
  SISWANT,               // 93
  TSONGA,                // 94
  TSWANA,                // 95
  VOLAPUK,               // 96
  ZHUANG,                // 97
  KHASI,                 // 98
  SCOTS,                 // 99
  GANDA,                 // 100
  MANX,                  // 101
  MONTENEGRIN,           // 102
  AKAN,                  // 103
  IGBO,                  // 104
  MAURITIAN_CREOLE,      // 105
  HAWAIIAN,              // 106
  CEBUANO,               // 107
  EWE,                   // 108
  GA,                    // 109
  HMONG,                 // 110
  KRIO,                  // 111
  LOZI,                  // 112
  LUBA_LULUA,            // 113
  LUO_KENYA_AND_TANZANIA,  // 114
  NYANJA,                // 115
  PAMPANGA,              // 116
  PEDI,                  // 117
  SESELWA,               // 118
  TUMBUKA,               // 119
  VENDA,                 // 120
  WARAY_PHILIPPINES,     // 121
  UNKNOWN_LANGUAGE,      // 122
  UNKNOWN_LANGUAGE,      // 123
  UNKNOWN_LANGUAGE,      // 124
  UNKNOWN_LANGUAGE,      // 125
  UNKNOWN_LANGUAGE,      // 126
  UNKNOWN_LANGUAGE,      // 127
  UNKNOWN_LANGUAGE,      // 128
  UNKNOWN_LANGUAGE,      // 129
  UNKNOWN_LANGUAGE,      // 130
  UNKNOWN_LANGUAGE,      // 131
  UNKNOWN_LANGUAGE,      // 132
  UNKNOWN_LANGUAGE,      // 133
  UNKNOWN_LANGUAGE,      // 134
  UNKNOWN_LANGUAGE,      // 135
  UNKNOWN_LANGUAGE,      // 136
  UNKNOWN_LANGUAGE,      // 137
  UNKNOWN_LANGUAGE,      // 138
  UNKNOWN_LANGUAGE,      // 139
  UNKNOWN_LANGUAGE,      // 140
  UNKNOWN_LANGUAGE,      // 141
  UNKNOWN_LANGUAGE,      // 142
  UNKNOWN_LANGUAGE,      // 143
  UNKNOWN_LANGUAGE,      // 144
  UNKNOWN_LANGUAGE,      // 145
  UNKNOWN_LANGUAGE,      // 146
  UNKNOWN_LANGUAGE,      // 147
  UNKNOWN_LANGUAGE,      // 148
  UNKNOWN_LANGUAGE,      // 149
  UNKNOWN_LANGUAGE,      // 150
  UNKNOWN_LANGUAGE,      // 151
  UNKNOWN_LANGUAGE,      // 152
  UNKNOWN_LANGUAGE,      // 153
  UNKNOWN_LANGUAGE,      // 154
  UNKNOWN_LANGUAGE,      // 155
  UNKNOWN_LANGUAGE,      // 156
  UNKNOWN_LANGUAGE,      // 157
  UNKNOWN_LANGUAGE,      // 158
  UNKNOWN_LANGUAGE,      // 159
  UNKNOWN_LANGUAGE,      // 160
  UNKNOWN_LANGUAGE,      // 161
  UNKNOWN_LANGUAGE,      // 162
  UNKNOWN_LANGUAGE,      // 163
  UNKNOWN_LANGUAGE,      // 164
  UNKNOWN_LANGUAGE,      // 165
  UNKNOWN_LANGUAGE,      // 166
  UNKNOWN_LANGUAGE,      // 167
  UNKNOWN_LANGUAGE,      // 168
  UNKNOWN_LANGUAGE,      // 169
  UNKNOWN_LANGUAGE,      // 170
  UNKNOWN_LANGUAGE,      // 171
  UNKNOWN_LANGUAGE,      // 172
  UNKNOWN_LANGUAGE,      // 173
  UNKNOWN_LANGUAGE,      // 174
  UNKNOWN_LANGUAGE,      // 175
  UNKNOWN_LANGUAGE,      // 176
  UNKNOWN_LANGUAGE,      // 177
  UNKNOWN_LANGUAGE,      // 178
  UNKNOWN_LANGUAGE,      // 179
  UNKNOWN_LANGUAGE,      // 180
  UNKNOWN_LANGUAGE,      // 181
  UNKNOWN_LANGUAGE,      // 182
  UNKNOWN_LANGUAGE,      // 183
  UNKNOWN_LANGUAGE,      // 184
  UNKNOWN_LANGUAGE,      // 185
  UNKNOWN_LANGUAGE,      // 186
  UNKNOWN_LANGUAGE,      // 187
  UNKNOWN_LANGUAGE,      // 188
  UNKNOWN_LANGUAGE,      // 189
  UNKNOWN_LANGUAGE,      // 190
  UNKNOWN_LANGUAGE,      // 191
  UNKNOWN_LANGUAGE,      // 192
  UNKNOWN_LANGUAGE,      // 193
  UNKNOWN_LANGUAGE,      // 194
  UNKNOWN_LANGUAGE,      // 195
  UNKNOWN_LANGUAGE,      // 196
  UNKNOWN_LANGUAGE,      // 197
  UNKNOWN_LANGUAGE,      // 198
  UNKNOWN_LANGUAGE,      // 199
  UNKNOWN_LANGUAGE,      // 200
  UNKNOWN_LANGUAGE,      // 201
  UNKNOWN_LANGUAGE,      // 202
  UNKNOWN_LANGUAGE,      // 203
  UNKNOWN_LANGUAGE,      // 204
  UNKNOWN_LANGUAGE,      // 205
  UNKNOWN_LANGUAGE,      // 206
  UNKNOWN_LANGUAGE,      // 207
  UNKNOWN_LANGUAGE,      // 208
  UNKNOWN_LANGUAGE,      // 209
  UNKNOWN_LANGUAGE,      // 210
  UNKNOWN_LANGUAGE,      // 211
  UNKNOWN_LANGUAGE,      // 212
  UNKNOWN_LANGUAGE,      // 213
  UNKNOWN_LANGUAGE,      // 214
  UNKNOWN_LANGUAGE,      // 215
  UNKNOWN_LANGUAGE,      // 216
  UNKNOWN_LANGUAGE,      // 217
  UNKNOWN_LANGUAGE,      // 218
  UNKNOWN_LANGUAGE,      // 219
  UNKNOWN_LANGUAGE,      // 220
  UNKNOWN_LANGUAGE,      // 221
  UNKNOWN_LANGUAGE,      // 222
  UNKNOWN_LANGUAGE,      // 223
  UNKNOWN_LANGUAGE,      // 224
  UNKNOWN_LANGUAGE,      // 225
  UNKNOWN_LANGUAGE,      // 226
  UNKNOWN_LANGUAGE,      // 227
  UNKNOWN_LANGUAGE,      // 228
  UNKNOWN_LANGUAGE,      // 229
  UNKNOWN_LANGUAGE,      // 230
  UNKNOWN_LANGUAGE,      // 231
  UNKNOWN_LANGUAGE,      // 232
  UNKNOWN_LANGUAGE,      // 233
  UNKNOWN_LANGUAGE,      // 234
  UNKNOWN_LANGUAGE,      // 235
  UNKNOWN_LANGUAGE,      // 236
  UNKNOWN_LANGUAGE,      // 237
  UNKNOWN_LANGUAGE,      // 238
  UNKNOWN_LANGUAGE,      // 239
  UNKNOWN_LANGUAGE,      // 240
  UNKNOWN_LANGUAGE,      // 241
  UNKNOWN_LANGUAGE,      // 242
  UNKNOWN_LANGUAGE,      // 243
  UNKNOWN_LANGUAGE,      // 244
  UNKNOWN_LANGUAGE,      // 245
  UNKNOWN_LANGUAGE,      // 246
  UNKNOWN_LANGUAGE,      // 247
  UNKNOWN_LANGUAGE,      // 248
  UNKNOWN_LANGUAGE,      // 249
  NDEBELE,               // 250
  X_BORK_BORK_BORK,      // 251
  X_PIG_LATIN,           // 252
  X_HACKER,              // 253
  X_KLINGON,             // 254
  X_ELMER_FUDD,          // 255
};

// Subscripted by PLang, for ULScript != Latn
extern const uint16_t kPLangToLanguageOthr[256] = {
  UNKNOWN_LANGUAGE,      // 0
  HEBREW,                // 1
  JAPANESE,              // 2
  KOREAN,                // 3
  RUSSIAN,               // 4
  CHINESE,               // 5
  GREEK,                 // 6
  BULGARIAN,             // 7
  UKRAINIAN,             // 8
  HINDI,                 // 9
  MACEDONIAN,            // 10
  BENGALI,               // 11
  MALAYALAM,             // 12
  NEPALI,                // 13
  TELUGU,                // 14
  TAMIL,                 // 15
  BELARUSIAN,            // 16
  ROMANIAN,              // 17
  URDU,                  // 18
  BIHARI,                // 19
  TG_UNKNOWN_LANGUAGE,   // 20
  UNKNOWN_LANGUAGE,      // 21  (updated 2013.09.07 dsites)
  THAI,                  // 22
  SERBIAN,               // 23
  ARABIC,                // 24
  KANNADA,               // 25
  TAGALOG,               // 26
  PUNJABI,               // 27
  MARATHI,               // 28
  CHINESE_T,             // 29
  AMHARIC,               // 30
  GEORGIAN,              // 31
  TIGRINYA,              // 32
  PERSIAN,               // 33
  SINHALESE,             // 34
  KYRGYZ,                // 35
  YIDDISH,               // 36
  MONGOLIAN,             // 37
  ARMENIAN,              // 38
  LAOTHIAN,              // 39
  SINDHI,                // 40
  KHMER,                 // 41
  TIBETAN,               // 42
  DHIVEHI,               // 43
  CHEROKEE,              // 44
  SYRIAC,                // 45
  LIMBU,                 // 46
  ORIYA,                 // 47
  UZBEK,                 // 48
  AZERBAIJANI,           // 49
  BOSNIAN,               // 50
  ASSAMESE,              // 51
  PASHTO,                // 52
  TAJIK,                 // 53
  ABKHAZIAN,             // 54
  BASHKIR,               // 55
  TURKMEN,               // 56
  DZONGKHA,              // 57
  INUKTITUT,             // 58
  KASHMIRI,              // 59
  UIGHUR,                // 60
  KURDISH,               // 61
  NEWARI,                // 62
  OSSETIAN,              // 63
  RAJASTHANI,            // 64
  BURMESE,               // 65
  UNKNOWN_LANGUAGE,      // 66
  UNKNOWN_LANGUAGE,      // 67
  KAZAKH,                // 68
  UNKNOWN_LANGUAGE,      // 69
  UNKNOWN_LANGUAGE,      // 70
  UNKNOWN_LANGUAGE,      // 71
  TATAR,                 // 72
  UNKNOWN_LANGUAGE,      // 73
  UNKNOWN_LANGUAGE,      // 74
  UNKNOWN_LANGUAGE,      // 75
  UNKNOWN_LANGUAGE,      // 76
  UNKNOWN_LANGUAGE,      // 77
  UNKNOWN_LANGUAGE,      // 78
  UNKNOWN_LANGUAGE,      // 79
  UNKNOWN_LANGUAGE,      // 80
  UNKNOWN_LANGUAGE,      // 81
  HAUSA,                 // 82
  UNKNOWN_LANGUAGE,      // 83
  UNKNOWN_LANGUAGE,      // 84
  UNKNOWN_LANGUAGE,      // 85
  UNKNOWN_LANGUAGE,      // 86
  UNKNOWN_LANGUAGE,      // 87
  UNKNOWN_LANGUAGE,      // 88
  UNKNOWN_LANGUAGE,      // 89
  UNKNOWN_LANGUAGE,      // 90
  UNKNOWN_LANGUAGE,      // 91
  SANSKRIT,              // 92
  UNKNOWN_LANGUAGE,      // 93
  UNKNOWN_LANGUAGE,      // 94
  UNKNOWN_LANGUAGE,      // 95
  UNKNOWN_LANGUAGE,      // 96
  ZHUANG,                // 97
  UNKNOWN_LANGUAGE,      // 98
  UNKNOWN_LANGUAGE,      // 99
  UNKNOWN_LANGUAGE,      // 100
  UNKNOWN_LANGUAGE,      // 101
  UNKNOWN_LANGUAGE,      // 102
  UNKNOWN_LANGUAGE,      // 103
  UNKNOWN_LANGUAGE,      // 104
  UNKNOWN_LANGUAGE,      // 105
  UNKNOWN_LANGUAGE,      // 106
  UNKNOWN_LANGUAGE,      // 107
  UNKNOWN_LANGUAGE,      // 108
  UNKNOWN_LANGUAGE,      // 109
  UNKNOWN_LANGUAGE,      // 110
  UNKNOWN_LANGUAGE,      // 111
  UNKNOWN_LANGUAGE,      // 112
  UNKNOWN_LANGUAGE,      // 113
  UNKNOWN_LANGUAGE,      // 114
  UNKNOWN_LANGUAGE,      // 115
  UNKNOWN_LANGUAGE,      // 116
  UNKNOWN_LANGUAGE,      // 117
  UNKNOWN_LANGUAGE,      // 118
  UNKNOWN_LANGUAGE,      // 119
  UNKNOWN_LANGUAGE,      // 120
  UNKNOWN_LANGUAGE,      // 121
  UNKNOWN_LANGUAGE,      // 122
  UNKNOWN_LANGUAGE,      // 123
  UNKNOWN_LANGUAGE,      // 124
  UNKNOWN_LANGUAGE,      // 125
  UNKNOWN_LANGUAGE,      // 126
  UNKNOWN_LANGUAGE,      // 127
  UNKNOWN_LANGUAGE,      // 128
  UNKNOWN_LANGUAGE,      // 129
  UNKNOWN_LANGUAGE,      // 130
  UNKNOWN_LANGUAGE,      // 131
  UNKNOWN_LANGUAGE,      // 132
  UNKNOWN_LANGUAGE,      // 133
  UNKNOWN_LANGUAGE,      // 134
  UNKNOWN_LANGUAGE,      // 135
  UNKNOWN_LANGUAGE,      // 136
  UNKNOWN_LANGUAGE,      // 137
  UNKNOWN_LANGUAGE,      // 138
  UNKNOWN_LANGUAGE,      // 139
  UNKNOWN_LANGUAGE,      // 140
  UNKNOWN_LANGUAGE,      // 141
  UNKNOWN_LANGUAGE,      // 142
  UNKNOWN_LANGUAGE,      // 143
  UNKNOWN_LANGUAGE,      // 144
  UNKNOWN_LANGUAGE,      // 145
  UNKNOWN_LANGUAGE,      // 146
  UNKNOWN_LANGUAGE,      // 147
  UNKNOWN_LANGUAGE,      // 148
  UNKNOWN_LANGUAGE,      // 149
  UNKNOWN_LANGUAGE,      // 150
  UNKNOWN_LANGUAGE,      // 151
  UNKNOWN_LANGUAGE,      // 152
  UNKNOWN_LANGUAGE,      // 153
  UNKNOWN_LANGUAGE,      // 154
  UNKNOWN_LANGUAGE,      // 155
  UNKNOWN_LANGUAGE,      // 156
  UNKNOWN_LANGUAGE,      // 157
  UNKNOWN_LANGUAGE,      // 158
  UNKNOWN_LANGUAGE,      // 159
  UNKNOWN_LANGUAGE,      // 160
  UNKNOWN_LANGUAGE,      // 161
  UNKNOWN_LANGUAGE,      // 162
  UNKNOWN_LANGUAGE,      // 163
  UNKNOWN_LANGUAGE,      // 164
  UNKNOWN_LANGUAGE,      // 165
  UNKNOWN_LANGUAGE,      // 166
  UNKNOWN_LANGUAGE,      // 167
  UNKNOWN_LANGUAGE,      // 168
  UNKNOWN_LANGUAGE,      // 169
  UNKNOWN_LANGUAGE,      // 170
  UNKNOWN_LANGUAGE,      // 171
  UNKNOWN_LANGUAGE,      // 172
  UNKNOWN_LANGUAGE,      // 173
  UNKNOWN_LANGUAGE,      // 174
  UNKNOWN_LANGUAGE,      // 175
  UNKNOWN_LANGUAGE,      // 176
  UNKNOWN_LANGUAGE,      // 177
  UNKNOWN_LANGUAGE,      // 178
  UNKNOWN_LANGUAGE,      // 179
  UNKNOWN_LANGUAGE,      // 180
  UNKNOWN_LANGUAGE,      // 181
  UNKNOWN_LANGUAGE,      // 182
  UNKNOWN_LANGUAGE,      // 183
  UNKNOWN_LANGUAGE,      // 184
  UNKNOWN_LANGUAGE,      // 185
  UNKNOWN_LANGUAGE,      // 186
  UNKNOWN_LANGUAGE,      // 187
  UNKNOWN_LANGUAGE,      // 188
  UNKNOWN_LANGUAGE,      // 189
  UNKNOWN_LANGUAGE,      // 190
  UNKNOWN_LANGUAGE,      // 191
  UNKNOWN_LANGUAGE,      // 192
  UNKNOWN_LANGUAGE,      // 193
  UNKNOWN_LANGUAGE,      // 194
  UNKNOWN_LANGUAGE,      // 195
  UNKNOWN_LANGUAGE,      // 196
  UNKNOWN_LANGUAGE,      // 197
  UNKNOWN_LANGUAGE,      // 198
  UNKNOWN_LANGUAGE,      // 199
  UNKNOWN_LANGUAGE,      // 200
  UNKNOWN_LANGUAGE,      // 201
  UNKNOWN_LANGUAGE,      // 202
  UNKNOWN_LANGUAGE,      // 203
  UNKNOWN_LANGUAGE,      // 204
  UNKNOWN_LANGUAGE,      // 205
  UNKNOWN_LANGUAGE,      // 206
  UNKNOWN_LANGUAGE,      // 207
  UNKNOWN_LANGUAGE,      // 208
  UNKNOWN_LANGUAGE,      // 209
  UNKNOWN_LANGUAGE,      // 210
  UNKNOWN_LANGUAGE,      // 211
  UNKNOWN_LANGUAGE,      // 212
  UNKNOWN_LANGUAGE,      // 213
  UNKNOWN_LANGUAGE,      // 214
  UNKNOWN_LANGUAGE,      // 215
  UNKNOWN_LANGUAGE,      // 216
  UNKNOWN_LANGUAGE,      // 217
  UNKNOWN_LANGUAGE,      // 218
  UNKNOWN_LANGUAGE,      // 219
  UNKNOWN_LANGUAGE,      // 220
  UNKNOWN_LANGUAGE,      // 221
  UNKNOWN_LANGUAGE,      // 222
  UNKNOWN_LANGUAGE,      // 223
  UNKNOWN_LANGUAGE,      // 224
  UNKNOWN_LANGUAGE,      // 225
  UNKNOWN_LANGUAGE,      // 226
  UNKNOWN_LANGUAGE,      // 227
  UNKNOWN_LANGUAGE,      // 228
  UNKNOWN_LANGUAGE,      // 229
  UNKNOWN_LANGUAGE,      // 230
  UNKNOWN_LANGUAGE,      // 231
  UNKNOWN_LANGUAGE,      // 232
  UNKNOWN_LANGUAGE,      // 233
  UNKNOWN_LANGUAGE,      // 234
  UNKNOWN_LANGUAGE,      // 235
  UNKNOWN_LANGUAGE,      // 236
  UNKNOWN_LANGUAGE,      // 237
  UNKNOWN_LANGUAGE,      // 238
  UNKNOWN_LANGUAGE,      // 239
  UNKNOWN_LANGUAGE,      // 240
  UNKNOWN_LANGUAGE,      // 241
  UNKNOWN_LANGUAGE,      // 242
  UNKNOWN_LANGUAGE,      // 243
  UNKNOWN_LANGUAGE,      // 244
  UNKNOWN_LANGUAGE,      // 245
  UNKNOWN_LANGUAGE,      // 246
  UNKNOWN_LANGUAGE,      // 247
  UNKNOWN_LANGUAGE,      // 248
  UNKNOWN_LANGUAGE,      // 249
  UNKNOWN_LANGUAGE,      // 250
  UNKNOWN_LANGUAGE,      // 251
  UNKNOWN_LANGUAGE,      // 252
  UNKNOWN_LANGUAGE,      // 253
  UNKNOWN_LANGUAGE,      // 254
  UNKNOWN_LANGUAGE,      // 255
};

}  // namespace CLD2
