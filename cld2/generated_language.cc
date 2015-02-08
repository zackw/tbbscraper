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
  "English",                   // 0 en
  "Danish",                    // 1 da
  "Dutch",                     // 2 nl
  "Finnish",                   // 3 fi
  "French",                    // 4 fr
  "German",                    // 5 de
  "Hebrew",                    // 6 iw
  "Italian",                   // 7 it
  "Japanese",                  // 8 ja
  "Korean",                    // 9 ko
  "Norwegian",                 // 10 no
  "Polish",                    // 11 pl
  "Portuguese",                // 12 pt
  "Russian",                   // 13 ru
  "Spanish",                   // 14 es
  "Swedish",                   // 15 sv
  "Chinese",                   // 16 zh
  "Czech",                     // 17 cs
  "Greek",                     // 18 el
  "Icelandic",                 // 19 is
  "Latvian",                   // 20 lv
  "Lithuanian",                // 21 lt
  "Romanian",                  // 22 ro
  "Hungarian",                 // 23 hu
  "Estonian",                  // 24 et
  "Not speech",                // 25 zxx (unclear how actually used)
  "Unknown",                   // 26 un
  "Bulgarian",                 // 27 bg
  "Croatian",                  // 28 hr
  "Serbian",                   // 29 sr
  "Irish",                     // 30 ga
  "Galician",                  // 31 gl
  "Tagalog",                   // 32 tl
  "Turkish",                   // 33 tr
  "Ukrainian",                 // 34 uk
  "Hindi",                     // 35 hi
  "Macedonian",                // 36 mk
  "Bengali",                   // 37 bn
  "Indonesian",                // 38 id
  "Latin",                     // 39 la
  "Malay",                     // 40 ms
  "Malayalam",                 // 41 ml
  "Welsh",                     // 42 cy
  "Nepali",                    // 43 ne
  "Telugu",                    // 44 te
  "Albanian",                  // 45 sq
  "Tamil",                     // 46 ta
  "Belarusian",                // 47 be
  "Javanese",                  // 48 jw
  "Occitan",                   // 49 oc
  "Urdu",                      // 50 ur
  "Bihari",                    // 51 bh
  "Gujarati",                  // 52 gu
  "Thai",                      // 53 th
  "Arabic",                    // 54 ar
  "Catalan",                   // 55 ca
  "Esperanto",                 // 56 eo
  "Basque",                    // 57 eu
  "Interlingua",               // 58 ia
  "Kannada",                   // 59 kn
  "Punjabi",                   // 60 pa
  "Scots Gaelic",              // 61 gd
  "Swahili",                   // 62 sw
  "Slovenian",                 // 63 sl
  "Marathi",                   // 64 mr
  "Maltese",                   // 65 mt
  "Vietnamese",                // 66 vi
  "Frisian",                   // 67 fy
  "Slovak",                    // 68 sk
  "Chinese (trad.)",           // 69 zh-Hant
  "Faroese",                   // 70 fo
  "Sundanese",                 // 71 su
  "Uzbek",                     // 72 uz
  "Amharic",                   // 73 am
  "Azerbaijani",               // 74 az
  "Georgian",                  // 75 ka
  "Tigrinya",                  // 76 ti
  "Persian",                   // 77 fa
  "Bosnian",                   // 78 bs
  "Sinhalese",                 // 79 si
  "Norwegian Nynorsk",         // 80 nn
  "81",                        // 81
  "82",                        // 82
  "Xhosa",                     // 83 xh
  "Zulu",                      // 84 zu
  "Guarani",                   // 85 gn
  "Sesotho",                   // 86 st
  "Turkmen",                   // 87 tk
  "Kyrgyz",                    // 88 ky
  "Breton",                    // 89 br
  "Twi",                       // 90 tw
  "Yiddish",                   // 91 yi
  "92",                        // 92
  "Somali",                    // 93 so
  "Uighur",                    // 94 ug
  "Kurdish",                   // 95 ku
  "Mongolian",                 // 96 mn
  "Armenian",                  // 97 hy
  "Laothian",                  // 98 lo
  "Sindhi",                    // 99 sd
  "Romansh",                   // 100 rm
  "Afrikaans",                 // 101 af
  "Luxembourgish",             // 102 lb
  "Burmese",                   // 103 my
  "Khmer",                     // 104 km
  "Tibetan",                   // 105 bo
  "Dhivehi",                   // 106 dv
  "Cherokee",                  // 107 chr
  "Syriac",                    // 108 syr
  "Limbu",                     // 109 lif
  "Oriya",                     // 110 or
  "Assamese",                  // 111 as
  "Corsican",                  // 112 co
  "Interlingue",               // 113 ie
  "Kazakh",                    // 114 kk
  "Lingala",                   // 115 ln
  "116",                       // 116
  "Pashto",                    // 117 ps
  "Quechua",                   // 118 qu
  "Shona",                     // 119 sn
  "Tajik",                     // 120 tg
  "Tatar",                     // 121 tt
  "Tonga",                     // 122 to
  "Yoruba",                    // 123 yo
  "124",                       // 124
  "125",                       // 125
  "126",                       // 126
  "127",                       // 127
  "Maori",                     // 128 mi
  "Wolof",                     // 129 wo
  "Abkhazian",                 // 130 ab
  "Afar",                      // 131 aa
  "Aymara",                    // 132 ay
  "Bashkir",                   // 133 ba
  "Bislama",                   // 134 bi
  "Dzongkha",                  // 135 dz
  "Fijian",                    // 136 fj
  "Greenlandic",               // 137 kl
  "Hausa",                     // 138 ha
  "Haitian Creole",            // 139 ht
  "Inupiak",                   // 140 ik
  "Inuktitut",                 // 141 iu
  "Kashmiri",                  // 142 ks
  "Kinyarwanda",               // 143 rw
  "Malagasy",                  // 144 mg
  "Nauru",                     // 145 na
  "Oromo",                     // 146 om
  "Rundi",                     // 147 rn
  "Samoan",                    // 148 sm
  "Sango",                     // 149 sg
  "Sanskrit",                  // 150 sa
  "Siswant",                   // 151 ss
  "Tsonga",                    // 152 ts
  "Tswana",                    // 153 tn
  "Volapuk",                   // 154 vo
  "Zhuang",                    // 155 za
  "Khasi",                     // 156 kha
  "Scots",                     // 157 sco
  "Ganda",                     // 158 lg
  "Manx",                      // 159 gv
  "Montenegrin",               // 160 sr-ME
  "Akan",                      // 161 ak
  "Igbo",                      // 162 ig
  "Mauritian Creole",          // 163 mfe
  "Hawaiian",                  // 164 haw
  "Cebuano",                   // 165 ceb
  "Ewe",                       // 166 ee
  "Ga",                        // 167 gaa
  "Hmong",                     // 168 hmn
  "Krio",                      // 169 kri
  "Lozi",                      // 170 loz
  "Tshiluba",                  // 171 lua
  "Dholuo",                    // 172 luo
  "Newari",                    // 173 new
  "Nyanja",                    // 174 ny
  "Ossetian",                  // 175 os
  "Pampanga",                  // 176 pam
  "Pedi",                      // 177 nso
  "Rajasthani",                // 178 raj
  "Seselwa",                   // 179 crs
  "Tumbuka",                   // 180 tum
  "Venda",                     // 181 ve
  "Waray-Waray",               // 182 war
  "183",                       // 183
  "184",                       // 184
  "185",                       // 185
  "186",                       // 186
  "187",                       // 187
  "188",                       // 188
  "189",                       // 189
  "190",                       // 190
  "191",                       // 191
  "192",                       // 192
  "193",                       // 193
  "194",                       // 194
  "195",                       // 195
  "196",                       // 196
  "197",                       // 197
  "198",                       // 198
  "199",                       // 199
  "200",                       // 200
  "201",                       // 201
  "202",                       // 202
  "203",                       // 203
  "204",                       // 204
  "205",                       // 205
  "206",                       // 206
  "207",                       // 207
  "208",                       // 208
  "209",                       // 209
  "210",                       // 210
  "211",                       // 211
  "212",                       // 212
  "213",                       // 213
  "214",                       // 214
  "215",                       // 215
  "216",                       // 216
  "217",                       // 217
  "218",                       // 218
  "219",                       // 219
  "220",                       // 220
  "221",                       // 221
  "222",                       // 222
  "223",                       // 223
  "224",                       // 224
  "225",                       // 225
  "226",                       // 226
  "227",                       // 227
  "228",                       // 228
  "229",                       // 229
  "230",                       // 230
  "231",                       // 231
  "232",                       // 232
  "233",                       // 233
  "234",                       // 234
  "235",                       // 235
  "236",                       // 236
  "237",                       // 237
  "238",                       // 238
  "239",                       // 239
  "240",                       // 240
  "241",                       // 241
  "242",                       // 242
  "243",                       // 243
  "244",                       // 244
  "245",                       // 245
  "246",                       // 246
  "247",                       // 247
  "248",                       // 248
  "249",                       // 249
  "250",                       // 250
  "251",                       // 251
  "252",                       // 252
  "253",                       // 253
  "254",                       // 254
  "255",                       // 255
  "256",                       // 256
  "257",                       // 257
  "258",                       // 258
  "259",                       // 259
  "260",                       // 260
  "261",                       // 261
  "262",                       // 262
  "263",                       // 263
  "264",                       // 264
  "265",                       // 265
  "266",                       // 266
  "267",                       // 267
  "268",                       // 268
  "269",                       // 269
  "270",                       // 270
  "271",                       // 271
  "272",                       // 272
  "273",                       // 273
  "274",                       // 274
  "275",                       // 275
  "276",                       // 276
  "277",                       // 277
  "278",                       // 278
  "279",                       // 279
  "280",                       // 280
  "281",                       // 281
  "282",                       // 282
  "283",                       // 283
  "284",                       // 284
  "285",                       // 285
  "286",                       // 286
  "287",                       // 287
  "288",                       // 288
  "289",                       // 289
  "290",                       // 290
  "291",                       // 291
  "292",                       // 292
  "293",                       // 293
  "294",                       // 294
  "295",                       // 295
  "296",                       // 296
  "297",                       // 297
  "298",                       // 298
  "299",                       // 299
  "300",                       // 300
  "301",                       // 301
  "302",                       // 302
  "303",                       // 303
  "304",                       // 304
  "305",                       // 305
  "306",                       // 306
  "307",                       // 307
  "308",                       // 308
  "309",                       // 309
  "310",                       // 310
  "311",                       // 311
  "312",                       // 312
  "313",                       // 313
  "314",                       // 314
  "315",                       // 315
  "316",                       // 316
  "317",                       // 317
  "318",                       // 318
  "319",                       // 319
  "320",                       // 320
  "321",                       // 321
  "322",                       // 322
  "323",                       // 323
  "324",                       // 324
  "325",                       // 325
  "326",                       // 326
  "327",                       // 327
  "328",                       // 328
  "329",                       // 329
  "330",                       // 330
  "331",                       // 331
  "332",                       // 332
  "333",                       // 333
  "334",                       // 334
  "335",                       // 335
  "336",                       // 336
  "337",                       // 337
  "338",                       // 338
  "339",                       // 339
  "340",                       // 340
  "341",                       // 341
  "342",                       // 342
  "343",                       // 343
  "344",                       // 344
  "345",                       // 345
  "346",                       // 346
  "347",                       // 347
  "348",                       // 348
  "349",                       // 349
  "350",                       // 350
  "351",                       // 351
  "352",                       // 352
  "353",                       // 353
  "354",                       // 354
  "355",                       // 355
  "356",                       // 356
  "357",                       // 357
  "358",                       // 358
  "359",                       // 359
  "360",                       // 360
  "361",                       // 361
  "362",                       // 362
  "363",                       // 363
  "364",                       // 364
  "365",                       // 365
  "366",                       // 366
  "367",                       // 367
  "368",                       // 368
  "369",                       // 369
  "370",                       // 370
  "371",                       // 371
  "372",                       // 372
  "373",                       // 373
  "374",                       // 374
  "375",                       // 375
  "376",                       // 376
  "377",                       // 377
  "378",                       // 378
  "379",                       // 379
  "380",                       // 380
  "381",                       // 381
  "382",                       // 382
  "383",                       // 383
  "384",                       // 384
  "385",                       // 385
  "386",                       // 386
  "387",                       // 387
  "388",                       // 388
  "389",                       // 389
  "390",                       // 390
  "391",                       // 391
  "392",                       // 392
  "393",                       // 393
  "394",                       // 394
  "395",                       // 395
  "396",                       // 396
  "397",                       // 397
  "398",                       // 398
  "399",                       // 399
  "400",                       // 400
  "401",                       // 401
  "402",                       // 402
  "403",                       // 403
  "404",                       // 404
  "405",                       // 405
  "406",                       // 406
  "407",                       // 407
  "408",                       // 408
  "409",                       // 409
  "410",                       // 410
  "411",                       // 411
  "412",                       // 412
  "413",                       // 413
  "414",                       // 414
  "415",                       // 415
  "416",                       // 416
  "417",                       // 417
  "418",                       // 418
  "419",                       // 419
  "420",                       // 420
  "421",                       // 421
  "422",                       // 422
  "423",                       // 423
  "424",                       // 424
  "425",                       // 425
  "426",                       // 426
  "427",                       // 427
  "428",                       // 428
  "429",                       // 429
  "430",                       // 430
  "431",                       // 431
  "432",                       // 432
  "433",                       // 433
  "434",                       // 434
  "435",                       // 435
  "436",                       // 436
  "437",                       // 437
  "438",                       // 438
  "439",                       // 439
  "440",                       // 440
  "441",                       // 441
  "442",                       // 442
  "443",                       // 443
  "444",                       // 444
  "445",                       // 445
  "446",                       // 446
  "447",                       // 447
  "448",                       // 448
  "449",                       // 449
  "450",                       // 450
  "451",                       // 451
  "452",                       // 452
  "453",                       // 453
  "454",                       // 454
  "455",                       // 455
  "456",                       // 456
  "457",                       // 457
  "458",                       // 458
  "459",                       // 459
  "460",                       // 460
  "461",                       // 461
  "462",                       // 462
  "463",                       // 463
  "464",                       // 464
  "465",                       // 465
  "466",                       // 466
  "467",                       // 467
  "468",                       // 468
  "469",                       // 469
  "470",                       // 470
  "471",                       // 471
  "472",                       // 472
  "473",                       // 473
  "474",                       // 474
  "475",                       // 475
  "476",                       // 476
  "477",                       // 477
  "478",                       // 478
  "479",                       // 479
  "480",                       // 480
  "481",                       // 481
  "482",                       // 482
  "483",                       // 483
  "484",                       // 484
  "485",                       // 485
  "486",                       // 486
  "487",                       // 487
  "488",                       // 488
  "489",                       // 489
  "490",                       // 490
  "491",                       // 491
  "492",                       // 492
  "493",                       // 493
  "494",                       // 494
  "495",                       // 495
  "496",                       // 496
  "497",                       // 497
  "498",                       // 498
  "499",                       // 499
  "500",                       // 500
  "501",                       // 501
  "502",                       // 502
  "503",                       // 503
  "504",                       // 504
  "505",                       // 505
  "Ndebele",                   // 506 nr

  // Pseudolanguages (and Klingon)
  "Swedish Chef idiolect",     // 507 qab
  "Pig Latin",                 // 508 qap
  "L33tsp34k",                 // 509 qah
  "Klingon",                   // 510 tlh
  "Elmer Fudd idiolect",       // 511 qae

  // Neither language nor script is recognized
  "Unknown script",            // 512 und-Zyyy

  // Script is recognized, language isn't
  "Latin script",                  // 513 und-Latn
  "Greek script",                  // 514 und-Grek
  "Cyrillic script",               // 515 und-Cyrl
  "Armenian script",               // 516 und-Armn
  "Hebrew script",                 // 517 und-Hebr
  "Arabic script",                 // 518 und-Arab
  "Syriac script",                 // 519 und-Syrc
  "Thaana script",                 // 520 und-Thaa
  "Devanagari script",             // 521 und-Deva
  "Bengali script",                // 522 und-Beng
  "Gurmukhi script",               // 523 und-Guru
  "Gujarati script",               // 524 und-Gujr
  "Oriya script",                  // 525 und-Orya
  "Tamil script",                  // 526 und-Taml
  "Telugu script",                 // 527 und-Telu
  "Kannada script",                // 528 und-Knda
  "Malayalam script",              // 529 und-Mlym
  "Sinhala script",                // 530 und-Sinh
  "Thai script",                   // 531 und-Thai
  "Lao script",                    // 532 und-Laoo
  "Tibetan script",                // 533 und-Tibt
  "Myanmar script",                // 534 und-Mymr
  "Georgian script",               // 535 und-Geor
  "Hangul script",                 // 536 und-Hang
  "Ethiopic script",               // 537 und-Ethi
  "Cherokee script",               // 538 und-Cher
  "Canadian Aboriginal script",    // 539 und-Cans
  "Ogham script",                  // 540 und-Ogam
  "Runic script",                  // 541 und-Runr
  "Khmer script",                  // 542 und-Khmr
  "Mongolian script",              // 543 und-Mong
  "Hiragana script",               // 544 und-Hira
  "Katakana script",               // 545 und-Kana
  "Bopomofo script",               // 546 und-Bopo
  "Han script",                    // 547 und-Hani
  "Yi script",                     // 548 und-Yiii
  "Old Italic script",             // 549 und-Ital
  "Gothic script",                 // 550 und-Goth
  "Deseret script",                // 551 und-Dsrt
  "Inherited script",              // 552 und-Qaai
  "Tagalog script",                // 553 und-Tglg
  "Hanunoo script",                // 554 und-Hano
  "Buhid script",                  // 555 und-Buhd
  "Tagbanwa script",               // 556 und-Tagb
  "Limbu script",                  // 557 und-Limb
  "Tai Le script",                 // 558 und-Tale
  "Linear B script",               // 559 und-Linb
  "Ugaritic script",               // 560 und-Ugar
  "Shavian script",                // 561 und-Shaw
  "Osmanya script",                // 562 und-Osma
  "Cypriot script",                // 563 und-Cprt
  "Braille script",                // 564 und-Brai
  "Buginese script",               // 565 und-Bugi
  "Coptic script",                 // 566 und-Copt
  "New Tai Lue script",            // 567 und-Talu
  "Glagolitic script",             // 568 und-Glag
  "Tifinagh script",               // 569 und-Tfng
  "Syloti Nagri script",           // 570 und-Sylo
  "Old Persian script",            // 571 und-Xpeo
  "Kharoshthi script",             // 572 und-Khar
  "Balinese script",               // 573 und-Bali
  "Cuneiform script",              // 574 und-Xsux
  "Phoenician script",             // 575 und-Phnx
  "Phags Pa script",               // 576 und-Phag
  "Nko script",                    // 577 und-Nkoo
  "Sundanese script",              // 578 und-Sund
  "Lepcha script",                 // 579 und-Lepc
  "Ol Chiki script",               // 580 und-Olck
  "Vai script",                    // 581 und-Vaii
  "Saurashtra script",             // 582 und-Saur
  "Kayah Li script",               // 583 und-Kali
  "Rejang script",                 // 584 und-Rjng
  "Lycian script",                 // 585 und-Lyci
  "Carian script",                 // 586 und-Cari
  "Lydian script",                 // 587 und-Lydi
  "Cham script",                   // 588 und-Cham
  "Tai Tham script",               // 589 und-Lana
  "Tai Viet script",               // 590 und-Tavt
  "Avestan script",                // 591 und-Avst
  "Egyptian Hieroglyphic script",  // 592 und-Egyp
  "Samaritan script",              // 593 und-Samr
  "Lisu script",                   // 594 und-Lisu
  "Bamum script",                  // 595 und-Bamu
  "Javanese script",               // 596 und-Java
  "Meetei Mayek script",           // 597 und-Mtei
  "Imperial Aramaic script",       // 598 und-Armi
  "Old South Arabian script",      // 599 und-Sarb
  "Inscriptional Parthian script", // 600 und-Prti
  "Inscriptional Pahlavi script",  // 601 und-Phli
  "Old Turkic script",             // 602 und-Orkh
  "Kaithi script",                 // 603 und-Kthi
  "Batak script",                  // 604 und-Batk
  "Brahmi script",                 // 605 und-Brah
  "Mandaic script",                // 606 und-Mand
  "Chakma script",                 // 607 und-Cakm
  "Meroitic Cursive script",       // 608 und-Merc
  "Meroitic Hieroglyphs script",   // 609 und-Mero
  "Miao script",                   // 610 und-Plrd
  "Sharada script",                // 611 und-Shrd
  "Sora Sompeng script",           // 612 und-Sora
  "Takri script",                  // 613 und-Takr
};

// Subscripted by enum Language
static const int kLanguageToCodeSize = 614;
extern const char* const kLanguageToCode[kLanguageToCodeSize] = {
  "en",    // 0 English
  "da",    // 1 Danish
  "nl",    // 2 Dutch
  "fi",    // 3 Finnish
  "fr",    // 4 French
  "de",    // 5 German
  "iw",    // 6 Hebrew
  "it",    // 7 Italian
  "ja",    // 8 Japanese
  "ko",    // 9 Korean
  "no",    // 10 Norwegian
  "pl",    // 11 Polish
  "pt",    // 12 Portuguese
  "ru",    // 13 Russian
  "es",    // 14 Spanish
  "sv",    // 15 Swedish
  "zh",    // 16 Chinese
  "cs",    // 17 Czech
  "el",    // 18 Greek
  "is",    // 19 Icelandic
  "lv",    // 20 Latvian
  "lt",    // 21 Lithuanian
  "ro",    // 22 Romanian
  "hu",    // 23 Hungarian
  "et",    // 24 Estonian
  "zxx",   // 25 Ignore
  "und",   // 26 Unknown
  "bg",    // 27 Bulgarian
  "hr",    // 28 Croatian
  "sr",    // 29 Serbian
  "ga",    // 30 Irish
  "gl",    // 31 Galician
  "tl",    // 32 Tagalog
  "tr",    // 33 Turkish
  "uk",    // 34 Ukrainian
  "hi",    // 35 Hindi
  "mk",    // 36 Macedonian
  "bn",    // 37 Bengali
  "id",    // 38 Indonesian
  "la",    // 39 Latin
  "ms",    // 40 Malay
  "ml",    // 41 Malayalam
  "cy",    // 42 Welsh
  "ne",    // 43 Nepali
  "te",    // 44 Telugu
  "sq",    // 45 Albanian
  "ta",    // 46 Tamil
  "be",    // 47 Belarusian
  "jw",    // 48 Javanese
  "oc",    // 49 Occitan
  "ur",    // 50 Urdu
  "bh",    // 51 Bihari
  "gu",    // 52 Gujarati
  "th",    // 53 Thai
  "ar",    // 54 Arabic
  "ca",    // 55 Catalan
  "eo",    // 56 Esperanto
  "eu",    // 57 Basque
  "ia",    // 58 Interlingua
  "kn",    // 59 Kannada
  "pa",    // 60 Punjabi
  "gd",    // 61 Scots Gaelic
  "sw",    // 62 Swahili
  "sl",    // 63 Slovenian
  "mr",    // 64 Marathi
  "mt",    // 65 Maltese
  "vi",    // 66 Vietnamese
  "fy",    // 67 Frisian
  "sk",    // 68 Slovak
  "zh-Hant",  // 69 ChineseT
  "fo",    // 70 Faroese
  "su",    // 71 Sundanese
  "uz",    // 72 Uzbek
  "am",    // 73 Amharic
  "az",    // 74 Azerbaijani
  "ka",    // 75 Georgian
  "ti",    // 76 Tigrinya
  "fa",    // 77 Persian
  "bs",    // 78 Bosnian
  "si",    // 79 Sinhalese
  "nn",    // 80 Norwegian Nynorsk
  "",      // 81 81
  "",      // 82 82
  "xh",    // 83 Xhosa
  "zu",    // 84 Zulu
  "gn",    // 85 Guarani
  "st",    // 86 Sesotho
  "tk",    // 87 Turkmen
  "ky",    // 88 Kyrgyz
  "br",    // 89 Breton
  "tw",    // 90 Twi
  "yi",    // 91 Yiddish
  "",      // 92 92
  "so",    // 93 Somali
  "ug",    // 94 Uighur
  "ku",    // 95 Kurdish
  "mn",    // 96 Mongolian
  "hy",    // 97 Armenian
  "lo",    // 98 Laothian
  "sd",    // 99 Sindhi
  "rm",    // 100 Romansh
  "af",    // 101 Afrikaans
  "lb",    // 102 Luxembourgish
  "my",    // 103 Burmese
  "km",    // 104 Khmer
  "bo",    // 105 Tibetan
  "dv",    // 106 Dhivehi
  "chr",   // 107 Cherokee
  "syr",   // 108 Syriac
  "lif",   // 109 Limbu
  "or",    // 110 Oriya
  "as",    // 111 Assamese
  "co",    // 112 Corsican
  "ie",    // 113 Interlingue
  "kk",    // 114 Kazakh
  "ln",    // 115 Lingala
  "",      // 116 116
  "ps",    // 117 Pashto
  "qu",    // 118 Quechua
  "sn",    // 119 Shona
  "tg",    // 120 Tajik
  "tt",    // 121 Tatar
  "to",    // 122 Tonga
  "yo",    // 123 Yoruba
  "",      // 124 124
  "",      // 125 125
  "",      // 126 126
  "",      // 127 127
  "mi",    // 128 Maori
  "wo",    // 129 Wolof
  "ab",    // 130 Abkhazian
  "aa",    // 131 Afar
  "ay",    // 132 Aymara
  "ba",    // 133 Bashkir
  "bi",    // 134 Bislama
  "dz",    // 135 Dzongkha
  "fj",    // 136 Fijian
  "kl",    // 137 Greenlandic
  "ha",    // 138 Hausa
  "ht",    // 139 Haitian Creole
  "ik",    // 140 Inupiak
  "iu",    // 141 Inuktitut
  "ks",    // 142 Kashmiri
  "rw",    // 143 Kinyarwanda
  "mg",    // 144 Malagasy
  "na",    // 145 Nauru
  "om",    // 146 Oromo
  "rn",    // 147 Rundi
  "sm",    // 148 Samoan
  "sg",    // 149 Sango
  "sa",    // 150 Sanskrit
  "ss",    // 151 Siswant
  "ts",    // 152 Tsonga
  "tn",    // 153 Tswana
  "vo",    // 154 Volapuk
  "za",    // 155 Zhuang
  "kha",   // 156 Khasi
  "sco",   // 157 Scots
  "lg",    // 158 Ganda
  "gv",    // 159 Manx
  "sr-ME", // 160 Montenegrin
  "ak",    // 161 AKan
  "ig",    // 162 Igbo
  "mfe",   // 163 Mauritian Creole
  "haw",   // 164 Hawaiian
  "ceb",   // 165 Cebuano
  "ee",    // 166 Ewe
  "gaa",   // 167 Ga
  "hmn",   // 168 Hmong
  "kri",   // 169 Krio
  "loz",   // 170 Lozi
  "lua",   // 171 Tshiluba
  "luo",   // 172 Dholuo
  "new",   // 173 Newari
  "ny",    // 174 Nyanja
  "os",    // 175 Ossetian
  "pam",   // 176 Pampanga
  "nso",   // 177 Pedi
  "raj",   // 178 Rajasthani
  "crs",   // 179 Seselwa
  "tum",   // 180 Tumbuka
  "ve",    // 181 Venda
  "war",   // 182 Waray-Waray
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
  "nr",    // 506 Ndebele
  "qab",   // 507 Swedish Chef idiolect
  "qap",   // 508 Pig Latin
  "qah",   // 509 L33tsp34k
  "tlh",   // 510 Klingon
  "qae",   // 511 Elmer Fudd idiolect

  // neither script nor language is recognized
  "und-Zyyy",  // 512 Unknown script

  // script recognized, language unrecognized
  "und-Latn",  // 513 Latin script
  "und-Grek",  // 514 Greek script
  "und-Cyrl",  // 515 Cyrillic script
  "und-Armn",  // 516 Armenian script
  "und-Hebr",  // 517 Hebrew script
  "und-Arab",  // 518 Arabic script
  "und-Syrc",  // 519 Syriac script
  "und-Thaa",  // 520 Thaana script
  "und-Deva",  // 521 Devanagari script
  "und-Beng",  // 522 Bengali script
  "und-Guru",  // 523 Gurmukhi script
  "und-Gujr",  // 524 Gujarati script
  "und-Orya",  // 525 Oriya script
  "und-Taml",  // 526 Tamil script
  "und-Telu",  // 527 Telugu script
  "und-Knda",  // 528 Kannada script
  "und-Mlym",  // 529 Malayalam script
  "und-Sinh",  // 530 Sinhala script
  "und-Thai",  // 531 Thai script
  "und-Laoo",  // 532 Lao script
  "und-Tibt",  // 533 Tibetan script
  "und-Mymr",  // 534 Myanmar script
  "und-Geor",  // 535 Georgian script
  "und-Hang",  // 536 Hangul script
  "und-Ethi",  // 537 Ethiopic script
  "und-Cher",  // 538 Cherokee script
  "und-Cans",  // 539 Canadian Aboriginal script
  "und-Ogam",  // 540 Ogham script
  "und-Runr",  // 541 Runic script
  "und-Khmr",  // 542 Khmer script
  "und-Mong",  // 543 Mongolian script
  "und-Hira",  // 544 Hiragana script
  "und-Kana",  // 545 Katakana script
  "und-Bopo",  // 546 Bopomofo script
  "und-Hani",  // 547 Han script
  "und-Yiii",  // 548 Yi script
  "und-Ital",  // 549 Old Italic script
  "und-Goth",  // 550 Gothic script
  "und-Dsrt",  // 551 Deseret script
  "und-Qaai",  // 552 Inherited script
  "und-Tglg",  // 553 Tagalog script
  "und-Hano",  // 554 Hanunoo script
  "und-Buhd",  // 555 Buhid script
  "und-Tagb",  // 556 Tagbanwa script
  "und-Limb",  // 557 Limbu script
  "und-Tale",  // 558 Tai Le script
  "und-Linb",  // 559 Linear B script
  "und-Ugar",  // 560 Ugaritic script
  "und-Shaw",  // 561 Shavian script
  "und-Osma",  // 562 Osmanya script
  "und-Cprt",  // 563 Cypriot script
  "und-Brai",  // 564 Braille script
  "und-Bugi",  // 565 Buginese script
  "und-Copt",  // 566 Coptic script
  "und-Talu",  // 567 New Tai Lue script
  "und-Glag",  // 568 Glagolitic script
  "und-Tfng",  // 569 Tifinagh script
  "und-Sylo",  // 570 Syloti Nagri script
  "und-Xpeo",  // 571 Old Persian script
  "und-Khar",  // 572 Kharoshthi script
  "und-Bali",  // 573 Balinese script
  "und-Xsux",  // 574 Cuneiform script
  "und-Phnx",  // 575 Phoenician script
  "und-Phag",  // 576 Phags Pa script
  "und-Nkoo",  // 577 Nko script
  "und-Sund",  // 578 Sundanese script
  "und-Lepc",  // 579 Lepcha script
  "und-Olck",  // 580 Ol Chiki script
  "und-Vaii",  // 581 Vai script
  "und-Saur",  // 582 Saurashtra script
  "und-Kali",  // 583 Kayah Li script
  "und-Rjng",  // 584 Rejang script
  "und-Lyci",  // 585 Lycian script
  "und-Cari",  // 586 Carian script
  "und-Lydi",  // 587 Lydian script
  "und-Cham",  // 588 Cham script
  "und-Lana",  // 589 Tai Tham script
  "und-Tavt",  // 590 Tai Viet script
  "und-Avst",  // 591 Avestan script
  "und-Egyp",  // 592 Egyptian Hieroglyphs script
  "und-Samr",  // 593 Samaritan script
  "und-Lisu",  // 594 Lisu script
  "und-Bamu",  // 595 Bamum script
  "und-Java",  // 596 Javanese script
  "und-Mtei",  // 597 Meetei Mayek script
  "und-Armi",  // 598 Imperial Aramaic script
  "und-Sarb",  // 599 Old South Arabian script
  "und-Prti",  // 600 Inscriptional Parthian script
  "und-Phli",  // 601 Inscriptional Pahlavi script
  "und-Orkh",  // 602 Old Turkic script
  "und-Kthi",  // 603 Kaithi script
  "und-Batk",  // 604 Batak script
  "und-Brah",  // 605 Brahmi script
  "und-Mand",  // 606 Mandaic script
  "und-Cakm",  // 607 Chakma script
  "und-Merc",  // 608 Meroitic Cursive script
  "und-Mero",  // 609 Meroitic Hieroglyphs script
  "und-Plrd",  // 610 Miao script
  "und-Shrd",  // 611 Sharada script
  "und-Sora",  // 612 Sora Sompeng script
  "und-Takr",  // 613 Takri script
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
