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

#include "scoreonescriptspan.h"

#include <stdint.h>

#include "cldutil.h"
#include "lang_script.h"
#include "tote.h"

namespace CLD2 {

static void AddLangProb(uint32_t langprob, Tote* chunk_tote) {
  ProcessProbV2Tote(langprob, chunk_tote);
}

static void ZeroPSLang(uint32_t langprob, Tote* chunk_tote) {
  uint8_t top1 = (langprob >> 8) & 0xff;
  chunk_tote->SetScore(top1, 0);
}

static bool SameCloseSet(Language lang1, Language lang2) {
  int lang1_close_set = LanguageCloseSet(lang1);
  if (lang1_close_set == 0) {return false;}
  int lang2_close_set = LanguageCloseSet(lang2);
  return (lang1_close_set == lang2_close_set);
}


// Needs expected score per 1KB in scoring context
static void SetChunkSummary(ULScript ulscript, int first_linear_in_chunk,
                     int offset, int len,
                     const ScoringContext* scoringcontext,
                     const Tote* chunk_tote,
                     ChunkSummary* chunksummary) {
  int key3[3];
  chunk_tote->CurrentTopThreeKeys(key3);
  Language lang1 = FromPerScriptNumber(ulscript, key3[0]);
  Language lang2 = FromPerScriptNumber(ulscript, key3[1]);

  int actual_score_per_kb = 0;
  if (len > 0) {
    actual_score_per_kb = (chunk_tote->GetScore(key3[0]) << 10) / len;
  }
  int expected_subscr = lang1 * 4 + LScript4(ulscript);
  int expected_score_per_kb =
     scoringcontext->scoringtables->kExpectedScore[expected_subscr];

  chunksummary->offset = offset;
  chunksummary->chunk_start = first_linear_in_chunk;
  chunksummary->lang1 = lang1;
  chunksummary->lang2 = lang2;
  chunksummary->score1 = chunk_tote->GetScore(key3[0]);
  chunksummary->score2 = chunk_tote->GetScore(key3[1]);
  chunksummary->bytes = len;
  chunksummary->grams = chunk_tote->GetScoreCount();
  chunksummary->ulscript = ulscript;
  chunksummary->reliability_delta = ReliabilityDelta(chunksummary->score1,
                                                     chunksummary->score2,
                                                     chunksummary->grams);
  // If lang1/lang2 in same close set, set delta reliability to 100%
  if (SameCloseSet(lang1, lang2)) {
    chunksummary->reliability_delta = 100;
  }
  chunksummary->reliability_score =
     ReliabilityExpected(actual_score_per_kb, expected_score_per_kb);
}

// Update scoring context distinct_boost for distinct octagram
// Keep last 4 used. Since these are mostly (except at splices) in
// hitbuffer, we might be able to just use a subscript and splice
static void AddDistinctBoost2(uint32_t langprob, ScoringContext* scoringcontext) {
// this is called 0..n times per chunk with decoded hitbuffer->distinct...
  LangBoosts* distinct_boost = &scoringcontext->distinct_boost.latn;
  if (scoringcontext->ulscript != ULScript_Latin) {
    distinct_boost = &scoringcontext->distinct_boost.othr;
  }
  int n = distinct_boost->n;
  distinct_boost->langprob[n] = langprob;
  distinct_boost->n = distinct_boost->wrap(n + 1);
}

// For each chunk, add extra weight for language priors (from content-lang and
// meta lang=xx) and distinctive tokens
static void ScoreBoosts(const ScoringContext* scoringcontext, Tote* chunk_tote) {
  // Get boosts for current script
  const LangBoosts* langprior_boost = &scoringcontext->langprior_boost.latn;
  const LangBoosts* langprior_whack = &scoringcontext->langprior_whack.latn;
  const LangBoosts* distinct_boost = &scoringcontext->distinct_boost.latn;
  if (scoringcontext->ulscript != ULScript_Latin) {
    langprior_boost = &scoringcontext->langprior_boost.othr;
    langprior_whack = &scoringcontext->langprior_whack.othr;
    distinct_boost = &scoringcontext->distinct_boost.othr;
  }

  for (int k = 0; k < kMaxBoosts; ++k) {
    uint32_t langprob = langprior_boost->langprob[k];
    if (langprob > 0) {AddLangProb(langprob, chunk_tote);}
  }
  for (int k = 0; k < kMaxBoosts; ++k) {
    uint32_t langprob = distinct_boost->langprob[k];
    if (langprob > 0) {AddLangProb(langprob, chunk_tote);}
  }
  // boost has a packed set of per-script langs and probabilites
  // whack has a packed set of per-script lang to be suppressed (zeroed)
  // When a language in a close set is given as an explicit hint, others in
  //  that set will be whacked here.
  for (int k = 0; k < kMaxBoosts; ++k) {
    uint32_t langprob = langprior_whack->langprob[k];
    if (langprob > 0) {ZeroPSLang(langprob, chunk_tote);}
  }
}

// Score all the bases, deltas, distincts, boosts for one chunk into chunk_tote
// After last chunk there is always a hitbuffer entry with an offset just off
// the end of the text.
// Sets delta_len, and distinct_len
static void ScoreOneChunk(ULScript ulscript,
                   const ScoringHitBuffer* hitbuffer,
                   int chunk_i,
                   ScoringContext* scoringcontext,
                   ChunkSpan* cspan, Tote* chunk_tote,
                   ChunkSummary* chunksummary) {
  int first_linear_in_chunk = hitbuffer->chunk_start[chunk_i];
  int first_linear_in_next_chunk = hitbuffer->chunk_start[chunk_i + 1];

  chunk_tote->Reinit();
  cspan->delta_len = 0;
  cspan->distinct_len = 0;

  // 2013.02.05 linear design: just use base and base_len for the span
  cspan->chunk_base = first_linear_in_chunk;
  cspan->base_len = first_linear_in_next_chunk - first_linear_in_chunk;
  for (int i = first_linear_in_chunk; i < first_linear_in_next_chunk; ++i) {
    uint32_t langprob = hitbuffer->linear[i].langprob;
    AddLangProb(langprob, chunk_tote);
    if (hitbuffer->linear[i].type <= QUADHIT) {
      chunk_tote->AddScoreCount();      // Just count quads, not octas
    }
    if (hitbuffer->linear[i].type == DISTINCTHIT) {
      AddDistinctBoost2(langprob, scoringcontext);
    }
  }

  // Score language prior boosts
  // Score distinct word boost
  ScoreBoosts(scoringcontext, chunk_tote);

  int lo = hitbuffer->linear[first_linear_in_chunk].offset;
  int hi = hitbuffer->linear[first_linear_in_next_chunk].offset;

  // Chunk_tote: get top langs, scores, etc. and fill in chunk summary
  SetChunkSummary(ulscript, first_linear_in_chunk, lo, hi - lo,
                  scoringcontext, chunk_tote, chunksummary);

  scoringcontext->prior_chunk_lang = static_cast<Language>(chunksummary->lang1);
}


// Score chunks of text described by hitbuffer, allowing each to be in a
// different language, and optionally adjusting the boundaries inbetween.
// Set last_cspan to the last chunkspan used
static void ScoreAllHits(ULScript ulscript,
                         const ScoringHitBuffer* hitbuffer,
                         ScoringContext* scoringcontext,
                         SummaryBuffer* summarybuffer, ChunkSpan* last_cspan) {
  ChunkSpan prior_cspan = {0, 0, 0, 0, 0, 0};
  ChunkSpan cspan = {0, 0, 0, 0, 0, 0};

  for (int i = 0; i < hitbuffer->next_chunk_start; ++i) {
    // Score one chunk
    // Sets delta_len, and distinct_len
    Tote chunk_tote;
    ChunkSummary chunksummary;
    ScoreOneChunk(ulscript,
                  hitbuffer, i,
                  scoringcontext, &cspan, &chunk_tote, &chunksummary);

    // Put result in summarybuffer
    if (summarybuffer->n < kMaxSummaries) {
      summarybuffer->chunksummary[summarybuffer->n] = chunksummary;
      summarybuffer->n += 1;
    }

    prior_cspan = cspan;
    cspan.chunk_base += cspan.base_len;
    cspan.chunk_delta += cspan.delta_len;
    cspan.chunk_distinct += cspan.distinct_len;
  }

  // Add one dummy off the end to hold first unused linear_in_chunk
  int linear_off_end = hitbuffer->next_linear;
  int offset_off_end = hitbuffer->linear[linear_off_end].offset;
  ChunkSummary* cs = &summarybuffer->chunksummary[summarybuffer->n];
  memset(cs, 0, sizeof(ChunkSummary));
  cs->offset = offset_off_end;
  cs->chunk_start = linear_off_end;
  *last_cspan = prior_cspan;
}

static void SummaryBufferToDocTote(const SummaryBuffer* summarybuffer,
                                   DocTote* doc_tote) {
  int cs_bytes_sum = 0;
  for (int i = 0; i < summarybuffer->n; ++i) {
    const ChunkSummary* cs = &summarybuffer->chunksummary[i];
    int reliability = minint(cs->reliability_delta, cs->reliability_score);
    // doc_tote uses full languages
    doc_tote->Add(cs->lang1, cs->bytes, cs->score1, reliability);
    cs_bytes_sum += cs->bytes;
  }
}

// Make a langprob that gives small weight to the default language for ulscript
static uint32_t DefaultLangProb(ULScript ulscript) {
  Language default_lang = DefaultLanguage(ulscript);
  return MakeLangProb(default_lang, 1);
}

// Effectively, do a merge-sort based on text offsets
// Look up each indirect value in appropriate scoring table and keep
// just the resulting langprobs
static void LinearizeAll(ScoringContext* scoringcontext, bool score_cjk,
                  ScoringHitBuffer* hitbuffer) {
  const CLD2TableSummary* base_obj;       // unigram or quadgram
  const CLD2TableSummary* base_obj2;      // quadgram dual table
  const CLD2TableSummary* delta_obj;      // bigram or octagram
  const CLD2TableSummary* distinct_obj;   // bigram or octagram
  uint16_t base_hit;
  if (score_cjk) {
    base_obj = scoringcontext->scoringtables->unigram_compat_obj;
    base_obj2 = scoringcontext->scoringtables->unigram_compat_obj;
    delta_obj = scoringcontext->scoringtables->deltabi_obj;
    distinct_obj = scoringcontext->scoringtables->distinctbi_obj;
    base_hit = UNIHIT;
  } else {
    base_obj = scoringcontext->scoringtables->quadgram_obj;
    base_obj2 = scoringcontext->scoringtables->quadgram_obj2;
    delta_obj = scoringcontext->scoringtables->deltaocta_obj;
    distinct_obj = scoringcontext->scoringtables->distinctocta_obj;
    base_hit = QUADHIT;
  }

  int base_limit = hitbuffer->next_base;
  int delta_limit = hitbuffer->next_delta;
  int distinct_limit = hitbuffer->next_distinct;
  int base_i = 0;
  int delta_i = 0;
  int distinct_i = 0;
  int linear_i = 0;

  // Start with an initial base hit for the default language for this script
  // Inserting this avoids edge effects with no hits at all
  hitbuffer->linear[linear_i].offset = hitbuffer->lowest_offset;
  hitbuffer->linear[linear_i].type = base_hit;
  hitbuffer->linear[linear_i].langprob =
    DefaultLangProb(scoringcontext->ulscript);
  ++linear_i;

  while ((base_i < base_limit) || (delta_i < delta_limit) ||
         (distinct_i < distinct_limit)) {
    int base_off = hitbuffer->base[base_i].offset;
    int delta_off = hitbuffer->delta[delta_i].offset;
    int distinct_off = hitbuffer->distinct[distinct_i].offset;

    // Do delta and distinct first, so that they are not lost at base_limit
    if ((delta_i < delta_limit) &&
        (delta_off <= base_off) && (delta_off <= distinct_off)) {
      // Add delta entry
      int indirect = hitbuffer->delta[delta_i].indirect;
      ++delta_i;
      uint32_t langprob = delta_obj->kCLDTableInd[indirect];
      if (langprob > 0) {
        hitbuffer->linear[linear_i].offset = delta_off;
        hitbuffer->linear[linear_i].type = DELTAHIT;
        hitbuffer->linear[linear_i].langprob = langprob;
        ++linear_i;
      }
    }
    else if ((distinct_i < distinct_limit) &&
             (distinct_off <= base_off) && (distinct_off <= delta_off)) {
      // Add distinct entry
      int indirect = hitbuffer->distinct[distinct_i].indirect;
      ++distinct_i;
      uint32_t langprob = distinct_obj->kCLDTableInd[indirect];
      if (langprob > 0) {
        hitbuffer->linear[linear_i].offset = distinct_off;
        hitbuffer->linear[linear_i].type = DISTINCTHIT;
        hitbuffer->linear[linear_i].langprob = langprob;
        ++linear_i;
      }
    }
    else {
      // Add one or two base entries
      int indirect = hitbuffer->base[base_i].indirect;
      // First, get right scoring table
      const CLD2TableSummary* local_base_obj = base_obj;
      if ((indirect & 0x80000000u) != 0) {
        local_base_obj = base_obj2;
        indirect &= ~0x80000000u;
      }
      ++base_i;
      // One langprob in kQuadInd[0..SingleSize),
      // two in kQuadInd[SingleSize..Size)
      if (indirect < static_cast<int>(local_base_obj->kCLDTableSizeOne)) {
        // Up to three languages at indirect
        uint32_t langprob = local_base_obj->kCLDTableInd[indirect];
        if (langprob > 0) {
          hitbuffer->linear[linear_i].offset = base_off;
          hitbuffer->linear[linear_i].type = base_hit;
          hitbuffer->linear[linear_i].langprob = langprob;
          ++linear_i;
        }
      } else {
        // Up to six languages at start + 2 * (indirect - start)
        indirect += (indirect - local_base_obj->kCLDTableSizeOne);
        uint32_t langprob = local_base_obj->kCLDTableInd[indirect];
        uint32_t langprob2 = local_base_obj->kCLDTableInd[indirect + 1];
        if (langprob > 0) {
          hitbuffer->linear[linear_i].offset = base_off;
          hitbuffer->linear[linear_i].type = base_hit;
          hitbuffer->linear[linear_i].langprob = langprob;
          ++linear_i;
        }
        if (langprob2 > 0) {
          hitbuffer->linear[linear_i].offset = base_off;
          hitbuffer->linear[linear_i].type = base_hit;
          hitbuffer->linear[linear_i].langprob = langprob2;
          ++linear_i;
        }
      }
    }
  }

  // Update
  hitbuffer->next_linear = linear_i;

  // Add a dummy entry off the end, just to capture final offset
  hitbuffer->linear[linear_i].offset =
  hitbuffer->base[hitbuffer->next_base].offset;
  hitbuffer->linear[linear_i].langprob = 0;
}

// Break linear array into chunks of ~20 quadgram hits or ~50 CJK unigram hits
static void ChunkAll(int letter_offset, bool score_cjk, ScoringHitBuffer* hitbuffer) {
  int chunksize;
  uint16_t base_hit;
  if (score_cjk) {
    chunksize = kChunksizeUnis;
    base_hit = UNIHIT;
  } else {
    chunksize = kChunksizeQuads;
    base_hit = QUADHIT;
  }

  int linear_i = 0;
  int linear_off_end = hitbuffer->next_linear;
  int text_i = letter_offset;               // Next unseen text offset
  int next_chunk_start = 0;
  int bases_left = hitbuffer->next_base;
  while (bases_left > 0) {
    // Linearize one chunk
    int base_len = chunksize;     // Default; may be changed below
    if (bases_left < (chunksize + (chunksize >> 1))) {
      // If within 1.5 chunks of the end, avoid runts by using it all
      base_len = bases_left;
    } else if (bases_left < (2 * chunksize)) {
      // Avoid runts by splitting 1.5 to 2 chunks in half (about 3/4 each)
      base_len = (bases_left + 1) >> 1;
    }

    hitbuffer->chunk_start[next_chunk_start] = linear_i;
    hitbuffer->chunk_offset[next_chunk_start] = text_i;
    ++next_chunk_start;

    int base_count = 0;
    while ((base_count < base_len) && (linear_i < linear_off_end)) {
      if (hitbuffer->linear[linear_i].type == base_hit) {++base_count;}
      ++linear_i;
    }
    text_i = hitbuffer->linear[linear_i].offset;    // Next unseen text offset
    bases_left -= base_len;
  }

  // If no base hits at all, make a single dummy chunk
  if (next_chunk_start == 0) {
     hitbuffer->chunk_start[next_chunk_start] = 0;
     hitbuffer->chunk_offset[next_chunk_start] = hitbuffer->linear[0].offset;
     ++next_chunk_start;
  }

  // Remember the linear array start of dummy entry
  hitbuffer->next_chunk_start = next_chunk_start;

  // Add a dummy entry off the end, just to capture final linear subscr
  hitbuffer->chunk_start[next_chunk_start] = hitbuffer->next_linear;
  hitbuffer->chunk_offset[next_chunk_start] = text_i;
}


// Merge-sort the individual hit arrays, go indirect on the scoring subscripts,
// break linear array into chunks.
//
// Input:
//  hitbuffer base, delta, distinct arrays
// Output:
//  linear array
//  chunk_start array
//
static void LinearizeHitBuffer(int letter_offset,
                        ScoringContext* scoringcontext,
                        bool score_cjk,
                        ScoringHitBuffer* hitbuffer) {
  LinearizeAll(scoringcontext, score_cjk, hitbuffer);
  ChunkAll(letter_offset, score_cjk, hitbuffer);
}



// The hitbuffer is in an awkward form -- three sets of base/delta/distinct
// scores, each with an indirect subscript to one of six scoring tables, some
// of which can yield two langprobs for six languages, others one langprob for
// three languages. The only correlation between base/delta/distinct is their
// offsets into the letters-only text buffer.
//
// SummaryBuffer needs to be built to linear, giving linear offset of start of
// each chunk
//
// So we first do all the langprob lookups and merge-sort by offset to make
// a single linear vector, building a side vector of chunk beginnings as we go.
// The sharpening is simply moving the beginnings, scoring is a simple linear
// sweep, etc.

static void ProcessHitBuffer(const LangSpan& scriptspan,
                      int letter_offset,
                      ScoringContext* scoringcontext,
                      DocTote* doc_tote,
                      bool score_cjk,
                      ScoringHitBuffer* hitbuffer) {

  LinearizeHitBuffer(letter_offset, scoringcontext, score_cjk, hitbuffer);

  SummaryBuffer summarybuffer;
  summarybuffer.n = 0;
  ChunkSpan last_cspan;
  ScoreAllHits(scriptspan.ulscript,
               hitbuffer, scoringcontext, &summarybuffer, &last_cspan);

  SummaryBufferToDocTote(&summarybuffer, doc_tote);
}

static void SpliceHitBuffer(ScoringHitBuffer* hitbuffer, int next_offset) {
  // Splice hitbuffer and summarybuffer for next round. With big chunks and
  // distinctive-word state carried across chunks, we might not need to do this.
  hitbuffer->next_base = 0;
  hitbuffer->next_delta = 0;
  hitbuffer->next_distinct = 0;
  hitbuffer->next_linear = 0;
  hitbuffer->next_chunk_start = 0;
  hitbuffer->lowest_offset = next_offset;
}


// Score RTypeNone or RTypeOne scriptspan into doc_tote, updating
// scoringcontext
static void ScoreEntireScriptSpan(const LangSpan& scriptspan,
                           ScoringContext* scoringcontext,
                           DocTote* doc_tote) {
  int bytes = scriptspan.text_bytes;
  // Artificially set score to 1024 per 1KB, or 1 per byte
  int score = bytes;
  int reliability = 100;
  // doc_tote uses full languages
  Language one_one_lang = DefaultLanguage(scriptspan.ulscript);
  doc_tote->Add(one_one_lang, bytes, score, reliability);

  scoringcontext->prior_chunk_lang = UNKNOWN_LANGUAGE;
}

// Score RTypeCJK scriptspan into doc_tote, updating scoringcontext
static void ScoreCJKScriptSpan(const LangSpan& scriptspan,
                        ScoringContext* scoringcontext,
                        DocTote* doc_tote) {
  // Allocate three parallel arrays of scoring hits
  ScoringHitBuffer* hitbuffer = new ScoringHitBuffer;
  hitbuffer->init();
  hitbuffer->ulscript = scriptspan.ulscript;

  scoringcontext->prior_chunk_lang = UNKNOWN_LANGUAGE;
  scoringcontext->oldest_distinct_boost = 0;

  // Incoming scriptspan has a single leading space at scriptspan.text[0]
  // and three trailing spaces then NUL at scriptspan.text[text_bytes + 0/1/2/3]

  int letter_offset = 1;        // Skip initial space
  hitbuffer->lowest_offset = letter_offset;
  int letter_limit = scriptspan.text_bytes;
  while (letter_offset < letter_limit) {
    //
    // Fill up one hitbuffer, possibly splicing onto previous fragment
    //
    // NOTE: GetUniHits deals with close repeats
    // NOTE: After last chunk there is always a hitbuffer entry with an offset
    // just off the end of the text = next_offset.
    int next_offset = GetUniHits(scriptspan.text, letter_offset, letter_limit,
                                  scoringcontext, hitbuffer);
    // NOTE: GetBiHitVectors deals with close repeats,
    // does one hash and two lookups (delta and distinct) per word
    GetBiHits(scriptspan.text, letter_offset, next_offset,
                scoringcontext, hitbuffer);

    //
    // Score one hitbuffer in chunks to summarybuffer
    //
    bool score_cjk = true;
    ProcessHitBuffer(scriptspan, letter_offset, scoringcontext, doc_tote,
                     score_cjk, hitbuffer);
    SpliceHitBuffer(hitbuffer, next_offset);

    letter_offset = next_offset;
  }

  delete hitbuffer;
  // Context across buffers is not connected yet
  scoringcontext->prior_chunk_lang = UNKNOWN_LANGUAGE;
}



// Score RTypeMany scriptspan into doc_tote and vec, updating scoringcontext
// We have a scriptspan with all lowercase text in one script. Look up
// quadgrams and octagrams, saving the hits in three parallel vectors.
// Score from those vectors in chunks, toting each chunk to get a single
// language, and combining into the overall document score. The hit vectors
// in general are not big enough to handle and entire scriptspan, so
// repeat until the entire scriptspan is scored.
// Caller deals with minimizing numbr of runt scriptspans
// This routine deals with minimizing number of runt chunks.
//
// Returns updated scoringcontext
// Returns updated doc_tote
static void ScoreQuadScriptSpan(const LangSpan& scriptspan,
                         ScoringContext* scoringcontext,
                         DocTote* doc_tote) {
  // Allocate three parallel arrays of scoring hits
  ScoringHitBuffer* hitbuffer = new ScoringHitBuffer;
  hitbuffer->init();
  hitbuffer->ulscript = scriptspan.ulscript;

  scoringcontext->prior_chunk_lang = UNKNOWN_LANGUAGE;
  scoringcontext->oldest_distinct_boost = 0;

  // Incoming scriptspan has a single leading space at scriptspan.text[0]
  // and three trailing spaces then NUL at scriptspan.text[text_bytes + 0/1/2/3]

  int letter_offset = 1;        // Skip initial space
  hitbuffer->lowest_offset = letter_offset;
  int letter_limit = scriptspan.text_bytes;
  while (letter_offset < letter_limit) {
    //
    // Fill up one hitbuffer, possibly splicing onto previous fragment
    //
    // NOTE: GetQuadHits deals with close repeats
    // NOTE: After last chunk there is always a hitbuffer entry with an offset
    // just off the end of the text = next_offset.
    int next_offset = GetQuadHits(scriptspan.text, letter_offset, letter_limit,
                                  scoringcontext, hitbuffer);
    // If true, there is more text to process in this scriptspan
    // NOTE: GetOctaHitVectors deals with close repeats,
    // does one hash and two lookups (delta and distinct) per word
    GetOctaHits(scriptspan.text, letter_offset, next_offset,
                scoringcontext, hitbuffer);

    //
    // Score one hitbuffer in chunks to summarybuffer
    //
    bool score_cjk = false;
    ProcessHitBuffer(scriptspan, letter_offset, scoringcontext, doc_tote,
                     score_cjk, hitbuffer);
    SpliceHitBuffer(hitbuffer, next_offset);

    letter_offset = next_offset;
  }

  delete hitbuffer;
}


// Score one scriptspan into doc_tote and vec, updating scoringcontext
// Inputs:
//  One scriptspan of perhaps 40-60KB, all same script lower-case letters
//    and single ASCII spaces. First character is a space to allow simple
//    begining-of-word detect. End of buffer has three spaces and NUL to
//    allow easy scan-to-end-of-word.
//  Scoring context of
//    scoring tables
//    flags
//    running boosts
// Outputs:
//  Updated doc_tote giving overall languages and byte counts
//  Optional updated chunk vector giving offset, length, language
//
// Caller initializes flags, boosts, doc_tote and vec.
// Caller aggregates across multiple scriptspans
// Caller calculates final document result
// Caller deals with detecting and triggering suppression of repeated text.
//
// This top-level routine just chooses the recognition type and calls one of
// the next-level-down routines.
//
void ScoreOneScriptSpan(const LangSpan& scriptspan,
                        ScoringContext* scoringcontext,
                        DocTote* doc_tote) {
  scoringcontext->prior_chunk_lang = UNKNOWN_LANGUAGE;
  scoringcontext->oldest_distinct_boost = 0;
  ULScriptRType rtype = ULScriptRecognitionType(scriptspan.ulscript);
  if (scoringcontext->flags_cld2_score_as_quads && (rtype != RTypeCJK)) {
    rtype = RTypeMany;
  }
  switch (rtype) {
  case RTypeNone:
  case RTypeOne:
    ScoreEntireScriptSpan(scriptspan, scoringcontext, doc_tote);
    break;
  case RTypeCJK:
    ScoreCJKScriptSpan(scriptspan, scoringcontext, doc_tote);
    break;
  case RTypeMany:
    ScoreQuadScriptSpan(scriptspan, scoringcontext, doc_tote);
    break;
  }
}

}       // End namespace CLD2

