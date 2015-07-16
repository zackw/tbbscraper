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


#ifndef I18N_ENCODINGS_CLD2_INTERNAL_GETONESCRIPTSPAN_H_
#define I18N_ENCODINGS_CLD2_INTERNAL_GETONESCRIPTSPAN_H_

#include <cstddef>
#include "langspan.h"

namespace CLD2 {

class ScriptScanner {
 public:
  ScriptScanner(const char* buffer, std::size_t buffer_length);
  ~ScriptScanner()
    {
      delete[] script_buffer_;
      delete[] script_buffer_lower_;
    }

  // Copy next run of same-script non-tag letters to buffer [NUL terminated]
  // Force Latin and Cyrillic scripts to be lowercase
  bool GetOneScriptSpanLower(LangSpan* span);

 private:

  // Copy next run of same-script non-tag letters to buffer [NUL terminated]
  bool GetOneScriptSpan(LangSpan* span);

  // Force Latin and Cyrillic scripts to be lowercase
  void LowerScriptSpan(LangSpan* span);

  // Skip over tags and non-letters
  int SkipToFrontOfSpan(const char* src, int len, int* script);

  const char* start_byte_;        // Starting byte of buffer to scan
  const char* next_byte_;         // First unscanned byte
  std::size_t byte_length_;       // Bytes left: next_byte_limit_ - next_byte_

  char* script_buffer_;           // Holds text with expanded entities
  char* script_buffer_lower_;     // Holds lowercased text
};

}  // namespace CLD2

#endif  // I18N_ENCODINGS_CLD2_INTERNAL_GETONESCRIPTSPAN_H_

