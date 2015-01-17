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
// These are weird things we need to do to get this compiling on
// random systems [subset].

#ifndef BASE_PORT_H_
#define BASE_PORT_H_

#include <string.h>
#include <stdint.h>

namespace CLD2 {

// Portable handling of unaligned load and store.
// Do not attempt to do clever things with casts -- even if that happens to
// work on a particular architecture it's a TBAA violation.

inline uint32_t UNALIGNED_LOAD32(const void *p) {
  uint32_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UNALIGNED_STORE32(void *p, uint32_t v) {
  memcpy(p, &v, sizeof v);
}

}       // End namespace CLD2

#endif  // BASE_PORT_H_
