/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef COMMON_OPTIONAL_WRAPPER_H_
#define COMMON_OPTIONAL_WRAPPER_H_

#ifdef __clang__
  #pragma clang diagnostic push
  #if __has_warning("-Wweak-vtables")
    #pragma clang diagnostic ignored "-Wweak-vtables"
  #endif
  #if __has_warning("-Wreserved-id-macro")
    #pragma clang diagnostic ignored "-Wreserved-id-macro"
  #endif
  #if __has_warning("-Wextra-semi")
    #pragma clang diagnostic ignored "-Wextra-semi"
  #endif
  #define TR2_OPTIONAL_DISABLE_EMULATION_OF_TYPE_TRAITS  //For Clang < 3_4_2
#endif

#include <optional.hpp>

#ifdef __clang__
  #undef TR2_OPTIONAL_DISABLE_EMULATION_OF_TYPE_TRAITS  //For Clang < 3_4_2
  #pragma clang diagnostic pop
#endif

#endif //COMMON_OPTIONAL_WRAPPER_H_
