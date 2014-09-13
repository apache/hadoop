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

#ifndef STRINGUTIL_H_
#define STRINGUTIL_H_

#include <stdint.h>
#include <vector>
#include <string>

namespace NativeTask {

using std::vector;
using std::string;

class StringUtil {
public:
  static string ToString(int32_t v);
  static string ToString(uint32_t v);
  static string ToString(int64_t v);
  static string ToString(int64_t v, char pad, int64_t len);
  static string ToString(uint64_t v);
  static string ToString(bool v);
  static string ToString(float v);
  static string ToString(double v);
  static string ToHexString(const void * v, uint32_t len);

  static int64_t toInt(const string & str);
  static bool toBool(const string & str);
  static float toFloat(const string & str);

  static string Format(const char * fmt, ...);

  static void Format(string & dest, const char * fmt, ...);

  static string ToLower(const string & name);

  static string Trim(const string & str);

  static void Split(const string & src, const string & sep, vector<string> & dest,
      bool clean = false);

  static string Join(const vector<string> & strs, const string & sep);

  static bool StartsWith(const string & str, const string & prefix);
  static bool EndsWith(const string & str, const string & suffix);
};

} // namespace NativeTask

#endif /* STRINGUTIL_H_ */
