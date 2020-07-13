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

#include <stdarg.h>
#include "lib/commons.h"
#include "util/StringUtil.h"

namespace NativeTask {

string StringUtil::ToString(int32_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%d", v);
  return tmp;
}

string StringUtil::ToString(uint32_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%u", v);
  return tmp;
}

string StringUtil::ToString(int64_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%"PRId64, v);
  return tmp;
}

string StringUtil::ToString(int64_t v, char pad, int64_t len) {
  char tmp[32];
  snprintf(tmp, 32, "%%%c%"PRId64""PRId64, pad, len);
  return Format(tmp, v);
}

string StringUtil::ToString(uint64_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%"PRIu64, v);
  return tmp;
}

string StringUtil::ToString(bool v) {
  if (v) {
    return "true";
  } else {
    return "false";
  }
}

string StringUtil::ToString(float v) {
  return Format("%f", v);
}

string StringUtil::ToString(double v) {
  return Format("%lf", v);
}

string StringUtil::ToHexString(const void * v, uint32_t len) {
  string ret = string(len * 2, '0');
  for (uint32_t i = 0; i < len; i++) {
    snprintf(&(ret[i*2]), 3, "%02x", ((char*)v)[i]);
  }
  return ret;
}

bool StringUtil::toBool(const string & str) {
  if (str == "true") {
    return true;
  } else {
    return false;
  }
}

int64_t StringUtil::toInt(const string & str) {
  return strtoll(str.c_str(), NULL, 10);
}

float StringUtil::toFloat(const string & str) {
  return strtof(str.c_str(), NULL);
}

string StringUtil::Format(const char * fmt, ...) {
  char tmp[256];
  string dest;
  va_list al;
  va_start(al, fmt);
  int len = vsnprintf(tmp, 255, fmt, al);
  va_end(al);
  if (len > 255) {
    char * destbuff = new char[len + 1];
    va_start(al, fmt);
    len = vsnprintf(destbuff, len + 1, fmt, al);
    va_end(al);
    dest.append(destbuff, len);
    delete [] destbuff;
  } else {
    dest.append(tmp, len);
  }
  return dest;
}

void StringUtil::Format(string & dest, const char * fmt, ...) {
  char tmp[256];
  va_list al;
  va_start(al, fmt);
  int len = vsnprintf(tmp, 255, fmt, al);
  if (len > 255) {
    char * destbuff = new char[len + 1];
    len = vsnprintf(destbuff, len + 1, fmt, al);
    dest.append(destbuff, len);
  } else {
    dest.append(tmp, len);
  }
  va_end(al);
}

string StringUtil::ToLower(const string & name) {
  string ret = name;
  for (size_t i = 0; i < ret.length(); i++) {
    ret.at(i) = ::tolower(ret[i]);
  }
  return ret;
}

string StringUtil::Trim(const string & str) {
  if (str.length() == 0) {
    return str;
  }
  size_t l = 0;
  while (l < str.length() && isspace(str[l])) {
    l++;
  }
  if (l >= str.length()) {
    return string();
  }
  size_t r = str.length();
  while (isspace(str[r - 1])) {
    r--;
  }
  return str.substr(l, r - l);
}

void StringUtil::Split(const string & src, const string & sep, vector<string> & dest, bool clean) {
  if (sep.length() == 0) {
    return;
  }
  size_t cur = 0;
  while (true) {
    size_t pos;
    if (sep.length() == 1) {
      pos = src.find(sep[0], cur);
    } else {
      pos = src.find(sep, cur);
    }
    string add = src.substr(cur, pos - cur);
    if (clean) {
      string trimed = Trim(add);
      if (trimed.length() > 0) {
        dest.push_back(trimed);
      }
    } else {
      dest.push_back(add);
    }
    if (pos == string::npos) {
      break;
    }
    cur = pos + sep.length();
  }
}

string StringUtil::Join(const vector<string> & strs, const string & sep) {
  string ret;
  for (size_t i = 0; i < strs.size(); i++) {
    if (i > 0) {
      ret.append(sep);
    }
    ret.append(strs[i]);
  }
  return ret;
}

bool StringUtil::StartsWith(const string & str, const string & prefix) {
  if ((prefix.length() > str.length())
      || (memcmp(str.data(), prefix.data(), prefix.length()) != 0)) {
    return false;
  }
  return true;
}

bool StringUtil::EndsWith(const string & str, const string & suffix) {
  if ((suffix.length() > str.length()) ||
      (memcmp(str.data() + str.length() - suffix.length(),
              suffix.data(), suffix.length()) != 0)) {
    return false;
  }
  return true;
}

} // namespace NativeTask

