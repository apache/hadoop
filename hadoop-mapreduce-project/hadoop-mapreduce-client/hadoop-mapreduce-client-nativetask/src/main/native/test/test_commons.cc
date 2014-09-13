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
#include "util/Random.h"
#include "lib/FileSystem.h"
#include "test_commons.h"

Config TestConfig = Config();

const char * GenerateSeed = "generate.seed";
const char * GenerateChoice = "generate.choice";
const char * GenerateLen = "generate.len";
const char * GenerateKeyLen = "generate.key.len";
const char * GenerateValueLen = "generate.value.len";
const char * GenerateRange = "generate.range";
const char * GenerateKeyRange = "generate.key.range";
const char * GenerateValueRange = "generate.value.range";

vector<string> & MakeStringArray(vector<string> & dest, ...) {
  va_list al;
  va_start(al, dest);
  while (true) {
    const char * s = va_arg(al, const char *);
    if (s == NULL) {
      break;
    }
    dest.push_back(s);
  }
  va_end(al);
  return dest;
}

GenerateType GetGenerateType(const string & type) {
  if (type == "word") {
    return GenWord;
  } else if (type == "number") {
    return GenNumber;
  } else if (type == "bytes") {
    return GenBytes;
  } else {
    THROW_EXCEPTION(UnsupportException, "GenerateType not support");
  }
}

string & GenerateOne(string & dest, Random & r, GenerateType gtype, int64_t choice, int64_t len,
    int64_t range) {
  switch (gtype) {
  case GenWord:
    r.nextWord(dest, choice);
    break;
  case GenNumber:
    uint64_t v;
    if (choice > 0) {
      v = r.next_int32(choice);
    } else {
      v = r.next_uint64();
    }
    if (len > 0) {
      dest = StringUtil::ToString(v, '0', len);
    } else {
      dest = StringUtil::ToString(v);
    }
    break;
  case GenBytes:
    if (range < 2) {
      if (len > 0) {
        dest = r.nextBytes(len, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
      } else {
        dest = r.nextBytes(r.next_int32(32), "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
      }
    } else {
      if (len > 0) {
        int64_t nlen = len - range / 2 + r.next_int32(range);
        if (nlen > 0) {
          dest = r.nextBytes(nlen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        } else {
          dest = "";
        }
      } else {
        dest = r.nextBytes(r.next_int32(range), "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
      }
    }
    break;
  default:
    THROW_EXCEPTION(IOException, "GenerateType not support");
  }
  return dest;
}

/**
 * Generate random string sequences
 * @param dest dest array
 * @param size output array size
 * @param type string type (word|number|bytes|tera)
 */
vector<string> & Generate(vector<string> & dest, uint64_t size, const string & type) {
  Random r;
  if (TestConfig.get(GenerateSeed) != NULL) {
    r.setSeed(TestConfig.getInt(GenerateSeed, 0));
  }
  GenerateType gtype = GetGenerateType(type);
  int64_t choice = TestConfig.getInt(GenerateChoice, -1);
  int64_t len = TestConfig.getInt(GenerateLen, -1);
  int64_t range = TestConfig.getInt(GenerateRange, 1);
  string temp;
  for (uint64_t i = 0; i < size; i++) {
    dest.push_back(GenerateOne(temp, r, gtype, choice, len, range));
  }
  return dest;
}

/**
 * Generate random string pair sequences
 * @param dest dest array
 * @param size output array size
 * @param type string type (word|number|bytes|tera)
 */
vector<pair<string, string> > & Generate(vector<pair<string, string> > & dest, uint64_t size,
    const string & type) {
  Random r;
  if (TestConfig.get(GenerateSeed) != NULL) {
    r.setSeed(TestConfig.getInt(GenerateSeed, 0));
  }
  GenerateType gtype = GetGenerateType(type);
  int64_t choice = TestConfig.getInt(GenerateChoice, -1);
  int64_t keylen = TestConfig.getInt(GenerateKeyLen, -1);
  int64_t valuelen = TestConfig.getInt(GenerateValueLen, -1);
  int64_t keyRange = TestConfig.getInt(GenerateKeyRange, 1);
  int64_t valueRange = TestConfig.getInt(GenerateValueRange, 1);
  string key, value;
  for (uint64_t i = 0; i < size; i++) {
    GenerateOne(key, r, gtype, choice, keylen, keyRange);
    GenerateOne(value, r, gtype, choice, valuelen, valueRange);
    dest.push_back(std::make_pair(key, value));
  }
  return dest;
}

/**
 * Generate random string pair sequences
 * @param dest dest array
 * @param length output bytes count
 * @param type string type (word|number|bytes|tera)
 */
vector<pair<string, string> > & GenerateLength(vector<pair<string, string> > & dest,
    uint64_t length, const string & type) {
  Random r;
  if (TestConfig.get(GenerateSeed) != NULL) {
    r.setSeed(TestConfig.getInt(GenerateSeed, 0));
  }
  GenerateType gtype = GetGenerateType(type);
  int64_t choice = TestConfig.getInt(GenerateChoice, -1);
  int64_t keylen = TestConfig.getInt(GenerateKeyLen, -1);
  int64_t valuelen = TestConfig.getInt(GenerateValueLen, -1);
  int64_t keyRange = TestConfig.getInt(GenerateKeyRange, 1);
  int64_t valueRange = TestConfig.getInt(GenerateValueRange, 1);
  string key, value;
  dest.reserve((size_t)(length / (keylen + valuelen) * 1.2));
  for (uint64_t i = 0; i < length;) {
    GenerateOne(key, r, gtype, choice, keylen, keyRange);
    GenerateOne(value, r, gtype, choice, valuelen, valueRange);
    dest.push_back(std::make_pair(key, value));
    i += (key.length() + value.length() + 2);
  }
  return dest;
}

/**
 * Generate random KV text:
 * Key0\tValue0\n
 * Key1\tValue1\n
 * ...
 * @param dest dest string contain generated text
 * @param size output array size
 * @param type string type (word|number|bytes|tera)
 */
string & GenerateKVText(string & dest, uint64_t size, const string & type) {
  Random r;
  if (TestConfig.get(GenerateSeed) != NULL) {
    r.setSeed(TestConfig.getInt(GenerateSeed, 0));
  }
  GenerateType gtype = GetGenerateType(type);
  int64_t choice = TestConfig.getInt(GenerateChoice, -1);
  int64_t keylen = TestConfig.getInt(GenerateKeyLen, -1);
  int64_t valuelen = TestConfig.getInt(GenerateValueLen, -1);
  int64_t keyRange = TestConfig.getInt(GenerateKeyRange, 1);
  int64_t valueRange = TestConfig.getInt(GenerateValueRange, 1);
  string key, value;
  for (uint64_t i = 0; i < size; i++) {
    GenerateOne(key, r, gtype, choice, keylen, keyRange);
    GenerateOne(value, r, gtype, choice, valuelen, valueRange);
    dest.append(key);
    dest.append("\t");
    dest.append(value);
    dest.append("\n");
  }
  return dest;
}

/**
 * Generate random KV text:
 * Key0\tValue0\n
 * Key1\tValue1\n
 * ...
 * @param dest dest string contain generated text
 * @param length output string length
 * @param type string type (word|number|bytes)
 */
string & GenerateKVTextLength(string & dest, uint64_t length, const string & type) {
  Random r;
  if (TestConfig.get(GenerateSeed) != NULL) {
    r.setSeed(TestConfig.getInt(GenerateSeed, 0));
  }
  GenerateType gtype = GetGenerateType(type);
  int64_t choice = TestConfig.getInt(GenerateChoice, -1);
  int64_t keylen = TestConfig.getInt(GenerateKeyLen, -1);
  int64_t valuelen = TestConfig.getInt(GenerateValueLen, -1);
  int64_t keyRange = TestConfig.getInt(GenerateKeyRange, 1);
  int64_t valueRange = TestConfig.getInt(GenerateValueRange, 1);
  string key, value;
  while (dest.length() < length) {
    GenerateOne(key, r, gtype, choice, keylen, keyRange);
    GenerateOne(value, r, gtype, choice, valuelen, valueRange);
    dest.append(key);
    dest.append("\t");
    dest.append(value);
    dest.append("\n");
  }
  return dest;
}

/**
 * File <-> String utilities
 */
string & ReadFile(string & dest, const string & path) {
  FILE * fin = fopen(path.c_str(), "rb");
  if (NULL == fin) {
    THROW_EXCEPTION(IOException, "file not found or can not open for read");
  }
  char buff[1024 * 16];
  while (true) {
    size_t rd = fread(buff, 1, 1024 * 16, fin);
    if (rd <= 0) {
      break;
    }
    dest.append(buff, rd);
  }
  fclose(fin);
  return dest;
}

void WriteFile(const string & content, const string & path) {
  FILE * fout = fopen(path.c_str(), "wb");
  if (NULL == fout) {
    THROW_EXCEPTION(IOException, "file can not open for write");
  }
  size_t wt = fwrite(content.c_str(), 1, content.length(), fout);
  if (wt != content.length()) {
    THROW_EXCEPTION(IOException, "write file error");
  }
  fclose(fout);
}

bool FileEqual(const string & lh, const string & rh) {
  string lhs, rhs;
  ReadFile(lhs, lh);
  ReadFile(rhs, rh);
  return lhs == rhs;
}

KVGenerator::KVGenerator(uint32_t keylen, uint32_t vallen, bool unique)
    : keylen(keylen), vallen(vallen), unique(unique) {
  factor = 2999999;
  keyb = new char[keylen + 32];
  valb = new char[vallen + 32];
  snprintf(keyformat, 32, "%%0%ulx", keylen);
}

KVGenerator::~KVGenerator() {
  delete[] keyb;
  delete[] valb;
}

char * KVGenerator::key(uint32_t & kl) {
  long v;
  if (unique) {
    while (true) {
      v = lrand48();
      if (old_keys.find(v) == old_keys.end()) {
        old_keys.insert(v);
        break;
      }
    }
  } else {
    v = lrand48();
  }
  snprintf(keyb, keylen + 32, keyformat, v);
  kl = keylen;
  return keyb;
}

char * KVGenerator::value(uint32_t & vl) {
  uint32_t off = 0;
  while (off < vallen) {
    long v = lrand48();
    v = (v / factor) * factor;
    uint32_t wn = snprintf(valb + off, vallen + 32 - off, "%09lx\t", v);
    off += wn;
  }
  vl = vallen;
  return valb;
}

void KVGenerator::write(FILE * fout, int64_t totallen) {
  while (totallen > 0) {
    uint32_t kl, vl;
    char * key = this->key(kl);
    char * value = this->value(vl);
    fwrite(key, kl, 1, fout);
    fputc('\t', fout);
    fwrite(value, vl, 1, fout);
    fputc('\n', fout);
    totallen -= (kl + vl + 2);
  }
  fflush(fout);
}

