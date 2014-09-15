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

#ifndef TEST_COMMONS_H_
#define TEST_COMMONS_H_

#include "gtest/gtest.h"
#include "commons.h"
#include "util/Random.h"
#include "util/StringUtil.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/BufferStream.h"

using std::pair;
using std::vector;
using std::set;
using std::map;
using std::string;

using namespace NativeTask;

extern Config TestConfig;

/**
 * e.g. MakeStringArray(dest, "a", "b", "c", NULL) = {"a","b","c"}
 */
vector<string> & MakeStringArray(vector<string> & dest, ...);

extern const char * GenerateSeed;
extern const char * GenerateChoice;
extern const char * GenerateLen;
extern const char * GenerateKeyLen;
extern const char * GenerateValueLen;
extern const char * GenerateRange;
extern const char * GenerateKeyRange;
extern const char * GenerateValueRange;

enum GenerateType {
  GenWord,
  GenNumber,
  GenBytes,
};

GenerateType GetGenerateType(const string & type);

string & GenerateOne(string & dest, Random & r, GenerateType gtype, int64_t choice, int64_t len,
    int64_t range = 0);
/**
 * Generate random string sequences
 * @param dest dest array
 * @param size output array size
 * @param type string type (word|number|bytes|tera)
 */
vector<string> & Generate(vector<string> & dest, uint64_t size, const string & type);

/**
 * Generate random string pair sequences
 * @param dest dest array
 * @param size output array size
 * @param type string type (word|number|bytes|tera)
 */
vector<pair<string, string> > & Generate(vector<pair<string, string> > & dest, uint64_t size,
    const string & type);

/**
 * Generate random string pair sequences
 * @param dest dest array
 * @param length output bytes count
 * @param type string type (word|number|bytes|tera)
 */
vector<pair<string, string> > & GenerateLength(vector<pair<string, string> > & dest,
    uint64_t length, const string & type);

/**
 * Generate random KV text:
 * Key0\tValue0\n
 * Key1\tValue1\n
 * ...
 * @param dest dest string contain generated text
 * @param size output kv pair count
 * @param type string type (word|number|bytes|tera)
 */
string & GenerateKVText(string & dest, uint64_t size, const string & type);

/**
 * Generate random KV text:
 * Key0\tValue0\n
 * Key1\tValue1\n
 * ...
 * @param dest dest string contain generated text
 * @param length output string length
 * @param type string type (word|number|bytes|tera)
 */
string & GenerateKVTextLength(string & dest, uint64_t length, const string & type);

/**
 * File <-> String utilities
 */
string & ReadFile(string & dest, const string & path);
void WriteFile(const string & content, const string & path);

/**
 * File compare
 */
bool FileEqual(const string & lh, const string & rh);

/**
 * generate k/v pairs with normal compression ratio
 *
 */
class KVGenerator {
protected:
  uint32_t keylen;
  uint32_t vallen;
  bool unique;
  long factor;
  char * keyb;
  char * valb;
  char keyformat[32];
  set<int64_t> old_keys;

public:
  KVGenerator(uint32_t keylen, uint32_t vallen, bool unique = false);

  ~KVGenerator();

  char * key(uint32_t & kl);

  char * value(uint32_t & vl);

  void write(FILE * fout, int64_t totallen);
};

#endif /* TEST_COMMONS_H_ */
