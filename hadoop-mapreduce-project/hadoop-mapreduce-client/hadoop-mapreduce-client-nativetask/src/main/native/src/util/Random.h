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

#ifndef RANDOM_H_
#define RANDOM_H_

#include <stdint.h>
#include <string>

namespace NativeTask {

using std::string;

/**
 * Copy of java.lang.Random & some random text/bytes generator
 */
class Random {
protected:
  static const int64_t multiplier = 0x5DEECE66DULL;
  static const int64_t addend = 0xBL;
  static const int64_t mask = (1ULL << 48) - 1;
protected:
  int64_t _seed;

  int32_t next(int bits);
public:
  Random();

  Random(int64_t seed);

  ~Random();

  /**
   * Set random seed
   */
  void setSeed(int64_t seed);

  /**
   * Returns uniformly distributed uint32_t in [INT_MIN, INT_MAX]
   */
  int32_t next_int32();

  /**
   * Returns uniformly distributed uint32_t in [0,(uint32_t)-1)
   */
  uint32_t next_uint32();

  /**
   * Returns uniformly distributed uint64_t in [0,(uint64_t)-1)
   */
  uint64_t next_uint64();

  /**
   * Returns uniformly distributed int32_t in [0,n)
   */
  int32_t next_int32(int32_t n);

  /**
   * Returns the next pseudorandom, uniformly distributed
   * {@code float} value between {@code 0.0} and
   * {@code 1.0} from this random number generator's sequence.
   */
  float nextFloat();

  /**
   * Returns the next pseudorandom, uniformly distributed
   * {@code double} value between {@code 0.0} and
   * {@code 1.0} from this random number generator's sequence.
   */
  double nextDouble();

  /**
   * Returns the next pseudorandom, log2-normal distributed
   * value between [0, MAX_UNIT64]
   */
  uint64_t nextLog2();

  /**
   * Returns the next pseudorandom, log2-normal distributed
   * value between [0, range]
   */
  uint64_t nextLog2(uint64_t range);

  /**
   * Returns the next pseudorandom, log10-normal distributed
   * value between [0, range]
   */
  uint64_t nextLog10(uint64_t range);

  /**
   * Returns uniformly distributed byte in range
   * @param range e.g. "ABCDEFG", "01234566789"
   */
  char nextByte(const string & range);

  /**
   * Return byte sequence of <code>length</code>
   * each byte in the sequence is generated using
   * <code>nextByte</code>
   */
  string nextBytes(uint32_t length, const string & range);

  /**
   * Generate random word from a 100 word collection(same
   * as RandomTextWriter), Just a utility function to
   * construct the test data.
   * @param limit use first <code>limit</code> words in
   *              the word collection
   */
  const char * nextWord(int64_t limit = -1);

  /**
   * Generate random word from a 100 word collection(same
   * as RandomTextWriter), Just a utility function to
   * construct the test data.
   * @param dest  assign the generated word to dest
   * @param limit use first <code>limit</code> words in
   *              the word collection
   */
  void nextWord(string & dest, int64_t limit = -1);
};

} // namespace NativeTask

#endif /* RANDOM_H_ */
