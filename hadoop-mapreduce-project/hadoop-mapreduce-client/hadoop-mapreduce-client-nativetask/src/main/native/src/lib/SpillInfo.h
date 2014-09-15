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

#ifndef PARTITIONINDEX_H_
#define PARTITIONINDEX_H_

#include <stdint.h>
#include <string>

namespace NativeTask {

using std::string;

/**
 * Store spill file segment information
 */
struct IFileSegment {
  // uncompressed stream end position
  uint64_t uncompressedEndOffset;
  // compressed stream end position
  uint64_t realEndOffset;
};

class SingleSpillInfo {
public:
  uint32_t length;
  std::string path;
  IFileSegment * segments;
  ChecksumType checkSumType;
  KeyValueType keyType;
  KeyValueType valueType;
  std::string codec;

  SingleSpillInfo(IFileSegment * segments, uint32_t len, const string & path, ChecksumType checksum,
      KeyValueType ktype, KeyValueType vtype, const string & inputCodec)
      : length(len), path(path), segments(segments), checkSumType(checksum), keyType(ktype),
          valueType(vtype), codec(inputCodec) {
  }

  ~SingleSpillInfo() {
    delete[] segments;
  }

  void deleteSpillFile();

  uint64_t getEndPosition() {
    return segments ? segments[length - 1].uncompressedEndOffset : 0;
  }

  uint64_t getRealEndPosition() {
    return segments ? segments[length - 1].realEndOffset : 0;
  }

  void writeSpillInfo(const std::string & filepath);
};

class SpillInfos {
public:
  std::vector<SingleSpillInfo*> spills;
  SpillInfos() {
  }

  ~SpillInfos() {
    for (size_t i = 0; i < spills.size(); i++) {
      delete spills[i];
    }
    spills.clear();
  }

  void deleteAllSpillFiles() {
    for (size_t i = 0; i < spills.size(); i++) {
      spills[i]->deleteSpillFile();
    }
  }

  void add(SingleSpillInfo * sri) {
    spills.push_back(sri);
  }

  uint32_t getSpillCount() const {
    return spills.size();
  }

  SingleSpillInfo* getSingleSpillInfo(int index) {
    return spills.at(index);
  }
};

} // namespace NativeTask

#endif /* PARTITIONINDEX_H_ */
