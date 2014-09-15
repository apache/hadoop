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
#ifndef COMBINER_H_
#define COMBINER_H_
#include "commons.h"
#include "lib/IFile.h"

namespace NativeTask {

class MemoryBufferKVIterator : public KVIterator {
public:
  virtual const char * getBase() = 0;
  virtual std::vector<uint32_t> * getKVOffsets() = 0;
};

enum CombineContextType {
  UNKNOWN = 0,
  CONTINUOUS_MEMORY_BUFFER = 1,
};

class CombineContext {

private:
  CombineContextType _type;

public:
  CombineContext(CombineContextType type)
      : _type(type) {
  }

public:
  CombineContextType getType() {
    return _type;
  }
};

class CombineInMemory : public CombineContext {
  CombineInMemory()
      : CombineContext(CONTINUOUS_MEMORY_BUFFER) {
  }
};

class ICombineRunner {
public:
  ICombineRunner() {
  }

  virtual void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) = 0;

  virtual ~ICombineRunner() {
  }
};

} /* namespace NativeTask */
#endif /* COMBINER_H_ */
