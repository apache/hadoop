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
#ifndef _COMBINEHANDLER_H_
#define _COMBINEHANDLER_H_

#include "lib/Combiner.h"
#include "BatchHandler.h"

namespace NativeTask {

enum SerializationFramework {
  WRITABLE_SERIALIZATION = 0,
  NATIVE_SERIALIZATION = 1
};

struct SerializeInfo {
  Buffer buffer;
  uint32_t outerLength;
  char varBytes[8];
};

class CombineHandler : public NativeTask::ICombineRunner, public NativeTask::BatchHandler {
public:
  static const Command COMBINE;

private:

  CombineContext * _combineContext;
  KVIterator * _kvIterator;
  IFileWriter * _writer;
  SerializeInfo _key;
  SerializeInfo _value;

  KeyValueType _kType;
  KeyValueType _vType;
  MapOutputSpec _mapOutputSpec;
  Config * _config;
  bool _kvCached;

  uint32_t _combineInputRecordCount;
  uint32_t _combineInputBytes;

  uint32_t _combineOutputRecordCount;
  uint32_t _combineOutputBytes;

  FixSizeContainer _asideBuffer;
  ByteArray _asideBytes;

public:
  CombineHandler();
  virtual ~CombineHandler();

  virtual void handleInput(ByteBuffer & byteBuffer);
  void finish();

  ResultBuffer * onCall(const Command& command, ParameterBuffer * param);

  void configure(Config * config);

  void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

  virtual void onLoadData();

private:
  void flushDataToWriter();
  void outputKeyOrValue(SerializeInfo & info, KeyValueType type);
  bool nextKeyValue(SerializeInfo & key, SerializeInfo & value);
  uint32_t feedDataToJava(SerializationFramework serializationType);
  uint32_t feedDataToJavaInWritableSerialization();
  void write(char * buf, uint32_t length);

};

} /* namespace NativeTask */
#endif /* _JAVACOMBINEHANDLER_H_ */
