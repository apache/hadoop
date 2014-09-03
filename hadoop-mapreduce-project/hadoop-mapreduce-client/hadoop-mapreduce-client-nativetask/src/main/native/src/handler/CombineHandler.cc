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
#include "CombineHandler.h"

namespace NativeTask {
const char * REFILL = "refill";
const int LENGTH_OF_REFILL_STRING = 6;

const Command CombineHandler::COMBINE(4, "Combine");

CombineHandler::CombineHandler()
    : _combineContext(NULL), _kvIterator(NULL), _writer(NULL), _kType(UnknownType),
        _vType(UnknownType), _config(NULL), _kvCached(false), _combineInputRecordCount(0),
        _combineInputBytes(0), _combineOutputRecordCount(0), _combineOutputBytes(0) {
}

CombineHandler::~CombineHandler() {
}

void CombineHandler::configure(Config * config) {

  _config = config;
  MapOutputSpec::getSpecFromConfig(_config, _mapOutputSpec);
  _kType = _mapOutputSpec.keyType;
  _vType = _mapOutputSpec.valueType;
}

uint32_t CombineHandler::feedDataToJavaInWritableSerialization() {

  uint32_t written = 0;
  bool firstKV = true;
  _out.position(0);

  if (_kvCached) {
    uint32_t kvLength = _key.outerLength + _value.outerLength + KVBuffer::headerLength();
    outputInt(bswap(_key.outerLength));
    outputInt(bswap(_value.outerLength));
    outputKeyOrValue(_key, _kType);
    outputKeyOrValue(_value, _vType);

    written += kvLength;
    _kvCached = false;
    firstKV = false;
  }

  uint32_t recordCount = 0;
  while (nextKeyValue(_key, _value)) {

    //::sleep(5);
    _kvCached = false;
    recordCount++;

    uint32_t kvLength = _key.outerLength + _value.outerLength + KVBuffer::headerLength();

    if (!firstKV && kvLength > _out.remain()) {
      _kvCached = true;
      break;
    } else {
      firstKV = false;
      //write final key length and final value length
      outputInt(bswap(_key.outerLength));
      outputInt(bswap(_value.outerLength));
      outputKeyOrValue(_key, _kType);
      outputKeyOrValue(_value, _vType);

      written += kvLength;
    }
  }

  if (_out.position() > 0) {
    flushOutput();
  }

  _combineInputRecordCount += recordCount;
  _combineInputBytes += written;
  return written;
}

/**
 * KV: key or value
 */
void CombineHandler::outputKeyOrValue(SerializeInfo & KV, KeyValueType type) {
  switch (type) {
  case TextType:
    output(KV.varBytes, KV.outerLength - KV.buffer.length());
    output(KV.buffer.data(), KV.buffer.length());
    break;
  case BytesType:
    outputInt(bswap(KV.buffer.length()));
    output(KV.buffer.data(), KV.buffer.length());
    break;
  default:
    output(KV.buffer.data(), KV.buffer.length());
    break;
  }
}

bool CombineHandler::nextKeyValue(SerializeInfo & key, SerializeInfo & value) {

  if (!_kvIterator->next(key.buffer, value.buffer)) {
    return false;
  }

  uint32_t varLength = 0;
  switch (_kType) {
  case TextType:
    WritableUtils::WriteVInt(key.buffer.length(), key.varBytes, varLength);
    key.outerLength = key.buffer.length() + varLength;
    break;
  case BytesType:
    key.outerLength = key.buffer.length() + 4;
    break;
  default:
    key.outerLength = key.buffer.length();
    break;
  }

  //prepare final value length
  uint32_t varValueLength = 0;
  switch (_vType) {
  case TextType:
    WritableUtils::WriteVInt(value.buffer.length(), value.varBytes, varValueLength);
    value.outerLength = value.buffer.length() + varValueLength;
    break;
  case BytesType:
    value.outerLength = value.buffer.length() + 4;
    break;
  default:
    value.outerLength = value.buffer.length();
    break;
  }

  return true;
}

uint32_t CombineHandler::feedDataToJava(SerializationFramework serializationType) {
  if (serializationType == WRITABLE_SERIALIZATION) {
    return feedDataToJavaInWritableSerialization();
  }
  THROW_EXCEPTION(IOException, "Native Serialization not supported");
}

void CombineHandler::handleInput(ByteBuffer & in) {
  char * buff = in.current();
  uint32_t length = in.remain();
  uint32_t remain = length;
  char * pos = buff;
  if (_asideBuffer.remain() > 0) {
    uint32_t filledLength = _asideBuffer.fill(pos, length);
    pos += filledLength;
    remain -= filledLength;
  }

  if (_asideBuffer.size() > 0 && _asideBuffer.remain() == 0) {
    _asideBuffer.position(0);
    write(_asideBuffer.current(), _asideBuffer.size());
    _asideBuffer.wrap(NULL, 0);
  }

  if (remain == 0) {
    return;
  }
  KVBuffer * kvBuffer = (KVBuffer *)pos;

  if (unlikely(remain < kvBuffer->headerLength())) {
    THROW_EXCEPTION(IOException, "k/v meta information incomplete");
  }

  uint32_t kvLength = kvBuffer->lengthConvertEndium();

  if (kvLength > remain) {
    _asideBytes.resize(kvLength);
    _asideBuffer.wrap(_asideBytes.buff(), _asideBytes.size());
    _asideBuffer.fill(pos, remain);
    pos += remain;
    remain = 0;
  } else {
    write(pos, remain);
  }
}

void CombineHandler::write(char * buf, uint32_t length) {
  KVBuffer * kv = NULL;
  char * pos = buf;
  uint32_t remain = length;

  uint32_t outputRecordCount = 0;
  while (remain > 0) {
    kv = (KVBuffer *)pos;
    kv->keyLength = bswap(kv->keyLength);
    kv->valueLength = bswap(kv->valueLength);
    _writer->write(kv->getKey(), kv->keyLength, kv->getValue(), kv->valueLength);
    outputRecordCount++;
    remain -= kv->length();
    pos += kv->length();
  }

  _combineOutputRecordCount += outputRecordCount;
  _combineOutputBytes += length;
}

string toString(uint32_t length) {
  string result;
  result.reserve(4);
  result.assign((char *)(&length), 4);
  return result;
}

void CombineHandler::onLoadData() {
  feedDataToJava(WRITABLE_SERIALIZATION);
}

ResultBuffer * CombineHandler::onCall(const Command& command, ParameterBuffer * param) {
  THROW_EXCEPTION(UnsupportException, "Command not supported by RReducerHandler");
}

void CombineHandler::combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) {

  _combineInputRecordCount = 0;
  _combineOutputRecordCount = 0;
  _combineInputBytes = 0;
  _combineOutputBytes = 0;

  this->_combineContext = &type;
  this->_kvIterator = kvIterator;
  this->_writer = writer;
  call(COMBINE, NULL);

  LOG("[CombineHandler] input Record Count: %d, input Bytes: %d, "
      "output Record Count: %d, output Bytes: %d",
      _combineInputRecordCount, _combineInputBytes,
      _combineOutputRecordCount, _combineOutputBytes);
  return;
}

void CombineHandler::finish() {
}

} /* namespace NativeTask */
