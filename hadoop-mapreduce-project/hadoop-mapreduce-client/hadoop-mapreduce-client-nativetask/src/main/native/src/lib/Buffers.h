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

#ifndef BUFFERS_H_
#define BUFFERS_H_

#include "lib/Streams.h"
#include "lib/Compressions.h"
#include "lib/Constants.h"

namespace NativeTask {


/**
 * A lightweight read buffer, act as buffered input stream
 */
class ReadBuffer {
protected:
  char * _buff;
  uint32_t _remain;
  uint32_t _size;
  uint32_t _capacity;

  InputStream * _stream;
  InputStream * _source;

protected:
  inline char * current() {
    return _buff + _size - _remain;
  }

  char * fillGet(uint32_t count);
  int32_t fillRead(char * buff, uint32_t len);
  int64_t fillReadVLong();
public:
  ReadBuffer();

  void init(uint32_t size, InputStream * stream, const string & codec);

  ~ReadBuffer();

  /**
   * use get() to get inplace continuous memory of small object
   */
  inline char * get(uint32_t count) {
    if (likely(count <= _remain)) {
      char * ret = current();
      _remain -= count;
      return ret;
    }
    return fillGet(count);
  }

  /**
   * read to outside buffer
   */
  inline int32_t read(char * buff, uint32_t len) {
    if (likely(len <= _remain)) {
      memcpy(buff, current(), len);
      _remain -= len;
      return len;
    }
    return fillRead(buff, len);
  }

  /**
   * read to outside buffer, use simple_memcpy
   */
  inline void readUnsafe(char * buff, uint32_t len) {
    if (likely(len <= _remain)) {
      simple_memcpy(buff, current(), len);
      _remain -= len;
      return;
    }
    fillRead(buff, len);
  }

  /**
   * read VUInt
   */
  inline int64_t readVLong() {
    if (likely(_remain > 0)) {
      char * mark = current();
      if (*(int8_t*)mark >= (int8_t)-112) {
        _remain--;
        return (int64_t)*mark;
      }
    }
    return fillReadVLong();
  }

  /**
   * read uint32_t little endian
   */
  inline uint32_t read_uint32_le() {
    return *(uint32_t*)get(4);
  }

  /**
   * read uint32_t big endian
   */
  inline uint32_t read_uint32_be() {
    return bswap(read_uint32_le());
  }
};

/**
 * A light weighted append buffer, used as buffered output streams
 */
class AppendBuffer {
protected:
  char * _buff;
  uint32_t _remain;
  uint32_t _capacity;
  uint64_t _counter;

  OutputStream * _stream;
  OutputStream * _dest;
  bool _compression;

protected:
  void flushd();

  inline char * current() {
    return _buff + _capacity - _remain;
  }

  void write_inner(const void * data, uint32_t len);
  void write_vlong_inner(int64_t v);
  void write_vuint2_inner(uint32_t v1, uint32_t v2);
public:
  AppendBuffer();

  ~AppendBuffer();

  void init(uint32_t size, OutputStream * stream, const string & codec);

  CompressStream * getCompressionStream();

  uint64_t getCounter() {
    return _counter;
  }

  inline char * borrowUnsafe(uint32_t len) {
    if (likely(_remain >= len)) {
      return current();
    }
    if (likely(_capacity >= len)) {
      flushd();
      return _buff;
    }
    return NULL;
  }

  inline void useUnsafe(uint32_t count) {
    _remain -= count;
  }

  inline void write(char c) {
    if (unlikely(_remain == 0)) {
      flushd();
    }
    *current() = c;
    _remain--;
  }

  inline void write(const void * data, uint32_t len) {
    if (likely(len <= _remain)) { // append directly
      simple_memcpy(current(), data, len);
      _remain -= len;
      return;
    }
    write_inner(data, len);
  }

  inline void write_uint32_le(uint32_t v) {
    if (unlikely(4 > _remain)) {
      flushd();
    }
    *(uint32_t*)current() = v;
    _remain -= 4;
    return;
  }

  inline void write_uint32_be(uint32_t v) {
    write_uint32_le(bswap(v));
  }

  inline void write_uint64_le(uint64_t v) {
    if (unlikely(8 > _remain)) {
      flushd();
    }
    *(uint64_t*)current() = v;
    _remain -= 8;
    return;
  }

  inline void write_uint64_be(uint64_t v) {
    write_uint64_le(bswap64(v));
  }

  inline void write_vlong(int64_t v) {
    if (likely(_remain > 0 && v <= 127 && v >= -112)) {
      *(char*)current() = (char)v;
      _remain--;
      return;
    }
    write_vlong_inner(v);
  }

  inline void write_vuint(uint32_t v) {
    if (likely(_remain > 0 && v <= 127)) {
      *(char*)current() = (char)v;
      _remain--;
      return;
    }
    write_vlong_inner(v);
  }

  inline void write_vuint2(uint32_t v1, uint32_t v2) {
    if (likely(_remain >= 2 && v1 <= 127 && v2 <= 127)) {
      *(char*)current() = (char)v1;
      *(char*)(current() + 1) = (char)v2;
      _remain -= 2;
      return;
    }
    write_vuint2_inner(v1, v2);
  }

  /**
   * flush current buffer, clear content
   */
  inline void flush() {
    if (_remain < _capacity) {
      flushd();
    }
  }
};

/**
 * Memory Key-Value buffer pair with direct address content, so can be
 * easily copied or dumped to file
 */
struct KVBuffer {
  uint32_t keyLength;
  uint32_t valueLength;
  char content[1];

  char * getKey() {
    return content;
  }

  char * getValue() {
    return content + keyLength;
  }

  KVBuffer * next() {
    return ((KVBuffer*)(content + keyLength + valueLength));
  }

  std::string str() {
    return std::string(content, keyLength) + "\t" + std::string(getValue(), valueLength);
  }

  uint32_t length() {
    return keyLength + valueLength + SIZE_OF_KEY_LENGTH + SIZE_OF_VALUE_LENGTH;
  }

  uint32_t lengthConvertEndium() {
    long value = bswap64(*((long *)this));
    return (value >> 32) + value + SIZE_OF_KEY_LENGTH + SIZE_OF_VALUE_LENGTH;
  }

  void fill(const void * key, uint32_t keylen, const void * value, uint32_t vallen) {
    keyLength = keylen;
    valueLength = vallen;

    if (keylen > 0) {
      simple_memcpy(getKey(), key, keylen);
    }
    if (vallen > 0) {
      simple_memcpy(getValue(), value, vallen);
    }
  }

  static uint32_t headerLength() {
    return SIZE_OF_KEY_LENGTH + SIZE_OF_VALUE_LENGTH;
  }
};

struct KVBufferWithParititionId {
  uint32_t partitionId;
  KVBuffer buffer;

  inline static uint32_t minLength() {
    return SIZE_OF_PARTITION_LENGTH + SIZE_OF_KV_LENGTH;
  }

  int length() {
    return 4 + buffer.length();
  }

  int lengthConvertEndium() {
    return 4 + buffer.lengthConvertEndium();
  }
};

/**
 * Native side abstraction of java ByteBuffer
 */
class ByteBuffer {
private:
  char * _buff;
  uint32_t _limit;
  uint32_t _position;
  uint32_t _capacity;

public:
  ByteBuffer()
      : _buff(NULL), _limit(0), _position(0), _capacity(0) {
  }

  ~ByteBuffer() {
  }

  void reset(char * buff, uint32_t inputCapacity) {
    this->_buff = buff;
    this->_capacity = inputCapacity;
    this->_position = 0;
    this->_limit = 0;
  }

  int capacity() {
    return this->_capacity;
  }

  int remain() {
    return _limit - _position;
  }

  int limit() {
    return _limit;
  }

  int advance(int positionOffset) {
    _position += positionOffset;
    return _position;
  }

  int position() {
    return this->_position;
  }

  void position(int newPos) {
    this->_position = newPos;
  }

  void rewind(int newPos, int newLimit) {
    this->_position = newPos;
    if (newLimit < 0 || newLimit > this->_capacity) {
      THROW_EXCEPTION(IOException, "length smaller than zero or larger than input buffer capacity");
    }
    this->_limit = newLimit;
  }

  char * current() {
    return _buff + _position;
  }

  char * base() {
    return _buff;
  }
};

class ByteArray {
private:
  char * _buff;
  uint32_t _length;
  uint32_t _capacity;

public:
  ByteArray()
      : _buff(NULL), _length(0), _capacity(0) {
  }

  ~ByteArray() {
    if (NULL != _buff) {
      delete[] _buff;
      _buff = NULL;
    }
    _length = 0;
    _capacity = 0;
  }

  void resize(uint32_t newSize) {
    if (newSize <= _capacity) {
      _length = newSize;
    } else {
      if (NULL != _buff) {
        delete[] _buff;
        _buff = NULL;
      }
      _capacity = 2 * newSize;
      _buff = new char[_capacity];
      _length = newSize;
    }
  }

  char * buff() {
    return _buff;
  }

  uint32_t size() {
    return _length;
  }
};

class FixSizeContainer {
private:
  char * _buff;
  uint32_t _pos;
  uint32_t _size;

public:
  FixSizeContainer()
      : _buff(NULL), _pos(0), _size(0) {
  }

  ~FixSizeContainer() {
  }

  void wrap(char * buff, uint32_t size) {
    _size = size;
    _buff = buff;
    _pos = 0;
  }

  void rewind() {
    _pos = 0;
  }

  uint32_t remain() {
    return _size - _pos;
  }

  char * current() {
    return _buff + _pos;
  }

  char * base() {
    return _buff;
  }

  uint32_t size() {
    return _size;
  }

  /**
   * return the length of actually filled data.
   */
  uint32_t fill(const char * source, uint32_t maxSize) {
    int remain = _size - _pos;
    if (remain <= 0) {
      return 0;
    }

    uint32_t length = (maxSize < remain) ? maxSize : remain;
    simple_memcpy(_buff + _pos, source, length);
    _pos += length;
    return length;
  }

  uint32_t position() {
    return _pos;
  }

  void position(int pos) {
    _pos = pos;
  }
};

class ReadWriteBuffer {
private:

  static const uint32_t INITIAL_LENGTH = 16;

  uint32_t _readPoint;
  uint32_t _writePoint;
  char * _buff;
  uint32_t _buffLength;
  bool _newCreatedBuff;

public:

  ReadWriteBuffer(uint32_t length)
      : _readPoint(0), _writePoint(0), _buff(NULL), _buffLength(0), _newCreatedBuff(false) {
    _buffLength = length;
    if (_buffLength > 0) {
      _buff = new char[_buffLength];
      _newCreatedBuff = true;
    }
  }

  ReadWriteBuffer()
      : _readPoint(0), _writePoint(0), _buff(NULL), _buffLength(0), _newCreatedBuff(false) {
  }

  ~ReadWriteBuffer() {
    if (_newCreatedBuff) {
      delete[] _buff;
      _buff = NULL;
    }
  }

  void setReadPoint(uint32_t pos) {
    _readPoint = pos;
  }

  void setWritePoint(uint32_t pos) {
    _writePoint = pos;
  }

  char * getBuff() {
    return _buff;
  }

  uint32_t getWritePoint() {
    return _writePoint;
  }

  uint32_t getReadPoint() {
    return _readPoint;
  }

  void writeInt(uint32_t param) {
    uint32_t written = param;

    checkWriteSpaceAndResizeIfNecessary(4);
    *((uint32_t *)(_buff + _writePoint)) = written;
    _writePoint += 4;
  }

  void writeLong(uint64_t param) {
    uint64_t written = param;

    checkWriteSpaceAndResizeIfNecessary(8);
    *((uint64_t *)(_buff + _writePoint)) = written;
    _writePoint += 8;
  }

  void writeString(const char * param, uint32_t length) {
    writeInt(length);
    checkWriteSpaceAndResizeIfNecessary(length);

    memcpy(_buff + _writePoint, param, length);
    _writePoint += length;
  }

  void writeString(std::string * param) {
    const char * str = param->c_str();
    int length = param->size();
    writeString(str, length);
  }

  void writePointer(void * param) {
    uint64_t written = (uint64_t)(param);
    writeLong(written);
  }

  uint32_t readInt() {
    char * readPos = _buff + _readPoint;
    uint32_t result = *((uint32_t *)(readPos));
    _readPoint += 4;
    return result;
  }

  uint64_t readLong() {
    char * readPos = _buff + _readPoint;
    uint64_t result = *((uint64_t *)(readPos));
    _readPoint += 8;
    return result;
  }

  std::string * readString() {
    uint32_t len = readInt();
    char * strBegin = _buff + _readPoint;
    _readPoint += len;
    return new std::string(strBegin, len);
  }

  void * readPointer() {
    uint64_t result = readLong();
    return (void *)(result);
  }

private:
  void checkWriteSpaceAndResizeIfNecessary(uint32_t toBeWritten) {
    if (_buffLength == 0) {
      _newCreatedBuff = true;
      _buffLength = INITIAL_LENGTH > toBeWritten ? INITIAL_LENGTH : toBeWritten;
      _buff = new char[_buffLength];
    }

    if (_buffLength - _writePoint >= toBeWritten) {
      return;
    }

    _buffLength = _buffLength + toBeWritten;
    _newCreatedBuff = true;
    char * newBuff = new char[_buffLength];
    memcpy(newBuff, _buff, _writePoint);
    delete[] _buff;
    _buff = newBuff;
  }
};

typedef ReadWriteBuffer ParameterBuffer;
typedef ReadWriteBuffer ResultBuffer;

} // namespace NativeTask

#endif /* BUFFERS_H_ */
