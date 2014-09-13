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

#ifndef STREAMS_H_
#define STREAMS_H_

#include "util/Checksum.h"

namespace NativeTask {

class OutputStream;

class InputStream {
public:
  InputStream() {
  }

  virtual ~InputStream() {
  }

  virtual void seek(uint64_t position);

  virtual uint64_t tell();

  virtual int32_t read(void * buff, uint32_t length) {
    return -1;
  }

  virtual void close() {
  }

  virtual int32_t readFully(void * buff, uint32_t length);

  void readAllTo(OutputStream & out, uint32_t bufferHint = 1024 * 4);
};

class OutputStream {
public:
  OutputStream() {
  }

  virtual ~OutputStream() {
  }

  virtual uint64_t tell();

  virtual void write(const void * buff, uint32_t length) {
  }

  virtual void flush() {
  }

  virtual void close() {
  }
};

class FilterInputStream : public InputStream {
protected:
  InputStream * _stream;
public:
  FilterInputStream(InputStream * stream)
      : _stream(stream) {
  }

  virtual ~FilterInputStream() {
  }

  void setStream(InputStream * stream) {
    _stream = stream;
  }

  InputStream * getStream() {
    return _stream;
  }

  virtual void seek(uint64_t position) {
    _stream->seek(position);
  }

  virtual uint64_t tell() {
    return _stream->tell();
  }

  virtual int32_t read(void * buff, uint32_t length) {
    return _stream->read(buff, length);
  }
};

class FilterOutputStream : public OutputStream {
protected:
  OutputStream * _stream;
public:
  FilterOutputStream(OutputStream * stream)
      : _stream(stream) {
  }

  virtual ~FilterOutputStream() {
  }

  void setStream(OutputStream * stream) {
    _stream = stream;
  }

  OutputStream * getStream() {
    return _stream;
  }

  virtual uint64_t tell() {
    return _stream->tell();
  }

  virtual void write(const void * buff, uint32_t length) {
    _stream->write(buff, length);
  }

  virtual void flush() {
    _stream->flush();
  }

  virtual void close() {
    flush();
  }
};

class LimitInputStream : public FilterInputStream {
protected:
  int64_t _limit;
public:
  LimitInputStream(InputStream * stream, int64_t limit)
      : FilterInputStream(stream), _limit(limit) {
  }

  virtual ~LimitInputStream() {
  }

  int64_t getLimit() {
    return _limit;
  }

  void setLimit(int64_t limit) {
    _limit = limit;
  }

  virtual int32_t read(void * buff, uint32_t length) {
    if (_limit < 0) {
      return _stream->read(buff, length);
    } else if (_limit == 0) {
      return -1;
    } else {
      int64_t rd = _limit < length ? _limit : length;
      int32_t ret = _stream->read(buff, rd);
      if (ret > 0) {
        _limit -= ret;
      }
      return ret;
    }
  }
};

class ChecksumInputStream : public FilterInputStream {
protected:
  ChecksumType _type;
  uint32_t _checksum;
  int64_t _limit;
public:
  ChecksumInputStream(InputStream * stream, ChecksumType type);

  virtual ~ChecksumInputStream() {
  }

  int64_t getLimit() {
    return _limit;
  }

  void setLimit(int64_t limit) {
    _limit = limit;
  }

  void resetChecksum();

  uint32_t getChecksum();

  virtual int32_t read(void * buff, uint32_t length);
};

class ChecksumOutputStream : public FilterOutputStream {
protected:
  ChecksumType _type;
  uint32_t _checksum;
public:
  ChecksumOutputStream(OutputStream * stream, ChecksumType type);

  virtual ~ChecksumOutputStream() {
  }

  void resetChecksum();

  uint32_t getChecksum();

  virtual void write(const void * buff, uint32_t length);

};

} // namespace NativeTask

#endif /* STREAMS_H_ */
