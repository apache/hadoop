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

#ifndef BUFFERSTREAM_H_
#define BUFFERSTREAM_H_

#include <string>
#include "lib/Streams.h"

namespace NativeTask {

using std::string;

class InputBuffer : public InputStream {
protected:
  const char * _buff;
  uint32_t _position;
  uint32_t _capacity;
public:
  InputBuffer()
      : _buff(NULL), _position(0), _capacity(0) {
  }

  InputBuffer(const char * buff, uint32_t capacity)
      : _buff(buff), _position(0), _capacity(capacity) {
  }

  InputBuffer(const string & src)
      : _buff(src.data()), _position(0), _capacity(src.length()) {
  }

  virtual ~InputBuffer() {
  }

  virtual void seek(uint64_t position) {
    if (position <= _capacity) {
      _position = position;
    } else {
      _position = _capacity;
    }
  }

  virtual uint64_t tell() {
    return _position;
  }

  virtual int32_t read(void * buff, uint32_t length);

  void reset(const char * buff, uint32_t capacity) {
    _buff = buff;
    _position = 0;
    _capacity = capacity;
  }

  void reset(const string & src) {
    _buff = src.data();
    _position = 0;
    _capacity = src.length();
  }

  void rewind() {
    _position = 0;
  }
};

class OutputBuffer : public OutputStream {
protected:
  char * _buff;
  uint32_t _position;
  uint32_t _capacity;
public:
  OutputBuffer()
      : _buff(NULL), _position(0), _capacity(0) {
  }

  OutputBuffer(char * buff, uint32_t capacity)
      : _buff(buff), _position(0), _capacity(capacity) {
  }

  virtual ~OutputBuffer() {
  }

  virtual uint64_t tell() {
    return _position;
  }

  virtual void write(const void * buff, uint32_t length);

  void clear() {
    _position = 0;
  }

  void reset(char * buff, uint32_t capacity) {
    _buff = buff;
    _position = 0;
    _capacity = capacity;
  }

  string getString() {
    return string(_buff, _position);
  }
};

class OutputStringStream : public OutputStream {
protected:
  string * _dest;
public:
  OutputStringStream()
      : _dest(NULL) {
  }

  OutputStringStream(string & dest)
      : _dest(&dest) {
  }
  virtual ~OutputStringStream() {
  }

  virtual uint64_t tell() {
    return _dest->length();
  }

  virtual void write(const void * buff, uint32_t length) {
    _dest->append((const char *)buff, length);
  }

  void reset(string * dest) {
    _dest = dest;
  }

  void clear() {
    _dest->clear();
  }

  string getString() {
    return *_dest;
  }
};

} // namespace NativeTask

#endif /* BUFFERSTREAM_H_ */
