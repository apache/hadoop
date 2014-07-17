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

#include "commons.h"
#include "BufferStream.h"

namespace NativeTask {

BufferedInputStream::BufferedInputStream(InputStream * stream, uint32_t bufferSize)
    : FilterInputStream(stream), _buff(NULL), _position(0), _limit(0), _capacity(0) {
  _buff = (char*)malloc(bufferSize);
  if (NULL != _buff) {
    LOG("[BuferStream] malloc failed when create BufferedInputStream with buffersize %u",
        bufferSize);
    _capacity = bufferSize;
  }
}

BufferedInputStream::~BufferedInputStream() {
  if (NULL != _buff) {
    free(_buff);
    _buff = NULL;
    _position = 0;
    _limit = 0;
    _capacity = 0;
  }
}

void BufferedInputStream::seek(uint64_t position) {
  if (_limit - _position > 0) {
    THROW_EXCEPTION(IOException, "temporary buffered data exists when fseek()");
  }
  _stream->seek(position);
}

uint64_t BufferedInputStream::tell() {
  return _stream->tell() - (_limit - _position);
}

int32_t BufferedInputStream::read(void * buff, uint32_t length) {
  uint32_t rest = _limit - _position;
  if (rest > 0) {
    // have some data in buffer, read from buffer
    uint32_t cp = rest < length ? rest : length;
    memcpy(buff, _buff + _position, cp);
    _position += cp;
    return cp;
  } else if (length >= _capacity / 2) {
    // dest buffer big enough, read to dest buffer directly
    return _stream->read(buff, length);
  } else {
    // read to buffer first, then copy part of it to dest
    _limit = 0;
    do {
      int32_t rd = _stream->read(_buff + _limit, _capacity - _limit);
      if (rd <= 0) {
        break;
      }
    } while (_limit < _capacity / 2);
    if (_limit == 0) {
      return -1;
    }
    uint32_t cp = _limit < length ? _limit : length;
    memcpy(buff, _buff, cp);
    _position = cp;
    return cp;
  }
}

/////////////////////////////////////////////////////////////////

BufferedOutputStream::BufferedOutputStream(InputStream * stream, uint32_t bufferSize)
    : FilterOutputStream(_stream), _buff(NULL), _position(0), _capacity(0) {
  _buff = (char*)malloc(bufferSize + sizeof(uint64_t));
  if (NULL != _buff) {
    LOG("[BuferStream] malloc failed when create BufferedOutputStream with buffersize %u",
        bufferSize);
    _capacity = bufferSize;
  }
}

BufferedOutputStream::~BufferedOutputStream() {
  if (NULL != _buff) {
    free(_buff);
    _buff = NULL;
    _position = 0;
    _capacity = 0;
  }
}

uint64_t BufferedOutputStream::tell() {
  return _stream->tell() + _position;
}

void BufferedOutputStream::write(const void * buff, uint32_t length) {
  if (length < _capacity / 2) {
    uint32_t rest = _capacity - _position;
    if (length < rest) {
      simple_memcpy(_buff + _position, buff, length);
      _position += length;
    } else {
      flush();
      simple_memcpy(_buff, buff, length);
      _position = length;
    }
  } else {
    flush();
    _stream->write(buff, length);
  }
}

void BufferedOutputStream::flush() {
  if (_position > 0) {
    _stream->write(_buff, _position);
    _position = 0;
  }
}

///////////////////////////////////////////////////////////

int32_t InputBuffer::read(void * buff, uint32_t length) {
  uint32_t rd = _capacity - _position < length ? _capacity - _position : length;
  if (rd > 0) {
    memcpy(buff, _buff + _position, rd);
    _position += rd;
    return rd;
  }
  return length == 0 ? 0 : -1;
}

void OutputBuffer::write(const void * buff, uint32_t length) {
  if (_position + length <= _capacity) {
    memcpy(_buff + _position, buff, length);
    _position += length;
  } else {
    THROW_EXCEPTION(IOException, "OutputBuffer too small to write");
  }
}

} // namespace NativeTask
