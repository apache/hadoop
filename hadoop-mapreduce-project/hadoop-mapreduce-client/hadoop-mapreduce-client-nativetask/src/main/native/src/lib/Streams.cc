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

#include "lib/commons.h"
#include "util/Checksum.h"
#include "lib/Streams.h"

namespace NativeTask {

/////////////////////////////////////////////////////////////

void InputStream::seek(uint64_t position) {
  THROW_EXCEPTION(UnsupportException, "seek not support");
}

uint64_t InputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

int32_t InputStream::readFully(void * buff, uint32_t length) {
  int32_t ret = 0;
  while (length > 0) {
    int32_t rd = read(buff, length);
    if (rd <= 0) {
      return ret > 0 ? ret : -1;
    }
    ret += rd;
    buff = ((char *)buff) + rd;
    length -= rd;
  }
  return ret;
}

void InputStream::readAllTo(OutputStream & out, uint32_t bufferHint) {
  char * buffer = new char[bufferHint];
  while (true) {
    int32_t rd = read(buffer, bufferHint);
    if (rd <= 0) {
      break;
    }
    out.write(buffer, rd);
  }
  delete buffer;
}

/////////////////////////////////////////////////////////////

uint64_t OutputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

///////////////////////////////////////////////////////////

ChecksumInputStream::ChecksumInputStream(InputStream * stream, ChecksumType type)
    : FilterInputStream(stream), _type(type), _limit(-1) {
  resetChecksum();
}

void ChecksumInputStream::resetChecksum() {
  _checksum = Checksum::init(_type);
}

uint32_t ChecksumInputStream::getChecksum() {
  return Checksum::getValue(_type, _checksum);
}

int32_t ChecksumInputStream::read(void * buff, uint32_t length) {
  if (_limit < 0) {
    int32_t ret = _stream->read(buff, length);
    if (ret > 0) {
      Checksum::update(_type, _checksum, buff, ret);
    }
    return ret;
  } else if (_limit == 0) {
    return -1;
  } else {
    int64_t rd = _limit < length ? _limit : length;
    int32_t ret = _stream->read(buff, rd);
    if (ret > 0) {
      _limit -= ret;
      Checksum::update(_type, _checksum, buff, ret);
    }
    return ret;
  }
}

///////////////////////////////////////////////////////////

ChecksumOutputStream::ChecksumOutputStream(OutputStream * stream, ChecksumType type)
    : FilterOutputStream(stream), _type(type) {
  resetChecksum();
}

void ChecksumOutputStream::resetChecksum() {
  _checksum = Checksum::init(_type);
}

uint32_t ChecksumOutputStream::getChecksum() {
  return Checksum::getValue(_type, _checksum);
}

void ChecksumOutputStream::write(const void * buff, uint32_t length) {
  Checksum::update(_type, _checksum, buff, length);
  _stream->write(buff, length);
}

} // namespace NativeTask
