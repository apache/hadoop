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

#include <string>

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/WritableUtils.h"
#include "lib/Buffers.h"

namespace NativeTask {


ReadBuffer::ReadBuffer()
    : _buff(NULL), _remain(0), _size(0), _capacity(0), _stream(NULL), _source(NULL) {
}

void ReadBuffer::init(uint32_t size, InputStream * stream, const string & codec) {
  if (size < 1024) {
    THROW_EXCEPTION_EX(UnsupportException, "ReadBuffer size %u not support.", size);
  }
  _buff = (char *)malloc(size);
  if (NULL == _buff) {
    THROW_EXCEPTION(OutOfMemoryException, "create append buffer");
  }
  _capacity = size;
  _remain = 0;
  _size = 0;
  _stream = stream;
  _source = _stream;
  if (codec.length() > 0) {
    if (!Compressions::support(codec)) {
      THROW_EXCEPTION(UnsupportException, "compression codec not support");
    }
    _source = Compressions::getDecompressionStream(codec, _stream, size);
  }
}

ReadBuffer::~ReadBuffer() {
  if (_source != _stream) {
    delete _source;
    _source = NULL;
  }
  if (NULL != _buff) {
    free(_buff);
    _buff = NULL;
    _capacity = 0;
    _remain = 0;
    _size = 0;
  }
}

char * ReadBuffer::fillGet(uint32_t count) {

  if (unlikely(count > _capacity)) {
    uint32_t newcap = _capacity * 2 > count ? _capacity * 2 : count;
    char * newbuff = (char*)malloc(newcap);


    if (newbuff == NULL) {
      THROW_EXCEPTION(OutOfMemoryException,
          StringUtil::Format("buff realloc failed, size=%u", newcap));
    }

    if (_remain > 0) {
      memcpy(newbuff, current(), _remain);
    }
    if (NULL != _buff) {
      free(_buff);
    }

    _buff = newbuff;
    _capacity = newcap;
  } else {
    if (_remain > 0) {
      memmove(_buff, current(), _remain);
    }
  }
  _size = _remain;
  while (_remain < count) {
    int32_t rd = _source->read(_buff + _size, _capacity - _size);
    if (rd <= 0) {
      THROW_EXCEPTION(IOException, "read reach EOF");
    }
    _remain += rd;
    _size += rd;
  }
  char * ret = current();
  _remain -= count;
  return ret;
}

int32_t ReadBuffer::fillRead(char * buff, uint32_t len) {
  uint32_t cp = _remain;
  if (cp > 0) {
    memcpy(buff, current(), cp);
    _remain = 0;
  }
  // TODO: read to buffer first
  int32_t ret = _source->readFully(buff + cp, len - cp);
  if (ret < 0 && cp == 0) {
    return ret;
  } else {
    return ret < 0 ? cp : ret + cp;
  }
}

int64_t ReadBuffer::fillReadVLong() {
  if (_remain == 0) {
    int32_t rd = _source->read(_buff, _capacity);
    if (rd <= 0) {
      THROW_EXCEPTION(IOException, "fillReadVLong reach EOF");
    }
    _remain = rd;
    _size = rd;
  }
  int8_t * pos = (int8_t*)current();
  if (*pos >= -112) {
    _remain--;
    return (int64_t)*pos;
  }
  bool neg = *pos < -120;
  uint32_t len = neg ? (-119 - *pos) : (-111 - *pos);
  pos = (int8_t*)get(len);
  const int8_t * end = pos + len;
  uint64_t value = 0;
  while (++pos < end) {
    value = (value << 8) | *(uint8_t*)pos;
  }
  return neg ? (value ^ -1LL) : value;
}

///////////////////////////////////////////////////////////

AppendBuffer::AppendBuffer()
    : _buff(NULL), _remain(0), _capacity(0), _counter(0), _stream(NULL), _dest(NULL),
        _compression(false) {
}

void AppendBuffer::init(uint32_t size, OutputStream * stream, const string & codec) {
  if (size < 1024) {
    THROW_EXCEPTION_EX(UnsupportException, "AppendBuffer size %u not support.", size);
  }
  _buff = (char *)malloc(size + 8);
  if (NULL == _buff) {
    THROW_EXCEPTION(OutOfMemoryException, "create append buffer");
  }
  _capacity = size;
  _remain = _capacity;
  _stream = stream;
  _dest = _stream;
  if (codec.length() > 0) {
    if (!Compressions::support(codec)) {
      THROW_EXCEPTION(UnsupportException, "compression codec not support");
    }
    _dest = Compressions::getCompressionStream(codec, _stream, size);
    _compression = true;
  }
}

CompressStream * AppendBuffer::getCompressionStream() {
  if (_compression) {
    return (CompressStream *)_dest;
  } else {
    return NULL;
  }
}

AppendBuffer::~AppendBuffer() {
  if (_dest != _stream) {
    delete _dest;
    _dest = NULL;
  }
  if (NULL != _buff) {
    free(_buff);
    _buff = NULL;
    _remain = 0;
    _capacity = 0;
  }
}

void AppendBuffer::flushd() {
  _dest->write(_buff, _capacity - _remain);
  _counter += _capacity - _remain;
  _remain = _capacity;
}

void AppendBuffer::write_inner(const void * data, uint32_t len) {
  flushd();
  if (len >= _capacity / 2) {
    _dest->write(data, len);
    _counter += len;
  } else {
    simple_memcpy(_buff, data, len);
    _remain -= len;
  }
}

void AppendBuffer::write_vlong_inner(int64_t v) {
  if (_remain < 9) {
    flushd();
  }
  uint32_t len;
  WritableUtils::WriteVLong(v, current(), len);
  _remain -= len;
}

void AppendBuffer::write_vuint2_inner(uint32_t v1, uint32_t v2) {
  if (_remain < 10) {
    flushd();
  }
  uint32_t len;
  WritableUtils::WriteVLong(v1, current(), len);
  _remain -= len;
  WritableUtils::WriteVLong(v2, current(), len);
  _remain -= len;
}

} // namespace NativeTask

