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
#include "NativeTask.h"
#include "BlockCodec.h"

namespace NativeTask {

BlockCompressStream::BlockCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : CompressStream(stream), _tempBuffer(NULL), _tempBufferSize(0), _compressedBytesWritten(0) {
  _hint = bufferSizeHint;
  _blockMax = bufferSizeHint / 2 * 3;
}

void BlockCompressStream::init() {
  _tempBufferSize = maxCompressedLength(_blockMax) + 8;
  _tempBuffer = new char[_tempBufferSize];
}

BlockCompressStream::~BlockCompressStream() {
  delete[] _tempBuffer;
  _tempBuffer = NULL;
  _tempBufferSize = 0;
}

void BlockCompressStream::write(const void * buff, uint32_t length) {
  while (length > 0) {
    uint32_t take = length < _blockMax ? length : _hint;
    compressOneBlock(buff, take);
    buff = ((const char *)buff) + take;
    length -= take;
  }
}

void BlockCompressStream::flush() {
  _stream->flush();
}

void BlockCompressStream::close() {
  flush();
}

void BlockCompressStream::writeDirect(const void * buff, uint32_t length) {
  _stream->write(buff, length);
  _compressedBytesWritten += length;
}

uint64_t BlockCompressStream::compressedBytesWritten() {
  return _compressedBytesWritten;
}

//////////////////////////////////////////////////////////////

BlockDecompressStream::BlockDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : DecompressStream(stream), _tempBuffer(NULL), _tempBufferSize(0) {
  _hint = bufferSizeHint;
  _blockMax = bufferSizeHint / 2 * 3;
  _tempDecompressBuffer = NULL;
  _tempDecompressBufferSize = 0;
  _tempDecompressBufferUsed = 0;
  _tempDecompressBufferCapacity = 0;
  _compressedBytesRead = 0;
}

void BlockDecompressStream::init() {
  _tempBufferSize = maxCompressedLength(_blockMax) + 8;
  _tempBuffer = (char*)malloc(_tempBufferSize);
}

BlockDecompressStream::~BlockDecompressStream() {
  close();
  if (NULL != _tempBuffer) {
    free(_tempBuffer);
    _tempBuffer = NULL;
  }
  _tempBufferSize = 0;
}

int32_t BlockDecompressStream::read(void * buff, uint32_t length) {
  if (_tempDecompressBufferSize == 0) {
    uint32_t sizes[2];
    int32_t rd = _stream->readFully(&sizes, sizeof(uint32_t) * 2);
    if (rd <= 0) {
      // EOF
      return -1;
    }
    if (rd != sizeof(uint32_t) * 2) {
      THROW_EXCEPTION(IOException, "readFully get incomplete data");
    }
    _compressedBytesRead += rd;
    sizes[0] = bswap(sizes[0]);
    sizes[1] = bswap(sizes[1]);
    if (sizes[0] <= length) {
      uint32_t len = decompressOneBlock(sizes[1], buff, sizes[0]);
      if (len != sizes[0]) {
        THROW_EXCEPTION(IOException, "Block decompress data error, length not match");
      }
      return len;
    } else {
      if (sizes[0] > _tempDecompressBufferCapacity) {
        char * newBuffer = (char *)realloc(_tempDecompressBuffer, sizes[0]);
        if (newBuffer == NULL) {
          THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
        }
        _tempDecompressBuffer = newBuffer;
        _tempDecompressBufferCapacity = sizes[0];
      }
      uint32_t len = decompressOneBlock(sizes[1], _tempDecompressBuffer, sizes[0]);
      if (len != sizes[0]) {
        THROW_EXCEPTION(IOException, "Block decompress data error, length not match");
      }
      _tempDecompressBufferSize = sizes[0];
      _tempDecompressBufferUsed = 0;
    }
  }
  if (_tempDecompressBufferSize > 0) {
    uint32_t left = _tempDecompressBufferSize - _tempDecompressBufferUsed;
    if (length < left) {
      memcpy(buff, _tempDecompressBuffer + _tempDecompressBufferUsed, length);
      _tempDecompressBufferUsed += length;
      return length;
    } else {
      memcpy(buff, _tempDecompressBuffer + _tempDecompressBufferUsed, left);
      _tempDecompressBufferSize = 0;
      _tempDecompressBufferUsed = 0;
      return left;
    }
  }
  // should not get here
  THROW_EXCEPTION(IOException, "Decompress logic error");
  return -1;
}

void BlockDecompressStream::close() {
  if (_tempDecompressBufferSize > 0) {
    LOG("[BlockDecompressStream] Some data left in the _tempDecompressBuffer when close()");
  }
  if (NULL != _tempDecompressBuffer) {
    free(_tempDecompressBuffer);
    _tempDecompressBuffer = NULL;
    _tempDecompressBufferCapacity = 0;
  }
  _tempDecompressBufferSize = 0;
  _tempDecompressBufferUsed = 0;
}

int32_t BlockDecompressStream::readDirect(void * buff, uint32_t length) {
  if (_tempDecompressBufferSize > 0) {
    THROW_EXCEPTION(IOException, "temp decompress data exists when call readDirect()");
  }
  int32_t ret = _stream->readFully(buff, length);
  if (ret > 0) {
    _compressedBytesRead += ret;
  }
  return ret;
}

uint64_t BlockDecompressStream::compressedBytesRead() {
  return _compressedBytesRead;
}

} // namespace NativeTask

