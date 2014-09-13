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

#include "config.h"

#if defined HADOOP_SNAPPY_LIBRARY
#include "lib/commons.h"
#include "NativeTask.h"
#include "SnappyCodec.h"

#include <snappy-c.h>

namespace NativeTask {

SnappyCompressStream::SnappyCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : BlockCompressStream(stream, bufferSizeHint) {
  init();
}

void SnappyCompressStream::compressOneBlock(const void * buff, uint32_t length) {
  size_t compressedLength = _tempBufferSize - 8;
  snappy_status ret = snappy_compress((const char*)buff, length, _tempBuffer + 8,
      &compressedLength);
  if (ret == SNAPPY_OK) {
    ((uint32_t*)_tempBuffer)[0] = bswap(length);
    ((uint32_t*)_tempBuffer)[1] = bswap((uint32_t)compressedLength);
    _stream->write(_tempBuffer, compressedLength + 8);
    _compressedBytesWritten += (compressedLength + 8);
  } else if (ret == SNAPPY_INVALID_INPUT) {
    THROW_EXCEPTION(IOException, "compress SNAPPY_INVALID_INPUT");
  } else if (ret == SNAPPY_BUFFER_TOO_SMALL) {
    THROW_EXCEPTION(IOException, "compress SNAPPY_BUFFER_TOO_SMALL");
  } else {
    THROW_EXCEPTION(IOException, "compress snappy failed");
  }
}

uint64_t SnappyCompressStream::maxCompressedLength(uint64_t origLength) {
  return snappy_max_compressed_length(origLength);
}

//////////////////////////////////////////////////////////////

SnappyDecompressStream::SnappyDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : BlockDecompressStream(stream, bufferSizeHint) {
  init();
}

uint32_t SnappyDecompressStream::decompressOneBlock(uint32_t compressedSize, void * buff,
    uint32_t length) {
  if (compressedSize > _tempBufferSize) {
    char * newBuffer = (char *)realloc(_tempBuffer, compressedSize);
    if (newBuffer == NULL) {
      THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
    }
    _tempBuffer = newBuffer;
    _tempBufferSize = compressedSize;
  }
  uint32_t rd = _stream->readFully(_tempBuffer, compressedSize);
  if (rd != compressedSize) {
    THROW_EXCEPTION(IOException, "readFully reach EOF");
  }
  _compressedBytesRead += rd;
  size_t uncompressedLength = length;
  snappy_status ret = snappy_uncompress(_tempBuffer, compressedSize, (char *)buff,
      &uncompressedLength);
  if (ret == SNAPPY_OK) {
    return uncompressedLength;
  } else if (ret == SNAPPY_INVALID_INPUT) {
    THROW_EXCEPTION(IOException, "decompress SNAPPY_INVALID_INPUT");
  } else if (ret == SNAPPY_BUFFER_TOO_SMALL) {
    THROW_EXCEPTION(IOException, "decompress SNAPPY_BUFFER_TOO_SMALL");
  } else {
    THROW_EXCEPTION(IOException, "decompress snappy failed");
  }
}

uint64_t SnappyDecompressStream::maxCompressedLength(uint64_t origLength) {
  return snappy_max_compressed_length(origLength);
}
} // namespace NativeTask

#endif // define HADOOP_SNAPPY_LIBRARY
