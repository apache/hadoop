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

#include <zconf.h>
#include <zlib.h>
#include "lib/commons.h"
#include "GzipCodec.h"
#include <iostream>

namespace NativeTask {

GzipCompressStream::GzipCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : CompressStream(stream), _compressedBytesWritten(0), _zstream(NULL), _finished(false) {
  _buffer = new char[bufferSizeHint];
  _capacity = bufferSizeHint;
  _zstream = malloc(sizeof(z_stream));
  z_stream * zstream = (z_stream*)_zstream;
  memset(zstream, 0, sizeof(z_stream));
  if (Z_OK != deflateInit2(zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 31, 8,
      Z_DEFAULT_STRATEGY)) {
    free(_zstream);
    _zstream = NULL;
    THROW_EXCEPTION(IOException, "deflateInit2 failed");
  }
  zstream->next_out = (Bytef *)_buffer;
  zstream->avail_out = _capacity;
}

GzipCompressStream::~GzipCompressStream() {
  if (_zstream != NULL) {
    deflateEnd((z_stream*)_zstream);
    free(_zstream);
    _zstream = NULL;
  }
  delete[] _buffer;
  _buffer = NULL;
}

void GzipCompressStream::write(const void * buff, uint32_t length) {
  z_stream * zstream = (z_stream*)_zstream;
  zstream->next_in = (Bytef*)buff;
  zstream->avail_in = length;
  while (true) {
    int ret = deflate(zstream, Z_NO_FLUSH);
    if (ret == Z_OK) {
      if (zstream->avail_out == 0) {
        _stream->write(_buffer, _capacity);
        _compressedBytesWritten += _capacity;
        zstream->next_out = (Bytef *)_buffer;
        zstream->avail_out = _capacity;
      }
      if (zstream->avail_in == 0) {
        break;
      }
    } else {
      THROW_EXCEPTION(IOException, "deflate return error");
    }
  }
  _finished = false;
}

void GzipCompressStream::flush() {
  z_stream * zstream = (z_stream*)_zstream;
  while (true) {
    int ret = deflate(zstream, Z_FINISH);
    if (ret == Z_OK) {
      if (zstream->avail_out == 0) {
        _stream->write(_buffer, _capacity);
        _compressedBytesWritten += _capacity;
        zstream->next_out = (Bytef *)_buffer;
        zstream->avail_out = _capacity;
      } else {
        THROW_EXCEPTION(IOException, "flush state error");
      }
    } else if (ret == Z_STREAM_END) {
      size_t wt = zstream->next_out - (Bytef*)_buffer;
      _stream->write(_buffer, wt);
      _compressedBytesWritten += wt;
      zstream->next_out = (Bytef *)_buffer;
      zstream->avail_out = _capacity;
      break;
    }
  }
  _finished = true;
  _stream->flush();
}

void GzipCompressStream::resetState() {
  z_stream * zstream = (z_stream*)_zstream;
  deflateReset(zstream);
}

void GzipCompressStream::close() {
  if (!_finished) {
    flush();
  }
}

void GzipCompressStream::writeDirect(const void * buff, uint32_t length) {
  if (!_finished) {
    flush();
  }
  _stream->write(buff, length);
  _compressedBytesWritten += length;
}

//////////////////////////////////////////////////////////////

GzipDecompressStream::GzipDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : DecompressStream(stream), _compressedBytesRead(0), _zstream(NULL) {
  _buffer = new char[bufferSizeHint];
  _capacity = bufferSizeHint;
  _zstream = malloc(sizeof(z_stream));
  z_stream * zstream = (z_stream*)_zstream;
  memset(zstream, 0, sizeof(z_stream));
  if (Z_OK != inflateInit2(zstream, 31)) {
    free(_zstream);
    _zstream = NULL;
    THROW_EXCEPTION(IOException, "inflateInit2 failed");
  }
  zstream->next_in = NULL;
  zstream->avail_in = 0;
  _eof = false;
}

GzipDecompressStream::~GzipDecompressStream() {
  if (_zstream != NULL) {
    inflateEnd((z_stream*)_zstream);
    free(_zstream);
    _zstream = NULL;
  }
  delete[] _buffer;
  _buffer = NULL;
}

int32_t GzipDecompressStream::read(void * buff, uint32_t length) {
  z_stream * zstream = (z_stream*)_zstream;
  zstream->next_out = (Bytef*)buff;
  zstream->avail_out = length;
  while (true) {
    if (zstream->avail_in == 0) {
      int32_t rd = _stream->read(_buffer, _capacity);
      if (rd <= 0) {
        _eof = true;
        size_t wt = zstream->next_out - (Bytef*)buff;
        return wt > 0 ? wt : -1;
      } else {
        _compressedBytesRead += rd;
        zstream->next_in = (Bytef*)_buffer;
        zstream->avail_in = rd;
      }
    }
    int ret = inflate(zstream, Z_NO_FLUSH);
    if (ret == Z_OK || ret == Z_STREAM_END) {
      if (zstream->avail_out == 0) {
        return length;
      }
    } else {
      return -1;
    }
  }
  return -1;
}

void GzipDecompressStream::close() {
}

int32_t GzipDecompressStream::readDirect(void * buff, uint32_t length) {
  int32_t ret = _stream->readFully(buff, length);
  if (ret > 0) {
    _compressedBytesRead += ret;
  }
  return ret;
}

} // namespace NativeTask

