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

#ifndef GZIPCODEC_H_
#define GZIPCODEC_H_

#include "lib/Compressions.h"

namespace NativeTask {

class GzipCompressStream : public CompressStream {
protected:
  uint64_t _compressedBytesWritten;
  char * _buffer;
  uint32_t _capacity;
  void * _zstream;
  bool _finished;
public:
  GzipCompressStream(OutputStream * stream, uint32_t bufferSizeHint);

  virtual ~GzipCompressStream();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();

  virtual void finish() {
    flush();
  }

  virtual void resetState();

  virtual void writeDirect(const void * buff, uint32_t length);

  virtual uint64_t compressedBytesWritten() {
    return _compressedBytesWritten;
  }
};

class GzipDecompressStream : public DecompressStream {
protected:
  uint64_t _compressedBytesRead;
  char * _buffer;
  uint32_t _capacity;
  void * _zstream;
  bool _eof;
public:
  GzipDecompressStream(InputStream * stream, uint32_t bufferSizeHint);

  virtual ~GzipDecompressStream();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();

  virtual int32_t readDirect(void * buff, uint32_t length);

  virtual uint64_t compressedBytesRead() {
    return _compressedBytesRead;
  }
};

} // namespace NativeTask

#endif /* GZIPCODEC_H_ */
