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

#ifndef BLOCKCODEC_H_
#define BLOCKCODEC_H_

#include "lib/Compressions.h"

namespace NativeTask {

class BlockCompressStream : public CompressStream {
protected:
  uint32_t _hint;
  uint32_t _blockMax;
  char * _tempBuffer;
  uint32_t _tempBufferSize;
  uint64_t _compressedBytesWritten;
public:
  BlockCompressStream(OutputStream * stream, uint32_t bufferSizeHint);

  virtual ~BlockCompressStream();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();

  virtual void writeDirect(const void * buff, uint32_t length);

  virtual uint64_t compressedBytesWritten();

  void init();

protected:
  virtual uint64_t maxCompressedLength(uint64_t origLength) {
    return origLength;
  }

  virtual void compressOneBlock(const void * buff, uint32_t length) {
  }
};

class BlockDecompressStream : public DecompressStream {
protected:
  uint32_t _hint;
  uint32_t _blockMax;
  char * _tempBuffer;
  uint32_t _tempBufferSize;
  char * _tempDecompressBuffer;
  uint32_t _tempDecompressBufferSize;
  uint32_t _tempDecompressBufferUsed;
  uint32_t _tempDecompressBufferCapacity;
  uint64_t _compressedBytesRead;
public:
  BlockDecompressStream(InputStream * stream, uint32_t bufferSizeHint);

  virtual ~BlockDecompressStream();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();

  virtual int32_t readDirect(void * buff, uint32_t length);

  virtual uint64_t compressedBytesRead();

  void init();

protected:
  virtual uint64_t maxCompressedLength(uint64_t origLength) {
    return origLength;
  }

  virtual uint32_t decompressOneBlock(uint32_t compressedSize, void * buff, uint32_t length) {
    //TODO: add implementation
    return 0;
  }
};

} // namespace NativeTask

#endif /* BLOCKCODEC_H_ */
