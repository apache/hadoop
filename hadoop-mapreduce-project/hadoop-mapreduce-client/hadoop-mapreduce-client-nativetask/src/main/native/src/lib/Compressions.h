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

#ifndef COMPRESSIONS_H_
#define COMPRESSIONS_H_

#include <string>
#include <vector>
#include "lib/Streams.h"

namespace NativeTask {

using std::vector;
using std::string;

class CompressStream : public FilterOutputStream {
public:
  CompressStream(OutputStream * stream)
      : FilterOutputStream(stream) {
  }

  virtual ~CompressStream();

  virtual void writeDirect(const void * buff, uint32_t length);

  virtual void finish() {
    flush();
  }

  virtual void resetState() {

  }

  virtual uint64_t compressedBytesWritten() {
    return 0;
  }
};

class DecompressStream : public FilterInputStream {
public:
  DecompressStream(InputStream * stream)
      : FilterInputStream(stream) {
  }

  virtual ~DecompressStream();

  virtual int32_t readDirect(void * buff, uint32_t length);

  virtual uint64_t compressedBytesRead() {
    return 0;
  }
};

class Compressions {
protected:
  class Codec {
  public:
    string name;
    string extension;

    Codec(const string & name, const string & extension)
        : name(name), extension(extension) {
    }
  };

  static vector<Codec> SupportedCodecs;

  static void initCodecs();

public:
  static const Codec GzipCodec;
  static const Codec SnappyCodec;
  static const Codec Lz4Codec;

public:
  static bool support(const string & codec);

  static const string getExtension(const string & codec);

  static const string getCodec(const string & extension);

  static const string getCodecByFile(const string & file);

  static CompressStream * getCompressionStream(const string & codec, OutputStream * stream,
      uint32_t bufferSizeHint);

  static DecompressStream * getDecompressionStream(const string & codec, InputStream * stream,
      uint32_t bufferSizeHint);
};

} // namespace NativeTask

#endif /* COMPRESSIONS_H_ */
