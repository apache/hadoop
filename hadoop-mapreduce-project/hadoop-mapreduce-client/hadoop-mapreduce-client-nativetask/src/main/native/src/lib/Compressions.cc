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
#include "config.h"
#include "lib/Compressions.h"
#include "util/SyncUtils.h"
#include "codec/GzipCodec.h"
#include "codec/SnappyCodec.h"
#include "codec/Lz4Codec.h"

namespace NativeTask {

CompressStream::~CompressStream() {
}

void CompressStream::writeDirect(const void * buff, uint32_t length) {
  THROW_EXCEPTION(UnsupportException, "writeDirect not support");
}

///////////////////////////////////////////////////////////

DecompressStream::~DecompressStream() {
}

int32_t DecompressStream::readDirect(void * buff, uint32_t length) {
  THROW_EXCEPTION(UnsupportException, "readDirect not support");
}

///////////////////////////////////////////////////////////

const Compressions::Codec Compressions::GzipCodec = Compressions::Codec(
    "org.apache.hadoop.io.compress.GzipCodec", ".gz");
const Compressions::Codec Compressions::SnappyCodec = Compressions::Codec(
    "org.apache.hadoop.io.compress.SnappyCodec", ".snappy");
const Compressions::Codec Compressions::Lz4Codec = Compressions::Codec(
    "org.apache.hadoop.io.compress.Lz4Codec", ".lz4");

vector<Compressions::Codec> Compressions::SupportedCodecs = vector<Compressions::Codec>();

void Compressions::initCodecs() {
  static Lock lock;
  ScopeLock<Lock> autolock(lock);
  if (SupportedCodecs.size() == 0) {
    SupportedCodecs.push_back(GzipCodec);
    SupportedCodecs.push_back(SnappyCodec);
    SupportedCodecs.push_back(Lz4Codec);
  }
}

bool Compressions::support(const string & codec) {
  initCodecs();
  for (size_t i = 0; i < SupportedCodecs.size(); i++) {
    if (codec == SupportedCodecs[i].name) {
      return true;
    }
  }
  return false;
}

const string Compressions::getExtension(const string & codec) {
  initCodecs();
  for (size_t i = 0; i < SupportedCodecs.size(); i++) {
    if (codec == SupportedCodecs[i].name) {
      return SupportedCodecs[i].extension;
    }
  }
  return string();
}

const string Compressions::getCodec(const string & extension) {
  initCodecs();
  for (size_t i = 0; i < SupportedCodecs.size(); i++) {
    if (extension == SupportedCodecs[i].extension) {
      return SupportedCodecs[i].name;
    }
  }
  return string();
}

const string Compressions::getCodecByFile(const string & file) {
  initCodecs();
  for (size_t i = 0; i < SupportedCodecs.size(); i++) {
    const string & extension = SupportedCodecs[i].extension;
    if ((file.length() > extension.length())
        && (file.substr(file.length() - extension.length()) == extension)) {
      return SupportedCodecs[i].name;
    }
  }
  return string();
}

CompressStream * Compressions::getCompressionStream(const string & codec, OutputStream * stream,
    uint32_t bufferSizeHint) {
  if (codec == GzipCodec.name) {
    return new GzipCompressStream(stream, bufferSizeHint);
  }
  if (codec == SnappyCodec.name) {
#if defined HADOOP_SNAPPY_LIBRARY
    return new SnappyCompressStream(stream, bufferSizeHint);
#else
    THROW_EXCEPTION(UnsupportException, "Snappy library is not loaded");
#endif
  }
  if (codec == Lz4Codec.name) {
    return new Lz4CompressStream(stream, bufferSizeHint);
  }
  return NULL;
}

DecompressStream * Compressions::getDecompressionStream(const string & codec, InputStream * stream,
    uint32_t bufferSizeHint) {
  if (codec == GzipCodec.name) {
    return new GzipDecompressStream(stream, bufferSizeHint);
  }
  if (codec == SnappyCodec.name) {
#if defined HADOOP_SNAPPY_LIBRARY
    return new SnappyDecompressStream(stream, bufferSizeHint);
#else
    THROW_EXCEPTION(UnsupportException, "Snappy library is not loaded");
#endif
  }
  if (codec == Lz4Codec.name) {
    return new Lz4DecompressStream(stream, bufferSizeHint);
  }
  return NULL;
}

} // namespace NativeTask

