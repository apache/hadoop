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
#include "util/StringUtil.h"
#include "lib/IFile.h"
#include "lib/Compressions.h"
#include "lib/FileSystem.h"

namespace NativeTask {

///////////////////////////////////////////////////////////

IFileReader::IFileReader(InputStream * stream, SingleSpillInfo * spill, bool deleteInputStream)
    :  _stream(stream), _source(NULL), _checksumType(spill->checkSumType), _kType(spill->keyType),
        _vType(spill->valueType), _codec(spill->codec), _segmentIndex(-1), _spillInfo(spill),
        _valuePos(NULL), _valueLen(0), _deleteSourceStream(deleteInputStream) {
  _source = new ChecksumInputStream(_stream, _checksumType);
  _source->setLimit(0);
  _reader.init(128 * 1024, _source, _codec);
}

IFileReader::~IFileReader() {

  delete _source;
  _source = NULL;

  if (_deleteSourceStream) {
    delete _stream;
    _stream = NULL;
  }
}

/**
 * 0 if success
 * 1 if end
 */
bool IFileReader::nextPartition() {
  if (0 != _source->getLimit()) {
    THROW_EXCEPTION(IOException, "bad ifile segment length");
  }
  if (_segmentIndex >= 0) {
    // verify checksum
    uint32_t chsum = 0;
    if (4 != _stream->readFully(&chsum, 4)) {
      THROW_EXCEPTION(IOException, "read ifile checksum failed");
    }
    uint32_t actual = bswap(chsum);
    uint32_t expect = _source->getChecksum();
    if (actual != expect) {
      THROW_EXCEPTION_EX(IOException, "read ifile checksum not match, actual %x expect %x", actual,
          expect);
    }
  }
  _segmentIndex++;
  if (_segmentIndex < (int)(_spillInfo->length)) {
    int64_t end_pos = (int64_t)_spillInfo->segments[_segmentIndex].realEndOffset;
    if (_segmentIndex > 0) {
      end_pos -= (int64_t)_spillInfo->segments[_segmentIndex - 1].realEndOffset;
    }
    if (end_pos < 0) {
      THROW_EXCEPTION(IOException, "bad ifile format");
    }
    // exclude checksum
    _source->setLimit(end_pos - 4);
    _source->resetChecksum();
    return true;
  } else {
    return false;
  }
}

///////////////////////////////////////////////////////////

IFileWriter * IFileWriter::create(const std::string & filepath, const MapOutputSpec & spec,
    Counter * spilledRecords) {
  OutputStream * fout = FileSystem::getLocal().create(filepath, true);
  IFileWriter * writer = new IFileWriter(fout, spec.checksumType, spec.keyType, spec.valueType,
      spec.codec, spilledRecords, true);
  return writer;
}

IFileWriter::IFileWriter(OutputStream * stream, ChecksumType checksumType, KeyValueType ktype,
    KeyValueType vtype, const string & codec, Counter * counter, bool deleteTargetStream)
    : _stream(stream), _dest(NULL), _checksumType(checksumType), _kType(ktype), _vType(vtype),
        _codec(codec), _recordCounter(counter), _recordCount(0), _deleteTargetStream(deleteTargetStream) {
  _dest = new ChecksumOutputStream(_stream, _checksumType);
  _appendBuffer.init(128 * 1024, _dest, _codec);
}

IFileWriter::~IFileWriter() {
  delete _dest;
  _dest = NULL;

  if (_deleteTargetStream) {
    delete _stream;
    _stream = NULL;
  }
}

void IFileWriter::startPartition() {
  _spillFileSegments.push_back(IFileSegment());
  _dest->resetChecksum();
}

void IFileWriter::endPartition() {
  char EOFMarker[2] = {-1, -1};
  _appendBuffer.write(EOFMarker, 2);
  _appendBuffer.flush();

  CompressStream * compressionStream = _appendBuffer.getCompressionStream();
  if (NULL != compressionStream) {
    compressionStream->finish();
    compressionStream->resetState();
  }

  uint32_t chsum = _dest->getChecksum();
  chsum = bswap(chsum);
  _stream->write(&chsum, sizeof(chsum));
  _stream->flush();
  IFileSegment * info = &(_spillFileSegments[_spillFileSegments.size() - 1]);
  info->uncompressedEndOffset = _appendBuffer.getCounter();
  info->realEndOffset = _stream->tell();
}

void IFileWriter::write(const char * key, uint32_t keyLen, const char * value, uint32_t valueLen) {
  // append KeyLength ValueLength KeyBytesLength
  uint32_t keyBuffLen = keyLen;
  uint32_t valBuffLen = valueLen;
  switch (_kType) {
  case TextType:
    keyBuffLen += WritableUtils::GetVLongSize(keyLen);
    break;
  case BytesType:
    keyBuffLen += 4;
    break;
  default:
    break;
  }

  switch (_vType) {
  case TextType:
    valBuffLen += WritableUtils::GetVLongSize(valueLen);
    break;
  case BytesType:
    valBuffLen += 4;
    break;
  default:
    break;
  }

  _appendBuffer.write_vuint2(keyBuffLen, valBuffLen);

  switch (_kType) {
  case TextType:
    _appendBuffer.write_vuint(keyLen);
    break;
  case BytesType:
    _appendBuffer.write_uint32_be(keyLen);
    break;
  default:
    break;
  }

  if (keyLen > 0) {
    _appendBuffer.write(key, keyLen);
  }

  if (NULL != _recordCounter) {
    _recordCounter->increase();
  }
  _recordCount++;

  switch (_vType) {
  case TextType:
    _appendBuffer.write_vuint(valueLen);
    break;
  case BytesType:
    _appendBuffer.write_uint32_be(valueLen);
    break;
  default:
    break;
  }
  if (valueLen > 0) {
    _appendBuffer.write(value, valueLen);
  }
}

IFileSegment * IFileWriter::toArray(std::vector<IFileSegment> *segments) {
  IFileSegment * segs = new IFileSegment[segments->size()];
  for (size_t i = 0; i < segments->size(); i++) {
    segs[i] = segments->at(i);
  }
  return segs;
}

SingleSpillInfo * IFileWriter::getSpillInfo() {
  const uint32_t size = _spillFileSegments.size();
  return new SingleSpillInfo(toArray(&_spillFileSegments), size, "", _checksumType, _kType, _vType,
      _codec);
}

void IFileWriter::getStatistics(uint64_t & offset, uint64_t & realOffset, uint64_t & recordCount) {
  if (_spillFileSegments.size() > 0) {
    offset = _spillFileSegments[_spillFileSegments.size() - 1].uncompressedEndOffset;
    realOffset = _spillFileSegments[_spillFileSegments.size() - 1].realEndOffset;
  } else {
    offset = 0;
    realOffset = 0;
  }
  recordCount = _recordCount;
}

} // namespace NativeTask

