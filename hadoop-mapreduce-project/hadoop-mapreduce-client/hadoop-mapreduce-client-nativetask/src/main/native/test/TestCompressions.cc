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

#include "lz4.h"
#include "config.h"
#include "lib/commons.h"
#include "lib/Path.h"
#include "lib/BufferStream.h"
#include "lib/FileSystem.h"
#include "lib/Compressions.h"
#include "test_commons.h"

#if defined HADOOP_SNAPPY_LIBRARY
#include <snappy.h>
#endif

void TestCodec(const string & codec) {
  string data;
  size_t length = TestConfig.getInt("compression.input.length", 100 * 1024 * 1024);
  uint32_t buffhint = TestConfig.getInt("compression.buffer.hint", 128 * 1024);
  string type = TestConfig.get("compression.input.type", "bytes");
  Timer timer;
  GenerateKVTextLength(data, length, type);
  LOG("%s", timer.getInterval("Generate data").c_str());

  InputBuffer inputBuffer = InputBuffer(data);
  size_t buffLen = data.length() / 2 * 3;

  timer.reset();
  char * buff = new char[buffLen];
  char * buff2 = new char[buffLen];
  memset(buff, 0, buffLen);
  memset(buff2, 0, buffLen);
  LOG("%s", timer.getInterval("memset buffer to prevent missing page").c_str());

  OutputBuffer outputBuffer = OutputBuffer(buff, buffLen);
  CompressStream * compressor = Compressions::getCompressionStream(codec, &outputBuffer, buffhint);

  LOG("%s", codec.c_str());
  timer.reset();
  for (size_t i = 0; i < data.length(); i += 128 * 1024) {
    compressor->write(data.c_str() + i, std::min(data.length() - i, (size_t)(128 * 1024)));
  }
  compressor->flush();
  LOG("%s",
      timer.getSpeedM2("compress origin/compressed", data.length(), outputBuffer.tell()).c_str());

  InputBuffer decompInputBuffer = InputBuffer(buff, outputBuffer.tell());
  DecompressStream * decompressor = Compressions::getDecompressionStream(codec, &decompInputBuffer,
      buffhint);
  size_t total = 0;
  timer.reset();
  while (true) {
    int32_t rd = decompressor->read(buff2 + total, buffLen - total);
    if (rd <= 0) {
      break;
    }
    total += rd;
  }
  LOG("%s", timer.getSpeedM2("decompress origin/uncompressed", outputBuffer.tell(), total).c_str());
  LOG("ratio: %.3lf", outputBuffer.tell() / (double )total);
  ASSERT_EQ(data.length(), total);
  ASSERT_EQ(0, memcmp(data.c_str(), buff2, total));

  delete[] buff;
  delete[] buff2;
  delete compressor;
  delete decompressor;
}

TEST(Perf, CompressionUtil) {
  string inputfile = TestConfig.get("input", "");
  string outputfile = TestConfig.get("output", "");
  uint32_t buffhint = TestConfig.getInt("compression.buffer.hint", 128 * 1024);
  string inputcodec = Compressions::getCodecByFile(inputfile);
  string outputcodec = Compressions::getCodecByFile(outputfile);
  size_t bufferSize = buffhint;
  if (inputcodec.length() > 0 && outputcodec.length() == 0) {
    // decompression
    InputStream * fin = FileSystem::getLocal().open(inputfile);
    if (fin == NULL) {
      THROW_EXCEPTION(IOException, "input file not found");
    }
    DecompressStream * source = Compressions::getDecompressionStream(inputcodec, fin, bufferSize);
    OutputStream * fout = FileSystem::getLocal().create(outputfile, true);
    char * buffer = new char[bufferSize];
    while (true) {
      int rd = source->read(buffer, bufferSize);
      if (rd <= 0) {
        break;
      }
      fout->write(buffer, rd);
    }
    source->close();
    delete source;
    fin->close();
    delete fin;
    fout->flush();
    fout->close();
    delete fout;
    delete buffer;
  } else if (inputcodec.length() == 0 && outputcodec.length() > 0) {
    // compression
    InputStream * fin = FileSystem::getLocal().open(inputfile);
    if (fin == NULL) {
      THROW_EXCEPTION(IOException, "input file not found");
    }
    OutputStream * fout = FileSystem::getLocal().create(outputfile, true);
    CompressStream * dest = Compressions::getCompressionStream(outputcodec, fout, bufferSize);
    char * buffer = new char[bufferSize];
    while (true) {
      int rd = fin->read(buffer, bufferSize);
      if (rd <= 0) {
        break;
      }
      dest->write(buffer, rd);
    }
    dest->flush();
    dest->close();
    delete dest;
    fout->close();
    delete fout;
    fin->close();
    delete fin;
    delete buffer;
  } else {
    LOG("Not compression or decompression, do nothing");
  }
}

class CompressResult {
 public:
  uint64_t uncompressedSize;
  uint64_t compressedSize;
  uint64_t compressTime;
  uint64_t uncompressTime;
  CompressResult()
      : uncompressedSize(0), compressedSize(0), compressTime(0), uncompressTime(0) {
  }
  CompressResult & operator+=(const CompressResult & rhs) {
    uncompressedSize += rhs.uncompressedSize;
    compressedSize += rhs.compressedSize;
    compressTime += rhs.compressTime;
    uncompressTime += rhs.uncompressTime;
    return *this;
  }
  string toString() {
    return StringUtil::Format("Compress: %4.0fM/s Decompress: %5.0fM/s(%5.0fM/s) ratio: %.1f%%",
        (uncompressedSize / 1024.0 / 1024) / (compressTime / 1000000000.),
        (compressedSize / 1024.0 / 1024) / (uncompressTime / 1000000000.),
        (uncompressedSize / 1024.0 / 1024) / (uncompressTime / 1000000000.),
        compressedSize / (float)uncompressedSize * 100);
  }
};

TEST(Perf, GzipCodec) {
  TestCodec("org.apache.hadoop.io.compress.GzipCodec");
}

void MeasureSingleFileLz4(const string & path, CompressResult & total, size_t blockSize,
    int times) {
  string data;
  ReadFile(data, path);
  size_t maxlength = std::max((size_t)(blockSize * 1.005), blockSize + 8);
  char * outputBuffer = new char[maxlength];
  char * dest = new char[blockSize + 8];
  CompressResult result;
  Timer t;
  for (size_t start = 0; start < data.length(); start += blockSize) {
    size_t currentblocksize = std::min(data.length() - start, blockSize);
    uint64_t startTime = t.now();
    for (int i = 0; i < times; i++) {
      int osize = LZ4_compress((char*)data.data() + start, outputBuffer, currentblocksize);
      result.compressedSize += osize;
      result.uncompressedSize += currentblocksize;
    }
    uint64_t endTime = t.now();
    result.compressTime += endTime - startTime;
    startTime = t.now();
    for (int i = 0; i < times; i++) {
      int osize = LZ4_decompress_fast(outputBuffer, dest, currentblocksize);
      ASSERT_EQ(currentblocksize, osize);
    }
    endTime = t.now();
    result.uncompressTime += endTime - startTime;
  }
  printf("%s - %s\n", result.toString().c_str(), Path::GetName(path).c_str());
  delete[] outputBuffer;
  delete[] dest;
  total += result;
}

TEST(Perf, RawCompressionLz4) {
  string inputdir = TestConfig.get("compressions.input.path", "");
  int64_t times = TestConfig.getInt("compression.time", 400);
  int64_t blockSize = TestConfig.getInt("compression.block.size", 1024 * 64);
  vector<FileEntry> inputfiles;
  FileSystem::getLocal().list(inputdir, inputfiles);
  CompressResult total;
  printf("Block size: %lldK\n", (long long int)(blockSize / 1024));
  for (size_t i = 0; i < inputfiles.size(); i++) {
    if (!inputfiles[i].isDirectory) {
      MeasureSingleFileLz4((inputdir + "/" + inputfiles[i].name).c_str(), total, blockSize, times);
    }
  }
  printf("%s - Total\n", total.toString().c_str());
}

TEST(Perf, Lz4Codec) {
  TestCodec("org.apache.hadoop.io.compress.Lz4Codec");
}

#if defined HADOOP_SNAPPY_LIBRARY

void MeasureSingleFileSnappy(const string & path, CompressResult & total, size_t blockSize,
    int times) {
  string data;
  ReadFile(data, path);
  size_t maxlength = snappy::MaxCompressedLength(blockSize);
  char * outputBuffer = new char[maxlength];
  char * dest = new char[blockSize];
  CompressResult result;
  Timer t;
  int compressedSize = -1;
  for (size_t start = 0; start < data.length(); start += blockSize) {
    size_t currentblocksize = std::min(data.length() - start, blockSize);
    uint64_t startTime = t.now();
    for (int i = 0; i < times; i++) {
      size_t osize = maxlength;
      snappy::RawCompress(data.data() + start, currentblocksize, outputBuffer, &osize);
      compressedSize = osize;
      result.compressedSize += osize;
      result.uncompressedSize += currentblocksize;
    }
    uint64_t endTime = t.now();
    result.compressTime += endTime - startTime;
    startTime = t.now();
    for (int i = 0; i < times; i++) {
      snappy::RawUncompress(outputBuffer, compressedSize, dest);
    }
    endTime = t.now();
    result.uncompressTime += endTime - startTime;
  }
  printf("%s - %s\n", result.toString().c_str(), Path::GetName(path).c_str());
  delete[] outputBuffer;
  delete[] dest;
  total += result;
}

TEST(Perf, RawCompressionSnappy) {
  string inputdir = TestConfig.get("compressions.input.path", "");
  int64_t times = TestConfig.getInt("compression.time", 400);
  int64_t blockSize = TestConfig.getInt("compression.block.size", 1024 * 64);
  vector<FileEntry> inputfiles;
  FileSystem::getLocal().list(inputdir, inputfiles);
  CompressResult total;
  printf("Block size: %"PRId64"K\n", blockSize / 1024);
  for (size_t i = 0; i < inputfiles.size(); i++) {
    if (!inputfiles[i].isDirectory) {
      MeasureSingleFileSnappy((inputdir + "/" + inputfiles[i].name).c_str(), total, blockSize,
          times);
    }
  }
  printf("%s - Total\n", total.toString().c_str());
}

TEST(Perf, SnappyCodec) {
  TestCodec("org.apache.hadoop.io.compress.SnappyCodec");
}

#endif // define HADOOP_SNAPPY_LIBRARY
