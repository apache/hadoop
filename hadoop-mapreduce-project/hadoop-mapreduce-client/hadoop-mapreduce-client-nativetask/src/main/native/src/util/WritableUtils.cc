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
#include "util/WritableUtils.h"

namespace NativeTask {

KeyValueType JavaClassToKeyValueType(const std::string & clazz) {
  if (clazz == "org.apache.hadoop.io.Text") {
    return TextType;
  }
  if (clazz == "org.apache.hadoop.io.BytesWritable") {
    return BytesType;
  }
  if (clazz == "org.apache.hadoop.io.ByteWritable") {
    return ByteType;
  }
  if (clazz == "org.apache.hadoop.io.BooleanWritable") {
    return BoolType;
  }
  if (clazz == "org.apache.hadoop.io.IntWritable") {
    return IntType;
  }
  if (clazz == "org.apache.hadoop.io.LongWritable") {
    return LongType;
  }
  if (clazz == "org.apache.hadoop.io.FloatWritable") {
    return FloatType;
  }
  if (clazz == "org.apache.hadoop.io.DoubleWritable") {
    return DoubleType;
  }
  if (clazz == "org.apache.hadoop.io.MD5Hash") {
    return MD5HashType;
  }
  if (clazz == "org.apache.hadoop.io.VIntWritable") {
    return VIntType;
  }
  if (clazz == "org.apache.hadoop.io.VLongWritable") {
    return VLongType;
  }
  return UnknownType;
}

int64_t WritableUtils::ReadVLongInner(const char * pos, uint32_t & len) {
  bool neg = *pos < -120;
  len = neg ? (-119 - *pos) : (-111 - *pos);
  const char * end = pos + len;
  int64_t value = 0;
  while (++pos < end) {
    value = (value << 8) | *(uint8_t*)pos;
  }
  return neg ? (value ^ -1LL) : value;
}

uint32_t WritableUtils::GetVLongSizeInner(int64_t value) {
  if (value < 0) {
    value ^= -1L; // take one's complement'
  }

  if (value < (1LL << 8)) {
    return 2;
  } else if (value < (1LL << 16)) {
    return 3;
  } else if (value < (1LL << 24)) {
    return 4;
  } else if (value < (1LL << 32)) {
    return 5;
  } else if (value < (1LL << 40)) {
    return 6;
  } else if (value < (1LL << 48)) {
    return 7;
  } else if (value < (1LL << 56)) {
    return 8;
  } else {
    return 9;
  }
}

void WritableUtils::WriteVLongInner(int64_t v, char * pos, uint32_t & len) {
  char base;
  if (v >= 0) {
    base = -113;
  } else {
    v ^= -1L; // take one's complement
    base = -121;
  }
  uint64_t value = v;
  if (value < (1 << 8)) {
    *(pos++) = base;
    *(uint8_t*)(pos) = value;
    len = 2;
  } else if (value < (1 << 16)) {
    *(pos++) = base - 1;
    *(uint8_t*)(pos++) = value >> 8;
    *(uint8_t*)(pos) = value;
    len = 3;
  } else if (value < (1 << 24)) {
    *(pos++) = base - 2;
    *(uint8_t*)(pos++) = value >> 16;
    *(uint8_t*)(pos++) = value >> 8;
    *(uint8_t*)(pos) = value;
    len = 4;
  } else if (value < (1ULL << 32)) {
    *(pos++) = base - 3;
    *(uint32_t*)(pos) = bswap((uint32_t)value);
    len = 5;
  } else if (value < (1ULL << 40)) {
    *(pos++) = base - 4;
    *(uint32_t*)(pos) = bswap((uint32_t)(value >> 8));
    *(uint8_t*)(pos + 4) = value;
    len = 6;
  } else if (value < (1ULL << 48)) {
    *(pos++) = base - 5;
    *(uint32_t*)(pos) = bswap((uint32_t)(value >> 16));
    *(uint8_t*)(pos + 4) = value >> 8;
    *(uint8_t*)(pos + 5) = value;
    len = 7;
  } else if (value < (1ULL << 56)) {
    *(pos++) = base - 6;
    *(uint32_t*)(pos) = bswap((uint32_t)(value >> 24));
    *(uint8_t*)(pos + 4) = value >> 16;
    *(uint8_t*)(pos + 5) = value >> 8;
    *(uint8_t*)(pos + 6) = value;
    len = 8;
  } else {
    *(pos++) = base - 7;
    *(uint64_t*)pos = bswap64(value);
    len = 9;
  }
}

// Stream interfaces
int64_t WritableUtils::ReadVLong(InputStream * stream) {
  char buff[10];
  if (stream->read(buff, 1) != 1) {
    THROW_EXCEPTION(IOException, "ReadVLong reach EOF");
  }
  uint32_t len = DecodeVLongSize(buff);
  if (len > 1) {
    if (stream->readFully(buff + 1, len - 1) != len - 1) {
      THROW_EXCEPTION(IOException, "ReadVLong reach EOF");
    }
  }
  return ReadVLong(buff, len);
}

int64_t WritableUtils::ReadLong(InputStream * stream) {
  int64_t ret;
  if (stream->readFully(&ret, 8) != 8) {
    THROW_EXCEPTION(IOException, "ReadLong reach EOF");
  }
  return (int64_t)bswap64(ret);
}

int32_t WritableUtils::ReadInt(InputStream * stream) {
  int32_t ret;
  if (stream->readFully(&ret, 4) != 4) {
    THROW_EXCEPTION(IOException, "ReadInt reach EOF");
  }
  return (int32_t)bswap(ret);
}

int16_t WritableUtils::ReadShort(InputStream * stream) {
  uint16_t ret;
  if (stream->readFully(&ret, 2) != 2) {
    THROW_EXCEPTION(IOException, "ReadShort reach EOF");
  }
  return (int16_t)((ret >> 8) | (ret << 8));
}

float WritableUtils::ReadFloat(InputStream * stream) {
  uint32_t ret;
  if (stream->readFully(&ret, 4) != 4) {
    THROW_EXCEPTION(IOException, "ReadFloat reach EOF");
  }
  ret = bswap(ret);
  return *(float*)&ret;
}

string WritableUtils::ReadText(InputStream * stream) {
  int64_t len = ReadVLong(stream);
  string ret = string(len, '\0');
  if (stream->readFully((void *)ret.data(), len) != len) {
    THROW_EXCEPTION_EX(IOException, "ReadString reach EOF, need %d", len);
  }
  return ret;
}

string WritableUtils::ReadBytes(InputStream * stream) {
  int32_t len = ReadInt(stream);
  string ret = string(len, '\0');
  if (stream->readFully((void *)ret.data(), len) != len) {
    THROW_EXCEPTION_EX(IOException, "ReadString reach EOF, need %d", len);
  }
  return ret;
}

string WritableUtils::ReadUTF8(InputStream * stream) {
  int16_t len = ReadShort(stream);
  string ret = string(len, '\0');
  if (stream->readFully((void *)ret.data(), len) != len) {
    THROW_EXCEPTION_EX(IOException, "ReadString reach EOF, need %d", len);
  }
  return ret;
}


void WritableUtils::WriteVLong(OutputStream * stream, int64_t v) {
  char buff[10];
  uint32_t len;
  WriteVLong(v, buff, len);
  stream->write(buff, len);
}

void WritableUtils::WriteLong(OutputStream * stream, int64_t v) {
  uint64_t be = bswap64((uint64_t)v);
  stream->write(&be, 8);
}

void WritableUtils::WriteInt(OutputStream * stream, int32_t v) {
  uint32_t be = bswap((uint32_t)v);
  stream->write(&be, 4);
}

void WritableUtils::WriteShort(OutputStream * stream, int16_t v) {
  uint16_t be = v;
  be = ((be >> 8) | (be << 8));
  stream->write(&be, 2);
}

void WritableUtils::WriteFloat(OutputStream * stream, float v) {
  uint32_t intv = *(uint32_t*)&v;
  intv = bswap(intv);
  stream->write(&intv, 4);
}

void WritableUtils::WriteText(OutputStream * stream, const string & v) {
  WriteVLong(stream, v.length());
  stream->write(v.c_str(), (uint32_t)v.length());
}

void WritableUtils::WriteBytes(OutputStream * stream, const string & v) {
  WriteInt(stream, (int32_t)v.length());
  stream->write(v.c_str(), (uint32_t)v.length());
}

void WritableUtils::WriteUTF8(OutputStream * stream, const string & v) {
  if (v.length() > 65535) {
    THROW_EXCEPTION_EX(IOException, "string too long (%lu) for WriteUTF8", v.length());
  }
  WriteShort(stream, (int16_t)v.length());
  stream->write(v.c_str(), (uint32_t)v.length());
}

void WritableUtils::toString(string & dest, KeyValueType type, const void * data, uint32_t length) {
  switch (type) {
  case TextType:
    dest.append((const char*)data, length);
    break;
  case BytesType:
    dest.append((const char*)data, length);
    break;
  case ByteType:
    dest.append(1, *(char*)data);
    break;
  case BoolType:
    dest.append(*(uint8_t*)data ? "true" : "false");
    break;
  case IntType:
    dest.append(StringUtil::ToString((int32_t)bswap(*(uint32_t*)data)));
    break;
  case LongType:
    dest.append(StringUtil::ToString((int64_t)bswap64(*(uint64_t*)data)));
    break;
  case FloatType:
    dest.append(StringUtil::ToString(*(float*)data));
    break;
  case DoubleType:
    dest.append(StringUtil::ToString(*(double*)data));
    break;
  case MD5HashType:
    dest.append(StringUtil::ToHexString(data, length));
    break;
  default:
    dest.append((const char*)data, length);
    break;
  }
}

} // namespace NativeTask

