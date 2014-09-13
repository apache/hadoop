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

#ifndef WRITABLEUTILS_H_
#define WRITABLEUTILS_H_

#include <stdint.h>
#include <string>
#include "lib/Streams.h"
#include "NativeTask.h"

namespace NativeTask {

KeyValueType JavaClassToKeyValueType(const std::string & clazz);

using std::string;

class WritableUtils {
protected:
  static int64_t ReadVLongInner(const char * pos, uint32_t & len);
  static void WriteVLongInner(int64_t value, char * pos, uint32_t & len);
  static uint32_t GetVLongSizeInner(int64_t value);
public:
  inline static uint32_t DecodeVLongSize(int8_t ch) {
    if (ch >= -112) {
      return 1;
    } else if (ch < -120) {
      return -119 - ch;
    }
    return -111 - ch;
  }

  inline static uint32_t DecodeVLongSize(const char * pos) {
    return DecodeVLongSize(*pos);
  }

  inline static uint32_t GetVLongSize(int64_t value) {
    if (value >= -112 && value <= 127) {
      return 1;
    }
    return GetVLongSizeInner(value);
  }

  inline static int64_t ReadVLong(const char * pos, uint32_t & len) {
    if (*pos >= (char)-112) {
      len = 1;
      return *pos;
    } else {
      return ReadVLongInner(pos, len);
    }
  }

  inline static int32_t ReadVInt(const char * pos, uint32_t & len) {
    return (int32_t)ReadVLong(pos, len);
  }

  inline static void WriteVLong(int64_t v, char * target, uint32_t & written) {
    if (v <= 127 && v >= -112) {
      written = 1;
      *target = (char)v;
    } else {
      WriteVLongInner(v, target, written);
    }
  }

  inline static void WriteVInt(int32_t v, char * target, uint32_t & written) {
    WriteVLong(v, target, written);
  }

  // Stream interfaces
  static int64_t ReadVLong(InputStream * stream);

  static int64_t ReadLong(InputStream * stream);

  static int32_t ReadInt(InputStream * stream);

  static int16_t ReadShort(InputStream * stream);

  static float ReadFloat(InputStream * stream);

  static string ReadText(InputStream * stream);

  static string ReadBytes(InputStream * stream);

  static string ReadUTF8(InputStream * stream);

  static void WriteVLong(OutputStream * stream, int64_t v);

  static void WriteLong(OutputStream * stream, int64_t v);

  static void WriteInt(OutputStream * stream, int32_t v);

  static void WriteShort(OutputStream * stream, int16_t v);

  static void WriteFloat(OutputStream * stream, float v);

  static void WriteText(OutputStream * stream, const string & v);

  static void WriteBytes(OutputStream * stream, const string & v);

  static void WriteUTF8(OutputStream * stream, const string & v);

  // Writable binary to string interface
  static void toString(string & dest, KeyValueType type, const void * data, uint32_t length);
};

} // namespace NativeTask

#endif /* WRITABLEUTILS_H_ */
