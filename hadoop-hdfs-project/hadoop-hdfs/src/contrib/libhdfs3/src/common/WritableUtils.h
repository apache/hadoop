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

#ifndef _HDFS_LIBHDFS_3_UTIL_WRITABLEUTILS_H_
#define _HDFS_LIBHDFS_3_UTIL_WRITABLEUTILS_H_

#include <string>

namespace hdfs {
namespace internal {

class WritableUtils {
public:
    WritableUtils(char *b, size_t l);

    int32_t ReadInt32();

    int64_t ReadInt64();

    void ReadRaw(char *buf, size_t size);

    std::string ReadText();

    int readByte();

    size_t WriteInt32(int32_t value);

    size_t WriteInt64(int64_t value);

    size_t WriteRaw(const char *buf, size_t size);

    size_t WriteText(const std::string &str);

private:
    int decodeWritableUtilsSize(int value);

    void writeByte(int val);

    bool isNegativeWritableUtils(int value);

    int32_t ReadBigEndian32();

private:
    char *buffer;
    size_t len;
    size_t current;
};

}
}
#endif /* _HDFS_LIBHDFS_3_UTIL_WritableUtils_H_ */
