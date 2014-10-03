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

#include "BufferedSocketReader.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"

#include <google/protobuf/io/coded_stream.h>

using namespace google::protobuf::io;

namespace hdfs {
namespace internal {

BufferedSocketReaderImpl::BufferedSocketReaderImpl(Socket & s) :
    cursor(0), size(0), sock(s), buffer(sizeof(int64_t)) {
}

int32_t BufferedSocketReaderImpl::read(char * b, int32_t s) {
    assert(s > 0 && NULL != b);
    int32_t done = s < size - cursor ? s : size - cursor;

    if (done > 0) {
        memcpy(b, &buffer[cursor], done);
        cursor += done;
        return done;
    } else {
        assert(size == cursor);
        size = cursor = 0;
        return sock.read(b, s);
    }
}

void BufferedSocketReaderImpl::readFully(char * b, int32_t s, int timeout) {
    assert(s > 0 && NULL != b);
    int32_t done = s < size - cursor ? s : size - cursor;
    memcpy(b, &buffer[cursor], done);
    cursor += done;

    if (done < s) {
        assert(size == cursor);
        size = cursor = 0;
        sock.readFully(b + done, s - done, timeout);
    }
}

int32_t BufferedSocketReaderImpl::readBigEndianInt32(int timeout) {
    char buf[sizeof(int32_t)];
    readFully(buf, sizeof(buf), timeout);
    return ntohl(*reinterpret_cast<int32_t *>(buf));
}

int32_t BufferedSocketReaderImpl::readVarint32(int timeout) {
    int32_t value;
    bool rc = false;
    int deadline = timeout;
    memmove(&buffer[0], &buffer[cursor], size - cursor);
    size -= cursor;
    cursor = 0;

    while (!rc) {
        CodedInputStream in(reinterpret_cast<uint8_t *>(&buffer[cursor]),
                            size - cursor);
        in.PushLimit(size - cursor);
        rc = in.ReadVarint32(reinterpret_cast<uint32_t *>(&value));

        if (rc) {
            cursor += size - cursor - in.BytesUntilLimit();
            return value;
        }

        steady_clock::time_point s = steady_clock::now();
        CheckOperationCanceled();

        if (size == static_cast<int32_t>(buffer.size())) {
            THROW(HdfsNetworkException,
                  "Invalid varint type or buffer is too small, buffer size = %d.",
                  static_cast<int>(buffer.size()));
        }

        if (sock.poll(true, false, deadline)) {
            size += sock.read(&buffer[size], buffer.size() - size);
        }

        steady_clock::time_point e = steady_clock::now();

        if (timeout > 0) {
            deadline -= ToMilliSeconds(s, e);
        }

        if (timeout >= 0 && deadline <= 0) {
            THROW(HdfsTimeoutException, "Read %d bytes timeout", size);
        }
    }

    return 0;
}

bool BufferedSocketReaderImpl::poll(int timeout) {
    if (cursor < size) {
        return true;
    }

    return sock.poll(true, false, timeout);
}

}
}
