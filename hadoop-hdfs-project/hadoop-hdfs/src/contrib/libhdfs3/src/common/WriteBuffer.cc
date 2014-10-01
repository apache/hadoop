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

#include "WriteBuffer.h"

#include <google/protobuf/io/coded_stream.h>

using google::protobuf::io::CodedOutputStream;
using google::protobuf::uint8;

#define WRITEBUFFER_INIT_SIZE 64

namespace hdfs {
namespace internal {

WriteBuffer::WriteBuffer() :
    size(0), buffer(WRITEBUFFER_INIT_SIZE) {
}

WriteBuffer::~WriteBuffer() {
}

void WriteBuffer::writeVarint32(int32_t value, size_t pos) {
    char buffer[5];
    uint8 *end = CodedOutputStream::WriteVarint32ToArray(value,
                  reinterpret_cast<uint8*>(buffer));
    write(buffer, reinterpret_cast<char*>(end) - buffer, pos);
}

char *WriteBuffer::alloc(size_t offset, size_t s) {
    assert(offset <= size && size <= buffer.size());

    if (offset > size) {
        return NULL;
    }

    size_t target = offset + s;

    if (target >= buffer.size()) {
        target = target > 2 * buffer.size() ? target : 2 * buffer.size();
        buffer.resize(target);
    }

    size = offset + s;
    return &buffer[offset];
}

void WriteBuffer::write(const void *bytes, size_t s, size_t pos) {
    assert(NULL != bytes);
    assert(pos <= size && pos < buffer.size());
    char *p = alloc(size, s);
    memcpy(p, bytes, s);
}

}
}
