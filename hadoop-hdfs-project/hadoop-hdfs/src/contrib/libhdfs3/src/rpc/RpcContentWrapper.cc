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

#include <google/protobuf/io/coded_stream.h>

#include "RpcContentWrapper.h"

using namespace ::google::protobuf;
using namespace ::google::protobuf::io;

namespace hdfs {
namespace internal {

RpcContentWrapper::RpcContentWrapper(Message * header, Message * msg) :
    header(header), msg(msg) {
}

int RpcContentWrapper::getLength() {
    int headerLen, msgLen = 0;
    headerLen = header->ByteSize();
    msgLen = msg == NULL ? 0 : msg->ByteSize();
    return headerLen + CodedOutputStream::VarintSize32(headerLen)
           + (msg == NULL ?
              0 : msgLen + CodedOutputStream::VarintSize32(msgLen));
}

void RpcContentWrapper::writeTo(WriteBuffer & buffer) {
    int size = header->ByteSize();
    buffer.writeVarint32(size);
    header->SerializeToArray(buffer.alloc(size), size);

    if (msg != NULL) {
        size = msg->ByteSize();
        buffer.writeVarint32(size);
        msg->SerializeToArray(buffer.alloc(size), size);
    }
}

}
}

