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

#ifndef _HDFS_LIBHDFS3_CLIENT_PIPELINEACK_H_
#define _HDFS_LIBHDFS3_CLIENT_PIPELINEACK_H_

#include "datatransfer.pb.h"

namespace hdfs {
namespace internal {

class PipelineAck {
public:
    PipelineAck() : invalid(true) {
    }

    PipelineAck(const char *buf, int size) : invalid(false) {
        readFrom(buf, size);
    }

    bool isInvalid() {
        return invalid;
    }

    int getNumOfReplies() {
        return proto.status_size();
    }

    int64_t getSeqno() {
        return proto.seqno();
    }

    ::hadoop::hdfs::Status getReply(int i) {
        return proto.status(i);
    }

    bool isSuccess() {
        int size = proto.status_size();

        for (int i = 0; i < size; ++i) {
            if (::hadoop::hdfs::Status::SUCCESS != proto.status(i)) {
                return false;
            }
        }

        return true;
    }

    void readFrom(const char *buf, int size) {
        invalid = !proto.ParseFromArray(buf, size);
    }

    void reset() {
        proto.Clear();
        invalid = true;
    }

private:
    ::hadoop::hdfs::PipelineAckProto proto;
    bool invalid;
};
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_PIPELINEACK_H_ */
