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

#ifndef _HDFS_LIBHDFS3_RPC_RPCCALL_H_
#define _HDFS_LIBHDFS3_RPC_RPCCALL_H_

#include "google/protobuf/message.h"

#include <string>

namespace hdfs {
namespace internal {

class RpcCall {
public:
    RpcCall(bool idemp, std::string n, google::protobuf::Message *req,
            google::protobuf::Message *resp) :
        idempotent(idemp), name(n), request(req), response(resp) {
    }

    bool isIdempotent() const {
        return idempotent;
    }

    const char *getName() const {
        return name.c_str();
    }

    void setIdempotent(bool idempotent) {
        this->idempotent = idempotent;
    }

    void setName(const std::string &name) {
        this->name = name;
    }

    google::protobuf::Message *getRequest() {
        return request;
    }

    void setRequest(google::protobuf::Message *request) {
        this->request = request;
    }

    google::protobuf::Message *getResponse() {
        return response;
    }

    void setResponse(google::protobuf::Message *response) {
        this->response = response;
    }

private:
    bool idempotent;
    std::string name;
    google::protobuf::Message *request;
    google::protobuf::Message *response;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_RPCCALL_H_ */
