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

#ifndef _HDFS_LIBHDFS_SERVER_NAMENODEINFO_H_
#define _HDFS_LIBHDFS_SERVER_NAMENODEINFO_H_

#include "XmlConfig.h"

#include <string>
#include <vector>

namespace hdfs {

class NamenodeInfo {
public:
    NamenodeInfo();

    const std::string &getHttpAddr() const {
        return http_addr;
    }

    void setHttpAddr(const std::string &httpAddr) {
        http_addr = httpAddr;
    }

    const std::string &getRpcAddr() const {
        return rpc_addr;
    }

    void setRpcAddr(const std::string &rpcAddr) {
        rpc_addr = rpcAddr;
    }

    static std::vector<NamenodeInfo> GetHANamenodeInfo(
          const std::string &service, const Config &conf);

private:
    std::string rpc_addr;
    std::string http_addr;
};

}

#endif /* _HDFS_LIBHDFS_SERVER_NAMENODEINFO_H_ */
