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

#include "Config.h"
#include "NamenodeInfo.h"
#include "StatusInternal.h"
#include "StringUtil.h"

#include <string>
#include <vector>

using namespace hdfs::internal;
using std::string;

namespace hdfs {

NamenodeInfo::NamenodeInfo() {
}

const char *const DFS_NAMESERVICES = "dfs.nameservices";
const char *const DFS_NAMENODE_HA = "dfs.ha.namenodes";
const char *const DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";
const char *const DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";

Status NamenodeInfo::GetHANamenodeInfo(const std::string &service,
                                       const Config &conf,
                                       std::vector<NamenodeInfo> *output) {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        std::string strNameNodes;
        std::vector<NamenodeInfo> &retval = *output;
        RETURN_NOT_OK(conf.getString(
                std::string(DFS_NAMENODE_HA) + "." + service, &strNameNodes));
        std::vector<std::string> nns = StringSplit(strNameNodes, ",");
        retval.resize(nns.size());

        for (size_t i = 0; i < nns.size(); ++i) {
            std::string dfsRpcAddressKey =
                StringTrim(std::string(DFS_NAMENODE_RPC_ADDRESS_KEY) + "." +
                           service + "." + StringTrim(nns[i]));
            string rpcAddress;
            RETURN_NOT_OK(conf.getString(dfsRpcAddressKey, "", &rpcAddress));
            retval[i].setRpcAddr(StringTrim(rpcAddress));

            std::string dfsHttpAddressKey =
                StringTrim(std::string(DFS_NAMENODE_HTTP_ADDRESS_KEY) + "." +
                           service + "." + StringTrim(nns[i]));
            string httpAddress;
            RETURN_NOT_OK(conf.getString(dfsHttpAddressKey, "", &httpAddress));
            retval[i].setHttpAddr(StringTrim(httpAddress));
        }

    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}
}
