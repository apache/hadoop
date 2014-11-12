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

#include "SessionConfig.h"

#include "Config.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Function.h"

#include <sstream>

#define ARRAYSIZE(A) (sizeof(A) / sizeof(A[0]))

using hdfs::Config;

namespace hdfs {
namespace internal {

template <typename T>
static void CheckRangeGE(const char *key, T const &value, T const &target) {
    if (!(value >= target)) {
        std::stringstream ss;
        ss << "Invalid configure item: \"" << key << "\", value: " << value
           << ", expected value should be larger than " << target;
        THROW(HdfsConfigInvalid, "%s", ss.str().c_str());
    }
}

template <typename T>
static void CheckMultipleOf(const char *key, const T &value, int unit) {
    if (value <= 0 || value % unit != 0) {
        THROW(HdfsConfigInvalid,
              "%s should be larger than 0 and be the multiple of %d.", key,
              unit);
    }
}

SessionConfig::SessionConfig(const Config &conf) {
    ConfigDefault<bool> boolValues[] = {
        {&rpcTcpNoDelay, "rpc.client.connect.tcpnodelay", true},
        {&readFromLocal, "dfs.client.read.shortcircuit", true},
        {&addDatanode, "output.replace-datanode-on-failure", true},
        {&notRetryAnotherNode, "input.notretry-another-node", false},
        {&useMappedFile, "input.localread.mappedfile", true}};
    ConfigDefault<int32_t> i32Values[] = {
        {&rpcMaxIdleTime, "rpc.client.max.idle", 10 * 1000,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&rpcPingTimeout, "rpc.client.ping.interval", 10 * 1000},
        {&rpcConnectTimeout, "rpc.client.connect.timeout", 600 * 1000},
        {&rpcReadTimeout, "rpc.client.read.timeout", 3600 * 1000},
        {&rpcWriteTimeout, "rpc.client.write.timeout", 3600 * 1000},
        {&rpcSocketLingerTimeout, "rpc.client.socekt.linger.timeout", -1},
        {&rpcMaxRetryOnConnect, "rpc.client.connect.retry", 10,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&rpcTimeout, "rpc.client.timeout", 3600 * 1000},
        {&defaultReplica, "dfs.default.replica", 3,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&inputConnTimeout, "input.connect.timeout", 600 * 1000},
        {&inputReadTimeout, "input.read.timeout", 3600 * 1000},
        {&inputWriteTimeout, "input.write.timeout", 3600 * 1000},
        {&localReadBufferSize, "input.localread.default.buffersize",
         1 * 1024 * 1024, bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&prefetchSize, "dfs.prefetchsize", 10,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&maxGetBlockInfoRetry, "input.read.getblockinfo.retry", 3,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&maxLocalBlockInfoCacheSize, "input.localread.blockinfo.cachesize",
         1000, bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&maxReadBlockRetry, "input.read.max.retry", 60,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&chunkSize, "output.default.chunksize", 512,
         bind(CheckMultipleOf<int32_t>, _1, _2, 512)},
        {&packetSize, "output.default.packetsize", 64 * 1024},
        {&blockWriteRetry, "output.default.write.retry", 10,
         bind(CheckRangeGE<int32_t>, _1, _2, 1)},
        {&outputConnTimeout, "output.connect.timeout", 600 * 1000},
        {&outputReadTimeout, "output.read.timeout", 3600 * 1000},
        {&outputWriteTimeout, "output.write.timeout", 3600 * 1000},
        {&closeFileTimeout, "output.close.timeout", 3600 * 1000},
        {&packetPoolSize, "output.packetpool.size", 1024},
        {&heartBeatInterval, "output.heeartbeat.interval", 10 * 1000},
        {&rpcMaxHARetry, "dfs.client.failover.max.attempts", 15,
         bind(CheckRangeGE<int32_t>, _1, _2, 0)}};
    ConfigDefault<int64_t> i64Values[] = {
        {&defaultBlockSize, "dfs.default.blocksize", 64 * 1024 * 1024,
         bind(CheckMultipleOf<int64_t>, _1, _2, 512)}};
    ConfigDefault<std::string> strValues[] = {
        {&defaultUri, "dfs.default.uri", "hdfs://localhost:9000"},
        {&rpcAuthMethod, "hadoop.security.authentication", "simple"},
        {&kerberosCachePath, "hadoop.security.kerberos.ticket.cache.path", ""},
        {&logSeverity, "dfs.client.log.severity", "INFO"}};

    for (size_t i = 0; i < ARRAYSIZE(boolValues); ++i) {
        Status status = conf.getBool(boolValues[i].key,
                              boolValues[i].value, boolValues[i].variable);
        if (status.isError()) {
            THROW(HdfsConfigInvalid, "%s", status.getErrorMsg());
        }

        if (boolValues[i].check) {
            boolValues[i].check(boolValues[i].key, *boolValues[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(i32Values); ++i) {
        Status status = conf.getInt32(i32Values[i].key,
                              i32Values[i].value, i32Values[i].variable);
        if (status.isError()) {
            THROW(HdfsConfigInvalid, "%s", status.getErrorMsg());
        }

        if (i32Values[i].check) {
            i32Values[i].check(i32Values[i].key, *i32Values[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(i64Values); ++i) {
        Status status = conf.getInt64(i64Values[i].key,
                              i64Values[i].value, i64Values[i].variable);
        if (status.isError()) {
            THROW(HdfsConfigInvalid, "%s", status.getErrorMsg());
        }

        if (i64Values[i].check) {
            i64Values[i].check(i64Values[i].key, *i64Values[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(strValues); ++i) {
        Status status = conf.getString(strValues[i].key,
                              strValues[i].value.c_str(),
                              strValues[i].variable);
        if (status.isError()) {
            THROW(HdfsConfigInvalid, "%s", status.getErrorMsg());
        }

        if (strValues[i].check) {
            strValues[i].check(strValues[i].key, *strValues[i].variable);
        }
    }
}
}
}
