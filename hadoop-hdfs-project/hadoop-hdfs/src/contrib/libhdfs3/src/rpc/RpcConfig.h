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

#ifndef _HDFS_LIBHDFS3_RPC_RPCCONFIG_H_
#define _HDFS_LIBHDFS3_RPC_RPCCONFIG_H_

#include "Hash.h"
#include "SessionConfig.h"

namespace hdfs {
namespace internal {

class RpcConfig {
public:

    RpcConfig(const SessionConfig &conf) {
        connectTimeout = conf.getRpcConnectTimeout();
        maxIdleTime = conf.getRpcMaxIdleTime();
        maxRetryOnConnect = conf.getRpcMaxRetryOnConnect();
        pingTimeout = conf.getRpcPingTimeout();
        readTimeout = conf.getRpcReadTimeout();
        writeTimeout = conf.getRpcWriteTimeout();
        tcpNoDelay = conf.isRpcTcpNoDelay();
        lingerTimeout = conf.getRpcSocketLingerTimeout();
        rpcTimeout = conf.getRpcTimeout();
    }

    size_t hash_value() const;

    int getConnectTimeout() const {
        return connectTimeout;
    }

    void setConnectTimeout(int connectTimeout) {
        this->connectTimeout = connectTimeout;
    }

    int getMaxIdleTime() const {
        return maxIdleTime;
    }

    void setMaxIdleTime(int maxIdleTime) {
        this->maxIdleTime = maxIdleTime;
    }

    int getMaxRetryOnConnect() const {
        return maxRetryOnConnect;
    }

    void setMaxRetryOnConnect(int maxRetryOnConnect) {
        this->maxRetryOnConnect = maxRetryOnConnect;
    }

    int getReadTimeout() const {
        return readTimeout;
    }

    void setReadTimeout(int readTimeout) {
        this->readTimeout = readTimeout;
    }

    bool isTcpNoDelay() const {
        return tcpNoDelay;
    }

    void setTcpNoDelay(bool tcpNoDelay) {
        this->tcpNoDelay = tcpNoDelay;
    }

    int getWriteTimeout() const {
        return writeTimeout;
    }

    void setWriteTimeout(int writeTimeout) {
        this->writeTimeout = writeTimeout;
    }

    int getPingTimeout() const {
        return pingTimeout;
    }

    void setPingTimeout(int maxPingTimeout) {
        this->pingTimeout = maxPingTimeout;
    }

    int getLingerTimeout() const {
        return lingerTimeout;
    }

    void setLingerTimeout(int lingerTimeout) {
        this->lingerTimeout = lingerTimeout;
    }

    int getRpcTimeout() const {
        return rpcTimeout;
    }

    void setRpcTimeout(int rpcTimeout) {
        this->rpcTimeout = rpcTimeout;
    }

    bool operator ==(const RpcConfig &other) const {
        return this->maxIdleTime == other.maxIdleTime
               && this->pingTimeout == other.pingTimeout
               && this->connectTimeout == other.connectTimeout
               && this->readTimeout == other.readTimeout
               && this->writeTimeout == other.writeTimeout
               && this->maxRetryOnConnect == other.maxRetryOnConnect
               && this->tcpNoDelay == other.tcpNoDelay
               && this->lingerTimeout == other.lingerTimeout
               && this->rpcTimeout == other.rpcTimeout;
    }

private:
    int maxIdleTime;
    int pingTimeout;
    int connectTimeout;
    int readTimeout;
    int writeTimeout;
    int maxRetryOnConnect;
    int lingerTimeout;
    int rpcTimeout;
    bool tcpNoDelay;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::RpcConfig);

#endif /* _HDFS_LIBHDFS3_RPC_RPCCONFIG_H_ */
