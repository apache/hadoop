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

#ifndef _HDFS_LIBHDFS3_COMMON_SESSIONCONFIG_H_
#define _HDFS_LIBHDFS3_COMMON_SESSIONCONFIG_H_

#include "Exception.h"
#include "ExceptionInternal.h"
#include "Function.h"
#include "Logger.h"
#include "XmlConfig.h"

#include <cassert>
#include <stdint.h>
#include <vector>

namespace hdfs {
namespace internal {

template<typename T>
struct ConfigDefault {
    T *variable; //variable this configure item should be bound to.
    const char *key; //configure key.
    T value; //default value.
    function<void(const char *, T const &)> check;   //the function to validate the value.
};

class SessionConfig {
public:

    SessionConfig(const Config &conf);

    /*
     * rpc configure
     */

    int32_t getRpcConnectTimeout() const {
        return rpcConnectTimeout;
    }

    int32_t getRpcMaxIdleTime() const {
        return rpcMaxIdleTime;
    }

    int32_t getRpcMaxRetryOnConnect() const {
        return rpcMaxRetryOnConnect;
    }

    int32_t getRpcPingTimeout() const {
        return rpcPingTimeout;
    }

    int32_t getRpcReadTimeout() const {
        return rpcReadTimeout;
    }

    bool isRpcTcpNoDelay() const {
        return rpcTcpNoDelay;
    }

    int32_t getRpcWriteTimeout() const {
        return rpcWriteTimeout;
    }

    /*
     * FileSystem configure
     */
    const std::string &getDefaultUri() const {
        return defaultUri;
    }

    int32_t getDefaultReplica() const {
        return defaultReplica;
    }

    int64_t getDefaultBlockSize() const {
        return defaultBlockSize;
    }

    /*
     * InputStream configure
     */
    int32_t getLocalReadBufferSize() const {
        return localReadBufferSize;
    }

    int32_t getInputReadTimeout() const {
        return inputReadTimeout;
    }

    int32_t getInputWriteTimeout() const {
        return inputWriteTimeout;
    }

    int32_t getInputConnTimeout() const {
        return inputConnTimeout;
    }

    int32_t getPrefetchSize() const {
        return prefetchSize;
    }

    bool isReadFromLocal() const {
        return readFromLocal;
    }

    int32_t getMaxGetBlockInfoRetry() const {
        return maxGetBlockInfoRetry;
    }

    int32_t getMaxLocalBlockInfoCacheSize() const {
        return maxLocalBlockInfoCacheSize;
    }

    /*
     * OutputStream configure
     */
    int32_t getDefaultChunkSize() const {
        return chunkSize;
    }

    int32_t getDefaultPacketSize() const {
        if (packetSize % chunkSize != 0) {
            THROW(HdfsConfigInvalid,
                  "output.default.packetsize should be larger than 0 "
                  "and be the multiple of output.default.chunksize.");
        }

        return packetSize;
    }

    int32_t getBlockWriteRetry() const {
        return blockWriteRetry;
    }

    int32_t getOutputConnTimeout() const {
        return outputConnTimeout;
    }

    int32_t getOutputReadTimeout() const {
        return outputReadTimeout;
    }

    int32_t getOutputWriteTimeout() const {
        return outputWriteTimeout;
    }

    bool canAddDatanode() const {
        return addDatanode;
    }

    int32_t getHeartBeatInterval() const {
        return heartBeatInterval;
    }

    int32_t getRpcMaxHaRetry() const {
        return rpcMaxHARetry;
    }

    void setRpcMaxHaRetry(int32_t rpcMaxHaRetry) {
        rpcMaxHARetry = rpcMaxHaRetry;
    }

    const std::string &getRpcAuthMethod() const {
        return rpcAuthMethod;
    }

    void setRpcAuthMethod(const std::string &rpcAuthMethod) {
        this->rpcAuthMethod = rpcAuthMethod;
    }

    const std::string &getKerberosCachePath() const {
        return kerberosCachePath;
    }

    void setKerberosCachePath(const std::string &kerberosCachePath) {
        this->kerberosCachePath = kerberosCachePath;
    }

    int32_t getRpcSocketLingerTimeout() const {
        return rpcSocketLingerTimeout;
    }

    void setRpcSocketLingerTimeout(int32_t rpcSocketLingerTimeout) {
        this->rpcSocketLingerTimeout = rpcSocketLingerTimeout;
    }

    LogSeverity getLogSeverity() const {
        for (size_t i = FATAL; i < NUM_SEVERITIES; ++i) {
            if (logSeverity == SeverityName[i]) {
                return static_cast<LogSeverity>(i);
            }
        }

        return DEFAULT_LOG_LEVEL;
    }

    void setLogSeverity(const std::string &logSeverityLevel) {
        this->logSeverity = logSeverityLevel;
    }

    int32_t getPacketPoolSize() const {
        return packetPoolSize;
    }

    void setPacketPoolSize(int32_t packetPoolSize) {
        this->packetPoolSize = packetPoolSize;
    }

    int32_t getCloseFileTimeout() const {
        return closeFileTimeout;
    }

    void setCloseFileTimeout(int32_t closeFileTimeout) {
        this->closeFileTimeout = closeFileTimeout;
    }

    int32_t getRpcTimeout() const {
        return rpcTimeout;
    }

    void setRpcTimeout(int32_t rpcTimeout) {
        this->rpcTimeout = rpcTimeout;
    }

    bool doesNotRetryAnotherNode() const {
        return notRetryAnotherNode;
    }

    void setIFNotRetryAnotherNode(bool notRetryAnotherNode) {
        this->notRetryAnotherNode = notRetryAnotherNode;
    }

    int32_t getMaxReadBlockRetry() const {
        return maxReadBlockRetry;
    }

    void setMaxReadBlockRetry(int32_t maxReadBlockRetry) {
        this->maxReadBlockRetry = maxReadBlockRetry;
    }

    bool doUseMappedFile() const {
        return useMappedFile;
    }

    void setUseMappedFile(bool useMappedFile) {
        this->useMappedFile = useMappedFile;
    }

public:
    /*
     * rpc configure
     */
    int32_t rpcMaxIdleTime;
    int32_t rpcPingTimeout;
    int32_t rpcConnectTimeout;
    int32_t rpcReadTimeout;
    int32_t rpcWriteTimeout;
    int32_t rpcMaxRetryOnConnect;
    int32_t rpcMaxHARetry;
    int32_t rpcSocketLingerTimeout;
    int32_t rpcTimeout;
    bool rpcTcpNoDelay;
    std::string rpcAuthMethod;

    /*
     * FileSystem configure
     */
    std::string defaultUri;
    std::string kerberosCachePath;
    std::string logSeverity;
    int32_t defaultReplica;
    int64_t defaultBlockSize;

    /*
     * InputStream configure
     */
    bool useMappedFile;
    bool readFromLocal;
    bool notRetryAnotherNode;
    int32_t inputConnTimeout;
    int32_t inputReadTimeout;
    int32_t inputWriteTimeout;
    int32_t localReadBufferSize;
    int32_t maxGetBlockInfoRetry;
    int32_t maxLocalBlockInfoCacheSize;
    int32_t maxReadBlockRetry;
    int32_t prefetchSize;

    /*
     * OutputStream configure
     */
    bool addDatanode;
    int32_t chunkSize;
    int32_t packetSize;
    int32_t blockWriteRetry; //retry on block not replicated yet.
    int32_t outputConnTimeout;
    int32_t outputReadTimeout;
    int32_t outputWriteTimeout;
    int32_t packetPoolSize;
    int32_t heartBeatInterval;
    int32_t closeFileTimeout;

};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_SESSIONCONFIG_H_ */
