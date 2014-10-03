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

#ifndef _HDFS_LIBHDFS3_SERVER_DATANODEINFO_H_
#define _HDFS_LIBHDFS3_SERVER_DATANODEINFO_H_

#include <stdint.h>
#include <string>
#include <sstream>

namespace hdfs {
namespace internal {

/**
 * This class extends the primary identifier of a Datanode with ephemeral
 * state, eg usage information, current administrative state, and the
 * network location that is communicated to clients.
 */
class DatanodeInfo {
public:
    const std::string &getHostName() const {
        return hostName;
    }

    void setHostName(const std::string &hostName) {
        this->hostName = hostName;
    }

    uint32_t getInfoPort() const {
        return infoPort;
    }

    void setInfoPort(uint32_t infoPort) {
        this->infoPort = infoPort;
    }

    const std::string &getIpAddr() const {
        return ipAddr;
    }

    void setIpAddr(const std::string &ipAddr) {
        this->ipAddr = ipAddr;
    }

    uint32_t getIpcPort() const {
        return ipcPort;
    }

    void setIpcPort(uint32_t ipcPort) {
        this->ipcPort = ipcPort;
    }

    const std::string &getDatanodeId() const {
        return datanodeId;
    }

    void setDatanodeId(const std::string &storageId) {
        this->datanodeId = storageId;
    }

    uint32_t getXferPort() const {
        return xferPort;
    }

    void setXferPort(uint32_t xferPort) {
        this->xferPort = xferPort;
    }

    const std::string formatAddress() const {
        std::stringstream ss;
        ss << hostName << "(" << getIpAddr() << ")";
        return ss.str();
    }

    bool operator <(const DatanodeInfo &other) const {
        return datanodeId < other.datanodeId;
    }

    bool operator ==(const DatanodeInfo &other) const {
        return this->datanodeId == other.datanodeId
               && this->ipAddr == other.ipAddr;
    }

    const std::string &getLocation() const {
        return location;
    }

    void setLocation(const std::string &location) {
        this->location = location;
    }

    std::string getXferAddr() const {
        std::stringstream ss;
        ss << getIpAddr() << ":" << getXferPort();
        return ss.str();
    }

private:
    uint32_t xferPort;
    uint32_t infoPort;
    uint32_t ipcPort;
    std::string ipAddr;
    std::string hostName;
    std::string datanodeId;
    std::string location;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_DATANODEINFO_H_ */
