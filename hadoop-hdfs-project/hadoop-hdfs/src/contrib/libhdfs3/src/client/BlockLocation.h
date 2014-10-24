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

#ifndef _HDFS_LIBHDFS3_CLIENT_BLOCKLOCATION_H_
#define _HDFS_LIBHDFS3_CLIENT_BLOCKLOCATION_H_

#include <string>
#include <vector>

namespace hdfs {

class BlockLocation {
public:
    bool isCorrupt() const {
        return corrupt;
    }

    void setCorrupt(bool corrupt) {
        this->corrupt = corrupt;
    }

    const std::vector<std::string> &getHosts() const {
        return hosts;
    }

    void setHosts(const std::vector<std::string> &hosts) {
        this->hosts = hosts;
    }

    int64_t getLength() const {
        return length;
    }

    void setLength(int64_t length) {
        this->length = length;
    }

    const std::vector<std::string> &getNames() const {
        return names;
    }

    void setNames(const std::vector<std::string> &names) {
        this->names = names;
    }

    int64_t getOffset() const {
        return offset;
    }

    void setOffset(int64_t offset) {
        this->offset = offset;
    }

    const std::vector<std::string> &getTopologyPaths() const {
        return topologyPaths;
    }

    void setTopologyPaths(const std::vector<std::string> &topologyPaths) {
        this->topologyPaths = topologyPaths;
    }

private:
    bool corrupt;
    int64_t length;
    int64_t offset;                  // Offset of the block in the file
    std::vector<std::string> hosts;  // Datanode hostnames
    std::vector<std::string> names;  // Datanode IP:xferPort for getting block
    std::vector<std::string> topologyPaths;  // Full path name in network topo
};
}

#endif /* _HDFS_LIBHDFS3_CLIENT_BLOCKLOCATION_H_ */
