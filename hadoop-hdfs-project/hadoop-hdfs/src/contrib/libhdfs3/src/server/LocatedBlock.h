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

#ifndef _HDFS_LIBHDFS3_SERVER_LOCATEDBLOCK_H_
#define _HDFS_LIBHDFS3_SERVER_LOCATEDBLOCK_H_

#include "client/Token.h"
#include "DatanodeInfo.h"
#include "ExtendedBlock.h"

#include <vector>
#include <stdint.h>

namespace hdfs {
namespace internal {

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, security token, etc).
 */
class LocatedBlock: public ExtendedBlock {
public:
    LocatedBlock() :
        offset(0), corrupt(false) {
    }

    LocatedBlock(int64_t position) :
        offset(position), corrupt(false) {
    }

    bool isCorrupt() const {
        return corrupt;
    }

    void setCorrupt(bool corrupt) {
        this->corrupt = corrupt;
    }

    const std::vector<DatanodeInfo> &getLocations() const {
        return locs;
    }

    std::vector<DatanodeInfo> &mutableLocations() {
        return locs;
    }

    void setLocations(const std::vector<DatanodeInfo> &locs) {
        this->locs = locs;
    }

    int64_t getOffset() const {
        return offset;
    }

    void setOffset(int64_t offset) {
        this->offset = offset;
    }

    const Token &getToken() const {
        return token;
    }

    void setToken(const Token &token) {
        this->token = token;
    }

    bool operator <(const LocatedBlock &that) const {
        return this->offset < that.offset;
    }

    const std::vector<std::string> &getStorageIDs() const {
        return storageIDs;
    }

    std::vector<std::string> &mutableStorageIDs() {
        return storageIDs;
    }
private:
    int64_t offset;
    bool corrupt;
    std::vector<DatanodeInfo> locs;
    std::vector<std::string> storageIDs;
    Token token;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_LOCATEDBLOCK_H_ */
