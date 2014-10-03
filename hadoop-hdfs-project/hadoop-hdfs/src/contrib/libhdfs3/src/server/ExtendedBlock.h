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

#ifndef _HDFS_LIBHDFS3_SERVER_EXTENDEDBLOCK_H_
#define _HDFS_LIBHDFS3_SERVER_EXTENDEDBLOCK_H_

#include "Hash.h"

#include <string>
#include <sstream>

namespace hdfs {
namespace internal {

/**
 * Identifies a Block uniquely across the block pools
 */
class ExtendedBlock {
public:
    ExtendedBlock() :
        blockId(0), generationStamp(0), numBytes(0) {
    }

    int64_t getBlockId() const {
        return blockId;
    }

    void setBlockId(int64_t blockId) {
        this->blockId = blockId;
    }

    int64_t getGenerationStamp() const {
        return generationStamp;
    }

    void setGenerationStamp(int64_t generationStamp) {
        this->generationStamp = generationStamp;
    }

    int64_t getNumBytes() const {
        return numBytes;
    }

    void setNumBytes(int64_t numBytes) {
        this->numBytes = numBytes;
    }

    const std::string &getPoolId() const {
        return poolId;
    }

    void setPoolId(const std::string &poolId) {
        this->poolId = poolId;
    }

    const std::string toString() const {
        std::stringstream ss;
        ss << "[block pool ID: " << poolId << " block ID " << blockId << "_"
           << generationStamp << "]";
        return ss.str();
    }

    size_t hash_value() const {
        size_t values[] = { Int64Hasher(blockId), StringHasher(poolId) };
        return CombineHasher(values, sizeof(values) / sizeof(values[0]));
    }

private:
    int64_t blockId;
    int64_t generationStamp;
    int64_t numBytes;
    std::string poolId;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::ExtendedBlock);

#endif /* _HDFS_LIBHDFS3_SERVER_EXTENDEDBLOCK_H_ */
