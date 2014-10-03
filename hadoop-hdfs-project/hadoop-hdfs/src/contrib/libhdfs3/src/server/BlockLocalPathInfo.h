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

#ifndef _HDFS_LIBHDFS3_SERVER_BLOCKLOCALPATHINFO_H_
#define _HDFS_LIBHDFS3_SERVER_BLOCKLOCALPATHINFO_H_

#include "ExtendedBlock.h"

#include <string>

namespace hdfs {
namespace internal {

class BlockLocalPathInfo {
public:
    const ExtendedBlock &getBlock() const {
        return block;
    }

    void setBlock(const ExtendedBlock &block) {
        this->block = block;
    }

    const char *getLocalBlockPath() const {
        return localBlockPath.c_str();
    }

    void setLocalBlockPath(const char *localBlockPath) {
        this->localBlockPath = localBlockPath;
    }

    const char *getLocalMetaPath() const {
        return localMetaPath.c_str();
    }

    void setLocalMetaPath(const char *localMetaPath) {
        this->localMetaPath = localMetaPath;
    }

private:
    ExtendedBlock block;
    std::string localBlockPath;
    std::string localMetaPath;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_BLOCKLOCALPATHINFO_H_ */
