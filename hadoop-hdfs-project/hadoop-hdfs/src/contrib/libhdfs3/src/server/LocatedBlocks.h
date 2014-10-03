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

#ifndef _HDFS_LIBHDFS3_SERVER_LOCATEDBLOCKS_H_
#define _HDFS_LIBHDFS3_SERVER_LOCATEDBLOCKS_H_

#include "LocatedBlock.h"
#include "common/SharedPtr.h"

#include <cassert>
#include <vector>

namespace hdfs {
namespace internal {

class LocatedBlocks {
public:
    virtual ~LocatedBlocks() {}

    virtual int64_t getFileLength() const = 0;

    virtual void setFileLength(int64_t fileLength) = 0;

    virtual bool isLastBlockComplete() const = 0;

    virtual void setIsLastBlockComplete(bool lastBlockComplete) = 0;

    virtual shared_ptr<LocatedBlock> getLastBlock() = 0;

    virtual void setLastBlock(shared_ptr<LocatedBlock> lastBlock) = 0;

    virtual bool isUnderConstruction() const = 0;

    virtual void setUnderConstruction(bool underConstruction) = 0;

    virtual const LocatedBlock *findBlock(int64_t position) = 0;

    virtual std::vector<LocatedBlock> &getBlocks() = 0;
};

/**
 * Collection of blocks with their locations and the file length.
 */
class LocatedBlocksImpl : public LocatedBlocks {
public:
    int64_t getFileLength() const {
        return fileLength;
    }

    void setFileLength(int64_t fileLength) {
        this->fileLength = fileLength;
    }

    bool isLastBlockComplete() const {
        return lastBlockComplete;
    }

    void setIsLastBlockComplete(bool lastBlockComplete) {
        this->lastBlockComplete = lastBlockComplete;
    }

    shared_ptr<LocatedBlock> getLastBlock() {
        assert(!lastBlockComplete);
        return lastBlock;
    }

    void setLastBlock(shared_ptr<LocatedBlock> lastBlock) {
        this->lastBlock = lastBlock;
    }

    bool isUnderConstruction() const {
        return underConstruction;
    }

    void setUnderConstruction(bool underConstruction) {
        this->underConstruction = underConstruction;
    }

    const LocatedBlock * findBlock(int64_t position);

    std::vector<LocatedBlock> & getBlocks() {
        return blocks;
    }

private:
    bool lastBlockComplete;
    bool underConstruction;
    int64_t fileLength;
    shared_ptr<LocatedBlock> lastBlock;
    std::vector<LocatedBlock> blocks;

};

}
}
#endif /* _HDFS_LIBHDFS3_SERVER_LOCATEDBLOCKS_H_ */
