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

#include "filesystem.h"

namespace hdfs {

using ::hadoop::hdfs::LocatedBlocksProto;

InputStream::~InputStream() {}

InputStreamImpl::InputStreamImpl(FileSystemImpl *fs,
                                 const LocatedBlocksProto *blocks,
                                 std::shared_ptr<BadDataNodeTracker> tracker)
    : fs_(fs), file_length_(blocks->filelength()), bad_node_tracker_(tracker) {
  for (const auto &block : blocks->blocks()) {
    blocks_.push_back(block);
  }

  if (blocks->has_lastblock() && blocks->lastblock().b().numbytes()) {
    blocks_.push_back(blocks->lastblock());
  }
}

void InputStreamImpl::PositionRead(
    void *buf, size_t nbyte, uint64_t offset,
    const std::function<void(const Status &, const std::string &, size_t)> &
        handler) {
  AsyncPreadSome(offset, asio::buffer(buf, nbyte), bad_node_tracker_, handler);
}
}
