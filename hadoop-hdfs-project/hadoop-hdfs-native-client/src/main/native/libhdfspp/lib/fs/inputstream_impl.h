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
#ifndef FS_INPUTSTREAM_IMPL_H_
#define FS_INPUTSTREAM_IMPL_H_

#include "reader/block_reader.h"

#include "common/continuation/asio.h"
#include "common/continuation/protobuf.h"
#include "filesystem.h"

#include <functional>
#include <future>
#include <type_traits>

namespace hdfs {

struct InputStreamImpl::RemoteBlockReaderTrait {
  typedef RemoteBlockReader Reader;
  struct State {
    std::shared_ptr<DataNodeConnection> dn_;
    std::shared_ptr<RemoteBlockReader> reader_;
    size_t transferred_;
    RemoteBlockReader *reader() { return reader_.get(); }
    size_t *transferred() { return &transferred_; }
    const size_t *transferred() const { return &transferred_; }
  };
  static continuation::Pipeline<State> *
  CreatePipeline(std::shared_ptr<DataNodeConnection> dn) {
    auto m = continuation::Pipeline<State>::Create();
    auto &s = m->state();
    s.reader_ = std::make_shared<RemoteBlockReader>(BlockReaderOptions(), dn);
    return m;
  }
};


}

#endif
