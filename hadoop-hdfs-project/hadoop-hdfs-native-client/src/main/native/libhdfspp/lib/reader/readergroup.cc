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
#include "readergroup.h"

#include <algorithm>

namespace hdfs {

void ReaderGroup::AddReader(std::shared_ptr<BlockReader> reader) {
  std::lock_guard<std::recursive_mutex> state_lock(state_lock_);
  ClearDeadReaders();
  std::weak_ptr<BlockReader> weak_ref = reader;
  readers_.push_back(weak_ref);
}

std::vector<std::shared_ptr<BlockReader>> ReaderGroup::GetLiveReaders() {
  std::lock_guard<std::recursive_mutex> state_lock(state_lock_);

  std::vector<std::shared_ptr<BlockReader>> live_readers;
  for(auto it=readers_.begin(); it != readers_.end(); it++) {
    std::shared_ptr<BlockReader> live_reader = it->lock();
    if(live_reader) {
      live_readers.push_back(live_reader);
    }
  }
  return live_readers;
}

void ReaderGroup::ClearDeadReaders() {
  std::lock_guard<std::recursive_mutex> state_lock(state_lock_);

  auto reader_is_dead = [](const std::weak_ptr<BlockReader> &ptr) {
    return ptr.expired();
  };

  auto it = std::remove_if(readers_.begin(), readers_.end(), reader_is_dead);
  readers_.erase(it, readers_.end());
}

} // end namespace hdfs
