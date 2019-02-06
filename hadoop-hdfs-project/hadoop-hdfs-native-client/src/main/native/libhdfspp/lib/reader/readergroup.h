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
#ifndef READER_READER_GROUP_H_
#define READER_READER_GROUP_H_

#include "block_reader.h"

#include <memory>
#include <vector>
#include <mutex>

namespace hdfs {

/**
 * Provide a way of logically grouping ephemeral block readers
 * so that their status can be monitored or changed.
 *
 * Note: This does not attempt to extend the reader life
 * cycle.  Readers are assumed to be owned by something else
 * using a shared_ptr.
 **/

class ReaderGroup {
 public:
  ReaderGroup() {};
  void AddReader(std::shared_ptr<BlockReader> reader);
  /* find live readers, promote to shared_ptr */
  std::vector<std::shared_ptr<BlockReader>> GetLiveReaders();
 private:
  /* remove weak_ptrs that don't point to live object */
  void ClearDeadReaders();
  std::recursive_mutex state_lock_;
  std::vector<std::weak_ptr<BlockReader>> readers_;
};

} // end namespace hdfs
#endif
