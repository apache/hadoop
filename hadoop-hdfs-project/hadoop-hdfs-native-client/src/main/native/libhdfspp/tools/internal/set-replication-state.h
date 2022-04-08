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

#ifndef LIBHDFSPP_TOOLS_HDFS_SET_REPLICATION_STATE
#define LIBHDFSPP_TOOLS_HDFS_SET_REPLICATION_STATE

#include <functional>
#include <mutex>

#include "hdfspp/hdfspp.h"

namespace hdfs::tools {
/**
 * {@class SetReplicationState} helps in handling the intermediate results while
 * running {@link Setrep}.
 */
struct SetReplicationState {
  SetReplicationState(const uint16_t replication,
                      std::function<void(const hdfs::Status &)> handler,
                      const uint64_t request_counter, const bool find_is_done)
      : replication{replication}, handler{std::move(handler)},
        request_counter{request_counter}, find_is_done{find_is_done} {}

  /**
   * The replication factor.
   */
  const uint16_t replication;

  /**
   * Handle the given {@link hdfs::Status}.
   */
  const std::function<void(const hdfs::Status &)> handler;

  /**
   * The request counter is incremented once every time SetReplication async
   * call is made.
   */
  uint64_t request_counter;

  /**
   * This boolean will be set when find returns the last result.
   */
  bool find_is_done;

  /**
   * Final status to be returned.
   */
  hdfs::Status status;

  /**
   * Shared variables will need protection with a lock.
   */
  std::mutex lock;
};
} // namespace hdfs::tools

#endif
