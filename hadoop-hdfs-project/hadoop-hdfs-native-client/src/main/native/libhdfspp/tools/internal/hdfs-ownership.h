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

#ifndef LIBHDFSPP_TOOLS_HDFS_OWNERSHIP
#define LIBHDFSPP_TOOLS_HDFS_OWNERSHIP

#include <functional>
#include <mutex>
#include <optional>
#include <utility>

#include "hdfspp/status.h"

namespace hdfs::tools {
/**
 * {@class Ownership} contains the user and group ownership information.
 */
struct Ownership {
  explicit Ownership(const std::string &user_and_group);

  [[nodiscard]] const std::string &GetUser() const { return user_; }

  [[nodiscard]] const std::optional<std::string> &GetGroup() const {
    return group_;
  }

  bool operator==(const Ownership &other) const;

private:
  std::string user_;
  std::optional<std::string> group_;
};

/**
 * {@class OwnerState} holds information needed for recursive traversal of some
 * of the HDFS APIs.
 */
struct OwnerState {
  OwnerState(std::string username, std::string group,
             std::function<void(const hdfs::Status &)> handler,
             const uint64_t request_counter, const bool find_is_done)
      : user{std::move(username)}, group{std::move(group)}, handler{std::move(
                                                                handler)},
        request_counter{request_counter}, find_is_done{find_is_done} {}

  const std::string user;
  const std::string group;
  const std::function<void(const hdfs::Status &)> handler;

  /**
   * The request counter is incremented once every time SetOwner async call is
   * made.
   */
  uint64_t request_counter;

  /**
   * This boolean will be set when find returns the last result.
   */
  bool find_is_done{false};

  /**
   * Final status to be returned.
   */
  hdfs::Status status{};

  /**
   * Shared variables will need protection with a lock.
   */
  std::mutex lock;
};
} // namespace hdfs::tools

#endif