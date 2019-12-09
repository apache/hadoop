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
 **/

#include "bad_datanode_tracker.h"

namespace hdfs {

NodeExclusionRule::~NodeExclusionRule() {}

BadDataNodeTracker::BadDataNodeTracker(const Options& options)
    : timeout_duration_(options.host_exclusion_duration),
      test_clock_shift_(0) {}

BadDataNodeTracker::~BadDataNodeTracker() {}

void BadDataNodeTracker::AddBadNode(const std::string& dn) {
  std::lock_guard<std::mutex> update_lock(datanodes_update_lock_);
  datanodes_[dn] = Clock::now();
}

bool BadDataNodeTracker::IsBadNode(const std::string& dn) {
  std::lock_guard<std::mutex> update_lock(datanodes_update_lock_);

  if (datanodes_.count(dn) == 1) {
    const TimePoint& entered_time = datanodes_[dn];
    if (TimeoutExpired(entered_time)) {
      datanodes_.erase(dn);
      return false;
    }
    /* node in set and still marked bad */
    return true;
  }
  return false;
}

void BadDataNodeTracker::TEST_set_clock_shift(int t) { test_clock_shift_ = t; }

bool BadDataNodeTracker::TimeoutExpired(const TimePoint& t) {
  TimePoint threshold = Clock::now() -
                        std::chrono::milliseconds(timeout_duration_) +
                        std::chrono::milliseconds(test_clock_shift_);
  if (t < threshold) {
    return true;
  }
  return false;
}

ExclusionSet::ExclusionSet(const std::set<std::string>& excluded)
    : excluded_(excluded) {}

ExclusionSet::~ExclusionSet() {}

bool ExclusionSet::IsBadNode(const std::string& node_uuid) {
  return excluded_.count(node_uuid) == 1;
}
}
