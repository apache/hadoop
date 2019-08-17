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

#ifndef LIBHDFSPP_BADDATANODETRACKER_H
#define LIBHDFSPP_BADDATANODETRACKER_H

#include <mutex>
#include <chrono>
#include <map>
#include <string>
#include <set>

#include "hdfspp/options.h"
#include "hdfspp/hdfspp.h"

namespace hdfs {

/**
 * ExclusionSet is a simple override that can be filled with known
 * bad node UUIDs and passed to AsyncPreadSome.
 **/
class ExclusionSet : public NodeExclusionRule {
 public:
  ExclusionSet(const std::set<std::string>& excluded);
  virtual ~ExclusionSet();
  virtual bool IsBadNode(const std::string& node_uuid);

 private:
  std::set<std::string> excluded_;
};

/**
 * BadDataNodeTracker keeps a timestamped list of datanodes that have
 * failed during past operations.  Entries present in this list will
 * not be used for new requests.  Entries will be evicted from the list
 * after a period of time has elapsed; the default is 10 minutes.
 */
class BadDataNodeTracker : public NodeExclusionRule {
 public:
  BadDataNodeTracker(const Options& options = Options());
  virtual ~BadDataNodeTracker();
  /* add a bad DN to the list */
  void AddBadNode(const std::string& dn);
  /* check if a node should be excluded */
  virtual bool IsBadNode(const std::string& dn);
  /* only for tests, shift clock by t milliseconds*/
  void TEST_set_clock_shift(int t);

 private:
  typedef std::chrono::steady_clock Clock;
  typedef std::chrono::time_point<Clock> TimePoint;
  bool TimeoutExpired(const TimePoint& t);
  /* after timeout_duration_ elapses remove DN */
  const unsigned int timeout_duration_; /* milliseconds */
  std::map<std::string, TimePoint> datanodes_;
  std::mutex datanodes_update_lock_;
  int test_clock_shift_;
};
}
#endif
