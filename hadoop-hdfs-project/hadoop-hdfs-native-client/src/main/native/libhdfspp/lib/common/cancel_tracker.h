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

#ifndef COMMON_CANCELTRACKER_H
#define COMMON_CANCELTRACKER_H

#include <memory>
#include <atomic>

namespace hdfs {

class CancelTracker : public std::enable_shared_from_this<CancelTracker> {
 public:
  CancelTracker();
  static std::shared_ptr<CancelTracker> New();
  void set_canceled();
  bool is_canceled();
 private:
  std::atomic_bool canceled_;
};

typedef std::shared_ptr<CancelTracker> CancelHandle;

}
#endif
