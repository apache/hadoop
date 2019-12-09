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

#ifndef LIB_RPC_NAMENODE_TRACKER_H
#define LIB_RPC_NAMENODE_TRACKER_H

#include "common/libhdfs_events_impl.h"
#include "common/namenode_info.h"

#include <asio/ip/tcp.hpp>

#include <memory>
#include <mutex>
#include <vector>

namespace hdfs {

/*
 *  Tracker gives the RpcEngine a quick way to use an endpoint that just
 *  failed in order to lookup a set of endpoints for a failover node.
 *
 *  Note: For now this only deals with 2 NameNodes, but that's the default
 *  anyway.
 */
class HANamenodeTracker {
 public:
  HANamenodeTracker(const std::vector<ResolvedNamenodeInfo> &servers,
                    std::shared_ptr<IoService> ioservice,
                    std::shared_ptr<LibhdfsEvents> event_handlers_);

  virtual ~HANamenodeTracker();

  bool is_enabled() const { return enabled_; }
  bool is_resolved() const { return resolved_; }

  // Pass in vector of endpoints held by RpcConnection, use endpoints to infer node
  // currently being used.  Swap internal state and set out to other node.
  // Note: This will always mutate internal state.  Use IsCurrentActive/Standby to
  // get info without changing state
  bool GetFailoverAndUpdate(const std::vector<::asio::ip::tcp::endpoint>& current_endpoints,
                            ResolvedNamenodeInfo& out);

 private:
  // See if endpoint ep is part of the list of endpoints for the active or standby NN
  bool IsCurrentActive_locked(const ::asio::ip::tcp::endpoint &ep) const;
  bool IsCurrentStandby_locked(const ::asio::ip::tcp::endpoint &ep) const;

  // If HA should be enabled, according to our options and runtime info like # nodes provided
  bool enabled_;
  // If we were able to resolve at least 1 HA namenode
  bool resolved_;

  // Keep service in case a second round of DNS lookup is required
  std::shared_ptr<IoService> ioservice_;

  // Event handlers, for now this is the simplest place to catch all failover events
  // and push info out to client application.  Possibly move into RPCEngine.
  std::shared_ptr<LibhdfsEvents> event_handlers_;

  // Only support 1 active and 1 standby for now.
  ResolvedNamenodeInfo active_info_;
  ResolvedNamenodeInfo standby_info_;

  // Aquire when switching from active-standby
  std::mutex swap_lock_;
};

} // end namespace hdfs
#endif // end include guard
