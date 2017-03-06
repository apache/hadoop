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

#include "namenode_tracker.h"

#include "common/logging.h"
#include "common/libhdfs_events_impl.h"
#include "common/util.h"

namespace hdfs {

static std::string format_endpoints(const std::vector<::asio::ip::tcp::endpoint> &pts) {
  std::stringstream ss;
  for(unsigned int i=0; i<pts.size(); i++)
    if(i == pts.size() - 1)
      ss << pts[i];
    else
      ss << pts[i] << ", ";
  return ss.str();
}

HANamenodeTracker::HANamenodeTracker(const std::vector<ResolvedNamenodeInfo> &servers,
                                     ::asio::io_service *ioservice,
                                     std::shared_ptr<LibhdfsEvents> event_handlers)
                  : enabled_(false), resolved_(false),
                    ioservice_(ioservice), event_handlers_(event_handlers)
{
  LOG_TRACE(kRPC, << "HANamenodeTracker got the following nodes");
  for(unsigned int i=0;i<servers.size();i++)
    LOG_TRACE(kRPC, << servers[i].str());

  if(servers.size() >= 2) {
    LOG_TRACE(kRPC, << "Creating HA namenode tracker");
    if(servers.size() > 2) {
      LOG_WARN(kRPC, << "Nameservice declares more than two nodes.  Some won't be used.");
    }

    active_info_ = servers[0];
    standby_info_ = servers[1];
    LOG_INFO(kRPC, << "Active namenode url  = " << active_info_.uri.str());
    LOG_INFO(kRPC, << "Standby namenode url = " << standby_info_.uri.str());

    enabled_ = true;
    if(!active_info_.endpoints.empty() || !standby_info_.endpoints.empty()) {
      resolved_ = true;
    }
  }
}

HANamenodeTracker::~HANamenodeTracker() {}

//  Pass in endpoint from current connection, this will do a reverse lookup
//  and return the info for the standby node. It will also swap its state internally.
ResolvedNamenodeInfo HANamenodeTracker::GetFailoverAndUpdate(::asio::ip::tcp::endpoint current_endpoint) {
  LOG_TRACE(kRPC, << "Swapping from endpoint " << current_endpoint);
  mutex_guard swap_lock(swap_lock_);

  ResolvedNamenodeInfo failover_node;

  // Connected to standby, switch standby to active
  if(IsCurrentActive_locked(current_endpoint)) {
    std::swap(active_info_, standby_info_);
    if(event_handlers_)
      event_handlers_->call(FS_NN_FAILOVER_EVENT, active_info_.nameservice.c_str(),
                            reinterpret_cast<int64_t>(active_info_.uri.str().c_str()));
    failover_node = active_info_;
  } else if(IsCurrentStandby_locked(current_endpoint)) {
    // Connected to standby
    if(event_handlers_)
      event_handlers_->call(FS_NN_FAILOVER_EVENT, active_info_.nameservice.c_str(),
                            reinterpret_cast<int64_t>(active_info_.uri.str().c_str()));
    failover_node = active_info_;
  } else {
    // Invalid state, throw for testing
    std::string ep1 = format_endpoints(active_info_.endpoints);
    std::string ep2 = format_endpoints(standby_info_.endpoints);

    std::stringstream msg;
    msg << "Looked for " << current_endpoint << " in\n";
    msg << ep1 << " and\n";
    msg << ep2 << std::endl;

    LOG_ERROR(kRPC, << "Unable to find RPC connection in config " << msg.str() << ". Bailing out.");
    throw std::runtime_error(msg.str());
  }

  if(failover_node.endpoints.empty()) {
    LOG_WARN(kRPC, << "No endpoints for node " << failover_node.uri.str() << " attempting to resolve again");
    if(!ResolveInPlace(ioservice_, failover_node)) {
      LOG_ERROR(kRPC, << "Fallback endpoint resolution for node " << failover_node.uri.str()
                      << "failed.  Please make sure your configuration is up to date.");
    }
  }
  return failover_node;
}

bool HANamenodeTracker::IsCurrentActive_locked(const ::asio::ip::tcp::endpoint &ep) const {
  for(unsigned int i=0;i<active_info_.endpoints.size();i++) {
    if(ep.address() == active_info_.endpoints[i].address()) {
      if(ep.port() != active_info_.endpoints[i].port())
        LOG_WARN(kRPC, << "Port mismatch: " << ep << " vs " << active_info_.endpoints[i] << " trying anyway..");
      return true;
    }
  }
  return false;
}

bool HANamenodeTracker::IsCurrentStandby_locked(const ::asio::ip::tcp::endpoint &ep) const {
  for(unsigned int i=0;i<standby_info_.endpoints.size();i++) {
    if(ep.address() == standby_info_.endpoints[i].address()) {
      if(ep.port() != standby_info_.endpoints[i].port())
        LOG_WARN(kRPC, << "Port mismatch: " << ep << " vs " << standby_info_.endpoints[i] << " trying anyway..");
      return true;
    }
  }
  return false;
}

} // end namespace hdfs
