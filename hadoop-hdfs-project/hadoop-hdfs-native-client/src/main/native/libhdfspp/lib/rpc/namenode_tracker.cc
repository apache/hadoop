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
                                     std::shared_ptr<IoService> ioservice,
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
    LOG_INFO(kRPC, << "HA enabled.  Using the following namenodes from the configuration."
                   << "\nNote: Active namenode cannot be determined until a connection has been made.")
    LOG_INFO(kRPC, << "First namenode url  = " << active_info_.uri.str());
    LOG_INFO(kRPC, << "Second namenode url = " << standby_info_.uri.str());

    enabled_ = true;
    if(!active_info_.endpoints.empty() || !standby_info_.endpoints.empty()) {
      resolved_ = true;
    }
  }
}

HANamenodeTracker::~HANamenodeTracker() {}

bool HANamenodeTracker::GetFailoverAndUpdate(const std::vector<::asio::ip::tcp::endpoint>& current_endpoints,
                                             ResolvedNamenodeInfo& out)
{
  mutex_guard swap_lock(swap_lock_);

  // Cannot look up without a key.
  if(current_endpoints.size() == 0) {
    event_handlers_->call(FS_NN_EMPTY_ENDPOINTS_EVENT, active_info_.nameservice.c_str(),
                          0 /*Not much to say about context without endpoints*/);
    LOG_ERROR(kRPC, << "HANamenodeTracker@" << this << "::GetFailoverAndUpdate requires at least 1 endpoint.");
    return false;
  }

  LOG_TRACE(kRPC, << "Swapping from endpoint " << current_endpoints[0]);

  if(IsCurrentActive_locked(current_endpoints[0])) {
    std::swap(active_info_, standby_info_);
    if(event_handlers_)
      event_handlers_->call(FS_NN_FAILOVER_EVENT, active_info_.nameservice.c_str(),
                            reinterpret_cast<int64_t>(active_info_.uri.str().c_str()));
    out = active_info_;
  } else if(IsCurrentStandby_locked(current_endpoints[0])) {
    // Connected to standby
    if(event_handlers_)
      event_handlers_->call(FS_NN_FAILOVER_EVENT, active_info_.nameservice.c_str(),
                            reinterpret_cast<int64_t>(active_info_.uri.str().c_str()));
    out = active_info_;
  } else {
    // Invalid state (or a NIC was added that didn't show up during DNS)
    std::stringstream errorMsg; // asio specializes endpoing operator<< for stringstream
    errorMsg << "Unable to find RPC connection in config. Looked for " << current_endpoints[0] << " in\n"
             << format_endpoints(active_info_.endpoints) << " and\n"
             << format_endpoints(standby_info_.endpoints) << std::endl;
    LOG_ERROR(kRPC, << errorMsg.str());
    return false;
  }

  // Extra DNS on swapped node to try and get EPs if it didn't already have them
  if(out.endpoints.empty()) {
    LOG_WARN(kRPC, << "No endpoints for node " << out.uri.str() << " attempting to resolve again");
    if(!ResolveInPlace(ioservice_, out)) {
      // Stuck retrying against the same NN that was able to be resolved in this case
      LOG_ERROR(kRPC, << "Fallback endpoint resolution for node " << out.uri.str()
                      << " failed.  Please make sure your configuration is up to date.");
    }
  }

  return true;
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
