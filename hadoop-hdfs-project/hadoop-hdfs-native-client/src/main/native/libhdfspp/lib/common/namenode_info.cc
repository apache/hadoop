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

#include "namenode_info.h"

#include "common/util.h"
#include "common/logging.h"
#include "hdfspp/ioservice.h"

#include <sstream>
#include <utility>
#include <future>


namespace hdfs {

ResolvedNamenodeInfo& ResolvedNamenodeInfo::operator=(const NamenodeInfo &info) {
  nameservice = info.nameservice;
  name = info.name;
  uri = info.uri;
  return *this;
}

std::string ResolvedNamenodeInfo::str() const {
  std::stringstream ss;
  ss << "ResolvedNamenodeInfo {nameservice: " << nameservice << ", name: " << name << ", uri: " << uri.str();
  ss << ", host: " << uri.get_host();

  if(uri.has_port())
    ss << ", port: " << uri.get_port();
  else
    ss << ", invalid port (uninitialized)";

  ss << ", scheme: " << uri.get_scheme();

  ss << " [";
  for(unsigned int i=0;i<endpoints.size();i++)
    ss << endpoints[i] << " ";
  ss << "] }";

  return ss.str();
}


bool ResolveInPlace(std::shared_ptr<IoService> ioservice, ResolvedNamenodeInfo &info) {
  // this isn't very memory friendly, but if it needs to be called often there are bigger issues at hand
  info.endpoints.clear();
  std::vector<ResolvedNamenodeInfo> resolved = BulkResolve(ioservice, {info});
  if(resolved.size() != 1)
    return false;

  info.endpoints = resolved[0].endpoints;
  if(info.endpoints.size() == 0)
    return false;
  return true;
}

typedef std::vector<asio::ip::tcp::endpoint> endpoint_vector;

// RAII wrapper
class ScopedResolver {
 private:
  std::shared_ptr<IoService> io_service_;
  std::string host_;
  std::string port_;
  ::asio::ip::tcp::resolver::query query_;
  ::asio::ip::tcp::resolver resolver_;
  endpoint_vector endpoints_;

  // Caller blocks on access if resolution isn't finished
  std::shared_ptr<std::promise<Status>> result_status_;
 public:
  ScopedResolver(std::shared_ptr<IoService> service, const std::string &host, const std::string &port) :
        io_service_(service), host_(host), port_(port), query_(host, port), resolver_(io_service_->GetRaw())
  {
    if(!io_service_)
      LOG_ERROR(kAsyncRuntime, << "ScopedResolver@" << this << " passed nullptr to io_service");
  }

  ~ScopedResolver() {
    resolver_.cancel();
  }

  bool BeginAsyncResolve() {
    // result_status_ would only exist if this was previously called.  Invalid state.
    if(result_status_) {
      LOG_ERROR(kAsyncRuntime, << "ScopedResolver@" << this << "::BeginAsyncResolve invalid call: may only be called once per instance");
      return false;
    } else if(!io_service_) {
      LOG_ERROR(kAsyncRuntime, << "ScopedResolver@" << this << "::BeginAsyncResolve invalid call: null io_service");
      return false;
    }

    // Now set up the promise, set it in async_resolve's callback
    result_status_ = std::make_shared<std::promise<Status>>();
    std::shared_ptr<std::promise<Status>> shared_result = result_status_;

    // Callback to pull a copy of endpoints out of resolver and set promise
    auto callback = [this, shared_result](const asio::error_code &ec, ::asio::ip::tcp::resolver::iterator out) {
      if(!ec) {
        std::copy(out, ::asio::ip::tcp::resolver::iterator(), std::back_inserter(endpoints_));
      }
      shared_result->set_value( ToStatus(ec) );
    };
    resolver_.async_resolve(query_, callback);
    return true;
  }

  Status Join() {
    if(!result_status_) {
      std::ostringstream errmsg;
      errmsg <<  "ScopedResolver@" << this << "Join invalid call: promise never set";
      return Status::InvalidArgument(errmsg.str().c_str());
    }

    std::future<Status> future_result = result_status_->get_future();
    Status res = future_result.get();
    return res;
  }

  endpoint_vector GetEndpoints() {
    // Explicitly return by value to decouple lifecycles.
    return endpoints_;
  }
};

std::vector<ResolvedNamenodeInfo> BulkResolve(std::shared_ptr<IoService> ioservice, const std::vector<NamenodeInfo> &nodes) {
  std::vector< std::unique_ptr<ScopedResolver> > resolvers;
  resolvers.reserve(nodes.size());

  std::vector<ResolvedNamenodeInfo> resolved_info;
  resolved_info.reserve(nodes.size());

  for(unsigned int i=0; i<nodes.size(); i++) {
    std::string host = nodes[i].get_host();
    std::string port = nodes[i].get_port();

    resolvers.emplace_back(new ScopedResolver(ioservice, host, port));
    resolvers[i]->BeginAsyncResolve();
  }

  // Join all async operations
  for(unsigned int i=0; i < resolvers.size(); i++) {
    Status asyncReturnStatus = resolvers[i]->Join();

    ResolvedNamenodeInfo info;
    info = nodes[i];

    if(asyncReturnStatus.ok()) {
      // Copy out endpoints if things went well
      info.endpoints = resolvers[i]->GetEndpoints();
    } else {
      LOG_ERROR(kAsyncRuntime, << "Unabled to resolve endpoints for host: " << nodes[i].get_host()
                                                               << " port: " << nodes[i].get_port());
    }

    resolved_info.push_back(info);
  }
  return resolved_info;
}

}
