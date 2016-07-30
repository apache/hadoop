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

#include "common/continuation/asio.h"
#include "common/logging.h"

#include <sstream>
#include <utility>
#include <future>
#include <memory>

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
  auto port = uri.get_port();
  if(port)
    ss << ", port: " << port.value();
  else
    ss << ", port: unable to parse";

  ss << ", scheme: " << uri.get_scheme();

  ss << " [";
  for(unsigned int i=0;i<endpoints.size();i++)
    ss << endpoints[i] << " ";
  ss << "] }";

  return ss.str();
}


bool ResolveInPlace(::asio::io_service *ioservice, ResolvedNamenodeInfo &info) {
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


std::vector<ResolvedNamenodeInfo> BulkResolve(::asio::io_service *ioservice, const std::vector<NamenodeInfo> &nodes) {
  using namespace asio_continuation;

  typedef std::vector<asio::ip::tcp::endpoint> endpoint_vector;
  typedef Pipeline<endpoint_vector> resolve_pipeline_t;


  std::vector<std::pair<resolve_pipeline_t*, std::shared_ptr<std::promise<Status>>>> pipelines;
  pipelines.reserve(nodes.size());

  std::vector<ResolvedNamenodeInfo> resolved_info;
  // This must never reallocate once async ops begin
  resolved_info.reserve(nodes.size());

  for(unsigned int i=0; i<nodes.size(); i++) {
    std::string host = nodes[i].get_host();
    std::string port = nodes[i].get_port();

    ResolvedNamenodeInfo resolved;
    resolved = nodes[i];
    resolved_info.push_back(resolved);

    // build the pipeline
    resolve_pipeline_t *pipeline = resolve_pipeline_t::Create();
    auto resolve_step = Resolve(ioservice, host, port, std::back_inserter(pipeline->state()));
    pipeline->Push(resolve_step);

    // make a status associated with current pipeline
    std::shared_ptr<std::promise<Status>> active_stat = std::make_shared<std::promise<Status>>();
    pipelines.push_back(std::make_pair(pipeline, active_stat));

    pipeline->Run([i,active_stat, &resolved_info](const Status &s, const endpoint_vector &ends){
      resolved_info[i].endpoints = ends;
      active_stat->set_value(s);
    });

  }

  // Join all async operations
  std::vector<ResolvedNamenodeInfo> return_set;
  for(unsigned int i=0; i<pipelines.size();i++) {
    std::shared_ptr<std::promise<Status>> promise = pipelines[i].second;

    std::future<Status> future = promise->get_future();
    Status stat = future.get();

    // Clear endpoints if we hit an error
    if(!stat.ok()) {
      LOG_WARN(kRPC, << "Unable to resolve endpoints for " << nodes[i].uri.str());
      resolved_info[i].endpoints.clear();
    }
  }

  return resolved_info;
}


}
