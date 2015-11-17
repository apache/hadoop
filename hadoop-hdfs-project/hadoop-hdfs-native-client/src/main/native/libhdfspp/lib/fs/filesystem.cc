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

#include "filesystem.h"
#include "common/continuation/asio.h"
#include "common/util.h"

#include <asio/ip/tcp.hpp>

#include <limits>

namespace hdfs {

static const char kNamenodeProtocol[] =
    "org.apache.hadoop.hdfs.protocol.ClientProtocol";
static const int kNamenodeProtocolVersion = 1;

using ::asio::ip::tcp;

FileSystem::~FileSystem() {}

void FileSystem::New(
    IoService *io_service, const Options &options, const std::string &server,
    const std::string &service,
    const std::function<void(const Status &, FileSystem *)> &handler) {
  FileSystemImpl *impl = new FileSystemImpl(io_service, options);
  impl->Connect(server, service, [impl, handler](const Status &stat) {
    if (stat.ok()) {
      handler(stat, impl);
    } else {
      delete impl;
      handler(stat, nullptr);
    }
  });
}

FileSystemImpl::FileSystemImpl(IoService *io_service, const Options &options)
    : io_service_(static_cast<IoServiceImpl *>(io_service)),
      engine_(&io_service_->io_service(), options,
              RpcEngine::GetRandomClientName(), kNamenodeProtocol,
              kNamenodeProtocolVersion),
      namenode_(&engine_),
      bad_node_tracker_(std::make_shared<BadDataNodeTracker>()) {}

void FileSystemImpl::Connect(const std::string &server,
                             const std::string &service,
                             std::function<void(const Status &)> &&handler) {
  using namespace continuation;
  typedef std::vector<tcp::endpoint> State;
  auto m = Pipeline<State>::Create();
  m->Push(Resolve(&io_service_->io_service(), server, service,
                  std::back_inserter(m->state())))
      .Push(Bind([this, m](const Continuation::Next &next) {
        engine_.Connect(m->state().front(), next);
      }));
  m->Run([this, handler](const Status &status, const State &) {
    if (status.ok()) {
      engine_.Start();
    }
    handler(status);
  });
}

void FileSystemImpl::Open(
    const std::string &path,
    const std::function<void(const Status &, InputStream *)> &handler) {
  using ::hadoop::hdfs::GetBlockLocationsRequestProto;
  using ::hadoop::hdfs::GetBlockLocationsResponseProto;

  struct State {
    GetBlockLocationsRequestProto req;
    std::shared_ptr<GetBlockLocationsResponseProto> resp;
  };

  auto m = continuation::Pipeline<State>::Create();
  auto &req = m->state().req;
  req.set_src(path);
  req.set_offset(0);
  req.set_length(std::numeric_limits<long long>::max());
  m->state().resp.reset(new GetBlockLocationsResponseProto());

  State *s = &m->state();
  m->Push(continuation::Bind(
      [this, s](const continuation::Continuation::Next &next) {
        namenode_.GetBlockLocations(&s->req, s->resp, next);
      }));
  m->Run([this, handler](const Status &stat, const State &s) {
    handler(stat, stat.ok() ? new InputStreamImpl(this, &s.resp->locations(),
                                                  bad_node_tracker_)
                            : nullptr);
  });
}
}
