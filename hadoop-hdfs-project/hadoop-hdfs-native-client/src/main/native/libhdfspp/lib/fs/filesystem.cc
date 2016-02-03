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

#include <functional>
#include <limits>
#include <future>
#include <tuple>
#include <iostream>
#include <pwd.h>

namespace hdfs {

static const char kNamenodeProtocol[] =
    "org.apache.hadoop.hdfs.protocol.ClientProtocol";
static const int kNamenodeProtocolVersion = 1;

using ::asio::ip::tcp;

/*****************************************************************************
 *                    NAMENODE OPERATIONS
 ****************************************************************************/

void NameNodeOperations::Connect(const std::string &server,
                             const std::string &service,
                             std::function<void(const Status &)> &&handler) {
  using namespace asio_continuation;
  typedef std::vector<tcp::endpoint> State;
  auto m = Pipeline<State>::Create();
  m->Push(Resolve(io_service_, server, service,
                  std::back_inserter(m->state())))
      .Push(Bind([this, m](const Continuation::Next &next) {
        engine_.Connect(m->state(), next);
      }));
  m->Run([this, handler](const Status &status, const State &) {
    handler(status);
  });
}

void NameNodeOperations::GetBlockLocations(const std::string & path,
  std::function<void(const Status &, std::shared_ptr<const struct FileInfo>)> handler)
{
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
    if (stat.ok()) {
      auto file_info = std::make_shared<struct FileInfo>();
      auto locations = s.resp->locations();

      file_info->file_length_ = locations.filelength();

      for (const auto &block : locations.blocks()) {
        file_info->blocks_.push_back(block);
      }

      if (locations.has_lastblock() && locations.lastblock().b().numbytes()) {
        file_info->blocks_.push_back(locations.lastblock());
      }

      handler(stat, file_info);
    } else {
      handler(stat, nullptr);
    }
  });
}


/*****************************************************************************
 *                    FILESYSTEM BASE CLASS
 ****************************************************************************/

FileSystem * FileSystem::New(
    IoService *&io_service, const std::string &user_name, const Options &options) {
  return new FileSystemImpl(io_service, user_name, options);
}

/*****************************************************************************
 *                    FILESYSTEM IMPLEMENTATION
 ****************************************************************************/

const std::string get_effective_user_name(const std::string &user_name) {
  if (!user_name.empty())
    return user_name;

  // If no user name was provided, try the HADOOP_USER_NAME and USER environment
  //    variables
  const char * env = getenv("HADOOP_USER_NAME");
  if (env) {
    return env;
  }

  env = getenv("USER");
  if (env) {
    return env;
  }

  // If running on POSIX, use the currently logged in user
#if defined(_POSIX_VERSION)
  uid_t uid = geteuid();
  struct passwd *pw = getpwuid(uid);
  if (pw && pw->pw_name)
  {
    return pw->pw_name;
  }
#endif

  return "unknown_user";
}

FileSystemImpl::FileSystemImpl(IoService *&io_service, const std::string &user_name,
                               const Options &options)
  :   io_service_(static_cast<IoServiceImpl *>(io_service)),
      nn_(&io_service_->io_service(), options,
      GetRandomClientName(), get_effective_user_name(user_name), kNamenodeProtocol,
      kNamenodeProtocolVersion), client_name_(GetRandomClientName()),
      bad_node_tracker_(std::make_shared<BadDataNodeTracker>())
{
  // Poor man's move
  io_service = nullptr;

  /* spawn background threads for asio delegation */
  unsigned int threads = 1 /* options.io_threads_, pending HDFS-9117 */;
  for (unsigned int i = 0; i < threads; i++) {
    AddWorkerThread();
  }
}

FileSystemImpl::~FileSystemImpl() {
  /**
   * Note: IoService must be stopped before getting rid of worker threads.
   * Once worker threads are joined and deleted the service can be deleted.
   **/
  io_service_->Stop();
  worker_threads_.clear();
}

void FileSystemImpl::Connect(const std::string &server,
                             const std::string &service,
                             const std::function<void(const Status &, FileSystem * fs)> &&handler) {
  /* IoService::New can return nullptr */
  if (!io_service_) {
    handler (Status::Error("Null IoService"), this);
  }

  nn_.Connect(server, service, [this, handler](const Status & s) {
    handler(s, this);
  });
}

Status FileSystemImpl::Connect(const std::string &server, const std::string &service) {
  /* synchronized */
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future = stat->get_future();

  auto callback = [stat](const Status &s, FileSystem *fs) {
    (void)fs;
    stat->set_value(s);
  };

  Connect(server, service, callback);

  /* block until promise is set */
  auto s = future.get();

  return s;
}


int FileSystemImpl::AddWorkerThread() {
  auto service_task = [](IoService *service) { service->Run(); };
  worker_threads_.push_back(
      WorkerPtr(new std::thread(service_task, io_service_.get())));
  return worker_threads_.size();
}

void FileSystemImpl::Open(
    const std::string &path,
    const std::function<void(const Status &, FileHandle *)> &handler) {

  nn_.GetBlockLocations(path, [this, handler](const Status &stat, std::shared_ptr<const struct FileInfo> file_info) {
    handler(stat, stat.ok() ? new FileHandleImpl(&io_service_->io_service(), client_name_, file_info, bad_node_tracker_)
                            : nullptr);
  });
}

Status FileSystemImpl::Open(const std::string &path,
                                         FileHandle **handle) {
  auto callstate = std::make_shared<std::promise<std::tuple<Status, FileHandle*>>>();
  std::future<std::tuple<Status, FileHandle*>> future(callstate->get_future());

  /* wrap async FileSystem::Open with promise to make it a blocking call */
  auto h = [callstate](const Status &s, FileHandle *is) {
    callstate->set_value(std::make_tuple(s, is));
  };

  Open(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  FileHandle *file_handle = std::get<1>(returnstate);

  if (!stat.ok()) {
    delete file_handle;
    return stat;
  }
  if (!file_handle) {
    return stat;
  }

  *handle = file_handle;
  return stat;
}

void FileSystemImpl::WorkerDeleter::operator()(std::thread *t) {
  // It is far too easy to destroy the filesystem (and thus the threadpool)
  //     from within one of the worker threads, leading to a deadlock.  Let's
  //     provide some explicit protection.
  if(t->get_id() == std::this_thread::get_id()) {
    //TODO: When we get good logging support, add it in here
    std::cerr << "FATAL: Attempted to destroy a thread pool from within a "
                 "callback of the thread pool.\n";
  }
  t->join();
  delete t;
}

}
