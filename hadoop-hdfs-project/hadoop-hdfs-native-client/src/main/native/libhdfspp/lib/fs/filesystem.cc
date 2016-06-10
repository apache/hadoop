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

#include <functional>
#include <limits>
#include <future>
#include <tuple>
#include <iostream>
#include <pwd.h>

#define FMT_THIS_ADDR "this=" << (void*)this

namespace hdfs {

static const char kNamenodeProtocol[] =
    "org.apache.hadoop.hdfs.protocol.ClientProtocol";
static const int kNamenodeProtocolVersion = 1;

using ::asio::ip::tcp;

static constexpr uint16_t kDefaultPort = 8020;

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

FileSystemImpl::FileSystemImpl(IoService *&io_service, const std::string &user_name, const Options &options) :
    options_(options), client_name_(GetRandomClientName()), io_service_(
        static_cast<IoServiceImpl *>(io_service)),
        nn_(
          &io_service_->io_service(), options, client_name_,
          get_effective_user_name(user_name), kNamenodeProtocol,
          kNamenodeProtocolVersion
        ), bad_node_tracker_(std::make_shared<BadDataNodeTracker>()),
        event_handlers_(std::make_shared<LibhdfsEvents>()) {

  LOG_TRACE(kFileSystem, << "FileSystemImpl::FileSystemImpl("
                         << FMT_THIS_ADDR << ") called");

  // Poor man's move
  io_service = nullptr;

  /* spawn background threads for asio delegation */
  unsigned int threads = 1 /* options.io_threads_, pending HDFS-9117 */;
  for (unsigned int i = 0; i < threads; i++) {
    AddWorkerThread();
  }
}

FileSystemImpl::~FileSystemImpl() {
  LOG_TRACE(kFileSystem, << "FileSystemImpl::~FileSystemImpl("
                         << FMT_THIS_ADDR << ") called");

  /**
   * Note: IoService must be stopped before getting rid of worker threads.
   * Once worker threads are joined and deleted the service can be deleted.
   **/
  io_service_->Stop();
  worker_threads_.clear();
}

void FileSystemImpl::Connect(const std::string &server,
                             const std::string &service,
                             const std::function<void(const Status &, FileSystem * fs)> &handler) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::Connect(" << FMT_THIS_ADDR
                        << ", server=" << server << ", service="
                        << service << ") called");

  /* IoService::New can return nullptr */
  if (!io_service_) {
    handler (Status::Error("Null IoService"), this);
  }

  cluster_name_ = server + ":" + service;

  nn_.Connect(cluster_name_, server, service, [this, handler](const Status & s) {
    handler(s, this);
  });
}

Status FileSystemImpl::Connect(const std::string &server, const std::string &service) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::[sync]Connect(" << FMT_THIS_ADDR
                        << ", server=" << server << ", service=" << service << ") called");

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

void FileSystemImpl::ConnectToDefaultFs(const std::function<void(const Status &, FileSystem *)> &handler) {
  std::string scheme = options_.defaultFS.get_scheme();
  if (strcasecmp(scheme.c_str(), "hdfs") != 0) {
    std::string error_message;
    error_message += "defaultFS of [" + options_.defaultFS.str() + "] is not supported";
    handler(Status::InvalidArgument(error_message.c_str()), nullptr);
    return;
  }

  std::string host = options_.defaultFS.get_host();
  if (host.empty()) {
    handler(Status::InvalidArgument("defaultFS must specify a hostname"), nullptr);
    return;
  }

  optional<uint16_t>  port = options_.defaultFS.get_port();
  if (!port) {
    port = kDefaultPort;
  }
  std::string port_as_string = std::to_string(*port);

  Connect(host, port_as_string, handler);
}

Status FileSystemImpl::ConnectToDefaultFs() {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future = stat->get_future();

  auto callback = [stat](const Status &s, FileSystem *fs) {
    (void)fs;
    stat->set_value(s);
  };

  ConnectToDefaultFs(callback);

  /* block until promise is set */
  auto s = future.get();

  return s;
}



int FileSystemImpl::AddWorkerThread() {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::AddWorkerThread("
                                  << FMT_THIS_ADDR << ") called."
                                  << " Existing thread count = " << worker_threads_.size());

  auto service_task = [](IoService *service) { service->Run(); };
  worker_threads_.push_back(
      WorkerPtr(new std::thread(service_task, io_service_.get())));
  return worker_threads_.size();
}

void FileSystemImpl::Open(
    const std::string &path,
    const std::function<void(const Status &, FileHandle *)> &handler) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::Open("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetBlockLocations(path, [this, path, handler](const Status &stat, std::shared_ptr<const struct FileInfo> file_info) {
    handler(stat, stat.ok() ? new FileHandleImpl(cluster_name_, path, &io_service_->io_service(), client_name_, file_info, bad_node_tracker_, event_handlers_)
                            : nullptr);
  });
}

Status FileSystemImpl::Open(const std::string &path,
                                         FileHandle **handle) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::[sync]Open("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

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

BlockLocation LocatedBlockToBlockLocation(const hadoop::hdfs::LocatedBlockProto & locatedBlock)
{
  BlockLocation result;

  result.setCorrupt(locatedBlock.corrupt());
  result.setOffset(locatedBlock.offset());

  std::vector<DNInfo> dn_info;
  dn_info.reserve(locatedBlock.locs_size());
  for (const hadoop::hdfs::DatanodeInfoProto & datanode_info: locatedBlock.locs()) {
    const hadoop::hdfs::DatanodeIDProto &id = datanode_info.id();
    DNInfo newInfo;
    if (id.has_ipaddr())
        newInfo.setIPAddr(id.ipaddr());
    if (id.has_hostname())
        newInfo.setHostname(id.hostname());
    if (id.has_xferport())
        newInfo.setXferPort(id.xferport());
    if (id.has_infoport())
        newInfo.setInfoPort(id.infoport());
    if (id.has_ipcport())
        newInfo.setIPCPort(id.ipcport());
    if (id.has_infosecureport())
      newInfo.setInfoSecurePort(id.infosecureport());
    dn_info.push_back(newInfo);
  }
  result.setDataNodes(dn_info);

  if (locatedBlock.has_b()) {
    const hadoop::hdfs::ExtendedBlockProto & b=locatedBlock.b();
    result.setLength(b.numbytes());
  }


  return result;
}

void FileSystemImpl::GetBlockLocations(const std::string & path,
  const std::function<void(const Status &, std::shared_ptr<FileBlockLocation> locations)> handler)
{
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetBlockLocations("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  auto conversion = [handler](const Status & status, std::shared_ptr<const struct FileInfo> fileInfo) {
    if (status.ok()) {
      auto result = std::make_shared<FileBlockLocation>();

      result->setFileLength(fileInfo->file_length_);
      result->setLastBlockComplete(fileInfo->last_block_complete_);
      result->setUnderConstruction(fileInfo->under_construction_);

      std::vector<BlockLocation> blocks;
      for (const hadoop::hdfs::LocatedBlockProto & locatedBlock: fileInfo->blocks_) {
          auto newLocation = LocatedBlockToBlockLocation(locatedBlock);
          blocks.push_back(newLocation);
      }
      result->setBlockLocations(blocks);

      handler(status, result);
    } else {
      handler(status, std::shared_ptr<FileBlockLocation>());
    }
  };

  nn_.GetBlockLocations(path, conversion);
}

Status FileSystemImpl::GetBlockLocations(const std::string & path,
  std::shared_ptr<FileBlockLocation> * fileBlockLocations)
{
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]GetBlockLocations("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  if (!fileBlockLocations)
    return Status::InvalidArgument("Null pointer passed to GetBlockLocations");

  auto callstate = std::make_shared<std::promise<std::tuple<Status, std::shared_ptr<FileBlockLocation>>>>();
  std::future<std::tuple<Status, std::shared_ptr<FileBlockLocation>>> future(callstate->get_future());

  /* wrap async call with promise/future to make it blocking */
  auto callback = [callstate](const Status &s, std::shared_ptr<FileBlockLocation> blockInfo) {
    callstate->set_value(std::make_tuple(s,blockInfo));
  };

  GetBlockLocations(path, callback);

  /* wait for async to finish */
  auto returnstate = future.get();
  auto stat = std::get<0>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  *fileBlockLocations = std::get<1>(returnstate);

  return stat;
}

void FileSystemImpl::GetFileInfo(
    const std::string &path,
    const std::function<void(const Status &, const StatInfo &)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetFileInfo("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetFileInfo(path, [handler](const Status &stat, const StatInfo &stat_info) {
    handler(stat, stat_info);
  });
}

Status FileSystemImpl::GetFileInfo(const std::string &path,
                                         StatInfo & stat_info) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]GetFileInfo("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status, StatInfo>>>();
  std::future<std::tuple<Status, StatInfo>> future(callstate->get_future());

  /* wrap async FileSystem::GetFileInfo with promise to make it a blocking call */
  auto h = [callstate](const Status &s, const StatInfo &si) {
    callstate->set_value(std::make_tuple(s, si));
  };

  GetFileInfo(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  StatInfo info = std::get<1>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  stat_info = info;
  return stat;
}

void FileSystemImpl::GetFsStats(
    const std::function<void(const Status &, const FsInfo &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::GetFsStats(" << FMT_THIS_ADDR << ") called");

  nn_.GetFsStats([handler](const Status &stat, const FsInfo &fs_info) {
    handler(stat, fs_info);
  });
}

Status FileSystemImpl::GetFsStats(FsInfo & fs_info) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]GetFsStats(" << FMT_THIS_ADDR << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status, FsInfo>>>();
  std::future<std::tuple<Status, FsInfo>> future(callstate->get_future());

  /* wrap async FileSystem::GetFsStats with promise to make it a blocking call */
  auto h = [callstate](const Status &s, const FsInfo &si) {
    callstate->set_value(std::make_tuple(s, si));
  };

  GetFsStats(h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  FsInfo info = std::get<1>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  fs_info = info;
  return stat;
}

/**
 * Helper function for recursive GetListing calls.
 *
 * Some compilers don't like recursive lambdas, so we make the lambda call a
 * method, which in turn creates a lambda calling itself.
 */
void FileSystemImpl::GetListingShim(const Status &stat, std::shared_ptr<std::vector<StatInfo>> &stat_infos, bool has_more,
                        std::string path,
                        const std::function<bool(const Status &, std::shared_ptr<std::vector<StatInfo>>&, bool)> &handler) {
  bool has_next = stat_infos && stat_infos->size() > 0;
  bool get_more = handler(stat, stat_infos, has_more && has_next);
  if (get_more && has_more && has_next ) {
    auto callback = [this, path, handler](const Status &stat, std::shared_ptr<std::vector<StatInfo>> &stat_infos, bool has_more) {
      GetListingShim(stat, stat_infos, has_more, path, handler);
    };

    std::string last = stat_infos->back().path;
    nn_.GetListing(path, callback, last);
  }
}

void FileSystemImpl::GetListing(
    const std::string &path,
    const std::function<bool(const Status &, std::shared_ptr<std::vector<StatInfo>>&, bool)> &handler) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::GetListing("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  // Caputure the state and push it into the shim
  auto callback = [this, path, handler](const Status &stat, std::shared_ptr<std::vector<StatInfo>> &stat_infos, bool has_more) {
    GetListingShim(stat, stat_infos, has_more, path, handler);
  };

  nn_.GetListing(path, callback);
}

Status FileSystemImpl::GetListing(const std::string &path, std::shared_ptr<std::vector<StatInfo>> &stat_infos) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::[sync]GetListing("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  // In this case, we're going to allocate the result on the heap and have the
  //   async code populate it.
  auto results = std::make_shared<std::vector<StatInfo>>();

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::GetListing with promise to make it a blocking call.
   *
     Keep requesting more until we get the entire listing, and don't set the promise
   * until we have the entire listing.
   */
  auto h = [callstate, results](const Status &s, std::shared_ptr<std::vector<StatInfo>> si, bool has_more) -> bool {
    if (si) {
      results->insert(results->end(), si->begin(), si->end());
    }

    bool done = !s.ok() || !has_more;
    if (done) {
      callstate->set_value(s);
      return false;
    }
    return true;
  };

  GetListing(path, h);

  /* block until promise is set */
  Status stat = future.get();

  if (!stat.ok()) {
    return stat;
  }

  stat_infos = results;
  return stat;
}

void FileSystemImpl::CreateSnapshot(const std::string &path,
    const std::string &name,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::CreateSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Argument 'path' cannot be empty"));
    return;
  }

  nn_.CreateSnapshot(path, name, [handler](const Status &stat) {
    handler(stat);
  });
}

Status FileSystemImpl::CreateSnapshot(const std::string &path,
    const std::string &name) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]CreateSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status>>>();
  std::future<std::tuple<Status>> future(callstate->get_future());

  /* wrap async FileSystem::CreateSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(std::make_tuple(s));
  };

  CreateSnapshot(path, name, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);

  return stat;
}

void FileSystemImpl::DeleteSnapshot(const std::string &path,
    const std::string &name,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::DeleteSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Argument 'path' cannot be empty"));
    return;
  }
  if (name.empty()) {
    handler(Status::InvalidArgument("Argument 'name' cannot be empty"));
    return;
  }

  nn_.DeleteSnapshot(path, name, [handler](const Status &stat) {
    handler(stat);
  });
}

Status FileSystemImpl::DeleteSnapshot(const std::string &path,
    const std::string &name) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]DeleteSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status>>>();
  std::future<std::tuple<Status>> future(callstate->get_future());

  /* wrap async FileSystem::DeleteSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(std::make_tuple(s));
  };

  DeleteSnapshot(path, name, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);

  return stat;
}

void FileSystemImpl::AllowSnapshot(const std::string &path,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::AllowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Argument 'path' cannot be empty"));
    return;
  }

  nn_.AllowSnapshot(path, [handler](const Status &stat) {
    handler(stat);
  });
}

Status FileSystemImpl::AllowSnapshot(const std::string &path) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]AllowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status>>>();
  std::future<std::tuple<Status>> future(callstate->get_future());

  /* wrap async FileSystem::AllowSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(std::make_tuple(s));
  };

  AllowSnapshot(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);

  return stat;
}

void FileSystemImpl::DisallowSnapshot(const std::string &path,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::DisallowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Argument 'path' cannot be empty"));
    return;
  }

  nn_.DisallowSnapshot(path, [handler](const Status &stat) {
    handler(stat);
  });
}

Status FileSystemImpl::DisallowSnapshot(const std::string &path) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]DisallowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status>>>();
  std::future<std::tuple<Status>> future(callstate->get_future());

  /* wrap async FileSystem::DisallowSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(std::make_tuple(s));
  };

  DisallowSnapshot(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);

  return stat;
}

void FileSystemImpl::WorkerDeleter::operator()(std::thread *t) {
  // It is far too easy to destroy the filesystem (and thus the threadpool)
  //     from within one of the worker threads, leading to a deadlock.  Let's
  //     provide some explicit protection.
  if(t->get_id() == std::this_thread::get_id()) {
    LOG_ERROR(kFileSystem, << "FileSystemImpl::WorkerDeleter::operator(treadptr="
                           << t << ") : FATAL: Attempted to destroy a thread pool"
                           "from within a callback of the thread pool!");
  }
  t->join();
  delete t;
}


void FileSystemImpl::SetFsEventCallback(fs_event_callback callback) {
  if (event_handlers_) {
    event_handlers_->set_fs_callback(callback);
    nn_.SetFsEventCallback(callback);
  }
}



std::shared_ptr<LibhdfsEvents> FileSystemImpl::get_event_handlers() {
  return event_handlers_;
}

}
