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

#include "common/namenode_info.h"

#include <functional>
#include <limits>
#include <future>
#include <tuple>
#include <iostream>
#include <pwd.h>
#include <fnmatch.h>

#define FMT_THIS_ADDR "this=" << (void*)this

namespace hdfs {

static const char kNamenodeProtocol[] =
    "org.apache.hadoop.hdfs.protocol.ClientProtocol";
static const int kNamenodeProtocolVersion = 1;

using ::asio::ip::tcp;

static constexpr uint16_t kDefaultPort = 8020;

// forward declarations
const std::string get_effective_user_name(const std::string &);

uint32_t FileSystem::GetDefaultFindMaxDepth() {
  return std::numeric_limits<uint32_t>::max();
}

uint16_t FileSystem::GetDefaultPermissionMask() {
  return 0755;
}

Status FileSystem::CheckValidPermissionMask(uint16_t permissions) {
  if (permissions > 01777) {
    std::stringstream errormsg;
    errormsg << "CheckValidPermissionMask: argument 'permissions' is " << std::oct
        << std::showbase << permissions << " (should be between 0 and 01777)";
    return Status::InvalidArgument(errormsg.str().c_str());
  }
  return Status::OK();
}

Status FileSystem::CheckValidReplication(uint16_t replication) {
  if (replication < 1 || replication > 512) {
    std::stringstream errormsg;
    errormsg << "CheckValidReplication: argument 'replication' is "
        << replication << " (should be between 1 and 512)";
    return Status::InvalidArgument(errormsg.str().c_str());
  }
  return Status::OK();
}

/*****************************************************************************
 *                    FILESYSTEM BASE CLASS
 ****************************************************************************/

FileSystem *FileSystem::New(
    IoService *&io_service, const std::string &user_name, const Options &options) {
  return new FileSystemImpl(io_service, user_name, options);
}

FileSystem *FileSystem::New() {
  // No, this pointer won't be leaked.  The FileSystem takes ownership.
  IoService *io_service = IoService::New();
  if(!io_service)
    return nullptr;
  std::string user_name = get_effective_user_name("");
  Options options;
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

  // DNS lookup here for namenode(s)
  std::vector<ResolvedNamenodeInfo> resolved_namenodes;

  auto name_service = options_.services.find(server);
  if(name_service != options_.services.end()) {
    cluster_name_ = name_service->first;
    resolved_namenodes = BulkResolve(&io_service_->io_service(), name_service->second);
  } else {
    cluster_name_ = server + ":" + service;

    // tmp namenode info just to get this in the right format for BulkResolve
    NamenodeInfo tmp_info;
    optional<URI> uri = URI::parse_from_string("hdfs://" + cluster_name_);
    if(!uri) {
      LOG_ERROR(kFileSystem, << "Unable to use URI for cluster " << cluster_name_);
      handler(Status::Error(("Invalid namenode " + cluster_name_ + " in config").c_str()), this);
    }
    tmp_info.uri = uri.value();

    resolved_namenodes = BulkResolve(&io_service_->io_service(), {tmp_info});
  }

  for(unsigned int i=0;i<resolved_namenodes.size();i++) {
    LOG_DEBUG(kFileSystem, << "Resolved Namenode");
    LOG_DEBUG(kFileSystem, << resolved_namenodes[i].str());
  }


  nn_.Connect(cluster_name_, /*server, service*/ resolved_namenodes, [this, handler](const Status & s) {
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
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::Open("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetBlockLocations(path, 0, std::numeric_limits<int64_t>::max(), [this, path, handler](const Status &stat, std::shared_ptr<const struct FileInfo> file_info) {
    if(!stat.ok()) {
      LOG_DEBUG(kFileSystem, << "FileSystemImpl::Open failed to get block locations. status=" << stat.ToString());
      if(stat.get_server_exception_type() == Status::kStandbyException) {
        LOG_DEBUG(kFileSystem, << "Operation not allowed on standby datanode");
      }
    }
    handler(stat, stat.ok() ? new FileHandleImpl(cluster_name_, path, &io_service_->io_service(), client_name_, file_info, bad_node_tracker_, event_handlers_)
                            : nullptr);
  });
}

Status FileSystemImpl::Open(const std::string &path,
                                         FileHandle **handle) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]Open("
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

void FileSystemImpl::GetBlockLocations(const std::string & path, uint64_t offset, uint64_t length,
  const std::function<void(const Status &, std::shared_ptr<FileBlockLocation> locations)> handler)
{
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetBlockLocations("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  //Protobuf gives an error 'Negative value is not supported'
  //if the high bit is set in uint64 in GetBlockLocations
  if (IsHighBitSet(offset)) {
    handler(Status::InvalidArgument("GetBlockLocations: argument 'offset' cannot have high bit set"), nullptr);
    return;
  }
  if (IsHighBitSet(length)) {
    handler(Status::InvalidArgument("GetBlockLocations: argument 'length' cannot have high bit set"), nullptr);
    return;
  }

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

  nn_.GetBlockLocations(path, offset, length, conversion);
}

Status FileSystemImpl::GetBlockLocations(const std::string & path, uint64_t offset, uint64_t length,
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

  GetBlockLocations(path, offset, length, callback);

  /* wait for async to finish */
  auto returnstate = future.get();
  auto stat = std::get<0>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  *fileBlockLocations = std::get<1>(returnstate);

  return stat;
}

void FileSystemImpl::GetPreferredBlockSize(const std::string &path,
    const std::function<void(const Status &, const uint64_t &)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetPreferredBlockSize("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetPreferredBlockSize(path, handler);
}

Status FileSystemImpl::GetPreferredBlockSize(const std::string &path, uint64_t & block_size) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]GetPreferredBlockSize("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status, uint64_t>>>();
  std::future<std::tuple<Status, uint64_t>> future(callstate->get_future());

  /* wrap async FileSystem::GetPreferredBlockSize with promise to make it a blocking call */
  auto h = [callstate](const Status &s, const uint64_t & bsize) {
    callstate->set_value(std::make_tuple(s, bsize));
  };

  GetPreferredBlockSize(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  uint64_t size = std::get<1>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  block_size = size;
  return stat;
}

void FileSystemImpl::SetReplication(const std::string & path, int16_t replication, std::function<void(const Status &)> handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::SetReplication(" << FMT_THIS_ADDR << ", path=" << path <<
      ", replication=" << replication << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetReplication: argument 'path' cannot be empty"));
    return;
  }
  Status replStatus = FileSystem::CheckValidReplication(replication);
  if (!replStatus.ok()) {
    handler(replStatus);
    return;
  }

  nn_.SetReplication(path, replication, handler);
}

Status FileSystemImpl::SetReplication(const std::string & path, int16_t replication) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]SetReplication(" << FMT_THIS_ADDR << ", path=" << path <<
      ", replication=" << replication << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::SetReplication with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  SetReplication(path, replication, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::SetTimes(const std::string & path, uint64_t mtime, uint64_t atime,
    std::function<void(const Status &)> handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::SetTimes(" << FMT_THIS_ADDR << ", path=" << path <<
      ", mtime=" << mtime << ", atime=" << atime << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetTimes: argument 'path' cannot be empty"));
    return;
  }

  nn_.SetTimes(path, mtime, atime, handler);
}

Status FileSystemImpl::SetTimes(const std::string & path, uint64_t mtime, uint64_t atime) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]SetTimes(" << FMT_THIS_ADDR << ", path=" << path <<
      ", mtime=" << mtime << ", atime=" << atime << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::SetTimes with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  SetTimes(path, mtime, atime, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::GetFileInfo(
    const std::string &path,
    const std::function<void(const Status &, const StatInfo &)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetFileInfo("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetFileInfo(path, handler);
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

  nn_.GetFsStats(handler);
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
void FileSystemImpl::GetListingShim(const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more,
                      std::string path, const std::function<bool(const Status &, const std::vector<StatInfo> &, bool)> &handler) {
  bool has_next = !stat_infos.empty();
  bool get_more = handler(stat, stat_infos, has_more && has_next);
  if (get_more && has_more && has_next ) {
    auto callback = [this, path, handler](const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more) {
      GetListingShim(stat, stat_infos, has_more, path, handler);
    };

    std::string last = stat_infos.back().path;
    nn_.GetListing(path, callback, last);
  }
}

void FileSystemImpl::GetListing(
    const std::string &path,
    const std::function<bool(const Status &, const std::vector<StatInfo> &, bool)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetListing("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  // Caputure the state and push it into the shim
  auto callback = [this, path, handler](const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more) {
    GetListingShim(stat, stat_infos, has_more, path, handler);
  };

  nn_.GetListing(path, callback);
}

Status FileSystemImpl::GetListing(const std::string &path, std::vector<StatInfo> * stat_infos) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]GetListing("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  if (!stat_infos) {
    return Status::InvalidArgument("FileSystemImpl::GetListing: argument 'stat_infos' cannot be NULL");
  }

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::GetListing with promise to make it a blocking call.
   *
     Keep requesting more until we get the entire listing, and don't set the promise
   * until we have the entire listing.
   */
  auto h = [callstate, stat_infos](const Status &s, const std::vector<StatInfo> & si, bool has_more) -> bool {
    if (!si.empty()) {
      stat_infos->insert(stat_infos->end(), si.begin(), si.end());
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

  return stat;
}

void FileSystemImpl::Mkdirs(const std::string & path, uint16_t permissions, bool createparent,
    std::function<void(const Status &)> handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::Mkdirs(" << FMT_THIS_ADDR << ", path=" << path <<
      ", permissions=" << permissions << ", createparent=" << createparent << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Mkdirs: argument 'path' cannot be empty"));
    return;
  }

  Status permStatus = FileSystem::CheckValidPermissionMask(permissions);
  if (!permStatus.ok()) {
    handler(permStatus);
    return;
  }

  nn_.Mkdirs(path, permissions, createparent, handler);
}

Status FileSystemImpl::Mkdirs(const std::string & path, uint16_t permissions, bool createparent) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]Mkdirs(" << FMT_THIS_ADDR << ", path=" << path <<
      ", permissions=" << permissions << ", createparent=" << createparent << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::Mkdirs with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  Mkdirs(path, permissions, createparent, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::Delete(const std::string &path, bool recursive,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::Delete(" << FMT_THIS_ADDR << ", path=" << path << ", recursive=" << recursive << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Delete: argument 'path' cannot be empty"));
    return;
  }

  nn_.Delete(path, recursive, handler);
}

Status FileSystemImpl::Delete(const std::string &path, bool recursive) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]Delete(" << FMT_THIS_ADDR << ", path=" << path << ", recursive=" << recursive << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::Delete with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  Delete(path, recursive, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::Rename(const std::string &oldPath, const std::string &newPath,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::Rename(" << FMT_THIS_ADDR << ", oldPath=" << oldPath << ", newPath=" << newPath << ") called");

  if (oldPath.empty()) {
    handler(Status::InvalidArgument("Rename: argument 'oldPath' cannot be empty"));
    return;
  }

  if (newPath.empty()) {
    handler(Status::InvalidArgument("Rename: argument 'newPath' cannot be empty"));
    return;
  }

  nn_.Rename(oldPath, newPath, handler);
}

Status FileSystemImpl::Rename(const std::string &oldPath, const std::string &newPath) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]Rename(" << FMT_THIS_ADDR << ", oldPath=" << oldPath << ", newPath=" << newPath << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::Rename with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  Rename(oldPath, newPath, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::SetPermission(const std::string & path,
    uint16_t permissions, const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::SetPermission(" << FMT_THIS_ADDR << ", path=" << path << ", permissions=" << permissions << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetPermission: argument 'path' cannot be empty"));
    return;
  }
  Status permStatus = FileSystem::CheckValidPermissionMask(permissions);
  if (!permStatus.ok()) {
    handler(permStatus);
    return;
  }

  nn_.SetPermission(path, permissions, handler);
}

Status FileSystemImpl::SetPermission(const std::string & path, uint16_t permissions) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]SetPermission(" << FMT_THIS_ADDR << ", path=" << path << ", permissions=" << permissions << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::SetPermission with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  SetPermission(path, permissions, h);

  /* block until promise is set */
  Status stat = future.get();

  return stat;
}

void FileSystemImpl::SetOwner(const std::string & path, const std::string & username,
    const std::string & groupname, const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::SetOwner(" << FMT_THIS_ADDR << ", path=" << path << ", username=" << username << ", groupname=" << groupname << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetOwner: argument 'path' cannot be empty"));
    return;
  }

  nn_.SetOwner(path, username, groupname, handler);
}

Status FileSystemImpl::SetOwner(const std::string & path, const std::string & username,
                                const std::string & groupname) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]SetOwner(" << FMT_THIS_ADDR << ", path=" << path << ", username=" << username << ", groupname=" << groupname << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::SetOwner with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  SetOwner(path, username, groupname, h);

  /* block until promise is set */
  Status stat = future.get();
  return stat;
}

/**
 * Helper function for recursive Find calls.
 *
 * Some compilers don't like recursive lambdas, so we make the lambda call a
 * method, which in turn creates a lambda calling itself.
 *
 * ***High-level explanation***
 *
 * Since we are allowing to use wild cards in both path and name, we start by expanding the path first.
 * Boolean search_path is set to true when we search for the path and false when we search for the name.
 * When we search for the path we break the given path pattern into sub-directories. Starting from the
 * first sub-directory we list them one-by-one and recursively continue into directories that matched the
 * path pattern at the current depth. Directories that are large will be requested to continue sending
 * the results. We keep track of the current depth within the path pattern in the 'depth' variable.
 * This continues recursively until the depth reaches the end of the path. Next that we start matching
 * the name pattern. All directories that we find we recurse now, and all names that match the given name
 * pattern are being stored in outputs and later sent back to the user.
 */
void FileSystemImpl::FindShim(const Status &stat, const std::vector<StatInfo> & stat_infos, bool directory_has_more,
                      std::shared_ptr<FindOperationalState> operational_state, std::shared_ptr<FindSharedState> shared_state) {
  //We buffer the outputs then send them back at the end
  std::vector<StatInfo> outputs;
  //Return on error
  if(!stat.ok()){
    std::lock_guard<std::mutex> find_lock(shared_state->lock);
    //We send true becuase we do not want the user code to exit before all our requests finished
    shared_state->handler(stat, outputs, true);
    shared_state->aborted = true;
  }
  if(!shared_state->aborted){
    //User did not abort the operation
    if (directory_has_more) {
      //Directory is large and has more results
      //We launch another async call to get more results
      shared_state->outstanding_requests++;
      auto callback = [this, operational_state, shared_state](const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more) {
        FindShim(stat, stat_infos, has_more, operational_state, shared_state);
      };
      std::string last = stat_infos.back().path;
      nn_.GetListing(operational_state->path, callback, last);
    }
    if(operational_state->search_path && operational_state->depth < shared_state->dirs.size() - 1){
      //We are searching for the path and did not reach the end of the path yet
      for (StatInfo const& si : stat_infos) {
        //If we are at the last depth and it matches both path and name, we need to output it.
        if (operational_state->depth == shared_state->dirs.size() - 2
            && !fnmatch(shared_state->dirs[operational_state->depth + 1].c_str(), si.path.c_str(), 0)
            && !fnmatch(shared_state->name.c_str(), si.path.c_str(), 0)) {
          outputs.push_back(si);
        }
        //Skip if not directory
        if(si.file_type != StatInfo::IS_DIR) {
          continue;
        }
        //Checking for a match with the path at the current depth
        if(!fnmatch(shared_state->dirs[operational_state->depth + 1].c_str(), si.path.c_str(), 0)){
          //Launch a new requests for every matched directory
          shared_state->outstanding_requests++;
          auto callback = [this, si, operational_state, shared_state](const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more) {
            std::shared_ptr<FindOperationalState> new_current_state = std::make_shared<FindOperationalState>(si.full_path, operational_state->depth + 1, true);  //true because searching for the path
            FindShim(stat, stat_infos, has_more, new_current_state, shared_state);
          };
          nn_.GetListing(si.full_path, callback);
        }
      }
    }
    else if(shared_state->maxdepth > operational_state->depth - shared_state->dirs.size() + 1){
      //We are searching for the name now and maxdepth has not been reached
      for (StatInfo const& si : stat_infos) {
        //Launch a new request for every directory
        if(si.file_type == StatInfo::IS_DIR) {
          shared_state->outstanding_requests++;
          auto callback = [this, si, operational_state, shared_state](const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more) {
            std::shared_ptr<FindOperationalState> new_current_state = std::make_shared<FindOperationalState>(si.full_path, operational_state->depth + 1, false); //false because searching for the name
            FindShim(stat, stat_infos, has_more, new_current_state, shared_state);
          };
          nn_.GetListing(si.full_path, callback);
        }
        //All names that match the specified name are saved to outputs
        if(!fnmatch(shared_state->name.c_str(), si.path.c_str(), 0)){
          outputs.push_back(si);
        }
      }
    }
  }
  //This section needs a lock to make sure we return the final chunk only once
  //and no results are sent after aborted is set
  std::lock_guard<std::mutex> find_lock(shared_state->lock);
  //Decrement the counter once since we are done with this chunk
  shared_state->outstanding_requests--;
  if(shared_state->outstanding_requests == 0){
    //Send the outputs back to the user and notify that this is the final chunk
    shared_state->handler(stat, outputs, false);
  } else {
    //There will be more results and we are not aborting
    if (outputs.size() > 0 && !shared_state->aborted){
      //Send the outputs back to the user and notify that there is more
      bool user_wants_more = shared_state->handler(stat, outputs, true);
      if(!user_wants_more) {
        //Abort if user doesn't want more
        shared_state->aborted = true;
      }
    }
  }
}

void FileSystemImpl::Find(
    const std::string &path, const std::string &name, const uint32_t maxdepth,
    const std::function<bool(const Status &, const std::vector<StatInfo> &, bool)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::Find("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ", name="
                                 << name << ") called");

  //Populating the operational state, which includes:
  //current search path, depth within the path, and the indication that we are currently searching for a path (not name yet).
  std::shared_ptr<FindOperationalState> operational_state  = std::make_shared<FindOperationalState>(path, 0, true);
  //Populating the shared state, which includes:
  //vector of sub-directories constructed from path, name to search, handler to use for result returning, outstanding_requests counter, and aborted flag.
  std::shared_ptr<FindSharedState> shared_state = std::make_shared<FindSharedState>(path, name, maxdepth, handler, 1, false);
  auto callback = [this, operational_state, shared_state](const Status &stat, const std::vector<StatInfo> & stat_infos, bool directory_has_more) {
    FindShim(stat, stat_infos, directory_has_more, operational_state, shared_state);
  };
  nn_.GetListing("/", callback);
}

Status FileSystemImpl::Find(const std::string &path, const std::string &name, const uint32_t maxdepth, std::vector<StatInfo> * stat_infos) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]Find("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ", name="
                                 << name << ") called");

  if (!stat_infos) {
    return Status::InvalidArgument("FileSystemImpl::Find: argument 'stat_infos' cannot be NULL");
  }

  // In this case, we're going to have the async code populate stat_infos.

  std::promise<void> promise = std::promise<void>();
  std::future<void> future(promise.get_future());
  Status status = Status::OK();

  /**
    * Keep requesting more until we get the entire listing. Set the promise
    * when we have the entire listing to stop.
    *
    * Find guarantees that the handler will only be called once at a time,
    * so we do not need any locking here
    */
  auto h = [&status, &promise, stat_infos](const Status &s, const std::vector<StatInfo> & si, bool has_more_results) -> bool {
    if (!si.empty()) {
      stat_infos->insert(stat_infos->end(), si.begin(), si.end());
    }
    if (!s.ok() && status.ok()){
      //We make sure we set 'status' only on the first error.
      status = s;
    }
    if (!has_more_results) {
      promise.set_value();
      return false;
    }
    return true;
  };

  Find(path, name, maxdepth, h);

  /* block until promise is set */
  future.get();
  return status;
}

void FileSystemImpl::CreateSnapshot(const std::string &path,
    const std::string &name,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::CreateSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("CreateSnapshot: argument 'path' cannot be empty"));
    return;
  }

  nn_.CreateSnapshot(path, name, handler);
}

Status FileSystemImpl::CreateSnapshot(const std::string &path,
    const std::string &name) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]CreateSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::CreateSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  CreateSnapshot(path, name, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::DeleteSnapshot(const std::string &path,
    const std::string &name,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::DeleteSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("DeleteSnapshot: argument 'path' cannot be empty"));
    return;
  }
  if (name.empty()) {
    handler(Status::InvalidArgument("DeleteSnapshot: argument 'name' cannot be empty"));
    return;
  }

  nn_.DeleteSnapshot(path, name, handler);
}

Status FileSystemImpl::DeleteSnapshot(const std::string &path,
    const std::string &name) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]DeleteSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::DeleteSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  DeleteSnapshot(path, name, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::AllowSnapshot(const std::string &path,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::AllowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("AllowSnapshot: argument 'path' cannot be empty"));
    return;
  }

  nn_.AllowSnapshot(path, handler);
}

Status FileSystemImpl::AllowSnapshot(const std::string &path) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]AllowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::AllowSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  AllowSnapshot(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
}

void FileSystemImpl::DisallowSnapshot(const std::string &path,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::DisallowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("DisallowSnapshot: argument 'path' cannot be empty"));
    return;
  }

  nn_.DisallowSnapshot(path, handler);
}

Status FileSystemImpl::DisallowSnapshot(const std::string &path) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::[sync]DisallowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::DisallowSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  DisallowSnapshot(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

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

Options FileSystemImpl::get_options() {
  return options_;
}

}
