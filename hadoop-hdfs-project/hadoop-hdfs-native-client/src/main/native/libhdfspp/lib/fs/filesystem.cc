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

#include "filehandle.h"
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

static const char kNamenodeProtocol[] = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
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

FileSystem::~FileSystem() {}

/*****************************************************************************
 *                    FILESYSTEM BASE CLASS
 ****************************************************************************/

FileSystem *FileSystem::New(
    IoService *&io_service, const std::string &user_name, const Options &options) {
  return new FileSystemImpl(io_service, user_name, options);
}

FileSystem *FileSystem::New(
    std::shared_ptr<IoService> io_service, const std::string &user_name, const Options &options) {
  return new FileSystemImpl(io_service, user_name, options);
}

FileSystem *FileSystem::New() {
  // No, this pointer won't be leaked.  The FileSystem takes ownership.
  std::shared_ptr<IoService> io_service = IoService::MakeShared();
  if(!io_service)
    return nullptr;
  int thread_count = io_service->InitDefaultWorkers();
  if(thread_count < 1)
    return nullptr;

  std::string user_name = get_effective_user_name("");
  Options options;
  return new FileSystemImpl(io_service, user_name, options);
}

/*****************************************************************************
 *                    FILESYSTEM IMPLEMENTATION
 ****************************************************************************/

struct FileSystemImpl::FindSharedState {
  //Name pattern (can have wild-cards) to find
  const std::string name;
  //Maximum depth to recurse after the end of path is reached.
  //Can be set to 0 for pure path globbing and ignoring name pattern entirely.
  const uint32_t maxdepth;
  //Vector of all sub-directories from the path argument (each can have wild-cards)
  std::vector<std::string> dirs;
  //Callback from Find
  const std::function<bool(const Status &, const std::vector<StatInfo> &, bool)> handler;
  //outstanding_requests is incremented once for every GetListing call.
  std::atomic<uint64_t> outstanding_requests;
  //Boolean needed to abort all recursion on error or on user command
  std::atomic<bool> aborted;
  //Shared variables will need protection with a lock
  std::mutex lock;
  FindSharedState(const std::string path_, const std::string name_, const uint32_t maxdepth_,
              const std::function<bool(const Status &, const std::vector<StatInfo> &, bool)> handler_,
              uint64_t outstanding_recuests_, bool aborted_)
      : name(name_),
        maxdepth(maxdepth_),
        handler(handler_),
        outstanding_requests(outstanding_recuests_),
        aborted(aborted_),
        lock() {
    //Constructing the list of sub-directories
    std::stringstream ss(path_);
    if(path_.back() != '/'){
      ss << "/";
    }
    for (std::string token; std::getline(ss, token, '/'); ) {
      dirs.push_back(token);
    }
  }
};

struct FileSystemImpl::FindOperationalState {
  const std::string path;
  const uint32_t depth;
  const bool search_path;
  FindOperationalState(const std::string path_, const uint32_t depth_, const bool search_path_)
      : path(path_),
        depth(depth_),
        search_path(search_path_) {
  }
};


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
     io_service_(io_service), options_(options),
     client_name_(GetRandomClientName()),
     nn_(
       io_service_, options, client_name_,
       get_effective_user_name(user_name), kNamenodeProtocol,
       kNamenodeProtocolVersion
     ),
     bad_node_tracker_(std::make_shared<BadDataNodeTracker>()),
     event_handlers_(std::make_shared<LibhdfsEvents>())
{

  LOG_DEBUG(kFileSystem, << "FileSystemImpl::FileSystemImpl("
                         << FMT_THIS_ADDR << ") called");

  // Poor man's move
  io_service = nullptr;

  unsigned int running_workers = 0;
  if(options.io_threads_ < 1) {
    LOG_DEBUG(kFileSystem, << "FileSystemImpl::FileSystemImpl Initializing default number of worker threads");
    running_workers = io_service_->InitDefaultWorkers();
  } else {
    LOG_DEBUG(kFileSystem, << "FileSystemImpl::FileSystenImpl Initializing " << options_.io_threads_ << " worker threads.");
    running_workers = io_service->InitWorkers(options_.io_threads_);
  }

  if(running_workers < 1) {
    LOG_WARN(kFileSystem, << "FileSystemImpl::FileSystemImpl was unable to start worker threads");
  }
}

FileSystemImpl::FileSystemImpl(std::shared_ptr<IoService> io_service, const std::string& user_name, const Options &options) :
     io_service_(io_service), options_(options),
     client_name_(GetRandomClientName()),
     nn_(
       io_service_, options, client_name_,
       get_effective_user_name(user_name), kNamenodeProtocol,
       kNamenodeProtocolVersion
     ),
     bad_node_tracker_(std::make_shared<BadDataNodeTracker>()),
     event_handlers_(std::make_shared<LibhdfsEvents>())
{
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::FileSystemImpl("
                         << FMT_THIS_ADDR << ", shared IoService@" << io_service_.get() << ") called");
  int worker_thread_count = io_service_->GetWorkerThreadCount();
  if(worker_thread_count < 1) {
    LOG_WARN(kFileSystem, << "FileSystemImpl::FileSystemImpl IoService provided doesn't have any worker threads. "
                          << "It needs at least 1 worker to connect to an HDFS cluster.")
  } else {
    LOG_DEBUG(kFileSystem, << "FileSystemImpl::FileSystemImpl using " << worker_thread_count << " worker threads.");
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
}

void FileSystemImpl::Connect(const std::string &server,
                             const std::string &service,
                             const std::function<void(const Status &, FileSystem * fs)> &handler) {
  LOG_INFO(kFileSystem, << "FileSystemImpl::Connect(" << FMT_THIS_ADDR
                        << ", server=" << server << ", service="
                        << service << ") called");
  connect_callback_.SetCallback(handler);

  /* IoService::New can return nullptr */
  if (!io_service_) {
    handler (Status::Error("Null IoService"), this);
  }

  // DNS lookup here for namenode(s)
  std::vector<ResolvedNamenodeInfo> resolved_namenodes;

  auto name_service = options_.services.find(server);
  if(name_service != options_.services.end()) {
    cluster_name_ = name_service->first;
    resolved_namenodes = BulkResolve(io_service_, name_service->second);
  } else {
    cluster_name_ = server + ":" + service;

    // tmp namenode info just to get this in the right format for BulkResolve
    NamenodeInfo tmp_info;
    try {
      tmp_info.uri = URI::parse_from_string("hdfs://" + cluster_name_);
    } catch (const uri_parse_error& e) {
      LOG_ERROR(kFileSystem, << "Unable to use URI for cluster " << cluster_name_);
      handler(Status::Error(("Invalid namenode " + cluster_name_ + " in config").c_str()), this);
    }

    resolved_namenodes = BulkResolve(io_service_, {tmp_info});
  }

  for(unsigned int i=0;i<resolved_namenodes.size();i++) {
    LOG_DEBUG(kFileSystem, << "Resolved Namenode");
    LOG_DEBUG(kFileSystem, << resolved_namenodes[i].str());
  }


  nn_.Connect(cluster_name_, /*server, service*/ resolved_namenodes, [this](const Status & s) {
    connect_callback_.GetCallback()(s, this);
  });
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

  int16_t port = options_.defaultFS.get_port_or_default(kDefaultPort);
  std::string port_as_string = std::to_string(port);

  Connect(host, port_as_string, handler);
}

int FileSystemImpl::AddWorkerThread() {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::AddWorkerThread("
                                  << FMT_THIS_ADDR << ") called."
                                  << " Existing thread count = " << WorkerThreadCount());

  if(!io_service_)
    return -1;

  io_service_->AddWorkerThread();
  return 1;
}

int FileSystemImpl::WorkerThreadCount() {
  if(!io_service_) {
    return -1;
  } else {
    return io_service_->GetWorkerThreadCount();
  }
}

bool FileSystemImpl::CancelPendingConnect() {
  if(connect_callback_.IsCallbackAccessed()) {
    // Temp fix for failover hangs, allow CancelPendingConnect to be called so it can push a flag through the RPC engine
    LOG_DEBUG(kFileSystem, << "FileSystemImpl@" << this << "::CancelPendingConnect called after Connect completed");
    return nn_.CancelPendingConnect();
  }

  if(!connect_callback_.IsCallbackSet()) {
    LOG_DEBUG(kFileSystem, << "FileSystemImpl@" << this << "::CancelPendingConnect called before Connect started");
    return false;
  }

  // First invoke callback, then do proper teardown in RpcEngine and RpcConnection
  ConnectCallback noop_callback = [](const Status &stat, FileSystem *fs) {
    LOG_DEBUG(kFileSystem, << "Dummy callback invoked for canceled FileSystem@" << fs << "::Connect with status: " << stat.ToString());
  };

  bool callback_swapped = false;
  ConnectCallback original_callback = connect_callback_.AtomicSwapCallback(noop_callback, callback_swapped);

  if(callback_swapped) {
    // Take original callback and invoke it as if it was canceled.
    LOG_DEBUG(kFileSystem, << "Swapped in dummy callback.  Invoking connect callback with canceled status.");
    std::function<void(void)> wrapped_callback = [original_callback, this](){
      // handling code expected to check status before dereferenceing 'this'
      original_callback(Status::Canceled(), this);
    };
    io_service_->PostTask(wrapped_callback);
  } else {
    LOG_INFO(kFileSystem, << "Unable to cancel FileSystem::Connect.  It hasn't been invoked yet or may have already completed.")
    return false;
  }

  // Now push cancel down to clean up where possible and make sure the RpcEngine
  // won't try to do retries in the background.  The rest of the memory cleanup
  // happens when this FileSystem is deleted by the user.
  return nn_.CancelPendingConnect();
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
    handler(stat, stat.ok() ? new FileHandleImpl(cluster_name_, path, io_service_, client_name_, file_info, bad_node_tracker_, event_handlers_)
                            : nullptr);
  });
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
    if (datanode_info.has_location())
      newInfo.setNetworkLocation(datanode_info.location());
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

void FileSystemImpl::GetPreferredBlockSize(const std::string &path,
    const std::function<void(const Status &, const uint64_t &)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetPreferredBlockSize("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetPreferredBlockSize(path, handler);
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


void FileSystemImpl::GetFileInfo(
    const std::string &path,
    const std::function<void(const Status &, const StatInfo &)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetFileInfo("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetFileInfo(path, handler);
}

void FileSystemImpl::GetContentSummary(
    const std::string &path,
    const std::function<void(const Status &, const ContentSummary &)> &handler) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::GetContentSummary("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  nn_.GetContentSummary(path, handler);
}

void FileSystemImpl::GetFsStats(
    const std::function<void(const Status &, const FsInfo &)> &handler) {
  LOG_DEBUG(kFileSystem,
      << "FileSystemImpl::GetFsStats(" << FMT_THIS_ADDR << ") called");

  nn_.GetFsStats(handler);
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
  std::string path_fixed = path;
  if(path.back() != '/'){
    path_fixed += "/";
  }
  // Caputure the state and push it into the shim
  auto callback = [this, path_fixed, handler](const Status &stat, const std::vector<StatInfo> & stat_infos, bool has_more) {
    GetListingShim(stat, stat_infos, has_more, path_fixed, handler);
  };

  nn_.GetListing(path_fixed, callback);
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

void FileSystemImpl::RenameSnapshot(const std::string &path,
    const std::string &old_name, const std::string &new_name,
    const std::function<void(const Status &)> &handler) {
  LOG_DEBUG(kFileSystem,
    << "FileSystemImpl::RenameSnapshot(" << FMT_THIS_ADDR << ", path=" << path <<
    ", old_name=" << old_name << ", new_name=" << new_name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("RenameSnapshot: argument 'path' cannot be empty"));
    return;
  }
  if (old_name.empty()) {
    handler(Status::InvalidArgument("RenameSnapshot: argument 'old_name' cannot be empty"));
    return;
  }
  if (new_name.empty()) {
    handler(Status::InvalidArgument("RenameSnapshot: argument 'new_name' cannot be empty"));
    return;
  }

  nn_.RenameSnapshot(path, old_name, new_name, handler);
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

std::string FileSystemImpl::get_cluster_name() {
  return cluster_name_;
}

}
