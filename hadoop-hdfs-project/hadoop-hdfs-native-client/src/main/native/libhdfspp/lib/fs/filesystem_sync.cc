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

#include <future>
#include <tuple>

#define FMT_THIS_ADDR "this=" << (void*)this

// Note: This is just a place to hold boilerplate async to sync shim code,
//       place actual filesystem logic in filesystem.cc
//
//
// Shim pattern pseudocode
//
// Status MySynchronizedMethod(method_args):
//  let stat = a promise<Status> wrapped in a shared_ptr
//
//  Create a lambda that captures stat and any other variables that need to
//  be set based on the async operation.  When invoked set variables with the
//  arguments passed (possibly do some translation), then set stat to indicate
//  the return status of the async call.
//
//  invoke MyAsyncMethod(method_args, handler_lambda)
//
//  block until stat value has been set while async work takes place
//
//  return stat

namespace hdfs {

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

Status FileSystemImpl::GetContentSummary(const std::string &path,
                                         ContentSummary & content_summary) {
  LOG_DEBUG(kFileSystem, << "FileSystemImpl::[sync]GetContentSummary("
                                 << FMT_THIS_ADDR << ", path="
                                 << path << ") called");

  auto callstate = std::make_shared<std::promise<std::tuple<Status, ContentSummary>>>();
  std::future<std::tuple<Status, ContentSummary>> future(callstate->get_future());

  /* wrap async FileSystem::GetContentSummary with promise to make it a blocking call */
  auto h = [callstate](const Status &s, const ContentSummary &si) {
    callstate->set_value(std::make_tuple(s, si));
  };

  GetContentSummary(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  ContentSummary cs = std::get<1>(returnstate);

  if (!stat.ok()) {
    return stat;
  }

  content_summary = cs;
  return stat;
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

Status FileSystemImpl::RenameSnapshot(const std::string &path,
    const std::string &old_name, const std::string &new_name) {
  LOG_DEBUG(kFileSystem,
    << "FileSystemImpl::[sync]RenameSnapshot(" << FMT_THIS_ADDR << ", path=" << path <<
    ", old_name=" << old_name << ", new_name=" << new_name << ") called");

  auto callstate = std::make_shared<std::promise<Status>>();
  std::future<Status> future(callstate->get_future());

  /* wrap async FileSystem::RenameSnapshot with promise to make it a blocking call */
  auto h = [callstate](const Status &s) {
    callstate->set_value(s);
  };

  RenameSnapshot(path, old_name, new_name, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = returnstate;

  return stat;
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

}
