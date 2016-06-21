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

#include <asio/ip/tcp.hpp>

#include <functional>
#include <limits>
#include <future>
#include <tuple>
#include <iostream>
#include <pwd.h>

#define FMT_THIS_ADDR "this=" << (void*)this

using ::asio::ip::tcp;

namespace hdfs {

/*****************************************************************************
 *                    NAMENODE OPERATIONS
 ****************************************************************************/

Status NameNodeOperations::CheckValidPermissionMask(short permissions) {
  if (permissions < 0 || permissions > 01777) {
    std::stringstream errormsg;
    errormsg << "IsValidPermissionMask: argument 'permissions' is " << std::oct
        << std::showbase << permissions << " (should be between 0 and 01777)";
    //Avoid copying by binding errormsg.str() to a const reference, which extends its lifetime
    const std::string& tmp = errormsg.str();
    return Status::InvalidArgument(tmp.c_str());
  }
  return Status::OK();
}

void NameNodeOperations::Connect(const std::string &cluster_name,
                                 const std::string &server,
                             const std::string &service,
                             std::function<void(const Status &)> &&handler) {
  using namespace asio_continuation;
  typedef std::vector<tcp::endpoint> State;
  auto m = Pipeline<State>::Create();
  m->Push(Resolve(io_service_, server, service,
                  std::back_inserter(m->state())))
      .Push(Bind([this, m, cluster_name](const Continuation::Next &next) {
        engine_.Connect(cluster_name, m->state(), next);
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

  LOG_TRACE(kFileSystem, << "NameNodeOperations::GetBlockLocations("
                           << FMT_THIS_ADDR << ", path=" << path << ", ...) called");

  if (path.empty()) {
    handler(Status::InvalidArgument("GetBlockLocations: argument 'path' cannot be empty"), nullptr);
    return;
  }

  GetBlockLocationsRequestProto req;
  req.set_src(path);
  req.set_offset(0);
  req.set_length(std::numeric_limits<long long>::max());

  auto resp = std::make_shared<GetBlockLocationsResponseProto>();

  namenode_.GetBlockLocations(&req, resp, [resp, handler](const Status &stat) {
    if (stat.ok()) {
      auto file_info = std::make_shared<struct FileInfo>();
      auto locations = resp->locations();

      file_info->file_length_ = locations.filelength();
      file_info->last_block_complete_ = locations.islastblockcomplete();
      file_info->under_construction_ = locations.underconstruction();

      for (const auto &block : locations.blocks()) {
        file_info->blocks_.push_back(block);
      }

      if (!locations.islastblockcomplete() &&
          locations.has_lastblock() && locations.lastblock().b().numbytes()) {
        file_info->blocks_.push_back(locations.lastblock());
        file_info->file_length_ += locations.lastblock().b().numbytes();
      }

      handler(stat, file_info);
    } else {
      handler(stat, nullptr);
    }
  });
}

void NameNodeOperations::GetFileInfo(const std::string & path,
  std::function<void(const Status &, const StatInfo &)> handler)
{
  using ::hadoop::hdfs::GetFileInfoRequestProto;
  using ::hadoop::hdfs::GetFileInfoResponseProto;

  LOG_TRACE(kFileSystem, << "NameNodeOperations::GetFileInfo("
                           << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("GetFileInfo: argument 'path' cannot be empty"), StatInfo());
    return;
  }

  GetFileInfoRequestProto req;
  req.set_src(path);

  auto resp = std::make_shared<GetFileInfoResponseProto>();

  namenode_.GetFileInfo(&req, resp, [resp, handler, path](const Status &stat) {
    if (stat.ok()) {
      // For non-existant files, the server will respond with an OK message but
      //   no fs in the protobuf.
      if(resp -> has_fs()){
          struct StatInfo stat_info;
          stat_info.path=path;
          HdfsFileStatusProtoToStatInfo(stat_info, resp->fs());
          handler(stat, stat_info);
        } else {
          std::string errormsg = "No such file or directory: " + path;
          Status statNew = Status::PathNotFound(errormsg.c_str());
          handler(statNew, StatInfo());
        }
    } else {
      handler(stat, StatInfo());
    }
  });
}

void NameNodeOperations::GetFsStats(
    std::function<void(const Status &, const FsInfo &)> handler) {
  using ::hadoop::hdfs::GetFsStatusRequestProto;
  using ::hadoop::hdfs::GetFsStatsResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::GetFsStats(" << FMT_THIS_ADDR << ") called");

  GetFsStatusRequestProto req;
  auto resp = std::make_shared<GetFsStatsResponseProto>();

  namenode_.GetFsStats(&req, resp, [resp, handler](const Status &stat) {
    if (stat.ok()) {
      struct FsInfo fs_info;
      GetFsStatsResponseProtoToFsInfo(fs_info, resp);
      handler(stat, fs_info);
    } else {
      handler(stat, FsInfo());
    }
  });
}

void NameNodeOperations::GetListing(
    const std::string & path,
    std::function<void(const Status &, std::shared_ptr<std::vector<StatInfo>> &, bool)> handler,
    const std::string & start_after) {
  using ::hadoop::hdfs::GetListingRequestProto;
  using ::hadoop::hdfs::GetListingResponseProto;

  LOG_TRACE(
      kFileSystem,
      << "NameNodeOperations::GetListing(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    std::shared_ptr<std::vector<StatInfo>> stat_infos;
    handler(Status::InvalidArgument("GetListing: argument 'path' cannot be empty"), stat_infos, false);
    return;
  }

  GetListingRequestProto req;
  req.set_src(path);
  req.set_startafter(start_after.c_str());
  req.set_needlocation(false);

  auto resp = std::make_shared<GetListingResponseProto>();

  namenode_.GetListing(
      &req,
      resp,
      [resp, handler, path](const Status &stat) {
        if (stat.ok()) {
          if(resp -> has_dirlist()){
            std::shared_ptr<std::vector<StatInfo>> stat_infos(new std::vector<StatInfo>);
            for (::hadoop::hdfs::HdfsFileStatusProto const& fs : resp->dirlist().partiallisting()) {
              StatInfo si;
              si.path=fs.path();
              HdfsFileStatusProtoToStatInfo(si, fs);
              stat_infos->push_back(si);
            }
            handler(stat, stat_infos, resp->dirlist().remainingentries() > 0);
          } else {
            std::string errormsg = "No such file or directory: " + path;
            Status statNew = Status::PathNotFound(errormsg.c_str());
            std::shared_ptr<std::vector<StatInfo>> stat_infos;
            handler(statNew, stat_infos, false);
          }
        } else {
          std::shared_ptr<std::vector<StatInfo>> stat_infos;
          handler(stat, stat_infos, false);
        }
      });
}

void NameNodeOperations::Mkdirs(const std::string & path, long permissions, bool createparent,
  std::function<void(const Status &)> handler)
{
  using ::hadoop::hdfs::MkdirsRequestProto;
  using ::hadoop::hdfs::MkdirsResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::Mkdirs(" << FMT_THIS_ADDR << ", path=" << path <<
      ", permissions=" << permissions << ", createparent=" << createparent << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Mkdirs: argument 'path' cannot be empty"));
    return;
  }

  MkdirsRequestProto req;
  req.set_src(path);
  hadoop::hdfs::FsPermissionProto *perm = req.mutable_masked();
  if (permissions < 0) {
    perm->set_perm(0755);
  } else {
    perm->set_perm(permissions);
  }
  req.set_createparent(createparent);

  auto resp = std::make_shared<MkdirsResponseProto>();

  namenode_.Mkdirs(&req, resp, [resp, handler, path](const Status &stat) {
    if (stat.ok()) {
      // Checking resp
      if(resp -> has_result() && resp ->result() == 1) {
        handler(stat);
      } else {
        //NameNode does not specify why there is no result, in my testing it was happening when the path is not found
        std::string errormsg = "No such file or directory: " + path;
        Status statNew = Status::PathNotFound(errormsg.c_str());
        handler(statNew);
      }
    } else {
      handler(stat);
    }
  });
}

void NameNodeOperations::Delete(const std::string & path, bool recursive, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::DeleteRequestProto;
  using ::hadoop::hdfs::DeleteResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::Delete(" << FMT_THIS_ADDR << ", path=" << path << ", recursive=" << recursive << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("Delete: argument 'path' cannot be empty"));
    return;
  }

  DeleteRequestProto req;
  req.set_src(path);
  req.set_recursive(recursive);

  auto resp = std::make_shared<DeleteResponseProto>();

  namenode_.Delete(&req, resp, [resp, handler, path](const Status &stat) {
    if (stat.ok()) {
      // Checking resp
      if(resp -> has_result() && resp ->result() == 1) {
        handler(stat);
      } else {
        //NameNode does not specify why there is no result, in my testing it was happening when the path is not found
        std::string errormsg = "No such file or directory: " + path;
        Status statNew = Status::PathNotFound(errormsg.c_str());
        handler(statNew);
      }
    } else {
      handler(stat);
    }
  });
}

void NameNodeOperations::Rename(const std::string & oldPath, const std::string & newPath, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::RenameRequestProto;
  using ::hadoop::hdfs::RenameResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::Rename(" << FMT_THIS_ADDR << ", oldPath=" << oldPath << ", newPath=" << newPath << ") called");

  if (oldPath.empty()) {
    handler(Status::InvalidArgument("Rename: argument 'oldPath' cannot be empty"));
    return;
  }

  if (newPath.empty()) {
    handler(Status::InvalidArgument("Rename: argument 'newPath' cannot be empty"));
    return;
  }

  RenameRequestProto req;
  req.set_src(oldPath);
  req.set_dst(newPath);

  auto resp = std::make_shared<RenameResponseProto>();

  namenode_.Rename(&req, resp, [resp, handler](const Status &stat) {
    if (stat.ok()) {
      // Checking resp
      if(resp -> has_result() && resp ->result() == 1) {
        handler(stat);
      } else {
        //Since NameNode does not specify why the result is not success, we set the general error
        std::string errormsg = "oldPath and parent directory of newPath must exist. newPath must not exist.";
        Status statNew = Status::InvalidArgument(errormsg.c_str());
        handler(statNew);
      }
    } else {
      handler(stat);
    }
  });
}

void NameNodeOperations::SetPermission(const std::string & path,
    short permissions, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::SetPermissionRequestProto;
  using ::hadoop::hdfs::SetPermissionResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::SetPermission(" << FMT_THIS_ADDR << ", path=" << path << ", permissions=" << permissions << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetPermission: argument 'path' cannot be empty"));
    return;
  }
  Status permStatus = CheckValidPermissionMask(permissions);
  if (!permStatus.ok()) {
    handler(permStatus);
    return;
  }

  SetPermissionRequestProto req;
  req.set_src(path);

  hadoop::hdfs::FsPermissionProto *perm = req.mutable_permission();
  perm->set_perm(permissions);

  auto resp = std::make_shared<SetPermissionResponseProto>();

  namenode_.SetPermission(&req, resp,
      [handler](const Status &stat) {
        handler(stat);
      });
}

void NameNodeOperations::SetOwner(const std::string & path,
    const std::string & username, const std::string & groupname, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::SetOwnerRequestProto;
  using ::hadoop::hdfs::SetOwnerResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::SetOwner(" << FMT_THIS_ADDR << ", path=" << path << ", username=" << username << ", groupname=" << groupname << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetOwner: argument 'path' cannot be empty"));
    return;
  }

  SetOwnerRequestProto req;
  req.set_src(path);

  if(!username.empty()) {
    req.set_username(username);
  }
  if(!groupname.empty()) {
    req.set_groupname(groupname);
  }

  auto resp = std::make_shared<SetOwnerResponseProto>();

  namenode_.SetOwner(&req, resp,
      [handler](const Status &stat) {
        handler(stat);
      });
}

void NameNodeOperations::CreateSnapshot(const std::string & path,
    const std::string & name, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::CreateSnapshotRequestProto;
  using ::hadoop::hdfs::CreateSnapshotResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::CreateSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("CreateSnapshot: argument 'path' cannot be empty"));
    return;
  }

  CreateSnapshotRequestProto req;
  req.set_snapshotroot(path);
  if (!name.empty()) {
    req.set_snapshotname(name);
  }

  auto resp = std::make_shared<CreateSnapshotResponseProto>();

  namenode_.CreateSnapshot(&req, resp,
      [handler](const Status &stat) {
        handler(stat);
      });
}

void NameNodeOperations::DeleteSnapshot(const std::string & path,
    const std::string & name, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::DeleteSnapshotRequestProto;
  using ::hadoop::hdfs::DeleteSnapshotResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::DeleteSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ", name=" << name << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("DeleteSnapshot: argument 'path' cannot be empty"));
    return;
  }
  if (name.empty()) {
    handler(Status::InvalidArgument("DeleteSnapshot: argument 'name' cannot be empty"));
    return;
  }

  DeleteSnapshotRequestProto req;
  req.set_snapshotroot(path);
  req.set_snapshotname(name);

  auto resp = std::make_shared<DeleteSnapshotResponseProto>();

  namenode_.DeleteSnapshot(&req, resp,
      [handler](const Status &stat) {
        handler(stat);
      });
}

void NameNodeOperations::AllowSnapshot(const std::string & path, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::AllowSnapshotRequestProto;
  using ::hadoop::hdfs::AllowSnapshotResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::AllowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("AllowSnapshot: argument 'path' cannot be empty"));
    return;
  }

  AllowSnapshotRequestProto req;
  req.set_snapshotroot(path);

  auto resp = std::make_shared<AllowSnapshotResponseProto>();

  namenode_.AllowSnapshot(&req, resp,
      [handler](const Status &stat) {
        handler(stat);
      });
}

void NameNodeOperations::DisallowSnapshot(const std::string & path, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::DisallowSnapshotRequestProto;
  using ::hadoop::hdfs::DisallowSnapshotResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::DisallowSnapshot(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("DisallowSnapshot: argument 'path' cannot be empty"));
    return;
  }

  DisallowSnapshotRequestProto req;
  req.set_snapshotroot(path);

  auto resp = std::make_shared<DisallowSnapshotResponseProto>();

  namenode_.DisallowSnapshot(&req, resp,
      [handler](const Status &stat) {
        handler(stat);
      });
}

void NameNodeOperations::SetFsEventCallback(fs_event_callback callback) {
  engine_.SetFsEventCallback(callback);
}

void NameNodeOperations::HdfsFileStatusProtoToStatInfo(
    hdfs::StatInfo & stat_info,
    const ::hadoop::hdfs::HdfsFileStatusProto & fs) {
  stat_info.file_type = fs.filetype();
  stat_info.length = fs.length();
  stat_info.permissions = fs.permission().perm();
  stat_info.owner = fs.owner();
  stat_info.group = fs.group();
  stat_info.modification_time = fs.modification_time();
  stat_info.access_time = fs.access_time();
  stat_info.symlink = fs.symlink();
  stat_info.block_replication = fs.block_replication();
  stat_info.blocksize = fs.blocksize();
  stat_info.fileid = fs.fileid();
  stat_info.children_num = fs.childrennum();
}

void NameNodeOperations::GetFsStatsResponseProtoToFsInfo(
    hdfs::FsInfo & fs_info,
    const std::shared_ptr<::hadoop::hdfs::GetFsStatsResponseProto> & fs) {
  fs_info.capacity = fs->capacity();
  fs_info.used = fs->used();
  fs_info.remaining = fs->remaining();
  fs_info.under_replicated = fs->under_replicated();
  fs_info.corrupt_blocks = fs->corrupt_blocks();
  fs_info.missing_blocks = fs->missing_blocks();
  fs_info.missing_repl_one_blocks = fs->missing_repl_one_blocks();
  if(fs->has_blocks_in_future()){
    fs_info.blocks_in_future = fs->blocks_in_future();
  }
}

}
