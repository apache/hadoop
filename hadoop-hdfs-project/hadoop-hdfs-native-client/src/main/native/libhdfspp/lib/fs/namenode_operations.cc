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
#include <utility>

#define FMT_THIS_ADDR "this=" << (void*)this

using ::asio::ip::tcp;

namespace hdfs {

/*****************************************************************************
 *                    NAMENODE OPERATIONS
 ****************************************************************************/

void NameNodeOperations::Connect(const std::string &cluster_name,
                                 const std::vector<ResolvedNamenodeInfo> &servers,
                                 std::function<void(const Status &)> &&handler) {
  engine_->Connect(cluster_name, servers, handler);
}

bool NameNodeOperations::CancelPendingConnect() {
  return engine_->CancelPendingConnect();
}

void NameNodeOperations::GetBlockLocations(const std::string & path, uint64_t offset, uint64_t length,
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

  GetBlockLocationsRequestProto req;
  req.set_src(path);
  req.set_offset(offset);
  req.set_length(length);

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

void NameNodeOperations::GetPreferredBlockSize(const std::string & path,
  std::function<void(const Status &, const uint64_t)> handler)
{
  using ::hadoop::hdfs::GetPreferredBlockSizeRequestProto;
  using ::hadoop::hdfs::GetPreferredBlockSizeResponseProto;

  LOG_TRACE(kFileSystem, << "NameNodeOperations::GetPreferredBlockSize("
                           << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("GetPreferredBlockSize: argument 'path' cannot be empty"), -1);
    return;
  }

  GetPreferredBlockSizeRequestProto req;
  req.set_filename(path);

  auto resp = std::make_shared<GetPreferredBlockSizeResponseProto>();

  namenode_.GetPreferredBlockSize(&req, resp, [resp, handler, path](const Status &stat) {
    if (stat.ok() && resp -> has_bsize()) {
      uint64_t block_size = resp -> bsize();
      handler(stat, block_size);
    } else {
      handler(stat, -1);
    }
  });
}

void NameNodeOperations::SetReplication(const std::string & path, int16_t replication,
  std::function<void(const Status &)> handler)
{
  using ::hadoop::hdfs::SetReplicationRequestProto;
  using ::hadoop::hdfs::SetReplicationResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::SetReplication(" << FMT_THIS_ADDR << ", path=" << path <<
      ", replication=" << replication << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetReplication: argument 'path' cannot be empty"));
    return;
  }
  Status replStatus = FileSystemImpl::CheckValidReplication(replication);
  if (!replStatus.ok()) {
    handler(replStatus);
    return;
  }
  SetReplicationRequestProto req;
  req.set_src(path);
  req.set_replication(replication);

  auto resp = std::make_shared<SetReplicationResponseProto>();

  namenode_.SetReplication(&req, resp, [resp, handler, path](const Status &stat) {
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

void NameNodeOperations::SetTimes(const std::string & path, uint64_t mtime, uint64_t atime,
  std::function<void(const Status &)> handler)
{
  using ::hadoop::hdfs::SetTimesRequestProto;
  using ::hadoop::hdfs::SetTimesResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::SetTimes(" << FMT_THIS_ADDR << ", path=" << path <<
      ", mtime=" << mtime << ", atime=" << atime << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetTimes: argument 'path' cannot be empty"));
    return;
  }

  SetTimesRequestProto req;
  req.set_src(path);
  req.set_mtime(mtime);
  req.set_atime(atime);

  auto resp = std::make_shared<SetTimesResponseProto>();

  namenode_.SetTimes(&req, resp, [resp, handler, path](const Status &stat) {
      handler(stat);
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
          stat_info.path = path;
          stat_info.full_path = path;
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

void NameNodeOperations::GetContentSummary(const std::string & path,
  std::function<void(const Status &, const ContentSummary &)> handler)
{
  using ::hadoop::hdfs::GetContentSummaryRequestProto;
  using ::hadoop::hdfs::GetContentSummaryResponseProto;

  LOG_TRACE(kFileSystem, << "NameNodeOperations::GetContentSummary("
                           << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("GetContentSummary: argument 'path' cannot be empty"), ContentSummary());
    return;
  }

  GetContentSummaryRequestProto req;
  req.set_path(path);

  auto resp = std::make_shared<GetContentSummaryResponseProto>();

  namenode_.GetContentSummary(&req, resp, [resp, handler, path](const Status &stat) {
    if (stat.ok()) {
      // For non-existant files, the server will respond with an OK message but
      //   no summary in the protobuf.
      if(resp -> has_summary()){
          struct ContentSummary content_summary;
          content_summary.path = path;
          ContentSummaryProtoToContentSummary(content_summary, resp->summary());
          handler(stat, content_summary);
        } else {
          std::string errormsg = "No such file or directory: " + path;
          Status statNew = Status::PathNotFound(errormsg.c_str());
          handler(statNew, ContentSummary());
        }
    } else {
      handler(stat, ContentSummary());
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
    std::function<void(const Status &, const std::vector<StatInfo> &, bool)> handler,
    const std::string & start_after) {
  using ::hadoop::hdfs::GetListingRequestProto;
  using ::hadoop::hdfs::GetListingResponseProto;

  LOG_TRACE(
      kFileSystem,
      << "NameNodeOperations::GetListing(" << FMT_THIS_ADDR << ", path=" << path << ") called");

  if (path.empty()) {
    std::vector<StatInfo> empty;
    handler(Status::InvalidArgument("GetListing: argument 'path' cannot be empty"), empty, false);
    return;
  }

  GetListingRequestProto req;
  req.set_src(path);
  req.set_startafter(start_after.c_str());
  req.set_needlocation(false);

  auto resp = std::make_shared<GetListingResponseProto>();

  namenode_.GetListing(&req, resp, [resp, handler, path](const Status &stat) {
    std::vector<StatInfo> stat_infos;
    if (stat.ok()) {
      if(resp -> has_dirlist()){
        for (::hadoop::hdfs::HdfsFileStatusProto const& fs : resp->dirlist().partiallisting()) {
          StatInfo si;
          si.path = fs.path();
          si.full_path = path + fs.path();
          if(si.full_path.back() != '/'){
            si.full_path += "/";
          }
          HdfsFileStatusProtoToStatInfo(si, fs);
          stat_infos.push_back(si);
        }
        handler(stat, stat_infos, resp->dirlist().remainingentries() > 0);
      } else {
        std::string errormsg = "No such file or directory: " + path;
        handler(Status::PathNotFound(errormsg.c_str()), stat_infos, false);
      }
    } else {
      handler(stat, stat_infos, false);
    }
  });
}

void NameNodeOperations::Mkdirs(const std::string & path, uint16_t permissions, bool createparent,
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
  Status permStatus = FileSystemImpl::CheckValidPermissionMask(permissions);
  if (!permStatus.ok()) {
    handler(permStatus);
    return;
  }
  req.set_src(path);
  hadoop::hdfs::FsPermissionProto *perm = req.mutable_masked();
  perm->set_perm(permissions);
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
    uint16_t permissions, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::SetPermissionRequestProto;
  using ::hadoop::hdfs::SetPermissionResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::SetPermission(" << FMT_THIS_ADDR << ", path=" << path << ", permissions=" << permissions << ") called");

  if (path.empty()) {
    handler(Status::InvalidArgument("SetPermission: argument 'path' cannot be empty"));
    return;
  }
  Status permStatus = FileSystemImpl::CheckValidPermissionMask(permissions);
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

void NameNodeOperations::RenameSnapshot(const std::string & path, const std::string & old_name,
    const std::string & new_name, std::function<void(const Status &)> handler) {
  using ::hadoop::hdfs::RenameSnapshotRequestProto;
  using ::hadoop::hdfs::RenameSnapshotResponseProto;

  LOG_TRACE(kFileSystem,
      << "NameNodeOperations::RenameSnapshot(" << FMT_THIS_ADDR << ", path=" << path <<
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

  RenameSnapshotRequestProto req;
  req.set_snapshotroot(path);
  req.set_snapshotoldname(old_name);
  req.set_snapshotnewname(new_name);

  auto resp = std::make_shared<RenameSnapshotResponseProto>();

  namenode_.RenameSnapshot(&req, resp,
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
  engine_->SetFsEventCallback(callback);
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

void NameNodeOperations::ContentSummaryProtoToContentSummary(
    hdfs::ContentSummary & content_summary,
    const ::hadoop::hdfs::ContentSummaryProto & csp) {
  content_summary.length = csp.length();
  content_summary.filecount = csp.filecount();
  content_summary.directorycount = csp.directorycount();
  content_summary.quota = csp.quota();
  content_summary.spaceconsumed = csp.spaceconsumed();
  content_summary.spacequota = csp.spacequota();
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
