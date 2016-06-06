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

void NameNodeOperations::GetListing(
    const std::string & path,
    std::function<void(const Status &, std::shared_ptr<std::vector<StatInfo>> &, bool)> handler,
    const std::string & start_after) {
  using ::hadoop::hdfs::GetListingRequestProto;
  using ::hadoop::hdfs::GetListingResponseProto;

  LOG_TRACE(
      kFileSystem,
      << "NameNodeOperations::GetListing(" << FMT_THIS_ADDR << ", path=" << path << ") called");

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

}
