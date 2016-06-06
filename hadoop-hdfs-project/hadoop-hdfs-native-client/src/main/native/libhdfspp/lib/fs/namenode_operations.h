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
#ifndef LIBHDFSPP_LIB_FS_NAMENODEOPERATIONS_H_
#define LIBHDFSPP_LIB_FS_NAMENODEOPERATIONS_H_

#include "rpc/rpc_engine.h"
#include "hdfspp/statinfo.h"
#include "ClientNamenodeProtocol.pb.h"
#include "ClientNamenodeProtocol.hrpc.inl"


namespace hdfs {

/**
* NameNodeConnection: abstracts the details of communicating with a NameNode
* and the implementation of the communications protocol.
*
* Will eventually handle retry and failover.
*
* Threading model: thread-safe; all operations can be called concurrently
* Lifetime: owned by a FileSystemImpl
*/
class NameNodeOperations {
public:
  MEMCHECKED_CLASS(NameNodeOperations);
  NameNodeOperations(::asio::io_service *io_service, const Options &options,
            const std::string &client_name, const std::string &user_name,
            const char *protocol_name, int protocol_version) :
  io_service_(io_service),
  engine_(io_service, options, client_name, user_name, protocol_name, protocol_version),
  namenode_(& engine_) {}

  void Connect(const std::string &cluster_name,
               const std::string &server,
               const std::string &service,
               std::function<void(const Status &)> &&handler);

  void GetBlockLocations(const std::string & path,
    std::function<void(const Status &, std::shared_ptr<const struct FileInfo>)> handler);

  void GetFileInfo(const std::string & path,
      std::function<void(const Status &, const StatInfo &)> handler);

  // start_after="" for initial call
  void GetListing(const std::string & path,
        std::function<void(const Status &, std::shared_ptr<std::vector<StatInfo>>&, bool)> handler,
        const std::string & start_after = "");

  void SetFsEventCallback(fs_event_callback callback);

private:
  static void HdfsFileStatusProtoToStatInfo(hdfs::StatInfo & si, const ::hadoop::hdfs::HdfsFileStatusProto & fs);
  static void DirectoryListingProtoToStatInfo(std::shared_ptr<std::vector<StatInfo>> stat_infos, const ::hadoop::hdfs::DirectoryListingProto & dl);

  ::asio::io_service * io_service_;
  RpcEngine engine_;
  ClientNamenodeProtocol namenode_;
};
}

#endif
