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
#ifndef LIBHDFSPP_LIB_FS_FILESYSTEM_H_
#define LIBHDFSPP_LIB_FS_FILESYSTEM_H_

#include "common/hdfs_public_api.h"
#include "libhdfspp/hdfs.h"
#include "fs/bad_datanode_tracker.h"
#include "rpc/rpc_engine.h"
#include "ClientNamenodeProtocol.pb.h"
#include "ClientNamenodeProtocol.hrpc.inl"

namespace hdfs {

class FileHandle;
class HadoopFileSystem;

class FileSystemImpl : public FileSystem {
 public:
  FileSystemImpl(IoService *io_service, const Options &options);
  void Connect(const std::string &server, const std::string &service,
               std::function<void(const Status &)> &&handler);
  virtual void Open(const std::string &path,
                    const std::function<void(const Status &, InputStream *)> &
                        handler) override;
  RpcEngine &rpc_engine() { return engine_; }

 private:
  IoServiceImpl *io_service_;
  RpcEngine engine_;
  ClientNamenodeProtocol namenode_;
  std::shared_ptr<BadDataNodeTracker> bad_node_tracker_;
};

class InputStreamImpl : public InputStream {
 public:
  InputStreamImpl(FileSystemImpl *fs,
                  const ::hadoop::hdfs::LocatedBlocksProto *blocks,
                  std::shared_ptr<BadDataNodeTracker> tracker);
  virtual void PositionRead(
      void *buf, size_t nbyte, uint64_t offset,
      const std::function<void(const Status &, const std::string &, size_t)> &
          handler) override;
  /**
   * If optional_rule_override is null then use the bad_datanode_tracker.  If
   * non-null use the provided NodeExclusionRule to determine eligible
   * datanodes.
   **/
  template <class MutableBufferSequence, class Handler>
  void AsyncPreadSome(size_t offset, const MutableBufferSequence &buffers,
                      std::shared_ptr<NodeExclusionRule> excluded_nodes,
                      const Handler &handler);

  template <class BlockReaderTrait, class MutableBufferSequence, class Handler>
  void AsyncReadBlock(const std::string &client_name,
                      const hadoop::hdfs::LocatedBlockProto &block,
                      const hadoop::hdfs::DatanodeInfoProto &dn, size_t offset,
                      const MutableBufferSequence &buffers,
                      const Handler &handler);
  uint64_t get_file_length() const;
 private:
  FileSystemImpl *fs_;
  unsigned long long file_length_;
  std::vector<::hadoop::hdfs::LocatedBlockProto> blocks_;
  template <class Reader>
  struct HandshakeContinuation;
  template <class Reader, class MutableBufferSequence>
  struct ReadBlockContinuation;
  struct RemoteBlockReaderTrait;
  friend class FileHandle;
  std::shared_ptr<BadDataNodeTracker> bad_node_tracker_;
};
}

#include "inputstream_impl.h"

#endif
