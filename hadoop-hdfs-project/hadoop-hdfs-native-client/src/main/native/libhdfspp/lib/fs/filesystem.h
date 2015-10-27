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
#include "rpc/rpc_engine.h"
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
 */
class NameNodeOperations {
public:
  NameNodeOperations(::asio::io_service *io_service, const Options &options,
            const std::string &client_name, const char *protocol_name,
            int protocol_version) :
  io_service_(io_service),
  engine_(io_service, options, client_name, protocol_name, protocol_version),
  namenode_(& engine_) {}

  void Connect(const std::string &server,
               const std::string &service,
               std::function<void(const Status &)> &handler);

  void GetBlockLocations(const std::string & path, std::function<void(const Status &, const ::hadoop::hdfs::LocatedBlocksProto*)> handler);
private:
  ::asio::io_service * io_service_;
  RpcEngine engine_;
  ClientNamenodeProtocol namenode_;
};

/*
 * FileSystem: The consumer's main point of interaction with the cluster as
 * a whole.
 *
 * Initially constructed in a disconnected state; call Connect before operating
 * on the FileSystem.
 *
 * All open files must be closed before the FileSystem is destroyed.
 *
 * Threading model: thread-safe for all operations
 */
class FileSystemImpl : public FileSystem {
public:
  FileSystemImpl(IoService *io_service, const Options &options);
  void Connect(const std::string &server, const std::string &service,
               std::function<void(const Status &)> &&handler);
  virtual void Open(const std::string &path,
                    const std::function<void(const Status &, InputStream *)>
                        &handler) override;
private:
  IoServiceImpl *io_service_;
  const std::string client_name_;
  NameNodeOperations nn_;
};

/*
 * InputStreamImpl: connects to DNs, does one-shot reads.
 *
 * This will eventually be split into DNConnection and ReadOperation classes
 *
 * Threading model: not thread-safe; consumers and io_service should not call
 *    concurrently
 */
class InputStreamImpl : public InputStream {
public:
  InputStreamImpl(::asio::io_service *io_service, const std::string &client_name,
                  const ::hadoop::hdfs::LocatedBlocksProto *blocks);
  virtual void
  PositionRead(void *buf, size_t nbyte, uint64_t offset,
               const std::set<std::string> &excluded_datanodes,
               const std::function<void(const Status &, const std::string &,
                                        size_t)> &handler) override;
  template <class MutableBufferSequence, class Handler>
  void AsyncPreadSome(size_t offset, const MutableBufferSequence &buffers,
                      const std::set<std::string> &excluded_datanodes,
                      const Handler &handler);
  template <class BlockReaderTrait, class MutableBufferSequence, class Handler>
  void AsyncReadBlock(const hadoop::hdfs::LocatedBlockProto &block,
                      const hadoop::hdfs::DatanodeInfoProto &dn, size_t offset,
                      const MutableBufferSequence &buffers,
                      const Handler &handler);

private:
  ::asio::io_service *io_service_;
  const std::string client_name_;
  unsigned long long file_length_;
  std::vector<::hadoop::hdfs::LocatedBlockProto> blocks_;
  template <class Reader> struct HandshakeContinuation;
  template <class Reader, class MutableBufferSequence>
  struct ReadBlockContinuation;
  struct RemoteBlockReaderTrait;
};
}

#include "inputstream_impl.h"

#endif
