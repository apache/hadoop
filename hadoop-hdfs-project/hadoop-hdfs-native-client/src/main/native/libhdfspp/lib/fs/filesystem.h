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
#include "common/async_stream.h"
#include "libhdfspp/hdfs.h"
#include "rpc/rpc_engine.h"
#include "reader/block_reader.h"
#include "ClientNamenodeProtocol.pb.h"
#include "ClientNamenodeProtocol.hrpc.inl"

#include "asio.hpp"

namespace hdfs {

/**
 * Information that is assumed to be unchaning about a file for the duration of
 * the operations.
 */
struct FileInfo {
  unsigned long long file_length_;
  std::vector<::hadoop::hdfs::LocatedBlockProto> blocks_;
};

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

void GetBlockLocations(const std::string & path, std::function<void(const Status &, std::shared_ptr<const struct FileInfo>)> handler);
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


class DataNodeConnection : public AsyncStream {
public:
    std::string uuid_;

    virtual void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) = 0;
};


class DataNodeConnectionImpl : public DataNodeConnection, public std::enable_shared_from_this<DataNodeConnectionImpl> {
public:
    std::unique_ptr<asio::ip::tcp::socket> conn_;
    std::array<asio::ip::tcp::endpoint, 1> endpoints_;
    std::string uuid_;


    DataNodeConnectionImpl(asio::io_service * io_service, const ::hadoop::hdfs::DatanodeInfoProto &dn_proto) {
      using namespace ::asio::ip;

      conn_.reset(new tcp::socket(*io_service));
      auto datanode_addr = dn_proto.id();
      endpoints_[0] = tcp::endpoint(address::from_string(datanode_addr.ipaddr()),
                                      datanode_addr.xferport());
      uuid_ = dn_proto.id().datanodeuuid();
    }

    void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) override;

    virtual void async_read(const asio::mutable_buffers_1	& buffers,
               std::function<void (const asio::error_code & error,
                                   std::size_t bytes_transferred) > completed_handler) 
      { asio::async_read(*conn_, buffers, completed_handler); }
    
    virtual void async_read(const asio::mutable_buffers_1	& buffers,
               std::function<size_t (const asio::error_code & error,
                                   std::size_t bytes_transferred) > completion_handler,
               std::function<void (const asio::error_code & error,
                                   std::size_t bytes_transferred) > completed_handler)
      { asio::async_read(*conn_, buffers, completion_handler, completed_handler); }
    virtual void async_write(const asio::const_buffers_1 & buffers, 
               std::function<void (const asio::error_code &ec, size_t)> handler)
      { asio::async_write(*conn_, buffers, handler); }
};

/*
 * ReadOperation: given DN connection, does one-shot reads.
 *
 * Threading model: not thread-safe; consumers and io_service should not call
 *    concurrently
 */
class InputStreamImpl : public InputStream {
public:
  InputStreamImpl(::asio::io_service *io_service, const std::string &client_name,
                  const std::shared_ptr<const struct FileInfo> file_info);
  virtual void
  PositionRead(void *buf, size_t nbyte, uint64_t offset,
               const std::set<std::string> &excluded_datanodes,
               const std::function<void(const Status &, const std::string &,
                                        size_t)> &handler) override;

  void AsyncPreadSome(size_t offset, const MutableBuffers &buffers,
                      const std::set<std::string> &excluded_datanodes,
                      const std::function<void(const Status &, const std::string &, size_t)> handler);
private:
  ::asio::io_service *io_service_;
  const std::string client_name_;
  const std::shared_ptr<const struct FileInfo> file_info_;
  struct RemoteBlockReaderTrait;

  std::shared_ptr<DataNodeConnectionImpl> dn_;  // The last DN connected to
};

class ReadOperation {
public:
  static void AsyncReadBlock(
    BlockReader * reader,
    const std::string & client_name,
    const hadoop::hdfs::LocatedBlockProto &block, size_t offset,
    const MutableBuffers &buffers,
    const std::function<void(const Status &, size_t)> handler);
private:
  struct HandshakeContinuation;
  struct ReadBlockContinuation;
};

}

#include "inputstream_impl.h"

#endif
