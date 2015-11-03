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
#include "reader/fileinfo.h"
#include "ClientNamenodeProtocol.pb.h"
#include "ClientNamenodeProtocol.hrpc.inl"

#include "asio.hpp"

#include <thread>

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
  FileSystemImpl(IoService *&io_service, const Options &options);
  ~FileSystemImpl();

  /* attempt to connect to namenode, return bad status on failure */
  void Connect(const std::string &server, const std::string &service,
               std::function<void(const Status &)> &&handler);
  /* attempt to connect to namenode, return bad status on failure */
  Status Connect(const std::string &server, const std::string &service);


  virtual void Open(const std::string &path,
                    const std::function<void(const Status &, FileHandle *)>
                        &handler) override;
  Status Open(const std::string &path, FileHandle **handle);
  
  
  /* add a new thread to handle asio requests, return number of threads in pool
   */
  int AddWorkerThread();

  /* how many worker threads are servicing asio requests */
  int WorkerThreadCount() { return worker_threads_.size(); }

  
private:
  std::unique_ptr<IoServiceImpl> io_service_;
  NameNodeOperations nn_;
  const std::string client_name_;

  struct WorkerDeleter {
    void operator()(std::thread *t) {
      t->join();
      delete t;
    }
  };
  typedef std::unique_ptr<std::thread, WorkerDeleter> WorkerPtr;
  std::vector<WorkerPtr> worker_threads_;

};


/*
 * ReadOperation: given DN connection, does one-shot reads.
 *
 * Threading model: not thread-safe; consumers and io_service should not call
 *    concurrently
 */
class FileHandleImpl : public FileHandle {
public:
  FileHandleImpl(::asio::io_service *io_service, const std::string &client_name,
                  const std::shared_ptr<const struct FileInfo> file_info);
  virtual void
  PositionRead(void *buf, size_t nbyte, uint64_t offset,
               const std::set<std::string> &excluded_datanodes,
               const std::function<void(const Status &, const std::string &,
                                        size_t)> &handler) override;

  size_t PositionRead(void *buf, size_t nbyte, off_t offset) override;

  void AsyncPreadSome(size_t offset, const MutableBuffers &buffers,
                      const std::set<std::string> &excluded_datanodes,
                      const std::function<void(const Status &, const std::string &, size_t)> handler);
private:
  ::asio::io_service *io_service_;
  const std::string client_name_;
  const std::shared_ptr<const struct FileInfo> file_info_;

  std::shared_ptr<DataNodeConnectionImpl> dn_;  // The last DN connected to
};

}

#endif
