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

#include "filehandle.h"
#include "common/hdfs_public_api.h"
#include "common/async_stream.h"
#include "libhdfspp/hdfs.h"
#include "fs/bad_datanode_tracker.h"
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
 * Lifetime: owned by a FileSystemImpl
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

  void GetBlockLocations(const std::string & path,
    std::function<void(const Status &, std::shared_ptr<const struct FileInfo>)> handler);

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
 * Lifetime: pointer created for consumer who is responsible for deleting it
 */
class FileSystemImpl : public FileSystem {
public:
  FileSystemImpl(IoService *&io_service, const Options &options);
  ~FileSystemImpl() override;

  /* attempt to connect to namenode, return bad status on failure */
  void Connect(const std::string &server, const std::string &service,
               std::function<void(const Status &)> &&handler);
  /* attempt to connect to namenode, return bad status on failure */
  Status Connect(const std::string &server, const std::string &service);


  virtual void Open(const std::string &path,
                    const std::function<void(const Status &, FileHandle *)>
                        &handler) override;
  Status Open(const std::string &path, FileHandle **handle) override;


  /* add a new thread to handle asio requests, return number of threads in pool
   */
  int AddWorkerThread();

  /* how many worker threads are servicing asio requests */
  int WorkerThreadCount() { return worker_threads_.size(); }


private:
  std::unique_ptr<IoServiceImpl> io_service_;
  NameNodeOperations nn_;
  const std::string client_name_;
  std::shared_ptr<BadDataNodeTracker> bad_node_tracker_;

  struct WorkerDeleter {
    void operator()(std::thread *t) {
      t->join();
      delete t;
    }
  };
  typedef std::unique_ptr<std::thread, WorkerDeleter> WorkerPtr;
  std::vector<WorkerPtr> worker_threads_;

};


}

#endif
