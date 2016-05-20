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
#ifndef LIBHDFSPP_LIB_CONNECTION_DATANODECONNECTION_H_
#define LIBHDFSPP_LIB_CONNECTION_DATANODECONNECTION_H_

#include "common/hdfs_public_api.h"
#include "common/async_stream.h"
#include "ClientNamenodeProtocol.pb.h"
#include "common/libhdfs_events_impl.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/new_delete.h"

#include "asio.hpp"

namespace hdfs {

class DataNodeConnection : public AsyncStream {
public:
  MEMCHECKED_CLASS(DataNodeConnection)
  std::string uuid_;
  std::unique_ptr<hadoop::common::TokenProto> token_;

  virtual ~DataNodeConnection();
  virtual void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) = 0;
  virtual void Cancel() = 0;
};


struct SocketDeleter {
  inline void operator()(asio::ip::tcp::socket *sock) {
    // Cancel may have already closed the socket.
    std::string err = SafeDisconnect(sock);
    if(!err.empty()) {
        LOG_WARN(kBlockReader, << "Error disconnecting socket: " << err);
    }
    delete sock;
  }
};

class DataNodeConnectionImpl : public DataNodeConnection, public std::enable_shared_from_this<DataNodeConnectionImpl>{
private:
  // held (briefly) while posting async ops to the asio task queue
  std::mutex state_lock_;
public:
  std::unique_ptr<asio::ip::tcp::socket, SocketDeleter> conn_;
  std::array<asio::ip::tcp::endpoint, 1> endpoints_;
  std::string uuid_;
  LibhdfsEvents *event_handlers_;

  virtual ~DataNodeConnectionImpl();
  DataNodeConnectionImpl(asio::io_service * io_service, const ::hadoop::hdfs::DatanodeInfoProto &dn_proto,
                          const hadoop::common::TokenProto *token,
                          LibhdfsEvents *event_handlers);

  void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) override;

  void Cancel() override;

  void async_read_some(const MutableBuffers &buf,
                         std::function<void (const asio::error_code & error, std::size_t bytes_transferred) > handler)
                       override {
    event_handlers_->call("DN_read_req", "", "", buf.end() - buf.begin());


    mutex_guard state_lock(state_lock_);
    conn_->async_read_some(buf, handler);
  };

  void async_write_some(const ConstBuffers &buf,
                          std::function<void (const asio::error_code & error, std::size_t bytes_transferred) > handler)
                        override {
    event_handlers_->call("DN_write_req", "", "", buf.end() - buf.begin());

    mutex_guard state_lock(state_lock_);
    conn_->async_write_some(buf, handler);
  }
};

}

#endif
