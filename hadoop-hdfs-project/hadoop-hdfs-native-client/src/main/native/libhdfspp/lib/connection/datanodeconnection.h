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

#include "asio.hpp"

namespace hdfs {

class DataNodeConnection : public AsyncStream {
public:
    std::string uuid_;
    std::unique_ptr<hadoop::common::TokenProto> token_;

    virtual void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) = 0;
};


class DataNodeConnectionImpl : public DataNodeConnection, public std::enable_shared_from_this<DataNodeConnectionImpl>{
public:
  std::unique_ptr<asio::ip::tcp::socket> conn_;
  std::array<asio::ip::tcp::endpoint, 1> endpoints_;
  std::string uuid_;


  DataNodeConnectionImpl(asio::io_service * io_service, const ::hadoop::hdfs::DatanodeInfoProto &dn_proto,
                          const hadoop::common::TokenProto *token) {
    using namespace ::asio::ip;

    conn_.reset(new tcp::socket(*io_service));
    auto datanode_addr = dn_proto.id();
    endpoints_[0] = tcp::endpoint(address::from_string(datanode_addr.ipaddr()),
                                    datanode_addr.xferport());
    uuid_ = dn_proto.id().datanodeuuid();

    if (token) {
      token_.reset(new hadoop::common::TokenProto());
      token_->CheckTypeAndMergeFrom(*token);
    }
  }

  void Connect(std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) override;

  void async_read_some(const MutableBuffers &buf,
        std::function<void (const asio::error_code & error,
                               std::size_t bytes_transferred) > handler) override {
    conn_->async_read_some(buf, handler);
  };

  void async_write_some(const ConstBuffers &buf,
            std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > handler) override {
    conn_->async_write_some(buf, handler);
  }

  void cancel() override {
    // Can't portably use asio::cancel; see
    // http://www.boost.org/doc/libs/1_59_0/doc/html/boost_asio/reference/basic_stream_socket/cancel/overload2.html
    //
    // When we implement connection re-use, we need to be sure to throw out
    //   connections on error
    conn_->close();
  }
};

}

#endif
