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

#include "datanodeconnection.h"
#include "common/util.h"

namespace hdfs {

DataNodeConnection::~DataNodeConnection(){}
DataNodeConnectionImpl::~DataNodeConnectionImpl(){}

DataNodeConnectionImpl::DataNodeConnectionImpl(asio::io_service * io_service,
                                                const ::hadoop::hdfs::DatanodeInfoProto &dn_proto,
                                                const hadoop::common::TokenProto *token)
{
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


void DataNodeConnectionImpl::Connect(
             std::function<void(Status status, std::shared_ptr<DataNodeConnection> dn)> handler) {
  // Keep the DN from being freed until we're done
  auto shared_this = shared_from_this();
  asio::async_connect(*conn_, endpoints_.begin(), endpoints_.end(),
          [shared_this, handler](const asio::error_code &ec, std::array<asio::ip::tcp::endpoint, 1>::iterator it) {
            (void)it;
            handler(ToStatus(ec), shared_this); });
}

void DataNodeConnectionImpl::Cancel() {
  // best to do a shutdown() first for portability
  conn_->shutdown(asio::ip::tcp::socket::shutdown_both);
  conn_->close();
}


}
