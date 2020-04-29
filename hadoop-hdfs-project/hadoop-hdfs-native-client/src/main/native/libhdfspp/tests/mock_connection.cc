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

#include "mock_connection.h"

namespace hdfs {

MockConnectionBase::MockConnectionBase(::asio::io_service *io_service)
    : io_service_(io_service)
{}

MockConnectionBase::~MockConnectionBase() {}

ProducerResult SharedMockConnection::Produce() {
  if (auto shared_prducer = shared_connection_data_.lock()) {
    return shared_prducer->Produce();
  } else {
    assert(false && "No producer registered");
    return std::make_pair(asio::error_code(), "");
  }
}

std::weak_ptr<SharedConnectionData> SharedMockConnection::shared_connection_data_;

}
