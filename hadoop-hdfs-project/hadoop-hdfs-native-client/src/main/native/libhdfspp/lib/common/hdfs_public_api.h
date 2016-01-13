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

#ifndef COMMON_HDFS_PUBLIC_API_H_
#define COMMON_HDFS_PUBLIC_API_H_

#include "hdfspp/hdfspp.h"

#include <asio/io_service.hpp>

namespace hdfs {

class IoServiceImpl : public IoService {
 public:
  virtual void Run() override {
    asio::io_service::work work(io_service_);
    io_service_.run();
  }
  virtual void Stop() override { io_service_.stop(); }
  ::asio::io_service &io_service() { return io_service_; }
 private:
  ::asio::io_service io_service_;
};

}

#endif
