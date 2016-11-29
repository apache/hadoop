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

#include "hdfs_ioservice.h"

#include "common/logging.h"

namespace hdfs {

IoService::~IoService() {}

IoService *IoService::New() { return new IoServiceImpl(); }

void IoServiceImpl::Run() {
  // The IoService executes callbacks provided by library users in the context of worker threads,
  // there is no way of preventing those callbacks from throwing but we can at least prevent them
  // from escaping this library and crashing the process.

  // As recommended in http://www.boost.org/doc/libs/1_39_0/doc/html/boost_asio/reference/io_service.html#boost_asio.reference.io_service.effect_of_exceptions_thrown_from_handlers
  asio::io_service::work work(io_service_);
  for(;;)
  {
    try
    {
      io_service_.run();
      break;
    } catch (const std::exception & e) {
      LOG_WARN(kFileSystem, << "Unexpected exception in libhdfspp worker thread: " << e.what());
    } catch (...) {
      LOG_WARN(kFileSystem, << "Unexpected value not derived from std::exception in libhdfspp worker thread");
    }
  }
}


}
