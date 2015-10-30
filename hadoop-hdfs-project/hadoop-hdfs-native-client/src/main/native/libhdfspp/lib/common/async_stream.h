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

#ifndef LIB_COMMON_ASYNC_STREAM_H_
#define LIB_COMMON_ASYNC_STREAM_H_

#include <asio.hpp>

namespace hdfs {

class AsyncStream {
public:
  virtual void async_read(const asio::mutable_buffers_1	& buffers,
             std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > completed_handler) = 0;
  virtual void async_read(const asio::mutable_buffers_1	& buffers,
             std::function<size_t (const asio::error_code & error,
                                 std::size_t bytes_transferred) > completion_handler,
             std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > completed_handler) = 0;
  virtual void async_write(const asio::const_buffers_1 & buffers, 
             std::function<void (const asio::error_code &ec, size_t)> handler) = 0;
};

}

#endif
