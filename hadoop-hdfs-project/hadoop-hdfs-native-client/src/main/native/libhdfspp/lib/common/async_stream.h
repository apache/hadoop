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

#include <asio/buffer.hpp>
#include <asio/error_code.hpp>
#include <functional>

namespace hdfs {

// Contiguous buffer types
typedef asio::mutable_buffers_1 MutableBuffer;
typedef asio::const_buffers_1   ConstBuffer;

/*
 * asio-compatible stream implementation.
 *
 * Lifecycle: should be managed using std::shared_ptr so the object can be
 *    handed from consumer to consumer
 * Threading model: async_read_some and async_write_some are not thread-safe.
 */
class AsyncStream  {
public:
  virtual void async_read_some(const MutableBuffer &buf,
          std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > handler) = 0;

  virtual void async_write_some(const ConstBuffer &buf,
            std::function<void (const asio::error_code & error,
                                 std::size_t bytes_transferred) > handler) = 0;
};

}

#endif
