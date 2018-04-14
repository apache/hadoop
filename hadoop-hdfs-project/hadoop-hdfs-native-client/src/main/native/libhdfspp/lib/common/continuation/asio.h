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
#ifndef LIB_COMMON_CONTINUATION_ASIO_H_
#define LIB_COMMON_CONTINUATION_ASIO_H_

#include "continuation.h"
#include "common/util.h"
#include "hdfspp/status.h"
#include <asio/write.hpp>
#include <memory>

namespace hdfs {
namespace asio_continuation {

using namespace continuation;

template <class Stream, class ConstBufferSequence>
class WriteContinuation : public Continuation {
public:
  WriteContinuation(std::shared_ptr<Stream>& stream, const ConstBufferSequence &buffer)
      : stream_(stream), buffer_(buffer) {}

  virtual void Run(const Next &next) override {
    auto handler =
        [next](const asio::error_code &ec, size_t) { next(ToStatus(ec)); };
    asio::async_write(*stream_, buffer_, handler);
  }

private:
  // prevent construction from raw ptr
  WriteContinuation(Stream *stream, ConstBufferSequence &buffer);
  std::shared_ptr<Stream> stream_;
  ConstBufferSequence buffer_;
};

template <class Stream, class ConstBufferSequence>
static inline Continuation *Write(std::shared_ptr<Stream> stream,
                                  const ConstBufferSequence &buffer) {
  return new WriteContinuation<Stream, ConstBufferSequence>(stream, buffer);
}

}
}

#endif
