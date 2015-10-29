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

#include "libhdfspp/status.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>
#include <asio/ip/tcp.hpp>

namespace hdfs {
namespace asio_continuation {

using namespace continuation;

template <class Stream, class MutableBufferSequence>
class ReadContinuation : public Continuation {
public:
  ReadContinuation(Stream *stream, const MutableBufferSequence &buffer)
      : stream_(stream), buffer_(buffer) {}
  virtual void Run(const Next &next) override {
    auto handler =
        [next](const asio::error_code &ec, size_t) { next(ToStatus(ec)); };
    asio::async_read(*stream_, buffer_, handler);
  }

private:
  Stream *stream_;
  MutableBufferSequence buffer_;
};

template <class Stream, class ConstBufferSequence>
class WriteContinuation : public Continuation {
public:
  WriteContinuation(Stream *stream, const ConstBufferSequence &buffer)
      : stream_(stream), buffer_(buffer) {}

  virtual void Run(const Next &next) override {
    auto handler =
        [next](const asio::error_code &ec, size_t) { next(ToStatus(ec)); };
    asio::async_write(*stream_, buffer_, handler);
  }

private:
  Stream *stream_;
  ConstBufferSequence buffer_;
};

template <class Socket, class Iterator>
class ConnectContinuation : public Continuation {
public:
  ConnectContinuation(Socket *socket, Iterator begin, Iterator end,
                      Iterator *connected_endpoint)
      : socket_(socket), begin_(begin), end_(end),
        connected_endpoint_(connected_endpoint) {}

  virtual void Run(const Next &next) override {
    auto handler = [this, next](const asio::error_code &ec, Iterator it) {
      if (connected_endpoint_) {
        *connected_endpoint_ = it;
      }
      next(ToStatus(ec));
    };
    asio::async_connect(*socket_, begin_, end_, handler);
  }

private:
  Socket *socket_;
  Iterator begin_;
  Iterator end_;
  Iterator *connected_endpoint_;
};

template <class OutputIterator>
class ResolveContinuation : public Continuation {
public:
  ResolveContinuation(::asio::io_service *io_service, const std::string &server,
                      const std::string &service, OutputIterator result)
      : resolver_(*io_service), query_(server, service), result_(result) {}

  virtual void Run(const Next &next) override {
    using resolver = ::asio::ip::tcp::resolver;
    auto handler =
        [this, next](const asio::error_code &ec, resolver::iterator it) {
          if (!ec) {
            std::copy(it, resolver::iterator(), result_);
          }
          next(ToStatus(ec));
        };
    resolver_.async_resolve(query_, handler);
  }

private:
  ::asio::ip::tcp::resolver resolver_;
  ::asio::ip::tcp::resolver::query query_;
  OutputIterator result_;
};

template <class Stream, class ConstBufferSequence>
static inline Continuation *Write(Stream *stream,
                                  const ConstBufferSequence &buffer) {
  return new WriteContinuation<Stream, ConstBufferSequence>(stream, buffer);
}

template <class Stream, class MutableBufferSequence>
static inline Continuation *Read(Stream *stream,
                                 const MutableBufferSequence &buffer) {
  return new ReadContinuation<Stream, MutableBufferSequence>(stream, buffer);
}

template <class Socket, class Iterator>
static inline Continuation *Connect(Socket *socket, Iterator begin,
                                    Iterator end) {
  return new ConnectContinuation<Socket, Iterator>(socket, begin, end, nullptr);
}

template <class OutputIterator>
static inline Continuation *
Resolve(::asio::io_service *io_service, const std::string &server,
        const std::string &service, OutputIterator result) {
  return new ResolveContinuation<OutputIterator>(io_service, server, service, result);
}
}
}

#endif
