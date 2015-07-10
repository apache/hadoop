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
#ifndef BLOCK_READER_H_
#define BLOCK_READER_H_

#include "libhdfspp/status.h"
#include "datatransfer.pb.h"

#include <memory>

namespace hdfs {

struct CacheStrategy {
  bool drop_behind_specified;
  bool drop_behind;
  bool read_ahead_specified;
  unsigned long long read_ahead;
  CacheStrategy()
      : drop_behind_specified(false), drop_behind(false),
        read_ahead_specified(false), read_ahead(false) {}
};

enum DropBehindStrategy {
  kUnspecified = 0,
  kEnableDropBehind = 1,
  kDisableDropBehind = 2,
};

enum EncryptionScheme {
  kNone = 0,
  kAESCTRNoPadding = 1,
};

struct BlockReaderOptions {
  bool verify_checksum;
  CacheStrategy cache_strategy;
  EncryptionScheme encryption_scheme;

  BlockReaderOptions()
      : verify_checksum(true), encryption_scheme(EncryptionScheme::kNone) {}
};

template <class Stream>
class RemoteBlockReader
    : public std::enable_shared_from_this<RemoteBlockReader<Stream>> {
public:
  explicit RemoteBlockReader(const BlockReaderOptions &options, Stream *stream)
      : stream_(stream), state_(kOpen), options_(options),
        chunk_padding_bytes_(0) {}

  template <class MutableBufferSequence, class ReadHandler>
  void async_read_some(const MutableBufferSequence &buffers,
                       const ReadHandler &handler);

  template <class MutableBufferSequence>
  size_t read_some(const MutableBufferSequence &buffers, Status *status);

  Status connect(const std::string &client_name,
                 const hadoop::common::TokenProto *token,
                 const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
                 uint64_t offset);

  template <class ConnectHandler>
  void async_connect(const std::string &client_name,
                     const hadoop::common::TokenProto *token,
                     const hadoop::hdfs::ExtendedBlockProto *block,
                     uint64_t length, uint64_t offset,
                     const ConnectHandler &handler);

private:
  struct ReadPacketHeader;
  struct ReadChecksum;
  struct ReadPadding;
  template <class MutableBufferSequence> struct ReadData;
  struct AckRead;
  enum State {
    kOpen,
    kReadPacketHeader,
    kReadChecksum,
    kReadPadding,
    kReadData,
    kFinished,
  };

  Stream *stream_;
  hadoop::hdfs::PacketHeaderProto header_;
  State state_;
  BlockReaderOptions options_;
  size_t packet_len_;
  int packet_data_read_bytes_;
  int chunk_padding_bytes_;
  long long bytes_to_read_;
  std::vector<char> checksum_;
};
}

#include "remote_block_reader_impl.h"

#endif
