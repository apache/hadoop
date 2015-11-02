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
#include "block_reader.h"

namespace hdfs {

hadoop::hdfs::OpReadBlockProto
ReadBlockProto(const std::string &client_name, bool verify_checksum,
               const hadoop::common::TokenProto *token,
               const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
               uint64_t offset) {
  using namespace hadoop::hdfs;
  using namespace hadoop::common;
  BaseHeaderProto *base_h = new BaseHeaderProto();
  base_h->set_allocated_block(new ExtendedBlockProto(*block));
  if (token) {
    base_h->set_allocated_token(new TokenProto(*token));
  }
  ClientOperationHeaderProto *h = new ClientOperationHeaderProto();
  h->set_clientname(client_name);
  h->set_allocated_baseheader(base_h);

  OpReadBlockProto p;
  p.set_allocated_header(h);
  p.set_offset(offset);
  p.set_len(length);
  p.set_sendchecksums(verify_checksum);
  // TODO: p.set_allocated_cachingstrategy();
  return p;
}

void RemoteBlockReader::async_request_block(
    const std::string &client_name, const hadoop::common::TokenProto *token,
    const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
    uint64_t offset, const std::function<void(Status)> &handler) {
  // The total number of bytes that we need to transfer from the DN is
  // the amount that the user wants (bytesToRead), plus the padding at
  // the beginning in order to chunk-align. Note that the DN may elect
  // to send more than this amount if the read starts/ends mid-chunk.
  bytes_to_read_ = length;

  struct State {
    std::string header;
    hadoop::hdfs::OpReadBlockProto request;
    hadoop::hdfs::BlockOpResponseProto response;
  };

  auto m = continuation::Pipeline<State>::Create();
  State *s = &m->state();

  s->header.insert(s->header.begin(),
                   {0, kDataTransferVersion, Operation::kReadBlock});
  s->request = std::move(ReadBlockProto(client_name, options_.verify_checksum,
                                        token, block, length, offset));

  auto read_pb_message =
      new continuation::ReadDelimitedPBMessageContinuation<AsyncStream, 16384>(
          stream_, &s->response);

  m->Push(async_stream_continuation::Write(stream_, asio::buffer(s->header)))
      .Push(asio_continuation::WriteDelimitedPBMessage(stream_, &s->request))
      .Push(read_pb_message);

  m->Run([this, handler, offset](const Status &status, const State &s) {    Status stat = status;
    if (stat.ok()) {
      const auto &resp = s.response;
      if (resp.status() == ::hadoop::hdfs::Status::SUCCESS) {
        if (resp.has_readopchecksuminfo()) {
          const auto &checksum_info = resp.readopchecksuminfo();
          chunk_padding_bytes_ = offset - checksum_info.chunkoffset();
        }
        state_ = kReadPacketHeader;
      } else {
        stat = Status::Error(s.response.message().c_str());
      }
    }
    handler(stat);
  });
}

Status RemoteBlockReader::request_block(
    const std::string &client_name, const hadoop::common::TokenProto *token,
    const hadoop::hdfs::ExtendedBlockProto *block, uint64_t length,
    uint64_t offset) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  async_request_block(client_name, token, block, length, offset,
                [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}

}
