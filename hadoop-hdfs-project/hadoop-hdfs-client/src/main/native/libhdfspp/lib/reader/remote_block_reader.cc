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
}
