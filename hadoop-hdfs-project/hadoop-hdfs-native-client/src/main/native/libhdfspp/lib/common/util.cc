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

#include "common/util.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace hdfs {

bool ReadDelimitedPBMessage(::google::protobuf::io::CodedInputStream *in,
                            ::google::protobuf::MessageLite *msg) {
  uint32_t size = 0;
  in->ReadVarint32(&size);
  auto limit = in->PushLimit(size);
  bool res = msg->ParseFromCodedStream(in);
  in->PopLimit(limit);

  return res;
}


std::string SerializeDelimitedProtobufMessage(const ::google::protobuf::MessageLite *msg,
                                              bool *err) {
  namespace pbio = ::google::protobuf::io;

  std::string buf;

  int size = msg->ByteSize();
  buf.reserve(pbio::CodedOutputStream::VarintSize32(size) + size);
  pbio::StringOutputStream ss(&buf);
  pbio::CodedOutputStream os(&ss);
  os.WriteVarint32(size);

  if(err)
    *err = msg->SerializeToCodedStream(&os);

  return buf;
}


std::string GetRandomClientName() {
  unsigned char buf[6];

  RAND_pseudo_bytes(buf, sizeof(buf));

  std::stringstream ss;
  ss << "libhdfs++_"
     << Base64Encode(std::string(reinterpret_cast<char *>(buf), sizeof(buf)));
  return ss.str();
}

}
