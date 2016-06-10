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
#include <exception>

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
  /**
   *  The server is requesting a 16-byte UUID:
   *  https://github.com/c9n/hadoop/blob/master/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/ClientId.java
   *
   *  This function generates a 16-byte UUID (version 4):
   *  https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29
   **/
  unsigned char buf[16];
  RAND_pseudo_bytes(buf, sizeof(buf));

  //clear the first four bits of byte 6 then set the second bit
  buf[6] = (buf[6] & 0x0f) | 0x40;

  //clear the second bit of byte 8 and set the first bit
  buf[8] = (buf[8] & 0xbf) | 0x80;

  std::stringstream ss;
  ss << std::string(reinterpret_cast<char *>(buf), sizeof(buf));
  return ss.str();
}

std::string SafeDisconnect(asio::ip::tcp::socket *sock) {
  std::string err;
  if(sock && sock->is_open()) {
    /**
     *  Even though we just checked that the socket is open it's possible
     *  it isn't in a state where it can properly send or receive.  If that's
     *  the case asio will turn the underlying error codes from shutdown()
     *  and close() into unhelpfully named std::exceptions.  Due to the
     *  relatively innocuous nature of most of these error codes it's better
     *  to just catch and return a flag so the caller can log failure.
     **/

    try {
      sock->shutdown(asio::ip::tcp::socket::shutdown_both);
    } catch (const std::exception &e) {
      err = std::string("shutdown() threw") + e.what();
    }

    try {
      sock->close();
    } catch (const std::exception &e) {
      // don't append if shutdown() already failed, first failure is the useful one
      if(err.empty())
        err = std::string("close() threw") + e.what();
    }

  }
  return err;
}

}
