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
#include "common/util_c.h"

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <exception>
#include <sstream>
#include <iomanip>
#include <thread>


namespace hdfs {

Status ToStatus(const ::asio::error_code &ec) {
  if (ec) {
    return Status(ec.value(), ec.message().c_str());
  } else {
    return Status::OK();
  }
}

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

int DelimitedPBMessageSize(const ::google::protobuf::MessageLite *msg) {
  size_t size = msg->ByteSize();
  return ::google::protobuf::io::CodedOutputStream::VarintSize32(size) + size;
}

std::string GetRandomClientName() {
  std::vector<unsigned char>buf(8);
  RAND_pseudo_bytes(&buf[0], 8);

  std::ostringstream oss;
  oss << "DFSClient_"  << getpid() <<  "_" <<
          std::this_thread::get_id() << "_" <<
          std::setw(2) << std::hex << std::uppercase << std::setfill('0');
  for (unsigned char b: buf)
    oss << static_cast<unsigned>(b);

  return oss.str();
}

std::string Base64Encode(const std::string &src) {
  //encoded size is (sizeof(buf) + 2) / 3 * 4
  static const std::string base64_chars =
               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
               "abcdefghijklmnopqrstuvwxyz"
               "0123456789+/";
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];
  unsigned const char *bytes_to_encode = reinterpret_cast<unsigned const char *>(&src[i]);
  unsigned int in_len = src.size();

  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
        ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      ret += base64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';
  }
  return ret;
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

bool IsHighBitSet(uint64_t num) {
  uint64_t firstBit = (uint64_t) 1 << 63;
  if (num & firstBit) {
    return true;
  } else {
    return false;
  }
}

}

void ShutdownProtobufLibrary_C() {
  google::protobuf::ShutdownProtobufLibrary();
}


