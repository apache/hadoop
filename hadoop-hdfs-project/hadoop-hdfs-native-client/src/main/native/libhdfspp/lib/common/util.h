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
#ifndef LIB_COMMON_UTIL_H_
#define LIB_COMMON_UTIL_H_

#include "hdfspp/status.h"

#include <sstream>

#include <asio/error_code.hpp>
#include <openssl/rand.h>

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>


namespace hdfs {

static inline Status ToStatus(const ::asio::error_code &ec) {
  if (ec) {
    return Status(ec.value(), ec.message().c_str());
  } else {
    return Status::OK();
  }
}

// Determine size of buffer that needs to be allocated in order to serialize msg
// in delimited format
static inline int DelimitedPBMessageSize(const ::google::protobuf::MessageLite *msg) {
  size_t size = msg->ByteSize();
  return ::google::protobuf::io::CodedOutputStream::VarintSize32(size) + size;
}

// Construct msg from the input held in the CodedInputStream
// return false on failure, otherwise return true
bool ReadDelimitedPBMessage(::google::protobuf::io::CodedInputStream *in,
                            ::google::protobuf::MessageLite *msg);

// Serialize msg into a delimited form (java protobuf compatible)
// err, if not null, will be set to false on failure
std::string SerializeDelimitedProtobufMessage(const ::google::protobuf::MessageLite *msg,
                                              bool *err);

std::string Base64Encode(const std::string &src);

// Return a new high-entropy client name
std::string GetRandomClientName();

// Returns true if _someone_ is holding the lock (not necessarily this thread,
// but a std::mutex doesn't track which thread is holding the lock)
template<class T>
bool lock_held(T & mutex) {
  bool result = !mutex.try_lock();
  if (!result)
    mutex.unlock();
  return result;
}

}

#endif
