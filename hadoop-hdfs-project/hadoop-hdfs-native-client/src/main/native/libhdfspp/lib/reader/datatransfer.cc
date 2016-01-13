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

#include "datatransfer.h"

#include "hdfspp/status.h"

namespace hdfs {

namespace DataTransferSaslStreamUtil {

static const auto kSUCCESS = hadoop::hdfs::DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS;

using hadoop::hdfs::DataTransferEncryptorMessageProto;

Status ConvertToStatus(const DataTransferEncryptorMessageProto *msg, std::string *payload) {
  using namespace hadoop::hdfs;
  auto s = msg->status();
  if (s == DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_ERROR_UNKNOWN_KEY) {
    payload->clear();
    return Status::Exception("InvalidEncryptionKeyException", msg->message().c_str());
  } else if (s == DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_ERROR) {
    payload->clear();
    return Status::Error(msg->message().c_str());
  } else {
    *payload = msg->payload();
    return Status::OK();
  }
}

void PrepareInitialHandshake(DataTransferEncryptorMessageProto *msg) {
  msg->set_status(kSUCCESS);
  msg->set_payload("");
}

}
}
