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
#ifndef LIB_COMMON_SASL_AUTHENTICATOR_H_
#define LIB_COMMON_SASL_AUTHENTICATOR_H_

#include "hdfspp/status.h"

namespace hdfs {

class DigestMD5AuthenticatorTest_TestResponse_Test;

/**
 * A specialized implementation of RFC 2831 for the HDFS
 * DataTransferProtocol.
 *
 * The current lacks the following features:
 *   * Encoding the username, realm, and password in ISO-8859-1 when
 * it is required by the RFC. They are always encoded in UTF-8.
 *   * Checking whether the challenges from the server are
 * well-formed.
 *   * Specifying authzid, digest-uri and maximum buffer size.
 *   * Supporting QOP other than the auth level.
 **/
class DigestMD5Authenticator {
public:
  Status EvaluateResponse(const std::string &payload, std::string *result);
  DigestMD5Authenticator(const std::string &username,
                         const std::string &password, bool mock_nonce = false);

private:
  Status GenerateFirstResponse(std::string *result);
  Status GenerateResponseValue(std::string *response_value);
  Status ParseFirstChallenge(const std::string &payload);

  static size_t NextToken(const std::string &payload, size_t off,
                          std::string *tok);
  void GenerateCNonce();
  std::string username_;
  std::string password_;
  std::string nonce_;
  std::string cnonce_;
  std::string realm_;
  std::string qop_;
  unsigned nonce_count_;

  const bool TEST_mock_cnonce_;
  friend class DigestMD5AuthenticatorTest_TestResponse_Test;
};
}

#endif
