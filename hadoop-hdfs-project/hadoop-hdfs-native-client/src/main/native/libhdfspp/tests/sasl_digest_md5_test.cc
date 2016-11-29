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
#include "common/sasl_authenticator.h"

#include <gtest/gtest.h>
#include <google/protobuf/stubs/common.h>

namespace hdfs {

/**
 * Testing whether the authenticator generates the MD5 digest correctly.
 **/
TEST(DigestMD5AuthenticatorTest, TestResponse) {
  const std::string username = "igFLnEx4OIx5PZWHAAAABGhtYWkAAAAoQlAtMTM3MDQ2OTk"
                               "zLTE5Mi4xNjguMS4yMjctMTQyNDIyMDM4MTM2M4xAAAABAQ"
                               "RSRUFE";
  const std::string password = "K5IFUibAynVVrApeCXLrBk9Sro8=";
  DigestMD5Authenticator auth(username, password, true);
  auth.cnonce_ = "KQlJwBDTseCHpAkFLZls4WcAktp6r5wTzje5feLY";
  std::string result;
  Status status =
      auth.EvaluateResponse("realm=\"0\",nonce=\"+GAWc+O6yEAWpew/"
                            "qKah8qh4QZLoOLCDcTtEKhlS\",qop=\"auth\",charset="
                            "utf-8,algorithm=md5-sess",
                            &result);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(result.find("response=3a286c2c385b92a06ebc66d58b8c4330") !=
              std::string::npos);

  google::protobuf::ShutdownProtobufLibrary();
}
}
