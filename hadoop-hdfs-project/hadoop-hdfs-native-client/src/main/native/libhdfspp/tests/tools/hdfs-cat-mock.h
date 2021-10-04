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

#ifndef LIBHDFSPP_TOOLS_HDFS_CAT_MOCK
#define LIBHDFSPP_TOOLS_HDFS_CAT_MOCK

#include <string>

#include <gmock/gmock.h>

#include "hdfs-cat.h"

namespace hdfs::tools::test {
class CatMock : public hdfs::tools::Cat {
public:
  CatMock(const int argc, char **argv) : Cat(argc, argv) {}

  CatMock(const CatMock &) = delete;
  CatMock(CatMock &&) = delete;
  CatMock &operator=(const CatMock &) = delete;
  CatMock &operator=(CatMock &&) = delete;
  ~CatMock() override;

  MOCK_METHOD(bool, HandleHelp, (), (const, override));

  MOCK_METHOD(bool, HandlePath, (const std::string &), (const, override));
};
} // namespace hdfs::tools::test

#endif
