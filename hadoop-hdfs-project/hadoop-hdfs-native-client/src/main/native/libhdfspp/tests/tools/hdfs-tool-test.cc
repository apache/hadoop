/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#include <functional>
#include <string>
#include <tuple>

#include <gtest/gtest.h>

#include "hdfs-allow-snapshot-mock.h"
#include "hdfs-tool-test.h"

INSTANTIATE_TEST_SUITE_P(HdfsAllowSnapshot, HdfsToolBasicTest,
                         testing::Values(
                             []() -> hdfs::tools::HdfsTool * {
                               constexpr auto argc = 2;
                               std::string exe("hdwfs_allow_Snapshot");
                               std::string arg1("a/b/c");

                               static char *argv[] = {exe.data(), arg1.data()};
                               return new HdfsAllowSnapshotMock(argc, argv);
                             },
                             []() -> hdfs::tools::HdfsTool * {
                               constexpr auto argc = 2;
                               std::string exe("hdwfs_allow_Snapshot");
                               std::string arg1("-h");

                               static char *argv[] = {exe.data(), arg1.data()};
                               return new HdfsAllowSnapshotMock(argc, argv);
                             }));

INSTANTIATE_TEST_SUITE_P(
    HdfsAllowSnapshot, HdfsToolNegativeTest,
    testing::Values([]() -> hdfs::tools::HdfsTool * {
      constexpr auto argc = 3;
      std::string exe("hdwfs_allow_Snapshot");
      std::string arg1("a/b/c");
      std::string arg2("d/e/f");

      static char *argv[] = {exe.data(), arg1.data(), arg2.data()};
      return new HdfsAllowSnapshotMock(argc, argv);
    }));