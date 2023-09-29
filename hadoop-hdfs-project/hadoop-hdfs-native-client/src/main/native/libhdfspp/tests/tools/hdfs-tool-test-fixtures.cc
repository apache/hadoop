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

#include <tuple>

#include <gtest/gtest.h>

#include "hdfs-tool-test-fixtures.h"

/**
 * Implementing virtual destructors out-of-line to avoid bulky v-table entries.
 */
HdfsToolBasicTest::~HdfsToolBasicTest() = default;
HdfsToolNegativeTestThrows::~HdfsToolNegativeTestThrows() = default;
HdfsToolNegativeTestNoThrow::~HdfsToolNegativeTestNoThrow() = default;

TEST_P(HdfsToolBasicTest, RunTool) { EXPECT_TRUE(this->hdfs_tool_->Do()); }

TEST_P(HdfsToolNegativeTestNoThrow, RunTool) {
  EXPECT_FALSE(this->hdfs_tool_->Do());
}

TEST_P(HdfsToolNegativeTestThrows, RunTool) {
  EXPECT_ANY_THROW({ std::ignore = this->hdfs_tool_->Do(); });
}
