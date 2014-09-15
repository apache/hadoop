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

#include "lib/commons.h"
#include "lib/BufferStream.h"
#include "lib/Buffers.h"
#include "test_commons.h"
#include "NativeTask.h"

namespace NativeTask {

TEST(Command, equals) {
  Command cmd1(100, "hello command");
  Command cmd2(100, "hello command 2");

  ASSERT_TRUE(cmd1.equals(cmd2));
  ASSERT_TRUE(cmd2.equals(cmd1));
  ASSERT_EQ(100, cmd1.id());

  std::string helloCommand = "hello command";
  ASSERT_EQ(helloCommand, cmd1.description());
}

} // namespace NativeTask
