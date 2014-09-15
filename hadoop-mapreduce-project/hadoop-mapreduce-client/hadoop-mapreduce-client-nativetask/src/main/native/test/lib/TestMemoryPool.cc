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
#include "test_commons.h"
#include "lib/PartitionBucket.h"
#include "lib/PartitionBucketIterator.h"
#include "lib/MemoryBlock.h"
#include "lib/IFile.h"

namespace NativeTask {

TEST(MemoryPool, general) {
  MemoryPool * pool = new MemoryPool();
  const uint32_t POOL_SIZE = 1024;

  pool->init(POOL_SIZE);

  uint32_t min = 1024;
  uint32_t expect = 2048;
  uint32_t allocated = 0;
  char * buff = pool->allocate(min, expect, allocated);
  ASSERT_NE((void *)NULL, buff);
  buff = pool->allocate(min, expect, allocated);
  ASSERT_EQ(NULL, buff);

  pool->reset();
  buff = pool->allocate(min, expect, allocated);
  ASSERT_NE((void *)NULL, buff);

  delete pool;
}
} // namespace NativeTask
