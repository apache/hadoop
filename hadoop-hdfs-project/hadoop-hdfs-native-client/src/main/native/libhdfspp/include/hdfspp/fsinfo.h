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
#ifndef HDFSPP_FSINFO_H_
#define HDFSPP_FSINFO_H_

#include <string>

namespace hdfs {

/**
 * Information that is assumed to be unchanging about a file system for the duration of
 * the operations.
 */
struct FsInfo {

  uint64_t capacity;
  uint64_t used;
  uint64_t remaining;
  uint64_t under_replicated;
  uint64_t corrupt_blocks;
  uint64_t missing_blocks;
  uint64_t missing_repl_one_blocks;
  uint64_t blocks_in_future;

  FsInfo();

  //Converts FsInfo object to std::string (hdfs_df format)
  std::string str(const std::string fs_name) const;
};

}

#endif
