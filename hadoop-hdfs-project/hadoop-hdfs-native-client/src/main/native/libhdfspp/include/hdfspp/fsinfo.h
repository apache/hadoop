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

namespace hdfs {

/**
 * Information that is assumed to be unchanging about a file system for the duration of
 * the operations.
 */
struct FsInfo {

  unsigned long int     capacity;
  unsigned long int     used;
  unsigned long int     remaining;
  unsigned long int     under_replicated;
  unsigned long int     corrupt_blocks;
  unsigned long int     missing_blocks;
  unsigned long int     missing_repl_one_blocks;
  unsigned long int     blocks_in_future;

  FsInfo()
      : capacity(0),
        used(0),
        remaining(0),
        under_replicated(0),
        corrupt_blocks(0),
        missing_blocks(0),
        missing_repl_one_blocks(0),
        blocks_in_future(0) {
  }
};

}

#endif
