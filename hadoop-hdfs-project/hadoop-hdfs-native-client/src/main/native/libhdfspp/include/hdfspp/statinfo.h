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
#ifndef HDFSPP_STATINFO_H_
#define HDFSPP_STATINFO_H_

#include <string>
#include <cstdint>

namespace hdfs {

/**
 * Information that is assumed to be unchanging about a file for the duration of
 * the operations.
 */
struct StatInfo {
  enum FileType {
    IS_DIR = 1,
    IS_FILE = 2,
    IS_SYMLINK = 3
  };

  int          file_type;
  std::string  path;
  std::string  full_path;
  uint64_t     length;
  uint64_t     permissions;  //Octal number as in POSIX permissions; e.g. 0777
  std::string  owner;
  std::string  group;
  uint64_t     modification_time;
  uint64_t     access_time;
  std::string  symlink;
  uint32_t     block_replication;
  uint64_t     blocksize;
  uint64_t     fileid;
  uint64_t     children_num;

  StatInfo();

  //Converts StatInfo object to std::string (hdfs_ls format)
  std::string str() const;
};

}

#endif
