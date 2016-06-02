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

  int                   file_type;
  ::std::string         path;
  unsigned long int     length;
  unsigned long int     permissions;  //Octal number as in POSIX permissions; e.g. 0777
  ::std::string         owner;
  ::std::string         group;
  unsigned long int     modification_time;
  unsigned long int     access_time;
  ::std::string         symlink;
  unsigned int          block_replication;
  unsigned long int     blocksize;
  unsigned long int     fileid;
  unsigned long int     children_num;
  StatInfo()
      : file_type(0),
        length(0),
        permissions(0),
        modification_time(0),
        access_time(0),
        block_replication(0),
        blocksize(0),
        fileid(0),
        children_num(0) {
  }
};

}

#endif
