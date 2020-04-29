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
#ifndef LIB_READER_FILEINFO_H_
#define LIB_READER_FILEINFO_H_

#include "ClientNamenodeProtocol.pb.h"

namespace hdfs {

/**
 * Information that is assumed to be unchanging about a file for the duration of
 * the operations.
 */
struct FileInfo {
  unsigned long long file_length_;
  bool               under_construction_;
  bool               last_block_complete_;
  std::vector<::hadoop::hdfs::LocatedBlockProto> blocks_;
};

}

#endif
