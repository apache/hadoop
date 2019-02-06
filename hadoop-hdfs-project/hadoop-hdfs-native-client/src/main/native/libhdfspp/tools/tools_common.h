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

#ifndef TOOLS_COMMON_H_
#define TOOLS_COMMON_H_

#include "hdfspp/hdfspp.h"
#include <mutex>

namespace hdfs {

  //Build all necessary objects and perform the connection
  std::shared_ptr<hdfs::FileSystem> doConnect(hdfs::URI & uri, bool max_timeout);

  //Open HDFS file at offset, read it to destination file, optionally delete source file
  void readFile(std::shared_ptr<hdfs::FileSystem> fs, std::string path, off_t offset, std::FILE* dst_file, bool to_delete);

  URI parse_path_or_exit(const std::string& path);
}

#endif
