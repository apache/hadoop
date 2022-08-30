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

#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-rm.h"
#include "tools_common.h"

namespace hdfs::tools {
Rm::Rm(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Rm::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Remove/unlink the files or directories.");
  add_options("recursive,R",
              "Remove the directories and their contents recursively.");
  add_options("path", po::value<std::string>(),
              "The path to the file that needs to be removed.");

  // We allow only one argument to be passed to this tool. An exception is
  // thrown if multiple arguments are passed.
  pos_opt_desc_.add("path", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

std::string Rm::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_rm [OPTION] FILE" << std::endl
       << std::endl
       << "Remove (unlink) the FILE(s) or directory(ies)." << std::endl
       << std::endl
       << "  -R        remove directories and their contents recursively"
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_rm hdfs://localhost.localdomain:8020/dir/file" << std::endl
       << "hdfs_rm -R /dir1/dir2" << std::endl;
  return desc.str();
}

bool Rm::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS rm tool" << std::endl;
    return false;
  }

  if (!ValidateConstraints()) {
    std::cout << GetDescription();
    return false;
  }

  if (opt_val_.count("help") > 0) {
    return HandleHelp();
  }

  if (opt_val_.count("path") > 0) {
    const auto path = opt_val_["path"].as<std::string>();
    const auto recursive = opt_val_.count("recursive") > 0;
    return HandlePath(recursive, path);
  }

  return false;
}

bool Rm::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Rm::HandlePath(const bool recursive, const std::string &path) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (fs == nullptr) {
    std::cerr << "Could not connect the file system." << std::endl;
    return false;
  }

  const auto status = fs->Delete(uri.get_path(), recursive);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
