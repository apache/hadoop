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

#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-allow-snapshot.h"
#include "tools_common.h"

namespace hdfs::tools {
AllowSnapshot::AllowSnapshot(const int argc, char **argv)
    : HdfsTool(argc, argv) {}

bool AllowSnapshot::Initialize() {
  opt_desc_.add_options()("help,h", "Show the help for hdfs_allowSnapshot")(
      "path", po::value<std::string>(),
      "The path to the directory to make it snapshot-able");

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

std::string AllowSnapshot::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_allowSnapshot [OPTION] PATH" << std::endl
       << std::endl
       << "Allowing snapshots of a directory at PATH to be created."
       << std::endl
       << "If the operation completes successfully, the directory becomes "
          "snapshottable."
       << std::endl
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_allowSnapshot hdfs://localhost.localdomain:8020/dir"
       << std::endl
       << "hdfs_allowSnapshot /dir1/dir2" << std::endl;
  return desc.str();
}

bool AllowSnapshot::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS allow snapshot tool" << std::endl;
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
    return HandlePath(path);
  }

  return true;
}

bool AllowSnapshot::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool AllowSnapshot::HandlePath(const std::string &path) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (fs == nullptr) {
    std::cerr << "Could not connect to the file system. " << std::endl;
    return false;
  }

  const auto status = fs->AllowSnapshot(uri.get_path());
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools