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

#include "hdfs-disallow-snapshot.h"
#include "tools_common.h"

namespace hdfs::tools {
DisallowSnapshot::DisallowSnapshot(const int argc, char **argv)
    : HdfsTool(argc, argv) {}

bool DisallowSnapshot::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Show the help for hdfs_disallowSnapshot");
  add_options(
      "path", po::value<std::string>(),
      "The path to the directory for which snapshot-ing must be disallowed");

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

std::string DisallowSnapshot::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_disallowSnapshot [OPTION] PATH" << std::endl
       << std::endl
       << "Disallowing snapshots of a directory at PATH to be created."
       << std::endl
       << "All snapshots of the directory must be deleted before disallowing "
          "snapshots."
       << std::endl
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_disallowSnapshot hdfs://localhost.localdomain:8020/dir"
       << std::endl
       << "hdfs_disallowSnapshot /dir1/dir2" << std::endl;
  return desc.str();
}

bool DisallowSnapshot::Do() {
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
    return HandleSnapshot(path);
  }

  return true;
}

bool DisallowSnapshot::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool DisallowSnapshot::HandleSnapshot(const std::string &path) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  const auto status = fs->DisallowSnapshot(uri.get_path());
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
