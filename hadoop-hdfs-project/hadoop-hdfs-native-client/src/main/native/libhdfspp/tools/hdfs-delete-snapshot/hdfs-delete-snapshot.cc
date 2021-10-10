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

#include "hdfs-delete-snapshot.h"
#include "tools_common.h"

namespace hdfs::tools {
DeleteSnapshot::DeleteSnapshot(const int argc, char **argv)
    : HdfsTool(argc, argv) {}

bool DeleteSnapshot::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Show the help for hdfs_deleteSnapshot");
  add_options("path", po::value<std::string>(),
              "The path to the directory that is snapshot-able");
  add_options("name", po::value<std::string>(), "The name of the snapshot");

  // Register "path" and "name" as positional arguments.
  // We allow only two arguments to be passed to this tool. An exception is
  // thrown if more than two arguments are passed.
  pos_opt_desc_.add("path", 1);
  pos_opt_desc_.add("name", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

bool DeleteSnapshot::ValidateConstraints() const {
  // Only "help" is allowed as single argument
  if (argc_ == 2) {
    if (opt_val_.count("help") > 0) {
      return true;
    }
    return false;
  }

  // Rest of the cases must contain more than 1 argument on the command line
  return argc_ > 2;
}

std::string DeleteSnapshot::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_deleteSnapshot [OPTION] PATH NAME" << std::endl
       << std::endl
       << "Delete a snapshot NAME from a snapshot-able directory." << std::endl
       << "This operation requires owner privilege of the snapshot-able "
          "directory."
       << std::endl
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_deleteSnapshot hdfs://localhost.localdomain:8020/dir mySnapshot"
       << std::endl
       << "hdfs_deleteSnapshot /dir1/dir2 mySnapshot" << std::endl;
  return desc.str();
}

bool DeleteSnapshot::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS delete snapshot tool" << std::endl;
    return false;
  }

  if (!ValidateConstraints()) {
    std::cout << GetDescription();
    return false;
  }

  if (opt_val_.count("help") > 0) {
    return HandleHelp();
  }

  if (opt_val_.count("path") > 0 && opt_val_.count("name") > 0) {
    const auto path = opt_val_["path"].as<std::string>();
    const auto name = opt_val_["name"].as<std::string>();
    return HandleSnapshot(path, name);
  }

  return true;
}

bool DeleteSnapshot::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool DeleteSnapshot::HandleSnapshot(const std::string &path,
                                    const std::string &name) const {
  // Building a URI object from the given path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  const auto status = fs->DeleteSnapshot(uri.get_path(), name);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
