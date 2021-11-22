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

#include "hdfs-rename-snapshot.h"
#include "tools_common.h"

namespace hdfs::tools {
RenameSnapshot::RenameSnapshot(const int argc, char **argv)
    : HdfsTool(argc, argv) {}

bool RenameSnapshot::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Show the help for hdfs_renameSnapshot");
  add_options("path", po::value<std::string>(),
              "The path to the directory that is snapshot-able");
  add_options("old-name", po::value<std::string>(),
              "The old/current name of the snapshot");
  add_options("new-name", po::value<std::string>(),
              "The new name for the snapshot");

  // Register "path", "old-name" and "new-name" as positional arguments.
  // We allow only three arguments to be passed to this tool. An exception is
  // thrown if more than three arguments are passed.
  pos_opt_desc_.add("path", 1);
  pos_opt_desc_.add("old-name", 1);
  pos_opt_desc_.add("new-name", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

bool RenameSnapshot::ValidateConstraints() const {
  // Only "help" is allowed as single argument
  if (argc_ == 2) {
    return opt_val_.count("help") > 0;
  }

  // Rest of the cases must contain more than 2 argument on the command line
  return argc_ > 3;
}

std::string RenameSnapshot::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_renameSnapshot [OPTION] PATH OLD_NAME NEW_NAME"
       << std::endl
       << std::endl
       << "Rename a snapshot from OLD_NAME to NEW_NAME." << std::endl
       << "This operation requires owner privilege of the snapshot-able "
          "directory."
       << std::endl
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_renameSnapshot hdfs://localhost.localdomain:8020/dir oldDir "
          "newDir"
       << std::endl
       << "hdfs_renameSnapshot /dir1/dir2 oldSnap newSnap" << std::endl;
  return desc.str();
}

bool RenameSnapshot::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS rename snapshot tool" << std::endl;
    return false;
  }

  if (!ValidateConstraints()) {
    std::cout << GetDescription();
    return false;
  }

  if (opt_val_.count("help") > 0) {
    return HandleHelp();
  }

  if (opt_val_.count("path") > 0 && opt_val_.count("old-name") > 0 &&
      opt_val_.count("new-name") > 0) {
    const auto path = opt_val_["path"].as<std::string>();
    const auto old_name = opt_val_["old-name"].as<std::string>();
    const auto new_name = opt_val_["new-name"].as<std::string>();
    return HandleSnapshot(path, old_name, new_name);
  }

  return true;
}

bool RenameSnapshot::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool RenameSnapshot::HandleSnapshot(const std::string &path,
                                    const std::string &old_name,
                                    const std::string &new_name) const {
  // Building a URI object from the given path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  const auto status = fs->RenameSnapshot(uri.get_path(), old_name, new_name);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
