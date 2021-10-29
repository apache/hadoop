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
#include <optional>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-create-snapshot.h"
#include "tools_common.h"

namespace hdfs::tools {
CreateSnapshot::CreateSnapshot(const int argc, char **argv)
    : HdfsTool(argc, argv) {}

bool CreateSnapshot::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Show the help for hdfs_createSnapshot");
  add_options("path", po::value<std::string>(),
              "The path to the directory for creating the snapshot");
  add_options("name,n", po::value<std::string>(),
              "The snapshot name, a default name is selected if omitted");

  // We allow only one argument to be passed to path option. An exception is
  // thrown if multiple arguments are passed to this.
  pos_opt_desc_.add("path", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

bool CreateSnapshot::ValidateConstraints() const {
  // If the name option is specified, there will be 4 arguments in total
  if (argc_ == 4) {
    return opt_val_.count("name") > 0;
  }

  // Rest of the cases must contain more than 1 argument on the command line
  return argc_ > 1;
}

std::string CreateSnapshot::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_createSnapshot [OPTION] PATH" << std::endl
       << std::endl
       << "Create a snapshot of a snapshot-able directory." << std::endl
       << "This operation requires owner privilege of the snapshot-able "
          "directory."
       << std::endl
       << std::endl
       << "  -n NAME   The snapshot name. When it is omitted, a default name "
          "is generated"
       << std::endl
       << "             using a timestamp with the format:" << std::endl
       << R"(             "'s'yyyyMMdd-HHmmss.SSS", e.g. s20130412-151029.033)"
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_createSnapshot hdfs://localhost.localdomain:8020/dir"
       << std::endl
       << "hdfs_createSnapshot -n MySnapshot /dir1/dir2" << std::endl;
  return desc.str();
}

bool CreateSnapshot::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS create snapshot tool" << std::endl;
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
    const auto name = opt_val_.count("name") > 0
                          ? std::optional{opt_val_["name"].as<std::string>()}
                          : std::nullopt;
    return HandleSnapshot(path, name);
  }

  return true;
}

bool CreateSnapshot::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool CreateSnapshot::HandleSnapshot(
    const std::string &path, const std::optional<std::string> &name) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  const auto status = fs->CreateSnapshot(uri.get_path(), name.value_or(""));
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
