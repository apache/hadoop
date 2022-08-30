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
#include <optional>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-mkdir.h"
#include "tools_common.h"

namespace hdfs::tools {
Mkdir::Mkdir(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Mkdir::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Create directory if it does not exist");
  add_options("create-parents,p", "Create parent directories as needed");
  add_options(
      "mode,m", po::value<std::string>(),
      "Set the permissions for the new directory (and newly created parents if "
      "any). The permissions are specified in octal representation");
  add_options("path", po::value<std::string>(),
              "The path to the directory that needs to be created");

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

std::string Mkdir::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_mkdir [OPTION] DIRECTORY" << std::endl
       << std::endl
       << "Create the DIRECTORY(ies), if they do not already exist."
       << std::endl
       << std::endl
       << "  -p        make parent directories as needed" << std::endl
       << "  -m  MODE  set file mode (octal permissions) for the new "
          "DIRECTORY(ies)"
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_mkdir hdfs://localhost.localdomain:8020/dir1/dir2" << std::endl
       << "hdfs_mkdir -p /extant_dir/non_extant_dir/non_extant_dir/new_dir"
       << std::endl;
  return desc.str();
}

bool Mkdir::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS mkdir tool" << std::endl;
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
    const auto create_parents = opt_val_.count("create-parents") > 0;
    const auto permissions =
        opt_val_.count("mode") > 0
            ? std::optional(opt_val_["mode"].as<std::string>())
            : std::nullopt;
    return HandlePath(create_parents, permissions, path);
  }

  return false;
}

bool Mkdir::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Mkdir::HandlePath(const bool create_parents,
                       const std::optional<std::string> &permissions,
                       const std::string &path) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (fs == nullptr) {
    std::cerr << "Could not connect the file system." << std::endl;
    return false;
  }

  const auto status =
      fs->Mkdirs(uri.get_path(), GetPermissions(permissions), create_parents);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }

  return true;
}

uint16_t Mkdir::GetPermissions(const std::optional<std::string> &permissions) {
  if (permissions) {
    // TODO : Handle the error returned by std::strtol.
    return static_cast<uint16_t>(
        std::strtol(permissions.value().c_str(), nullptr, 8));
  }

  return hdfs::FileSystem::GetDefaultPermissionMask();
}
} // namespace hdfs::tools
