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

#include <cstdio>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-copy-to-local.h"
#include "tools_common.h"

namespace hdfs::tools {
CopyToLocal::CopyToLocal(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool CopyToLocal::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Copies the file from the given HDFS source path to "
                        "the destination path on the local machine");

  add_options("source", po::value<std::string>(), "The HDFS source file path");
  add_options("target", po::value<std::string>(),
              "The target local system file path");

  // We allow only two arguments to be passed to this tool. An exception is
  // thrown if more arguments are passed.
  pos_opt_desc_.add("source", 1);
  pos_opt_desc_.add("target", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

bool CopyToLocal::ValidateConstraints() const {
  // Only "help" is allowed as single argument
  if (argc_ == 2) {
    return opt_val_.count("help") > 0;
  }

  // Rest of the cases must contain exactly 2 arguments
  return argc_ == 3;
}

std::string CopyToLocal::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_" << GetToolName() << " [OPTION] SRC_FILE DST_FILE"
       << std::endl
       << std::endl
       << "Copy SRC_FILE from hdfs to DST_FILE on the local file system."
       << std::endl
       << std::endl
       << "  -h  display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_" << GetToolName()
       << " hdfs://localhost.localdomain:8020/dir/file "
          "/home/usr/myfile"
       << std::endl
       << "hdfs_" << GetToolName() << " /dir/file /home/usr/dir/file"
       << std::endl;
  return desc.str();
}

bool CopyToLocal::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS " << GetToolName() << " tool"
              << std::endl;
    return false;
  }

  if (!ValidateConstraints()) {
    std::cout << GetDescription();
    return false;
  }

  if (opt_val_.count("help") > 0) {
    return HandleHelp();
  }

  if (opt_val_.count("source") > 0 && opt_val_.count("target") > 0) {
    const auto source = opt_val_["source"].as<std::string>();
    const auto target = opt_val_["target"].as<std::string>();
    return HandlePath(source, target);
  }

  return false;
}

bool CopyToLocal::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool CopyToLocal::HandlePath(const std::string &source,
                             const std::string &target) const {
  // Building a URI object from the given path
  auto uri = hdfs::parse_path_or_exit(source);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  auto dst_file = std::fopen(target.c_str(), "wb");
  if (!dst_file) {
    std::cerr << "Unable to open the destination file: " << target << std::endl;
    return false;
  }

  readFile(fs, uri.get_path(), 0, dst_file, false);
  std::fclose(dst_file);
  return true;
}

std::string CopyToLocal::GetToolName() const { return "copyToLocal"; }
} // namespace hdfs::tools
