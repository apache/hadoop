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

#include "hdfs-move-to-local.h"
#include "tools_common.h"

namespace hdfs::tools {
MoveToLocal::MoveToLocal(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool MoveToLocal::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Moves the file from the given HDFS source path to "
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

bool MoveToLocal::ValidateConstraints() const {
  // Only "help" is allowed as single argument
  if (argc_ == 2) {
    return opt_val_.count("help") > 0;
  }

  // Rest of the cases must contain exactly 2 arguments
  return argc_ == 3;
}

std::string MoveToLocal::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_moveToLocal [OPTION] SRC_FILE DST_FILE" << std::endl
       << std::endl
       << "Move SRC_FILE from hdfs to DST_FILE on the local file system."
       << std::endl
       << "Moving is done by copying SRC_FILE to DST_FILE, and then"
       << std::endl
       << "deleting DST_FILE if copy succeeded." << std::endl
       << std::endl
       << "  -h  display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_moveToLocal hdfs://localhost.localdomain:8020/dir/file "
          "/home/usr/myfile"
       << std::endl
       << "hdfs_moveToLocal /dir/file /home/usr/dir/file" << std::endl;
  return desc.str();
}

bool MoveToLocal::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS moveToLocal tool" << std::endl;
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

bool MoveToLocal::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool MoveToLocal::HandlePath(const std::string &source,
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

  readFile(fs, uri.get_path(), 0, dst_file, true);
  std::fclose(dst_file);
  return true;
}
} // namespace hdfs::tools
