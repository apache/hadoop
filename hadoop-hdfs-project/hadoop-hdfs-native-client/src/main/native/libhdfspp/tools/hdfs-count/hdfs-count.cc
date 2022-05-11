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

#include "hdfs-count.h"
#include "tools_common.h"

namespace hdfs::tools {
Count::Count(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Count::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options(
      "help,h",
      "Count the number of directories, files and bytes under the given path");
  add_options("show-quota,q", "Output additional columns before the rest: "
                              "QUOTA, SPACE_QUOTA, SPACE_CONSUMED");
  add_options("path", po::value<std::string>(),
              "The path to the file that needs to be count-ed");

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

std::string Count::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_count [OPTION] FILE" << std::endl
       << std::endl
       << "Count the number of directories, files and bytes under the path "
          "that match the specified FILE pattern."
       << std::endl
       << "The output columns with -count are: DIR_COUNT, FILE_COUNT, "
          "CONTENT_SIZE, PATHNAME"
       << std::endl
       << std::endl
       << "  -q    output additional columns before the rest: QUOTA, "
          "SPACE_QUOTA, SPACE_CONSUMED"
       << std::endl
       << "  -h    display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_count hdfs://localhost.localdomain:8020/dir" << std::endl
       << "hdfs_count -q /dir1/dir2" << std::endl;
  return desc.str();
}

bool Count::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS count tool" << std::endl;
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
    const auto show_quota = opt_val_.count("show-quota") > 0;
    return HandlePath(show_quota, path);
  }

  return false;
}

bool Count::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Count::HandlePath(const bool show_quota, const std::string &path) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, false);
  if (fs == nullptr) {
    std::cerr << "Could not connect the file system." << std::endl;
    return false;
  }

  hdfs::ContentSummary content_summary;
  const auto status = fs->GetContentSummary(uri.get_path(), content_summary);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }

  std::cout << content_summary.str(show_quota) << std::endl;
  return true;
}
} // namespace hdfs::tools
