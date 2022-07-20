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

#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-find.h"
#include "tools_common.h"

namespace hdfs::tools {
Find::Find(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Find::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options(
      "help,h",
      "Finds all files recursively starting from the specified PATH and prints "
      "their file paths. This hdfs_find tool mimics the POSIX find.");
  add_options(
      "name,n", po::value<std::string>(),
      "If provided, all results will be matching the NAME pattern otherwise, "
      "the implicit '*' will be used NAME allows wild-cards");
  add_options(
      "max-depth,m", po::value<uint32_t>(),
      "If provided, the maximum depth to recurse after the end of the path is "
      "reached will be limited by MAX_DEPTH otherwise, the maximum depth to "
      "recurse is unbound MAX_DEPTH can be set to 0 for pure globbing and "
      "ignoring the NAME option (no recursion after the end of the path)");
  add_options("path", po::value<std::string>(),
              "The path where we want to start the find operation");

  // We allow only one positional argument to be passed to this tool. An
  // exception is thrown if multiple arguments are passed.
  pos_opt_desc_.add("path", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

std::string Find::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_find [OPTION] PATH" << std::endl
       << std::endl
       << "Finds all files recursively starting from the" << std::endl
       << "specified PATH and prints their file paths." << std::endl
       << "This hdfs_find tool mimics the POSIX find." << std::endl
       << std::endl
       << "Both PATH and NAME can have wild-cards." << std::endl
       << std::endl
       << "  -n NAME       if provided all results will be matching the NAME "
          "pattern"
       << std::endl
       << "                otherwise, the implicit '*' will be used"
       << std::endl
       << "                NAME allows wild-cards" << std::endl
       << std::endl
       << "  -m MAX_DEPTH  if provided the maximum depth to recurse after the "
          "end of"
       << std::endl
       << "                the path is reached will be limited by MAX_DEPTH"
       << std::endl
       << "                otherwise, the maximum depth to recurse is unbound"
       << std::endl
       << "                MAX_DEPTH can be set to 0 for pure globbing and "
          "ignoring"
       << std::endl
       << "                the NAME option (no recursion after the end of the "
          "path)"
       << std::endl
       << std::endl
       << "  -h            display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_find hdfs://localhost.localdomain:8020/dir?/tree* -n "
          "some?file*name"
       << std::endl
       << "hdfs_find / -n file_name -m 3" << std::endl;
  return desc.str();
}

bool Find::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS find tool" << std::endl;
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
    const auto name =
        opt_val_.count("name") > 0 ? opt_val_["name"].as<std::string>() : "*";
    const auto max_depth = opt_val_.count("max-depth") <= 0
                               ? hdfs::FileSystem::GetDefaultFindMaxDepth()
                               : opt_val_["max-depth"].as<uint32_t>();
    return HandlePath(path, name, max_depth);
  }

  return false;
}

bool Find::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Find::HandlePath(const std::string &path, const std::string &name,
                      const uint32_t max_depth) const {
  // Building a URI object from the given path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system." << std::endl;
    return false;
  }

  const auto promise = std::make_shared<std::promise<void>>();
  std::future<void> future(promise->get_future());
  auto final_status = hdfs::Status::OK();

  /**
   * Keep requesting more until we get the entire listing. Set the promise
   * when we have the entire listing to stop.
   *
   * Find guarantees that the handler will only be called once at a time,
   * so we do not need any locking here. It also guarantees that the handler
   * will be only called once with has_more_results set to false.
   */
  auto handler = [promise,
                  &final_status](const hdfs::Status &status,
                                 const std::vector<hdfs::StatInfo> &stat_info,
                                 const bool has_more_results) -> bool {
    // Print result chunks as they arrive
    if (!stat_info.empty()) {
      for (hdfs::StatInfo const &info : stat_info) {
        std::cout << info.str() << std::endl;
      }
    }
    if (!status.ok() && final_status.ok()) {
      // We make sure we set 'status' only on the first error
      final_status = status;
    }
    if (!has_more_results) {
      promise->set_value(); // Set promise
      return false;         // Request stop sending results
    }
    return true; // request more results
  };

  // Asynchronous call to Find
  fs->Find(uri.get_path(), name, max_depth, handler);

  // Block until promise is set
  future.get();
  if (!final_status.ok()) {
    std::cerr << "Error: " << final_status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
