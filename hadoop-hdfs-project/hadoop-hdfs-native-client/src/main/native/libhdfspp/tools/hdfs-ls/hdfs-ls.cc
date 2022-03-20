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

#include <future>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "hdfs-ls.h"
#include "tools_common.h"

namespace hdfs::tools {
Ls::Ls(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Ls::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h",
              "Displays sizes of files and directories contained in the given "
              "PATH or the length of a file in case PATH is just a file");
  add_options("recursive,R", "Operate on files and directories recursively");
  add_options("path", po::value<std::string>(),
              "The path indicating the filesystem that needs to be du-ed");

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

std::string Ls::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_ls [OPTION] PATH" << std::endl
       << std::endl
       << "Lss all files recursively starting from the" << std::endl
       << "specified PATH and prints their file paths." << std::endl
       << "This hdfs_ls tool mimics the POSIX ls." << std::endl
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
       << "hdfs_ls hdfs://localhost.localdomain:8020/dir?/tree* -n "
          "some?file*name"
       << std::endl
       << "hdfs_ls / -n file_name -m 3" << std::endl;
  return desc.str();
}

bool Ls::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS ls tool" << std::endl;
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
    const auto recursive = opt_val_.count("recursive") > 0;
    return HandlePath(path, recursive);
  }

  return false;
}

bool Ls::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Ls::HandlePath(const std::string &path, const bool recursive) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  const auto promise = std::make_shared<std::promise<void>>();
  auto future(promise->get_future());
  auto result = hdfs::Status::OK();

  /**
   * Keep requesting more until we get the entire listing. Set the promise
   * when we have the entire listing to stop.
   *
   * Find and GetListing guarantee that the handler will only be called once at
   * a time, so we do not need any locking here. They also guarantee that the
   * handler will be only called once with has_more_results set to false.
   */
  auto handler = [promise,
                  &result](const hdfs::Status &status,
                           const std::vector<hdfs::StatInfo> &stat_info,
                           const bool has_more_results) -> bool {
    // Print result chunks as they arrive
    if (!stat_info.empty()) {
      for (const auto &info : stat_info) {
        std::cout << info.str() << std::endl;
      }
    }
    if (!status.ok() && result.ok()) {
      // We make sure we set 'status' only on the first error.
      result = status;
    }
    if (!has_more_results) {
      promise->set_value(); // set promise
      return false;         // request stop sending results
    }
    return true; // request more results
  };

  if (!recursive) {
    // Asynchronous call to GetListing
    fs->GetListing(uri.get_path(), handler);
  } else {
    // Asynchronous call to Find
    fs->Find(uri.get_path(), "*", hdfs::FileSystem::GetDefaultFindMaxDepth(),
             handler);
  }

  // Block until promise is set
  future.get();
  if (!result.ok()) {
    std::cerr << "Error: " << result.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
