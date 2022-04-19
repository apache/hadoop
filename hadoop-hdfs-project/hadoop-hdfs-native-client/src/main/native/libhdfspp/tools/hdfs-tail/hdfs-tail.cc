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

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>

#include "hdfs-tail.h"
#include "tools_common.h"

namespace hdfs::tools {
Tail::Tail(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Tail::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h", "Displays last kilobyte of the file to stdout.");
  add_options("follow,f",
              "Append data to the output as the file grows, as in Unix.");
  add_options("path", po::value<std::string>(),
              "The path indicating the filesystem that needs to be tailed.");

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

std::string Tail::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_tail [OPTION] FILE" << std::endl
       << std::endl
       << "Displays last kilobyte of the file to stdout." << std::endl
       << std::endl
       << "  -f  append data to the output as the file grows, as in Unix"
       << std::endl
       << "  -h  display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_tail hdfs://localhost.localdomain:8020/dir/file" << std::endl
       << "hdfs_tail /dir/file" << std::endl;
  return desc.str();
}

bool Tail::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS tail tool" << std::endl;
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
    const auto follow = opt_val_.count("follow") > 0;
    return HandlePath(path, follow);
  }

  return false;
}

bool Tail::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Tail::HandlePath(const std::string &path, const bool follow) const {
  // Building a URI object from the given path.
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect to the file system." << std::endl;
    return false;
  }

  // We need to get the size of the file using stat.
  hdfs::StatInfo stat_info;
  auto status = fs->GetFileInfo(uri.get_path(), stat_info);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }

  // Determine where to start reading.
  off_t offset{0};
  if (stat_info.length > tail_size_in_bytes) {
    offset = static_cast<off_t>(stat_info.length - tail_size_in_bytes);
  }

  do {
    const auto current_length = static_cast<off_t>(stat_info.length);
    readFile(fs, uri.get_path(), offset, stdout, false);

    // Exit if -f flag was not set.
    if (!follow) {
      break;
    }

    do {
      // Sleep for the refresh rate.
      std::this_thread::sleep_for(std::chrono::seconds(refresh_rate_in_sec));

      // Use stat to check the new file size.
      status = fs->GetFileInfo(uri.get_path(), stat_info);
      if (!status.ok()) {
        std::cerr << "Error: " << status.ToString() << std::endl;
        return false;
      }

      // If file became longer, loop back and print the difference.
    } while (static_cast<off_t>(stat_info.length) <= current_length);
  } while (true);

  return true;
}
} // namespace hdfs::tools
