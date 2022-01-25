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
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "hdfs-chown.h"
#include "tools_common.h"

namespace hdfs::tools {
Chown::Chown(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Chown::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options(
      "help,h",
      "Change the owner and/or group of each FILE to OWNER and/or GROUP.");
  add_options("file", po::value<std::string>(),
              "The path to the file whose ownership needs to be modified");
  add_options("recursive,R", "Operate on files and directories recursively");
  add_options(
      "user-group", po::value<std::string>(),
      "The user:group to which the file's ownership needs to be changed to");

  // An exception is thrown if these arguments are missing or if the arguments'
  // count doesn't tally.
  pos_opt_desc_.add("user-group", 1);
  pos_opt_desc_.add("file", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

bool Chown::ValidateConstraints() const {
  // Only "help" is allowed as single argument
  if (argc_ == 2) {
    return opt_val_.count("help");
  }

  // Rest of the cases must contain more than 2 arguments on the command line
  return argc_ > 2;
}

std::string Chown ::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_chown [OPTION] [OWNER][:[GROUP]] FILE" << std::endl
       << std::endl
       << "Change the owner and/or group of each FILE to OWNER and/or GROUP."
       << std::endl
       << "The user must be a super-user. Additional information is in the "
          "Permissions Guide:"
       << std::endl
       << "https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/"
          "hadoop-hdfs/HdfsPermissionsGuide.html"
       << std::endl
       << std::endl
       << "  -R  operate on files and directories recursively" << std::endl
       << "  -h  display this help and exit" << std::endl
       << std::endl
       << "Owner is unchanged if missing.  Group is unchanged if missing."
       << std::endl
       << "OWNER and GROUP may be numeric as well as symbolic." << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_chown -R new_owner:new_group "
          "hdfs://localhost.localdomain:8020/dir/file"
       << std::endl
       << "hdfs_chown new_owner /dir/file" << std::endl;
  return desc.str();
}

bool Chown::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS chown tool" << std::endl;
    return false;
  }

  if (!ValidateConstraints()) {
    std::cout << GetDescription();
    return false;
  }

  if (opt_val_.count("help") > 0) {
    return HandleHelp();
  }

  if (opt_val_.count("file") > 0 && opt_val_.count("user-group") > 0) {
    const auto file = opt_val_["file"].as<std::string>();
    const auto recursive = opt_val_.count("recursive") > 0;
    const Ownership ownership(opt_val_["user-group"].as<std::string>());
    return HandlePath(ownership, recursive, file);
  }

  return true;
}

bool Chown::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Chown::HandlePath(const Ownership &ownership, const bool recursive,
                       const std::string &file) const {
  // Building a URI object from the given uri_path
  auto uri = hdfs::parse_path_or_exit(file);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    return false;
  }

  // Wrap async FileSystem::SetOwner with promise to make it a blocking call
  auto promise = std::make_shared<std::promise<hdfs::Status>>();
  auto future(promise->get_future());
  auto handler = [promise](const hdfs::Status &s) { promise->set_value(s); };

  if (!recursive) {
    fs->SetOwner(uri.get_path(), ownership.GetUser(),
                 ownership.GetGroup().value_or(""), handler);
  } else {
    /*
     * Allocating shared state, which includes: username and groupname to be
     * set, handler to be called, request counter, and a boolean to keep track
     * if find is done
     */
    auto state = std::make_shared<OwnerState>(ownership.GetUser(),
                                              ownership.GetGroup().value_or(""),
                                              handler, 0, false);

    /*
     * Keep requesting more from Find until we process the entire listing. Call
     * handler when Find is done and request counter is 0. Find guarantees that
     * the handler will only be called once at a time so we do not need locking
     * in handler_find.
     */
    auto handler_find = [fs,
                         state](const hdfs::Status &status_find,
                                const std::vector<hdfs::StatInfo> &stat_infos,
                                const bool has_more_results) -> bool {
      /*
       * For each result returned by Find we call async SetOwner with the
       * handler below. SetOwner DOES NOT guarantee that the handler will only
       * be called once at a time, so we DO need locking in handler_set_owner.
       */
      auto handler_set_owner = [state](const hdfs::Status &status_set_owner) {
        std::lock_guard guard(state->lock);

        // Decrement the counter once since we are done with this async call
        if (!status_set_owner.ok() && state->status.ok()) {
          // We make sure we set state->status only on the first error.
          state->status = status_set_owner;
        }

        state->request_counter--;
        if (state->request_counter == 0 && state->find_is_done) {
          state->handler(state->status); // exit
        }
      };

      if (!stat_infos.empty() && state->status.ok()) {
        for (const auto &s : stat_infos) {
          // Launch an asynchronous call to SetOwner for every returned result
          state->request_counter++;
          fs->SetOwner(s.full_path, state->user, state->group,
                       handler_set_owner);
        }
      }

      /*
       * Lock this section because handler_set_owner might be accessing the same
       * shared variables simultaneously.
       */
      std::lock_guard guard(state->lock);
      if (!status_find.ok() && state->status.ok()) {
        // We make sure we set state->status only on the first error.
        state->status = status_find;
      }

      if (!has_more_results) {
        state->find_is_done = true;
        if (state->request_counter == 0) {
          state->handler(state->status); // exit
        }
        return false;
      }
      return true;
    };

    // Asynchronous call to Find
    fs->Find(uri.get_path(), "*", hdfs::FileSystem::GetDefaultFindMaxDepth(),
             handler_find);
  }

  // Block until promise is set
  const auto status = future.get();
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
