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

#include "hdfs-setrep.h"
#include "internal/set-replication-state.h"
#include "tools_common.h"

namespace hdfs::tools {
Setrep::Setrep(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Setrep::Initialize() {
  auto add_options = opt_desc_.add_options();
  add_options("help,h",
              "Changes the replication factor of a file at PATH. If PATH is a "
              "directory then the command recursively changes the replication "
              "factor of all files under the directory tree rooted at PATH.");
  add_options(
      "replication-factor", po::value<std::string>(),
      "The replication factor to set for the given path and its children.");
  add_options("path", po::value<std::string>(),
              "The path for which the replication factor needs to be set.");

  // We allow only one positional argument to be passed to this tool. An
  // exception is thrown if multiple arguments are passed.
  pos_opt_desc_.add("replication-factor", 1);
  pos_opt_desc_.add("path", 1);

  po::store(po::command_line_parser(argc_, argv_)
                .options(opt_desc_)
                .positional(pos_opt_desc_)
                .run(),
            opt_val_);
  po::notify(opt_val_);
  return true;
}

bool Setrep::ValidateConstraints() const {
  // Only "help" is allowed as single argument.
  if (argc_ == 2) {
    return opt_val_.count("help");
  }

  // Rest of the cases must contain more than 2 arguments on the command line.
  return argc_ > 2;
}

std::string Setrep::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_setrep [OPTION] NUM_REPLICAS PATH" << std::endl
       << std::endl
       << "Changes the replication factor of a file at PATH. If PATH is a "
          "directory then the command"
       << std::endl
       << "recursively changes the replication factor of all files under the "
          "directory tree rooted at PATH."
       << std::endl
       << std::endl
       << "  -h  display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_setrep 5 hdfs://localhost.localdomain:8020/dir/file"
       << std::endl
       << "hdfs_setrep 3 /dir1/dir2" << std::endl;
  return desc.str();
}

bool Setrep::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS setrep tool" << std::endl;
    return false;
  }

  if (!ValidateConstraints()) {
    std::cout << GetDescription();
    return false;
  }

  if (opt_val_.count("help") > 0) {
    return HandleHelp();
  }

  if (opt_val_.count("path") > 0 && opt_val_.count("replication-factor") > 0) {
    const auto replication_factor =
        opt_val_["replication-factor"].as<std::string>();
    const auto path = opt_val_["path"].as<std::string>();
    return HandlePath(path, replication_factor);
  }

  return false;
}

bool Setrep::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Setrep::HandlePath(const std::string &path,
                        const std::string &replication_factor) const {
  // Building a URI object from the given path.
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect to the file system." << std::endl;
    return false;
  }

  /*
   * Wrap async FileSystem::SetReplication with promise to make it a blocking
   * call.
   */
  auto promise = std::make_shared<std::promise<hdfs::Status>>();
  std::future future(promise->get_future());
  auto handler = [promise](const hdfs::Status &s) { promise->set_value(s); };

  const auto replication = static_cast<uint16_t>(
      std::strtol(replication_factor.c_str(), nullptr, 8));
  /*
   * Allocating shared state, which includes:
   * replication to be set, handler to be called, request counter, and a boolean
   * to keep track if find is done
   */
  auto state =
      std::make_shared<SetReplicationState>(replication, handler, 0, false);

  /*
   * Keep requesting more from Find until we process the entire listing. Call
   * handler when Find is done and request counter is 0. Find guarantees that
   * the handler will only be called once at a time so we do not need locking in
   * handler_find.
   */
  auto handler_find = [fs, state](const hdfs::Status &status_find,
                                  const std::vector<hdfs::StatInfo> &stat_infos,
                                  const bool has_more_results) -> bool {
    /*
     * For each result returned by Find we call async SetReplication with the
     * handler below. SetReplication DOES NOT guarantee that the handler will
     * only be called once at a time, so we DO need locking in
     * handler_set_replication.
     */
    auto handler_set_replication =
        [state](const hdfs::Status &status_set_replication) {
          std::lock_guard guard(state->lock);

          // Decrement the counter once since we are done with this async call.
          if (!status_set_replication.ok() && state->status.ok()) {
            // We make sure we set state->status only on the first error.
            state->status = status_set_replication;
          }
          state->request_counter--;
          if (state->request_counter == 0 && state->find_is_done) {
            state->handler(state->status); // Exit.
          }
        };
    if (!stat_infos.empty() && state->status.ok()) {
      for (hdfs::StatInfo const &stat_info : stat_infos) {
        // Launch an asynchronous call to SetReplication for every returned
        // file.
        if (stat_info.file_type == hdfs::StatInfo::IS_FILE) {
          state->request_counter++;
          fs->SetReplication(stat_info.full_path, state->replication,
                             handler_set_replication);
        }
      }
    }

    /*
     * Lock this section because handlerSetReplication might be accessing the
     * same shared variables simultaneously.
     */
    std::lock_guard guard(state->lock);
    if (!status_find.ok() && state->status.ok()) {
      // We make sure we set state->status only on the first error.
      state->status = status_find;
    }
    if (!has_more_results) {
      state->find_is_done = true;
      if (state->request_counter == 0) {
        state->handler(state->status); // Exit.
      }
      return false;
    }
    return true;
  };

  // Asynchronous call to Find.
  fs->Find(uri.get_path(), "*", hdfs::FileSystem::GetDefaultFindMaxDepth(),
           handler_find);

  // Block until promise is set.
  const auto status = future.get();
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
