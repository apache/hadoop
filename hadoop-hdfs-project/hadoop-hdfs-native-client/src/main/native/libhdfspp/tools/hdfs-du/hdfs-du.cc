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

#include "hdfs-du.h"
#include "internal/get-content-summary-state.h"
#include "tools_common.h"

namespace hdfs::tools {
Du::Du(const int argc, char **argv) : HdfsTool(argc, argv) {}

bool Du::Initialize() {
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

std::string Du::GetDescription() const {
  std::stringstream desc;
  desc << "Usage: hdfs_du [OPTION] PATH" << std::endl
       << std::endl
       << "Displays sizes of files and directories contained in the given PATH"
       << std::endl
       << "or the length of a file in case PATH is just a file" << std::endl
       << std::endl
       << "  -R        operate on files and directories recursively"
       << std::endl
       << "  -h        display this help and exit" << std::endl
       << std::endl
       << "Examples:" << std::endl
       << "hdfs_du hdfs://localhost.localdomain:8020/dir/file" << std::endl
       << "hdfs_du -R /dir1/dir2" << std::endl;
  return desc.str();
}

bool Du::Do() {
  if (!Initialize()) {
    std::cerr << "Unable to initialize HDFS du tool" << std::endl;
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

bool Du::HandleHelp() const {
  std::cout << GetDescription();
  return true;
}

bool Du::HandlePath(const std::string &path, const bool recursive) const {
  // Building a URI object from the given path.
  auto uri = hdfs::parse_path_or_exit(path);

  const auto fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect to the file system." << std::endl;
    return false;
  }

  /*
   * Wrap async FileSystem::GetContentSummary with promise to make it a blocking
   * call.
   */
  const auto promise = std::make_shared<std::promise<hdfs::Status>>();
  std::future future(promise->get_future());
  auto handler = [promise](const hdfs::Status &s) { promise->set_value(s); };

  /*
   * Allocating shared state, which includes: handler to be called, request
   * counter, and a boolean to keep track if find is done.
   */
  const auto state =
      std::make_shared<GetContentSummaryState>(handler, 0, false);

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
     * For each result returned by Find we call async GetContentSummary with the
     * handler below. GetContentSummary DOES NOT guarantee that the handler will
     * only be called once at a time, so we DO need locking in
     * handler_get_content_summary.
     */
    auto handler_get_content_summary =
        [state](const hdfs::Status &status_get_summary,
                const hdfs::ContentSummary &si) {
          std::lock_guard guard(state->lock);
          std::cout << si.str_du() << std::endl;
          // Decrement the counter once since we are done with this async call.
          if (!status_get_summary.ok() && state->status.ok()) {
            // We make sure we set state->status only on the first error.
            state->status = status_get_summary;
          }
          state->request_counter--;
          if (state->request_counter == 0 && state->find_is_done) {
            state->handler(state->status); // exit
          }
        };

    if (!stat_infos.empty() && state->status.ok()) {
      for (hdfs::StatInfo const &s : stat_infos) {
        /*
         * Launch an asynchronous call to GetContentSummary for every returned
         * result.
         */
        state->request_counter++;
        fs->GetContentSummary(s.full_path, handler_get_content_summary);
      }
    }

    /*
     * Lock this section because handler_get_content_summary might be accessing
     * the same shared variables simultaneously.
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

  // Asynchronous call to Find.
  if (!recursive) {
    fs->GetListing(uri.get_path(), handler_find);
  } else {
    fs->Find(uri.get_path(), "*", hdfs::FileSystem::GetDefaultFindMaxDepth(),
             handler_find);
  }

  // Block until promise is set.
  const auto status = future.get();
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}
} // namespace hdfs::tools
