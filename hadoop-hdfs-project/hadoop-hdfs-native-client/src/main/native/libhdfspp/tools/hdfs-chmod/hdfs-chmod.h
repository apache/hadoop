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

#ifndef LIBHDFSPP_TOOLS_HDFS_CHMOD
#define LIBHDFSPP_TOOLS_HDFS_CHMOD

#include <functional>
#include <mutex>
#include <string>
#include <utility>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"
#include "hdfspp/status.h"

namespace hdfs::tools {
struct PermissionState {
  PermissionState(const uint16_t permissions,
                  std::function<void(const hdfs::Status &)> handler,
                  const uint64_t request_counter, const bool find_is_done)
      : permissions(permissions), handler(std::move(handler)),
        request_counter(request_counter), find_is_done(find_is_done) {}

  const uint16_t permissions;
  const std::function<void(const hdfs::Status &)> handler;

  /**
   * The request counter is incremented once every time SetOwner async call is
   * made
   */
  uint64_t request_counter;

  /**
   * This boolean will be set when find returns the last result
   */
  bool find_is_done{false};

  /**
   * Final status to be returned
   */
  hdfs::Status status;

  /**
   * Shared variables will need protection with a lock
   */
  std::mutex lock;
};

/**
 * {@class Chmod} is an {@class HdfsTool} that changes the permissions to a
 * file or folder.
 */
class Chmod : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  Chmod(int argc, char **argv);

  // Abiding to the Rule of 5
  Chmod(const Chmod &) = default;
  Chmod(Chmod &&) = default;
  Chmod &operator=(const Chmod &) = delete;
  Chmod &operator=(Chmod &&) = delete;
  ~Chmod() override = default;

  /**
   * {@inheritdoc}
   */
  [[nodiscard]] std::string GetDescription() const override;

  /**
   * {@inheritdoc}
   */
  [[nodiscard]] bool Do() override;

protected:
  /**
   * {@inheritdoc}
   */
  [[nodiscard]] bool Initialize() override;

  /**
   * {@inheritdoc}
   */
  [[nodiscard]] bool ValidateConstraints() const override;

  /**
   * {@inheritdoc}
   */
  [[nodiscard]] bool HandleHelp() const override;

  /**
   * Handle the file to the file argument that's passed to this tool.
   *
   * @param permissions An octal representation of the new permissions to be
   * assigned.
   * @param recursive Whether this operation needs to be performed recursively
   * on all the files in the given path's sub-directory.
   * @param file The path to the file whose ownership needs to be changed.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool HandlePath(const std::string &permissions,
                                        bool recursive,
                                        const std::string &file) const;

private:
  /**
   * A boost data-structure containing the description of positional arguments
   * passed to the command-line.
   */
  po::positional_options_description pos_opt_desc_;
};
} // namespace hdfs::tools

#endif
