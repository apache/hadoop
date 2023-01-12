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

#ifndef LIBHDFSPP_TOOLS_HDFS_CHOWN
#define LIBHDFSPP_TOOLS_HDFS_CHOWN

#include <string>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"
#include "internal/hdfs-ownership.h"

namespace hdfs::tools {
/**
 * {@class Chown} is an {@class HdfsTool} that changes the owner and/or group of
 * each file to owner and/or group.
 */
class Chown : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  Chown(int argc, char **argv);

  // Abiding to the Rule of 5
  Chown(const Chown &) = default;
  Chown(Chown &&) = default;
  Chown &operator=(const Chown &) = delete;
  Chown &operator=(Chown &&) = delete;
  ~Chown() override = default;

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
   * @param ownership The owner's user and group names.
   * @param recursive Whether this operation needs to be performed recursively
   * on all the files in the given path's sub-directory.
   * @param file The path to the file whose ownership needs to be changed.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool HandlePath(const Ownership &ownership,
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
