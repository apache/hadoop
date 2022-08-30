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

#ifndef LIBHDFSPP_TOOLS_HDFS_MKDIR
#define LIBHDFSPP_TOOLS_HDFS_MKDIR

#include <optional>
#include <string>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"

namespace hdfs::tools {
/**
 * {@class Mkdir} is an {@class HdfsTool} that creates directory if it does not
 * exist.
 */
class Mkdir : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  Mkdir(int argc, char **argv);

  // Abiding to the Rule of 5
  Mkdir(const Mkdir &) = default;
  Mkdir(Mkdir &&) = default;
  Mkdir &operator=(const Mkdir &) = delete;
  Mkdir &operator=(Mkdir &&) = delete;
  ~Mkdir() override = default;

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
  [[nodiscard]] bool ValidateConstraints() const override { return argc_ > 1; }

  /**
   * {@inheritdoc}
   */
  [[nodiscard]] bool HandleHelp() const override;

  /**
   * Handle the path argument that's passed to this tool.
   *
   * @param create_parents Creates parent directories as needed if this boolean
   * is set to true.
   * @param permissions An octal representation of the permissions to be stamped
   * to each directory that gets created.
   * @param path The path in the filesystem where the directory must be created.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool
  HandlePath(bool create_parents, const std::optional<std::string> &permissions,
             const std::string &path) const;

  /**
   * @param permissions The permissions string to convert to octal value.
   * @return The octal representation of the permissions supplied as parameter
   * to this tool.
   */
  [[nodiscard]] static uint16_t
  GetPermissions(const std::optional<std::string> &permissions);

private:
  /**
   * A boost data-structure containing the description of positional arguments
   * passed to the command-line.
   */
  po::positional_options_description pos_opt_desc_;
};
} // namespace hdfs::tools
#endif
