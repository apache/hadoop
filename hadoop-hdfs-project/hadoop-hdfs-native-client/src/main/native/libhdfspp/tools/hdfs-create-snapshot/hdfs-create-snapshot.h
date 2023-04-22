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

#ifndef LIBHDFSPP_TOOLS_HDFS_CREATE_SNAPSHOT
#define LIBHDFSPP_TOOLS_HDFS_CREATE_SNAPSHOT

#include <optional>
#include <string>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"

namespace hdfs::tools {
/**
 * {@class CreateSnapshot} is an {@class HdfsTool} that facilitates the creation
 * of the snapshot of a snapshot-able directory located at PATH.
 */
class CreateSnapshot : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  CreateSnapshot(int argc, char **argv);

  // Abiding to the Rule of 5
  CreateSnapshot(const CreateSnapshot &) = default;
  CreateSnapshot(CreateSnapshot &&) = default;
  CreateSnapshot &operator=(const CreateSnapshot &) = delete;
  CreateSnapshot &operator=(CreateSnapshot &&) = delete;
  ~CreateSnapshot() override = default;

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
   * Handle the path argument that's passed to this tool.
   *
   * @param path The path to the snapshot-able directory for which the snapshot
   * needs to be created.
   * @param name The name to assign to the snapshot after creating it.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool
  HandleSnapshot(const std::string &path,
                 const std::optional<std::string> &name = std::nullopt) const;

private:
  /**
   * A boost data-structure containing the description of positional arguments
   * passed to the command-line.
   */
  po::positional_options_description pos_opt_desc_;
};
} // namespace hdfs::tools

#endif
