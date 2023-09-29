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

#ifndef LIBHDFSPP_TOOLS_HDFS_DELETE_SNAPSHOT
#define LIBHDFSPP_TOOLS_HDFS_DELETE_SNAPSHOT

#include <string>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"

namespace hdfs::tools {
/**
 * {@class DeleteSnapshot} is an {@class HdfsTool} that facilitates the
 * snapshots of a directory at PATH to be created, causing the directory to be
 * snapshot-able.
 */
class DeleteSnapshot : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  DeleteSnapshot(int argc, char **argv);

  // Abiding to the Rule of 5
  DeleteSnapshot(const DeleteSnapshot &) = default;
  DeleteSnapshot(DeleteSnapshot &&) = default;
  DeleteSnapshot &operator=(const DeleteSnapshot &) = delete;
  DeleteSnapshot &operator=(DeleteSnapshot &&) = delete;
  ~DeleteSnapshot() override = default;

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
   * Handle the arguments that are passed to this tool.
   *
   * @param path The path to the directory that is snapshot-able.
   * @param name The name of the snapshot.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool HandleSnapshot(const std::string &path,
                                            const std::string &name) const;

private:
  /**
   * A boost data-structure containing the description of positional arguments
   * passed to the command-line.
   */
  po::positional_options_description pos_opt_desc_;
};
} // namespace hdfs::tools
#endif
