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

#ifndef LIBHDFSPP_TOOLS_HDFS_LS
#define LIBHDFSPP_TOOLS_HDFS_LS

#include <string>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"

namespace hdfs::tools {
/**
 * {@class Ls} is an {@class HdfsTool} that lists information about the files.
 */
class Ls : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  Ls(int argc, char **argv);

  // Abiding to the Rule of 5
  Ls(const Ls &) = default;
  Ls(Ls &&) = default;
  Ls &operator=(const Ls &) = delete;
  Ls &operator=(Ls &&) = delete;
  ~Ls() override = default;

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
   * @param path The path to the directory for which we need to ls.
   * @param recursive A boolean indicating whether ls needs to be
   * performed recursively for the given path.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool HandlePath(const std::string &path,
                                        bool recursive) const;

private:
  /**
   * A boost data-structure containing the description of positional arguments
   * passed to the command-line.
   */
  po::positional_options_description pos_opt_desc_;
};
} // namespace hdfs::tools
#endif
