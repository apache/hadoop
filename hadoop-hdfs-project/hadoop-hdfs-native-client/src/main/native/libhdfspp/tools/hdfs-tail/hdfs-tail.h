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

#ifndef LIBHDFSPP_TOOLS_HDFS_TAIL
#define LIBHDFSPP_TOOLS_HDFS_TAIL

#include <string>

#include <boost/program_options.hpp>

#include "hdfs-tool.h"

namespace hdfs::tools {
/**
 * {@class Tail} is an {@class HdfsTool} displays last kilobyte of the file to
 * stdout.
 */
class Tail : public HdfsTool {
public:
  /**
   * {@inheritdoc}
   */
  Tail(int argc, char **argv);

  // Abiding to the Rule of 5
  Tail(const Tail &) = default;
  Tail(Tail &&) = default;
  Tail &operator=(const Tail &) = delete;
  Tail &operator=(Tail &&) = delete;
  ~Tail() override = default;

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
   * @param path The path to the file which needs to be tailed.
   * @param follow Append data to the output as the file grows, as in Unix.
   *
   * @return A boolean indicating the result of this operation.
   */
  [[nodiscard]] virtual bool HandlePath(const std::string &path,
                                        bool follow) const;

  /**
   * The tail size in bytes.
   */
  static constexpr uint64_t tail_size_in_bytes{1024};

  /**
   * The refresh rate for {@link hdfs::tools::Tail} in seconds.
   */
  static constexpr int refresh_rate_in_sec{1};

private:
  /**
   * A boost data-structure containing the description of positional arguments
   * passed to the command-line.
   */
  po::positional_options_description pos_opt_desc_;
};
} // namespace hdfs::tools
#endif
