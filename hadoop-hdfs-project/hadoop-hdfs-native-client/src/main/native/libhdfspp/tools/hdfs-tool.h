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

#ifndef LIBHDFSPP_TOOLS_HDFS_TOOL
#define LIBHDFSPP_TOOLS_HDFS_TOOL

#include <boost/program_options.hpp>

namespace hdfs::tools {
namespace po = boost::program_options;

/**
 * {@class HdfsTool} is the base class for HDFS utility tools.
 * It serves as an interface for data flow from the command-line
 * to invoking the corresponding HDFS API.
 */
class HdfsTool {
public:
  /**
   * @param argc Count of the arguments on command-line.
   * @param argv Pointer to pointer to an array of chars containing the
   * command-line arguments.
   */
  HdfsTool(const int argc, char **argv) : argc_{argc}, argv_{argv} {}

  // Abiding to the Rule of 5
  HdfsTool(const HdfsTool &) = default;
  HdfsTool(HdfsTool &&) = default;
  HdfsTool &operator=(const HdfsTool &) = delete;
  HdfsTool &operator=(HdfsTool &&) = delete;
  virtual ~HdfsTool();

  /**
   * @return The description of this tool.
   */
  [[nodiscard]] virtual std::string GetDescription() const = 0;

  /**
   * Perform the core task of this tool.
   *
   * @return A boolean indicating the result of the task performed by this tool.
   */
  [[nodiscard]] virtual bool Do() = 0;

protected:
  /**
   * Initialize the members. It's expected that the Do method calls
   * Initialize method before doing anything. We're doing the
   * initialization in a method (instead of the constructor) for better
   * handling. We typically do the parsing of the command-line arguments here.
   *
   * @return A boolean indicating the result of the initialization.
   */
  [[nodiscard]] virtual bool Initialize() = 0;

  /**
   * Validates whether the tool has the necessary input data to perform its
   * task.
   *
   * @return A boolean indicating the result of the validation.
   *
   */
  [[nodiscard]] virtual bool ValidateConstraints() const = 0;

  /**
   * All derivatives of HdfsTool must implement a way to help the user,
   * by displaying the relevant information about the tool in the general case.
   *
   * @return A boolean indicating the result of the help task.
   */
  [[nodiscard]] virtual bool HandleHelp() const = 0;

  /**
   * Count of the arguments on command-line.
   */
  int argc_{0};

  /**
   * Pointer to pointer to an array of chars containing the command-line
   * arguments.
   */
  char **argv_{nullptr};

  /**
   * A boost data-structure containing the mapping between the option and the
   * value passed to the command-line.
   */
  po::variables_map opt_val_;

  /**
   * A boost data-structure containing the description of the options supported
   * by this tool and also, the description of the tool itself.
   */
  po::options_description opt_desc_;
};
} // namespace hdfs::tools

#endif