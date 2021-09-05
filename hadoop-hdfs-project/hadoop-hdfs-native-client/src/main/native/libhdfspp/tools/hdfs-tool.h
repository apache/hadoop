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

class HdfsTool {
public:
  HdfsTool(const int argc, char **argv) : argc_{argc}, argv_{argv} {}
  virtual ~HdfsTool();

  [[nodiscard]] virtual bool Initialize() = 0;
  [[nodiscard]] virtual bool ValidateConstraints() const = 0;
  [[nodiscard]] virtual std::string GetDescription() const = 0;
  [[nodiscard]] virtual bool Do() = 0;
  [[nodiscard]] virtual bool HandleHelp() const = 0;

protected:
  int argc_{0};
  char **argv_{nullptr};
  po::variables_map opt_val_;
  po::options_description opt_desc_;
};
} // namespace hdfs::tools

#endif