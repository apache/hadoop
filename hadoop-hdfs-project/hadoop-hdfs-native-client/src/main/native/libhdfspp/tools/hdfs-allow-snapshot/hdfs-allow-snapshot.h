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

#ifndef LIBHDFSPP_TOOLS_HDFS_ALLOW_SNAPSHOT
#define LIBHDFSPP_TOOLS_HDFS_ALLOW_SNAPSHOT

#include <string>

#include <boost/program_options.hpp>

namespace hdfs::tools {
namespace po = boost::program_options;

class AllowSnapshot {
public:
  AllowSnapshot(int argc, char **argv);

  [[nodiscard]] bool Initialize();
  [[nodiscard]] bool ValidateConstraints() const { return argc_ > 1; }
  static std::string GetDescription();
  [[nodiscard]] bool Do();
  [[nodiscard]] bool HandleHelp() const;
  [[nodiscard]] bool HandlePath(const std::string &path) const;

private:
  int argc_;
  char **argv_;
  po::variables_map opt_val_;
  po::options_description opt_desc_;
  po::positional_options_description pos_opt_desc_;
};

} // namespace hdfs::tools
#endif
