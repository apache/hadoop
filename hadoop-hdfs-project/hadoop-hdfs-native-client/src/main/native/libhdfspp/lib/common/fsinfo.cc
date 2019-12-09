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

#include <hdfspp/fsinfo.h>
#include <sstream>
#include <iomanip>

namespace hdfs {

FsInfo::FsInfo()
  : capacity(0),
    used(0),
    remaining(0),
    under_replicated(0),
    corrupt_blocks(0),
    missing_blocks(0),
    missing_repl_one_blocks(0),
    blocks_in_future(0) {
}

std::string FsInfo::str(const std::string fs_name) const {
  std::string fs_name_label = "Filesystem";
  std::string size = std::to_string(capacity);
  std::string size_label = "Size";
  std::string used = std::to_string(this->used);
  std::string used_label = "Used";
  std::string available = std::to_string(remaining);
  std::string available_label = "Available";
  std::string use_percentage = std::to_string(this->used * 100 / capacity) + "%";
  std::string use_percentage_label = "Use%";
  std::stringstream ss;
  ss  << std::left << std::setw(std::max(fs_name.size(), fs_name_label.size())) << fs_name_label
      << std::right << std::setw(std::max(size.size(), size_label.size()) + 2) << size_label
      << std::right << std::setw(std::max(used.size(), used_label.size()) + 2) << used_label
      << std::right << std::setw(std::max(available.size(), available_label.size()) + 2) << available_label
      << std::right << std::setw(std::max(use_percentage.size(), use_percentage_label.size()) + 2) << use_percentage_label
      << std::endl
      << std::left << std::setw(std::max(fs_name.size(), fs_name_label.size())) << fs_name
      << std::right << std::setw(std::max(size.size(), size_label.size()) + 2) << size
      << std::right << std::setw(std::max(used.size(), used_label.size()) + 2) << used
      << std::right << std::setw(std::max(available.size(), available_label.size()) + 2) << available
      << std::right << std::setw(std::max(use_percentage.size(), use_percentage_label.size()) + 2) << use_percentage;
  return ss.str();
}

}
