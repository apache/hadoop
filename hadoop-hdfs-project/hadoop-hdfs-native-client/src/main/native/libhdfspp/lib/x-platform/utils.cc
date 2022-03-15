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

#include "utils.h"

#include <filesystem>
#include <string>
#include <vector>

std::string XPlatform::Utils::Basename(const std::string& file_path) {
  if (file_path.empty()) {
    return ".";
  }

  const std::filesystem::path path(file_path);
  std::vector<std::string> parts;
  for (const auto& part : std::filesystem::path(file_path)) {
    parts.emplace_back(part.string());
  }

  /* Handle the case of trailing slash */
  if (parts.back().empty()) {
    parts.pop_back();
  }
  return parts.back();
}
