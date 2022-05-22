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

#include <algorithm>
#include <cerrno>
#include <iostream>
#include <iterator>
#include <system_error>
#include <variant>

#include "x-platform/c-api/dirent.h"
#include "x-platform/dirent.h"

DIR *opendir(const char *dir_path) {
  const auto dir = new DIR;
  dir->x_platform_dirent_ptr = new XPlatform::Dirent(dir_path);
  return dir;
}

struct dirent *readdir(const DIR *dir) {
  static struct dirent static_dir_entry;
  const auto x_platform_dirent =
      static_cast<XPlatform::Dirent *>(dir->x_platform_dirent_ptr);
  const auto dir_entry = x_platform_dirent->NextFile();

  if (std::holds_alternative<std::monostate>(dir_entry)) {
    return nullptr;
  }

  if (std::holds_alternative<std::error_code>(dir_entry)) {
    const auto err = std::get<std::error_code>(dir_entry);
    errno = err.value();

#ifdef X_PLATFORM_C_API_DIRENT_DEBUG
    std::cerr << "Error in listing directory: " << err.message() << std::endl;
#endif

    return nullptr;
  }

  if (std::holds_alternative<std::filesystem::directory_entry>(dir_entry)) {
    const auto entry = std::get<std::filesystem::directory_entry>(dir_entry);
    const auto filename = entry.path().filename().string();
    // TODO : Add a check - filename's length shouldn't exceed 256.
    std::fill(std::begin(static_dir_entry.d_name),
              std::end(static_dir_entry.d_name), '\0');
    std::copy(filename.begin(), filename.end(),
              std::begin(static_dir_entry.d_name));
  }
  return &static_dir_entry;
}

int closedir(const DIR *dir) {
  const auto x_platform_dirent =
      static_cast<XPlatform::Dirent *>(dir->x_platform_dirent_ptr);
  delete x_platform_dirent;
  delete dir;
  return 0;
}
