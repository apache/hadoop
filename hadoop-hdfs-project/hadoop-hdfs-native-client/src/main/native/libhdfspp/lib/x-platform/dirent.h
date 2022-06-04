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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_DIRENT
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_DIRENT

#include <filesystem>
#include <string>
#include <system_error>
#include <variant>

namespace XPlatform {
/**
 * {@class XPlatform::Dirent} provides the functionality to perform a one-time
 * iteration per {@link XPlatform::Dirent} through the child files or folders
 * under a given path.
 */
class Dirent {
public:
  Dirent(const std::string &path)
      : dir_it_{std::filesystem::path{path}, dir_it_err_} {}

  // Abiding to the Rule of 5
  Dirent(const Dirent &) = default;
  Dirent(Dirent &&) = default;
  Dirent &operator=(const Dirent &) = default;
  Dirent &operator=(Dirent &&) = default;
  ~Dirent() = default;

  /**
   * Advances the iterator {@link XPlatform::Dirent#dir_it_} to the next file in
   * the given path.
   *
   * @return An {@link std::variant} comprising of any one of the following
   * types:
   * 1. {@link std::monostate} which indicates the end of iteration of all the
   * files in the given path.
   * 2. {@link std::filesystem::directory_entry} which is the directory entry of
   * the current file.
   * 3. {@link std::error_code} which corresponds to the error in retrieving the
   * file.
   */
  std::variant<std::monostate, std::filesystem::directory_entry,
               std::error_code>
  NextFile();

private:
  /**
   * Indicates the error corresponding to the most recent invocation of
   * directory iteration by {@link XPlatform::Dirent#dir_it_}.
   */
  std::error_code dir_it_err_{};

  /**
   * The iterator used for iterating through the files or folders under the
   * given path.
   */
  std::filesystem::directory_iterator dir_it_;
};
} // namespace XPlatform

#endif
