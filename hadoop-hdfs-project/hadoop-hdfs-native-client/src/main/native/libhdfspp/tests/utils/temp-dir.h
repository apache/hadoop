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

#ifndef NATIVE_LIBHDFSPP_TESTS_UTILS_TEMP_DIR
#define NATIVE_LIBHDFSPP_TESTS_UTILS_TEMP_DIR

#include <string>

namespace TestUtils {
/*
 * Creates a temporary directory and deletes its contents recursively
 * upon destruction of its instance.
 *
 * Creates a directory in /tmp by default.
 */
class TempDir {
public:
  TempDir();

  TempDir(const TempDir &other) = default;

  TempDir(TempDir &&other) noexcept;

  TempDir &operator=(const TempDir &other);

  TempDir &operator=(TempDir &&other) noexcept;

  [[nodiscard]] const std::string &GetPath() const { return path_; }

  ~TempDir();

private:
  std::string path_{"/tmp/test_dir_XXXXXXXXXX"};
  bool is_path_init_{false};
};
} // namespace TestUtils

#endif