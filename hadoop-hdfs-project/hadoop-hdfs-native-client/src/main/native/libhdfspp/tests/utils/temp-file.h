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

#ifndef NATIVE_LIBHDFSPP_TESTS_UTILS_TEMP_FILE
#define NATIVE_LIBHDFSPP_TESTS_UTILS_TEMP_FILE

#include <string>

namespace TestUtils {
/**
 * Creates a temporary file and deletes it
 * upon destruction of the TempFile instance.
 *
 * The temporary file gets created in /tmp directory
 * by default.
 */
class TempFile {
public:
  TempFile();

  TempFile(std::string filename);

  TempFile(const TempFile &other) = default;

  TempFile(TempFile &&other) noexcept;

  TempFile &operator=(const TempFile &other);

  TempFile &operator=(TempFile &&other) noexcept;

  [[nodiscard]] const std::string &GetFileName() const { return filename_; }

  ~TempFile();

private:
  std::string filename_{"/tmp/test_XXXXXXXXXX"};
  int fd_{-1};
};
} // namespace TestUtils

#endif