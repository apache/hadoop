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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_SYSCALL
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_SYSCALL

#include <string>

/**
 * The {@link XPlatform} namespace contains components that
 * aid in writing cross-platform code.
 */
namespace XPlatform {
class Syscall {
 public:
  /**
   * Writes the given string to the application's
   * standard output stream.
   *
   * @param message The string to write to stdout.
   * @returns A boolean indicating whether the write
   * was successful.
   */
  static bool WriteToStdout(const std::string& message);

  /**
   * Writes the given char pointer to the application's
   * standard output stream.
   *
   * @param message The char pointer to write to stdout.
   * @returns A boolean indicating whether the write
   * was successful.
   */
  static int WriteToStdout(const char* message);

  /**
   * Checks whether the {@link str} argument matches the {@link pattern}
   * argument, which is a shell wildcard pattern.
   *
   * @param pattern The wildcard pattern to use.
   * @param str The string to match.
   * @returns A boolean indicating whether the given {@link str}
   * matches {@link pattern}.
   */
  static bool FnMatch(const std::string& pattern, const std::string& str);

  /**
   * Clears the given {@link buffer} upto {@link sz_bytes} by
   * filling them with zeros. This method is immune to compiler
   * optimizations and guarantees that the first {@link sz_bytes} of
   * {@link buffer} is cleared. The {@link buffer} must be at least
   * as big as {@link sz_bytes}, the behaviour is undefined otherwise.
   *
   * @param buffer the pointer to the buffer to clear.
   * @param sz_bytes the count of the bytes to clear.
   */
  static void ClearBufferSafely(void* buffer, size_t sz_bytes);
  static bool StringCompareIgnoreCase(const std::string& a,
                                      const std::string& b);

 private:
  static bool WriteToStdoutImpl(const char* message);
};
}  // namespace XPlatform

#endif
