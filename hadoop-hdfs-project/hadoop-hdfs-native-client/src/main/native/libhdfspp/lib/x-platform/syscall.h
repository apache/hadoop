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

 private:
  static bool WriteToStdoutImpl(const char* message);
};
}  // namespace XPlatform

#endif
