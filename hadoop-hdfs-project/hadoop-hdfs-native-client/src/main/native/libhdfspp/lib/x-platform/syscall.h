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
#include <vector>

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

  /**
   * Performs a case insensitive equality comparison for the two
   * given strings {@link a} and {@link b}.
   *
   * @param a the first string parameter to compare.
   * @param b the second string parameter to compare.
   * @returns A boolean indicating whether to two strings are the
   * same irrespective of their case. Returns true if they match,
   * else false.
   */
  static bool StringCompareIgnoreCase(const std::string& a,
                                      const std::string& b);

  /**
   * Creates and opens a temporary file with a given {@link pattern}.
   * The {@link pattern} must end with a minimum of 6 'X' characters.
   * This function will first modify the last 6 'X' characters with
   * random character values, which serve as the temporary file name
   * Subsequently opens the file and returns the file descriptor for
   * the same. The behaviour of this function is the same as that of
   * POSIX mkstemp function. The file must be later closed by the
   * application and is not handled by this function.
   *
   * @param pattern the pattern to be used for the temporary filename.
   * @returns an integer representing the file descriptor for the
   * opened temporary file. Returns -1 in the case of error and sets
   * the global errno with the appropriate error code.
   */
  static int CreateAndOpenTempFile(std::vector<char>& pattern);

  /**
   * Closes the file corresponding to given {@link file_descriptor}.
   *
   * @param file_descriptor the file descriptor of the file to close.
   * @returns a boolean indicating the status of the call to this
   * function. true if it's a success, false in the case of an error.
   * The global errno is set if the call to this function was not
   * successful.
   */
  static bool CloseFile(int file_descriptor);

  /**
   * Creates and opens a temporary file with a given {@link pattern}.
   * The {@link pattern} must end with a minimum of 6 'X' characters.
   * This function will first modify the last 6 'X' characters with
   * random character values, which serve as the temporary file name.
   * Subsequently opens the file and returns the file descriptor for
   * the same. The behaviour of this function is the same as that of
   * POSIX mkstemp function. The file must be later closed by the
   * application and is not handled by this function.
   *
   * @param pattern the pattern to be used for the temporary filename.
   * @returns true if the creation of directory succeeds, false
   * otherwise.
   */
  static bool CreateTempDir(std::vector<char>& pattern);

 private:
  static bool WriteToStdoutImpl(const char* message);
};
}  // namespace XPlatform

#endif
