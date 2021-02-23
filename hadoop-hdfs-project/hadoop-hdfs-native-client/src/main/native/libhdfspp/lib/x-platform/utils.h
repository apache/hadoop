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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_UTILS
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_UTILS

#include <string>

/**
 * The {@link XPlatform} namespace contains components that
 * aid in writing cross-platform code.
 */
namespace XPlatform {
class Utils {
 public:
  /**
   * A cross-platform implementation of basename in linux.
   * Please refer https://www.man7.org/linux/man-pages/man3/basename.3.html
   * for more details.
   *
   * @param file_path The input path to get the basename.
   *
   * @returns The trailing component of the given {@link file_path}
   */
  static std::string Basename(const std::string& file_path);
};
}  // namespace XPlatform

#endif
