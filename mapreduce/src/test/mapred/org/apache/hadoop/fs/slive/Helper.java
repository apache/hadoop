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

package org.apache.hadoop.fs.slive;

/**
 * Simple slive helper methods (may not exist in 0.20)
 */
class Helper {

  private Helper() {

  }

  private static final String[] emptyStringArray = {};

  /**
   * Splits strings on comma and trims accordingly
   * 
   * @param str
   * @return array of split
   */
  static String[] getTrimmedStrings(String str) {
    if (null == str || "".equals(str.trim())) {
      return emptyStringArray;
    }
    return str.trim().split("\\s*,\\s*");
  }

  /**
   * Converts a byte value into a useful string for output
   * 
   * @param bytes
   * 
   * @return String
   */
  static String toByteInfo(long bytes) {
    StringBuilder str = new StringBuilder();
    if (bytes < 0) {
      bytes = 0;
    }
    str.append(bytes);
    str.append(" bytes or ");
    str.append(bytes / 1024);
    str.append(" kilobytes or ");
    str.append(bytes / (1024 * 1024));
    str.append(" megabytes or ");
    str.append(bytes / (1024 * 1024 * 1024));
    str.append(" gigabytes");
    return str.toString();
  }

  /**
   * Stringifys an array using the given separator.
   * 
   * @param args
   *          the array to format
   * @param sep
   *          the separator string to use (ie comma or space)
   * 
   * @return String representing that array
   */
  static String stringifyArray(Object[] args, String sep) {
    StringBuilder optStr = new StringBuilder();
    for (int i = 0; i < args.length; ++i) {
      optStr.append(args[i]);
      if ((i + 1) != args.length) {
        optStr.append(sep);
      }
    }
    return optStr.toString();
  }

}
