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
package org.apache.hadoop.fs.shell;

final class CommandUtils {

  private static final String FILE_STATUS_STRING_KV_SEPARATOR = "=";
  private static final String FILE_STATUS_STRING_DELIMITER = ";";
  private static final String FILE_STATUS_STRING_TERMINATOR = "}";

  /**
   * Parse the FileStatus.toString() results,
   * and return the value of the given key.
   *
   * @param fileStatusStr String from FileStatus.toString()
   * @param key Key String
   * @return Value of the key in the String, null if not found.
   */
  static String getValueFromFileStatusString(String fileStatusStr, String key) {
    String res = null;
    // Search backwards since this function is only used for fileId for now,
    // which we know we placed it at the end of the FileStatus String.
    int start = fileStatusStr.lastIndexOf(key);
    int end = -1;
    if (start > 0) {
      // fileId field found, move the start pointer to the start of the value
      start += key.length() + FILE_STATUS_STRING_KV_SEPARATOR.length();
      // Find delimiter ";" to mark value string's end
      end = fileStatusStr.indexOf(FILE_STATUS_STRING_DELIMITER, start);
      if (end < 0) {
        // Delimiter not found, try terminator "}"
        end = fileStatusStr.indexOf(FILE_STATUS_STRING_TERMINATOR, start);
      }
    }
    if (end > 0) {
      // If value end pointer is not -1
      res = fileStatusStr.substring(start, end);
    }
    return res;
  }

  static String formatDescription(String usage, String... desciptions) {
    StringBuilder b = new StringBuilder(usage + ": " + desciptions[0]);
    for(int i = 1; i < desciptions.length; i++) {
      b.append("\n\t\t" + desciptions[i]);
    }
    return b.toString();
  }
}
