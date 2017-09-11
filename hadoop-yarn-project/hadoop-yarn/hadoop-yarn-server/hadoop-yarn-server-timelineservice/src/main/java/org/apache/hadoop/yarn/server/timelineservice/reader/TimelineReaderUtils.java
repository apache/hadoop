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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Set of utility methods to be used across timeline reader.
 */
public final class TimelineReaderUtils {
  private TimelineReaderUtils() {
  }

  /**
   * Default delimiter for joining strings.
   */
  @VisibleForTesting
  public static final char DEFAULT_DELIMITER_CHAR = '!';

  /**
   * Default escape character used for joining strings.
   */
  @VisibleForTesting
  public static final char DEFAULT_ESCAPE_CHAR = '*';

  public static final String FROMID_KEY = "FROM_ID";

  @VisibleForTesting
  public static final String UID_KEY = "UID";

  /**
   * Split the passed string along the passed delimiter character while looking
   * for escape char to interpret the splitted parts correctly. For delimiter or
   * escape character to be interpreted as part of the string, they have to be
   * escaped by putting an escape character in front.
   * @param str string to be split.
   * @param delimiterChar delimiter used for splitting.
   * @param escapeChar delimiter and escape character will be escaped using this
   *     character.
   * @return a list of strings after split.
   * @throws IllegalArgumentException if string is not properly escaped.
   */
  static List<String> split(final String str, final char delimiterChar,
      final char escapeChar) throws IllegalArgumentException {
    if (str == null) {
      return null;
    }
    int len = str.length();
    if (len == 0) {
      return Collections.emptyList();
    }
    List<String> list = new ArrayList<String>();
    // Keeps track of offset of the passed string.
    int offset = 0;
    // Indicates start offset from which characters will be copied from original
    // string to destination string. Resets when an escape or delimiter char is
    // encountered.
    int startOffset = 0;
    StringBuilder builder = new StringBuilder(len);
    // Iterate over the string till we reach the end.
    while (offset < len) {
      if (str.charAt(offset) == escapeChar) {
        // An escape character must be followed by a delimiter or escape char
        // but we have reached the end and have no further character to look at.
        if (offset + 1 >= len) {
          throw new IllegalArgumentException(
              "Escape char not properly escaped.");
        }
        char nextChar = str.charAt(offset + 1);
        // Next character must be a delimiter or an escape char.
        if (nextChar != escapeChar && nextChar != delimiterChar) {
          throw new IllegalArgumentException(
              "Escape char or delimiter char not properly escaped.");
        }
        // Copy contents from the offset where last escape or delimiter char was
        // encountered.
        if (startOffset < offset) {
          builder.append(str.substring(startOffset, offset));
        }
        builder.append(nextChar);
        offset += 2;
        // Reset the start offset as an escape char has been encountered.
        startOffset = offset;
        continue;
      } else if (str.charAt(offset) == delimiterChar) {
        // A delimiter has been encountered without an escape character.
        // String needs to be split here. Copy remaining chars and add the
        // string to list.
        builder.append(str.substring(startOffset, offset));
        list.add(builder.toString().trim());
        // Reset the start offset as a delimiter has been encountered.
        startOffset = ++offset;
        builder = new StringBuilder(len - offset);
        continue;
      }
      offset++;
    }
    // Copy rest of the characters.
    if (!str.isEmpty()) {
      builder.append(str.substring(startOffset));
    }
    // Add the last part of delimited string to list.
    list.add(builder.toString().trim());
    return list;
  }

  private static String escapeString(final String str, final char delimiterChar,
      final char escapeChar) {
    if (str == null) {
      return null;
    }
    int len = str.length();
    if (len == 0) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    // Keeps track of offset of the passed string.
    int offset = 0;
    // Indicates start offset from which characters will be copied from original
    // string to destination string. Resets when an escape or delimiter char is
    // encountered.
    int startOffset = 0;
    // Iterate over the string till we reach the end.
    while (offset < len) {
      char charAtOffset = str.charAt(offset);
      if (charAtOffset == escapeChar || charAtOffset == delimiterChar) {
        // If an escape or delimiter character is encountered, copy characters
        // from the offset where escape or delimiter was last encountered.
        if (startOffset < offset) {
          builder.append(str.substring(startOffset, offset));
        }
        // Append escape char before delimiter/escape char.
        builder.append(escapeChar).append(charAtOffset);
        // Reset start offset for copying characters when next escape/delimiter
        // char is encountered.
        startOffset = offset + 1;
      }
      offset++;
    }
    // Copy remaining characters.
    builder.append(str.substring(startOffset));
    return builder.toString();
  }

  /**
   * Join different strings in the passed string array delimited by passed
   * delimiter with delimiter and escape character escaped using passed escape
   * char.
   * @param strs strings to be joined.
   * @param delimiterChar delimiter used to join strings.
   * @param escapeChar escape character used to escape delimiter and escape
   *     char.
   * @return a single string joined using delimiter and properly escaped.
   */
  static String joinAndEscapeStrings(final String[] strs,
      final char delimiterChar, final char escapeChar) {
    int len = strs.length;
    // Escape each string in string array.
    for (int index = 0; index < len; index++) {
      if (strs[index] == null) {
        return null;
      }
      strs[index] = escapeString(strs[index], delimiterChar, escapeChar);
    }
    // Join the strings after they have been escaped.
    return StringUtils.join(strs, delimiterChar);
  }

  public static List<String> split(final String str)
      throws IllegalArgumentException {
    return split(str, DEFAULT_DELIMITER_CHAR, DEFAULT_ESCAPE_CHAR);
  }

  public static String joinAndEscapeStrings(final String[] strs) {
    return joinAndEscapeStrings(strs, DEFAULT_DELIMITER_CHAR,
        DEFAULT_ESCAPE_CHAR);
  }
}
