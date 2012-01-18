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
package org.apache.hadoop.tools.rumen;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ParsedLine {
  Properties content;
  LogRecordType type;

  static final String KEY = "(\\w+)";
  /**
   * The value string is enclosed in double quotation marks ('"') and
   * occurrences of '"' and '\' are escaped with a '\'. So the escaped value
   * string is essentially a string of escaped sequence ('\' followed by any
   * character) or any character other than '"' and '\'.
   * 
   * The straightforward REGEX to capture the above is "((?:[^\"\\\\]|\\\\.)*)".
   * Unfortunately Java's REGEX implementation is "broken" that it does not
   * perform the NFA-to-DFA conversion and such expressions would lead to
   * backtracking and stack overflow when matching with long strings. The
   * following is a manual "unfolding" of the REGEX to get rid of backtracking.
   */
  static final String VALUE = "([^\"\\\\]*+(?:\\\\.[^\"\\\\]*+)*+)";
  /**
   * REGEX to match the Key-Value pairs in an input line. Capture group 1
   * matches the key and capture group 2 matches the value (without quotation
   * marks).
   */
  static final Pattern keyValPair = Pattern.compile(KEY + "=" + "\"" + VALUE + "\"");

  @SuppressWarnings("unused")
  ParsedLine(String fullLine, int version) {
    super();

    content = new Properties();

    int firstSpace = fullLine.indexOf(" ");

    if (firstSpace < 0) {
      firstSpace = fullLine.length();
    }

    if (firstSpace == 0) {
      return; // This is a junk line of some sort
    }

    type = LogRecordType.intern(fullLine.substring(0, firstSpace));

    String propValPairs = fullLine.substring(firstSpace + 1);

    Matcher matcher = keyValPair.matcher(propValPairs);

    while(matcher.find()){
      String key = matcher.group(1);
      String value = matcher.group(2);
      content.setProperty(key, value);
    }
  }

  protected LogRecordType getType() {
    return type;
  }

  protected String get(String key) {
    return content.getProperty(key);
  }

  protected long getLong(String key) {
    String val = get(key);

    return Long.parseLong(val);
  }
}
