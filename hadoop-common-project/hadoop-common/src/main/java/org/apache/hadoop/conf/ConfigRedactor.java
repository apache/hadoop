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

package org.apache.hadoop.conf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.CommonConfigurationKeys.*;

/**
 * Tool for redacting sensitive information when displaying config parameters.
 *
 * <p>Some config parameters contain sensitive information (for example, cloud
 * storage keys). When these properties are displayed in plaintext, we should
 * redactor their values as appropriate.
 */
public class ConfigRedactor {

  private static final String REDACTED_TEXT = "<redacted>";

  private List<Pattern> compiledPatterns;

  public ConfigRedactor(Configuration conf) {
    String sensitiveRegexList = conf.get(
        HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
        HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT);
    List<String> sensitiveRegexes = Arrays.asList(sensitiveRegexList.split(","));
    compiledPatterns = new ArrayList<Pattern>();
    for (String regex : sensitiveRegexes) {
      Pattern p = Pattern.compile(regex);
      compiledPatterns.add(p);
    }
  }

  /**
   * Given a key / value pair, decides whether or not to redact and returns
   * either the original value or text indicating it has been redacted.
   *
   * @param key
   * @param value
   * @return Original value, or text indicating it has been redacted
   */
  public String redact(String key, String value) {
    if (configIsSensitive(key)) {
      return REDACTED_TEXT;
    }
    return value;
  }

  /**
   * Matches given config key against patterns and determines whether or not
   * it should be considered sensitive enough to redact in logs and other
   * plaintext displays.
   *
   * @param key
   * @return True if parameter is considered sensitive
   */
  private boolean configIsSensitive(String key) {
    for (Pattern regex : compiledPatterns) {
      if (regex.matcher(key).find()) {
        return true;
      }
    }
    return false;
  }
}
