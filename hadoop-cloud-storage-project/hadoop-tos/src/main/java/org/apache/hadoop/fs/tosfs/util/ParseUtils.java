/*
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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Preconditions;

public final class ParseUtils {
  private static final String ERROR_MSG = "Failed to parse value %s as %s, property key %s";

  private ParseUtils() {
  }

  public static String envAsString(String key) {
    return envAsString(key, true);
  }

  public static String envAsString(String key, boolean allowNull) {
    String value = System.getenv(key);
    if (!allowNull) {
      Preconditions.checkNotNull(value, "os env key: %s cannot be null", key);
    }
    return value;
  }

  public static String envAsString(String key, String defaultValue) {
    String value = System.getenv(key);
    return StringUtils.isEmpty(value) ? defaultValue : value;
  }

  public static boolean envAsBoolean(String key, boolean defaultValue) {
    String value = System.getenv(key);
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    checkBoolean(key, value);
    return Boolean.parseBoolean(value);
  }

  public static boolean isBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  public static void checkBoolean(String key, String value) {
    if (!isBoolean(value)) {
      throw new IllegalArgumentException(String.format(ERROR_MSG, value, "boolean", key));
    }
  }
}
