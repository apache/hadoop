/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

/**
 * Utillity for Strings.
 */
public class Strings {
  public final static String DEFAULT_SEPARATOR = "=";
  public final static String DEFAULT_KEYVALUE_SEPARATOR = ", ";
  
  /**
   * Append to a StringBuilder a key/value.
   * Uses default separators.
   * @param sb StringBuilder to use
   * @param key Key to append.
   * @param value Value to append.
   * @return Passed <code>sb</code> populated with key/value.
   */
  public static StringBuilder appendKeyValue(final StringBuilder sb,
      final String key, final Object value) {
    return appendKeyValue(sb, key, value, DEFAULT_SEPARATOR,
      DEFAULT_KEYVALUE_SEPARATOR);
  }

  /**
   * Append to a StringBuilder a key/value.
   * Uses default separators.
   * @param sb StringBuilder to use
   * @param key Key to append.
   * @param value Value to append.
   * @param separator Value to use between key and value.
   * @param keyValueSeparator Value to use between key/value sets.
   * @return Passed <code>sb</code> populated with key/value.
   */
  public static StringBuilder appendKeyValue(final StringBuilder sb,
      final String key, final Object value, final String separator,
      final String keyValueSeparator) {
    if (sb.length() > 0) {
      sb.append(keyValueSeparator);
    }
    return sb.append(key).append(separator).append(value);
  }
}