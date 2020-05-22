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

package org.apache.hadoop.fs.statistics.impl;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in the implementation.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IOStatisticsImplementationUtils {

  /** Pattern used for each entry. */
  public static final String ENTRY_PATTERN = "(%s=%s)";

  /** String to return when a source is null. */
  @VisibleForTesting
  public static final String NULL_SOURCE = "()";

  /**
   * Convert an entry to the string format used in logging.
   *
   * @param entry entry to evaluate
   * @return formatted string
   */
  public static String entrytoString(final Map.Entry<String, Long> entry) {
    return entrytoString(entry.getKey(), entry.getValue());
  }

  /**
   * Convert entry values to the string format used in logging.
   *
   * @param name statistic name
   * @param value stat value
   * @return formatted string
   */
  public static String entrytoString(final String name, final Long value) {
    return String.format(
        ENTRY_PATTERN,
        name,
        value);
  }
}
