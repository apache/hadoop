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

import org.apache.hadoop.fs.statistics.IOStatisticEntry;

/**
 * A map entry for implementations to use if they need to.
 */
public final class StatsMapEntry
    implements Map.Entry<String, IOStatisticEntry> {

  /**
   * Key.
   */
  private final String key;

  /**
   * Value.
   */
  private IOStatisticEntry value;

  /**
   * Constructor.
   * @param key key
   * @param value value
   */
  StatsMapEntry(final String key, final IOStatisticEntry value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public IOStatisticEntry getValue() {
    return value;
  }

  @Override
  public IOStatisticEntry setValue(final IOStatisticEntry val) {
    this.value = val;
    return val;
  }

  @Override
  public String toString() {
    return String.format("(%s, %s)", key, value);
  }


  /**
   * Counter value.
   * @param key key
   * @param value value
   */
  public static StatsMapEntry counter(final String key, final long value) {
    return new StatsMapEntry(key,
        (Long) value);
  }
}
