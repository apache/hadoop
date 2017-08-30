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

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;

/**
 * Helper class for Timeline service.
 */
@LimitedPrivate({ "MapReduce", "YARN" })
public final class TimelineServiceHelper {

  private TimelineServiceHelper() {
    // Utility classes should not have a public or default constructor.
  }

  /**
   * Cast map to HashMap for generic type.
   * @param originalMap the map need to be casted
   * @param <E> key type
   * @param <V> value type
   * @return casted HashMap object
   */
  public static <E, V> HashMap<E, V> mapCastToHashMap(
      Map<E, V> originalMap) {
    return originalMap == null ? null : originalMap instanceof HashMap ?
        (HashMap<E, V>) originalMap : new HashMap<E, V>(originalMap);
  }

  /**
   * Inverts the given key.
   * @param key value to be inverted .
   * @return inverted long
   */
  public static long invertLong(long key) {
    return Long.MAX_VALUE - key;
  }
}
