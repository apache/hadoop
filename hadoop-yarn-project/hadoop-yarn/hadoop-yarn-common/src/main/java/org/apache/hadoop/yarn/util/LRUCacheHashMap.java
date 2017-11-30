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

package org.apache.hadoop.yarn.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU cache with a configurable maximum cache size and access order.
 */
public class LRUCacheHashMap<K, V> extends LinkedHashMap<K, V> {

  private static final long serialVersionUID = 1L;

  // Maximum size of the cache
  private int maxSize;

  /**
   * Constructor.
   *
   * @param maxSize max size of the cache
   * @param accessOrder true for access-order, false for insertion-order
   */
  public LRUCacheHashMap(int maxSize, boolean accessOrder) {
    super(maxSize, 0.75f, accessOrder);
    this.maxSize = maxSize;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() > maxSize;
  }
}
