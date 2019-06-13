/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db.cache;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.util.Iterator;
import java.util.Map;

/**
 * Cache used for RocksDB tables.
 * @param <CACHEKEY>
 * @param <CACHEVALUE>
 */

@Private
@Evolving
public interface TableCache<CACHEKEY extends CacheKey,
    CACHEVALUE extends CacheValue> {

  /**
   * Return the value for the key if it is present, otherwise return null.
   * @param cacheKey
   * @return CACHEVALUE
   */
  CACHEVALUE get(CACHEKEY cacheKey);

  /**
   * Add an entry to the cache, if the key already exists it overrides.
   * @param cacheKey
   * @param value
   */
  void put(CACHEKEY cacheKey, CACHEVALUE value);

  /**
   * Removes all the entries from the cache which are having epoch value less
   * than or equal to specified epoch value. For FullTable Cache this is a
   * do-nothing operation.
   * @param epoch
   */
  void cleanup(long epoch);

  /**
   * Return the size of the cache.
   * @return size
   */
  int size();

  /**
   * Return an iterator for the cache.
   * @return iterator of the underlying cache for the table.
   */
  Iterator<Map.Entry<CACHEKEY, CACHEVALUE>> iterator();
}
