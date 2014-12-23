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
package org.apache.hadoop.hdfs.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

/**
 * Class for de-duplication of instances. <br>
 * Hold the references count to a single instance. If there are no references
 * then the entry will be removed.<br>
 * Type E should implement {@link ReferenceCounter}<br>
 * Note: This class is NOT thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReferenceCountMap<E extends ReferenceCountMap.ReferenceCounter> {

  private Map<E, E> referenceMap = new HashMap<E, E>();

  /**
   * Add the reference. If the instance already present, just increase the
   * reference count.
   * 
   * @param key Key to put in reference map
   * @return Referenced instance
   */
  public E put(E key) {
    E value = referenceMap.get(key);
    if (value == null) {
      value = key;
      referenceMap.put(key, value);
    }
    value.incrementAndGetRefCount();
    return value;
  }

  /**
   * Delete the reference. Decrease the reference count for the instance, if
   * any. On all references removal delete the instance from the map.
   * 
   * @param key Key to remove the reference.
   */
  public void remove(E key) {
    E value = referenceMap.get(key);
    if (value != null && value.decrementAndGetRefCount() == 0) {
      referenceMap.remove(key);
    }
  }

  /**
   * Get entries in the reference Map.
   * 
   * @return
   */
  @VisibleForTesting
  public ImmutableList<E> getEntries() {
    return new ImmutableList.Builder<E>().addAll(referenceMap.keySet()).build();
  }

  /**
   * Get the reference count for the key
   */
  public long getReferenceCount(E key) {
    ReferenceCounter counter = referenceMap.get(key);
    if (counter != null) {
      return counter.getRefCount();
    }
    return 0;
  }

  /**
   * Get the number of unique elements
   */
  public int getUniqueElementsSize() {
    return referenceMap.size();
  }

  /**
   * Clear the contents
   */
  @VisibleForTesting
  public void clear() {
    referenceMap.clear();
  }

  /**
   * Interface for the reference count holder
   */
  public static interface ReferenceCounter {
    public int getRefCount();

    public int incrementAndGetRefCount();

    public int decrementAndGetRefCount();
  }
}
