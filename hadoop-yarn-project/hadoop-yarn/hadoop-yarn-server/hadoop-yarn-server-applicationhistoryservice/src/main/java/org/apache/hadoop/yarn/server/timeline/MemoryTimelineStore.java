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

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * In-memory implementation of {@link TimelineStore}. This
 * implementation is for test purpose only. If users improperly instantiate it,
 * they may encounter reading and writing history data in different memory
 * store.
 * 
 * The methods are synchronized to avoid concurrent modification on the memory.
 * 
 */
@Private
@Unstable
public class MemoryTimelineStore extends KeyValueBasedTimelineStore {

  static class HashMapStoreAdapter<K, V>
      implements TimelineStoreMapAdapter<K, V> {
    Map<K, V> internalMap = new HashMap<>();

    @Override
    public V get(K key) {
      return internalMap.get(key);
    }

    @Override
    public void put(K key, V value) {
      internalMap.put(key, value);
    }

    @Override
    public void remove(K key) {
      internalMap.remove(key);
    }

    @Override
    public Iterator<V>
    valueSetIterator() {
      return new TreeSet<>(internalMap.values()).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<V> valueSetIterator(V minV) {
      if (minV instanceof Comparable) {
        TreeSet<V> tempTreeSet = new TreeSet<>();
        for (V value : internalMap.values()) {
          if (((Comparable) value).compareTo(minV) >= 0) {
            tempTreeSet.add(value);
          }
        }
        return tempTreeSet.iterator();
      } else {
        return valueSetIterator();
      }
    }
  }

  public MemoryTimelineStore() {
    this(MemoryTimelineStore.class.getName());
  }

  public MemoryTimelineStore(String name) {
    super(name);
    entities = new HashMapStoreAdapter<>();
    entityInsertTimes = new HashMapStoreAdapter<>();
    domainById = new HashMapStoreAdapter<>();
    domainsByOwner = new HashMapStoreAdapter<>();
  }

}
