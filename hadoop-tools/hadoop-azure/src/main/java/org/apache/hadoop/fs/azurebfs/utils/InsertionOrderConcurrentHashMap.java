/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.azurebfs.services.BlockWithId;

public class InsertionOrderConcurrentHashMap<K, V> {

    private final Map<K, V> map = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<K> queue = new ConcurrentLinkedQueue<K>();

    Comparator<BlockWithId> comparator = (o1, o2) -> (int) (o1.getOffset() - o2.getOffset());

    public V put(K key, V value) {
        V result = map.put(key, value);
        if (result == null) {
            queue.add(key);
        }
        return result;
    }

    public V get(Object key) {
        return map.get(key);
    }

    public V remove(Object key) {
        V result = map.remove(key);
        if (result != null) {
            queue.remove(key);
        }
        return result;
    }

    public K peek() {
        return queue.peek();
    }

    public K poll() {
        K key = queue.poll();
        if (key != null) {
            map.remove(key);
        }
        return key;
    }

    public int size() {
        return map.size();
    }

    public ConcurrentLinkedQueue<K> getQueue() {
        ConcurrentLinkedQueue<K> sortedQueue = queue.stream()
                .sorted((Comparator<? super K>) comparator)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        return sortedQueue;
    }

    public boolean containsKey(K key) {
        for (Map.Entry<K, V> entry : entrySet()) {
            if (entry.getKey().equals(key)) {
                return true;
            }
        }
        return false;
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return new HashSet<>(map.entrySet());
    }

    public void clear() {
        map.clear();
    }
}

