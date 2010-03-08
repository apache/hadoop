/**
 * Copyright 2010 The Apache Software Foundation
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

import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A Map that uses Soft Reference values internally. Use as a simple cache.
 * 
 * @param <K> key class
 * @param <V> value class
 */
public class SoftValueMap<K,V> implements Map<K,V> {
  private final Map<K, SoftValue<K,V>> internalMap =
    new HashMap<K, SoftValue<K,V>>();
  private final ReferenceQueue<?> rq;
  
  public SoftValueMap() {
    this(new ReferenceQueue());
  }
  
  public SoftValueMap(final ReferenceQueue<?> rq) {
    this.rq = rq;
  }

  /**
   * Checks soft references and cleans any that have been placed on
   * ReferenceQueue.
   * @return How many references cleared.
   */
  @SuppressWarnings({"unchecked"})
  public int checkReferences() {
    int i = 0;
    for (Object obj; (obj = this.rq.poll()) != null;) {
      i++;
      this.internalMap.remove(((SoftValue<K,V>)obj).getKey());
    }
    return i;
  }

  public V put(K key, V value) {
    checkReferences();
    SoftValue<K,V> oldValue = this.internalMap.put(key,
      new SoftValue<K,V>(key, value, this.rq));
    return oldValue == null ? null : oldValue.get();
  }
  
  @SuppressWarnings("unchecked")
  public void putAll(Map map) {
    throw new RuntimeException("Not implemented");
  }
  
  @SuppressWarnings({"SuspiciousMethodCalls"})
  public V get(Object key) {
    checkReferences();
    SoftValue<K,V> value = this.internalMap.get(key);
    if (value == null) {
      return null;
    }
    if (value.get() == null) {
      this.internalMap.remove(key);
      return null;
    }
    return value.get();
  }

  public V remove(Object key) {
    checkReferences();
    SoftValue<K,V> value = this.internalMap.remove(key);
    return value == null ? null : value.get();
  }

  public boolean containsKey(Object key) {
    checkReferences(); 
    return this.internalMap.containsKey(key);
  }
  
  public boolean containsValue(Object value) {
/*    checkReferences();
    return internalMap.containsValue(value);*/
    throw new UnsupportedOperationException("Don't support containsValue!");
  }
  
  public boolean isEmpty() {
    checkReferences();
    return this.internalMap.isEmpty();
  }

  public int size() {
    checkReferences();
    return this.internalMap.size();
  }

  public void clear() {
    checkReferences();
    this.internalMap.clear();
  }

  public Set<K> keySet() {
    checkReferences();
    return this.internalMap.keySet();
  }

  public Set<Map.Entry<K,V>> entrySet() {
    checkReferences();
    Set<Map.Entry<K, SoftValue<K,V>>> entries = this.internalMap.entrySet();
    Set<Map.Entry<K, V>> real_entries = new HashSet<Map.Entry<K,V>>();
    for(Map.Entry<K, SoftValue<K,V>> entry : entries) {
      real_entries.add(entry.getValue());
    }
    return real_entries;
  }

  public Collection<V> values() {
    checkReferences();
    Collection<SoftValue<K,V>> softValues = this.internalMap.values();
    ArrayList<V> hardValues = new ArrayList<V>();
    for(SoftValue<K,V> softValue : softValues) {
      hardValues.add(softValue.get());
    }
    return hardValues;
  }
}