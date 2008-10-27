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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A SortedMap implementation that uses Soft Reference values
 * internally to make it play well with the GC when in a low-memory
 * situation. Use as a cache where you also need SortedMap functionality.
 * 
 * @param <K> key class
 * @param <V> value class
 */
public class SoftValueSortedMap<K,V> implements SortedMap<K,V> {
  private final SortedMap<K, SoftValue<K,V>> internalMap;
  private final ReferenceQueueUtil<K,SoftValue<K,V>> rq;
  
  /** Constructor */
  public SoftValueSortedMap() {
    this(new TreeMap<K, SoftValue<K,V>>());
  }
  
  /**
   * Constructor
   * @param c
   */
  public SoftValueSortedMap(final Comparator<K> c) {
    this(new TreeMap<K, SoftValue<K,V>>(c));
  }
  
  /** For headMap and tailMap support */
  private SoftValueSortedMap(SortedMap<K,SoftValue<K,V>> original) {
    this.internalMap = original;
    this.rq = new ReferenceQueueUtil<K,SoftValue<K,V>>(this.internalMap);
  }
  
  public V put(K key, V value) {
    this.rq.checkReferences();
    SoftValue<K,V> oldValue = this.internalMap.put(key,
      new SoftValue<K,V>(key, value, this.rq.getReferenceQueue()));
    return oldValue == null ? null : oldValue.get();
  }
  
  @SuppressWarnings("unchecked")
  public void putAll(@SuppressWarnings("unused") Map map) {
    throw new RuntimeException("Not implemented");
  }
  
  public V get(Object key) {
    this.rq.checkReferences();
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
    this.rq.checkReferences();
    SoftValue<K,V> value = this.internalMap.remove(key);
    return value == null ? null : value.get();
  }

  public boolean containsKey(Object key) {
    this.rq.checkReferences(); 
    return this.internalMap.containsKey(key);
  }
  
  public boolean containsValue(@SuppressWarnings("unused") Object value) {
/*    checkReferences();
    return internalMap.containsValue(value);*/
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public K firstKey() {
    this.rq.checkReferences();
    return internalMap.firstKey();
  }

  public K lastKey() {
    this.rq.checkReferences();
    return internalMap.lastKey();
  }
  
  public SoftValueSortedMap<K,V> headMap(K key) {
    this.rq.checkReferences();
    return new SoftValueSortedMap<K,V>(this.internalMap.headMap(key));
  }
  
  public SoftValueSortedMap<K,V> tailMap(K key) {
    this.rq.checkReferences();
    return new SoftValueSortedMap<K,V>(this.internalMap.tailMap(key));
  }
  
  public SoftValueSortedMap<K,V> subMap(K fromKey, K toKey) {
    this.rq.checkReferences();
    return new SoftValueSortedMap<K,V>(this.internalMap.subMap(fromKey, toKey));
  }
  
  public boolean isEmpty() {
    this.rq.checkReferences();
    return this.internalMap.isEmpty();
  }

  public int size() {
    this.rq.checkReferences();
    return this.internalMap.size();
  }

  public void clear() {
    this.rq.checkReferences();
    this.internalMap.clear();
  }

  public Set<K> keySet() {
    this.rq.checkReferences();
    return this.internalMap.keySet();
  }

  @SuppressWarnings("unchecked")
  public Comparator comparator() {
    return this.internalMap.comparator();
  }

  public Set<Map.Entry<K,V>> entrySet() {
    this.rq.checkReferences();
    Set<Map.Entry<K, SoftValue<K,V>>> entries = this.internalMap.entrySet();
    Set<Map.Entry<K, V>> real_entries = new TreeSet<Map.Entry<K,V>>();
    for(Map.Entry<K, SoftValue<K,V>> entry : entries) {
      real_entries.add(entry.getValue());
    }
    return real_entries;
  }

  public Collection<V> values() {
    this.rq.checkReferences();
    Collection<SoftValue<K,V>> softValues = this.internalMap.values();
    ArrayList<V> hardValues = new ArrayList<V>();
    for(SoftValue<K,V> softValue : softValues) {
      hardValues.add(softValue.get());
    }
    return hardValues;
  }
}