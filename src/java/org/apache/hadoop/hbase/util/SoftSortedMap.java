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

import java.util.SortedMap;
import java.util.TreeMap;
import java.lang.ref.SoftReference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Comparator;
import java.util.Collection;
import java.util.ArrayList;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A SortedMap implementation that uses SoftReferences internally to make it
 * play well with the GC when in a low-memory situation.
 * 
 * @param <K> key class
 * @param <V> value class
 */
public class SoftSortedMap<K,V> implements SortedMap<K,V> {
  private static final Log LOG = LogFactory.getLog(SoftSortedMap.class);  
  private final SortedMap<K, SoftValue<K,V>> internalMap;
  private ReferenceQueue<K> referenceQueue = new ReferenceQueue<K>();
  
  /** Constructor */
  public SoftSortedMap() {
    this(new TreeMap<K, SoftValue<K,V>>());
  }
  
  /**
   * Constructor
   * @param c
   */
  public SoftSortedMap(final Comparator<K> c) {
    this(new TreeMap<K, SoftValue<K,V>>(c));
  }
  
  /** For headMap and tailMap support */
  private SoftSortedMap(SortedMap<K,SoftValue<K,V>> original) {
    internalMap = original;
  }
  
  /* Client methods */
  public V put(K key, V value) {
    checkReferences();
    SoftValue<K,V> oldValue = 
      internalMap.put(key, new SoftValue<K,V>(key, value, referenceQueue));
    return oldValue == null ? null : oldValue.get();
  }
  
  @SuppressWarnings("unchecked")
  public void putAll(@SuppressWarnings("unused") Map map) {
    throw new RuntimeException("Not implemented");
  }
  
  public V get(Object key) {
    checkReferences();
    SoftValue<K,V> value = internalMap.get(key);
    if (value == null) {
      return null;
    }
    if (value.get() == null) {
      internalMap.remove(key);
      return null;
    }
    return value.get();
  }

  public V remove(Object key) {
    checkReferences();
    SoftValue<K,V> value = internalMap.remove(key);
    return value == null ? null : value.get();
  }

  public boolean containsKey(Object key) {
    checkReferences(); 
    return internalMap.containsKey(key);
  }
  
  public boolean containsValue(@SuppressWarnings("unused") Object value) {
/*    checkReferences();
    return internalMap.containsValue(value);*/
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public K firstKey() {
    checkReferences();
    return internalMap.firstKey();
  }

  public K lastKey() {
    checkReferences();
    return internalMap.lastKey();
  }
  
  public SoftSortedMap<K,V> headMap(K key) {
    checkReferences();
    return new SoftSortedMap<K,V>(internalMap.headMap(key));
  }
  
  public SoftSortedMap<K,V> tailMap(K key) {
    checkReferences();
    return new SoftSortedMap<K,V>(internalMap.tailMap(key));
  }
  
  public SoftSortedMap<K,V> subMap(K fromKey, K toKey) {
    checkReferences();
    return new SoftSortedMap<K,V>(internalMap.subMap(fromKey, toKey));
  }
  
  public boolean isEmpty() {
    checkReferences();
    return internalMap.isEmpty();
  }

  public int size() {
    checkReferences();
    return internalMap.size();
  }

  public void clear() {
    internalMap.clear();
  }

  public Set<K> keySet() {
    return internalMap.keySet();
  }

  @SuppressWarnings("unchecked")
  public Comparator comparator() {
    return internalMap.comparator();
  }

  public Set<Map.Entry<K,V>> entrySet() {
    Set<Map.Entry<K, SoftValue<K,V>>> entries = internalMap.entrySet();
    Set<Map.Entry<K, V>> real_entries = new TreeSet<Map.Entry<K,V>>();
    for(Map.Entry<K, SoftValue<K,V>> entry : entries) {
      real_entries.add(entry.getValue());
    }
    return real_entries;
  }

  public Collection<V> values() {
    checkReferences();
    Collection<SoftValue<K,V>> softValues = internalMap.values();
    ArrayList<V> hardValues = new ArrayList<V>();
    for(SoftValue<K,V> softValue : softValues) {
      hardValues.add(softValue.get());
    }
    return hardValues;
  }

  /**
   * Check the reference queue and delete anything that has since gone away
   */ 
  @SuppressWarnings("unchecked")
  private void checkReferences() {
    SoftValue<K,V> sv;
    Object obj;
    while((obj = referenceQueue.poll()) != null) {
      if (LOG.isDebugEnabled()) {
        Object k = ((SoftValue<K,V>)obj).key;
        String name = (k instanceof byte [])? Bytes.toString((byte [])k): k.toString();
        LOG.debug("Reference for key " + name + " has been cleared.");
      }
      internalMap.remove(((SoftValue<K,V>)obj).key);
    }
  }
  
  /**
   * A SoftReference derivative so that we can track down what keys to remove.
   */ 
  private class SoftValue<K2, V2> extends SoftReference<V2> implements Map.Entry<K2,V2> {
    K2 key;
    
    SoftValue(K2 key, V2 value, ReferenceQueue queue) {
      super(value, queue);
      this.key = key;
    }
    
    public K2 getKey() {return key;}
    public V2 getValue() {return get();}

    public V2 setValue(@SuppressWarnings("unused") V2 value) {
      throw new RuntimeException("Not implemented");
    }
  }
}
