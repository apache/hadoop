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
package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable SortedMap.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SortedMapWritable<K extends WritableComparable<? super K>> extends AbstractMapWritable
  implements SortedMap<K, Writable> {
  
  private SortedMap<K, Writable> instance;
  
  /** default constructor. */
  public SortedMapWritable() {
    super();
    this.instance = new TreeMap<K, Writable>();
  }
  
  /**
   * Copy constructor.
   * 
   * @param other the map to copy from
   */
  public SortedMapWritable(SortedMapWritable<K> other) {
    this();
    copy(other);
  }

  @Override
  public Comparator<? super K> comparator() {
    // Returning null means we use the natural ordering of the keys
    return null;
  }

  @Override
  public K firstKey() {
    return instance.firstKey();
  }

  @Override
  public SortedMap<K, Writable> headMap(K toKey) {
    return instance.headMap(toKey);
  }

  @Override
  public K lastKey() {
    return instance.lastKey();
  }

  @Override
  public SortedMap<K, Writable> subMap(K fromKey, K toKey) {
    return instance.subMap(fromKey, toKey);
  }

  @Override
  public SortedMap<K, Writable> tailMap(K fromKey) {
    return instance.tailMap(fromKey);
  }

  @Override
  public void clear() {
    instance.clear();
  }

  @Override
  public boolean containsKey(Object key) {
    return instance.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return instance.containsValue(value);
  }

  @Override
  public Set<Map.Entry<K, Writable>> entrySet() {
    return instance.entrySet();
  }

  @Override
  public Writable get(Object key) {
    return instance.get(key);
  }

  @Override
  public boolean isEmpty() {
    return instance.isEmpty();
  }

  @Override
  public Set<K> keySet() {
    return instance.keySet();
  }

  @Override
  public Writable put(K key, Writable value) {
    addToMap(key.getClass());
    addToMap(value.getClass());
    return instance.put(key, value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends Writable> t) {
    for (Map.Entry<? extends K, ? extends Writable> e:
      t.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public Writable remove(Object key) {
    return instance.remove(key);
  }

  @Override
  public int size() {
    return instance.size();
  }

  @Override
  public Collection<Writable> values() {
    return instance.values();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    
    // Read the number of entries in the map
    
    int entries = in.readInt();
    
    // Then read each key/value pair
    
    for (int i = 0; i < entries; i++) {
      K key =
        (K) ReflectionUtils.newInstance(getClass(
            in.readByte()), getConf());
      
      key.readFields(in);
      
      Writable value = (Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      value.readFields(in);
      instance.put(key, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    
    // Write out the number of entries in the map
    
    out.writeInt(instance.size());
    
    // Then write out each key/value pair
    
    for (Map.Entry<K, Writable> e: instance.entrySet()) {
      out.writeByte(getId(e.getKey().getClass()));
      e.getKey().write(out);
      out.writeByte(getId(e.getValue().getClass()));
      e.getValue().write(out);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof SortedMapWritable) {
      Map<?,?> map = (Map<?,?>) obj;
      if (size() != map.size()) {
        return false;
      }

      return entrySet().equals(map.entrySet());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return instance.hashCode();
  }
}
