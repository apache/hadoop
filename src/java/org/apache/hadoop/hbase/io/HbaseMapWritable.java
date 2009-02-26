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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable Map.
 * Like {@link org.apache.hadoop.io.MapWritable} but dumb. It will fail
 * if passed a value type that it has not already been told about. Its  been
 * primed with hbase Writables and byte [].  Keys are always byte arrays.
 *
 * @param <K> <byte []> key  TODO: Parameter K is never used, could be removed.
 * @param <V> value Expects a Writable or byte [].
 */
public class HbaseMapWritable <K,V>
implements SortedMap<byte[],V>, Configurable, Writable,
  CodeToClassAndBack{
  
  private AtomicReference<Configuration> conf = null;
  protected SortedMap<byte [], V> instance = null;
  
  // Static maps of code to class and vice versa.  Includes types used in hbase
  // only. These maps are now initialized in a static loader interface instead
  // of in a static contructor for this class, this is done so that it is
  // possible to have a regular contructor here, so that different params can
  // be used.
  
  // Removed the old types like Text from the maps, if needed to add more types
  // this can be done in the StaticHBaseMapWritableLoader interface. Only
  // byte[] and Cell are supported now.
  //   static final Map<Byte, Class<?>> CODE_TO_CLASS =
  //     new HashMap<Byte, Class<?>>();
  //   static final Map<Class<?>, Byte> CLASS_TO_CODE =
  //     new HashMap<Class<?>, Byte>();

  /**
   * The default contructor where a TreeMap is used
   **/
   public HbaseMapWritable(){
     this (new TreeMap<byte [], V>(Bytes.BYTES_COMPARATOR));
   }

   /**
  * Contructor where another SortedMap can be used
  * 
  * @param map the SortedMap to be used 
  **/
  public HbaseMapWritable(SortedMap<byte[], V> map){
    conf = new AtomicReference<Configuration>();
    instance = map;
  }
  
  
  /** @return the conf */
  public Configuration getConf() {
    return conf.get();
  }

  /** @param conf the conf to set */
  public void setConf(Configuration conf) {
    this.conf.set(conf);
  }

  public void clear() {
    instance.clear();
  }

  public boolean containsKey(Object key) {
    return instance.containsKey(key);
  }

  public boolean containsValue(Object value) {
    return instance.containsValue(value);
  }

  public Set<Entry<byte [], V>> entrySet() {
    return instance.entrySet();
  }

  public V get(Object key) {
    return instance.get(key);
  }
  
  public boolean isEmpty() {
    return instance.isEmpty();
  }

  public Set<byte []> keySet() {
    return instance.keySet();
  }

  public int size() {
    return instance.size();
  }

  public Collection<V> values() {
    return instance.values();
  }

  public void putAll(Map<? extends byte [], ? extends V> m) {
    this.instance.putAll(m);
  }

  public V remove(Object key) {
    return this.instance.remove(key);
  }

  public V put(byte [] key, V value) {
    return this.instance.put(key, value);
  }

  public Comparator<? super byte[]> comparator() {
    return this.instance.comparator();
  }

  public byte[] firstKey() {
    return this.instance.firstKey();
  }

  public SortedMap<byte[], V> headMap(byte[] toKey) {
    return this.instance.headMap(toKey);
  }

  public byte[] lastKey() {
    return this.instance.lastKey();
  }

  public SortedMap<byte[], V> subMap(byte[] fromKey, byte[] toKey) {
    return this.instance.subMap(fromKey, toKey);
  }

  public SortedMap<byte[], V> tailMap(byte[] fromKey) {
    return this.instance.tailMap(fromKey);
  }
  
  // Writable

  /** @return the Class class for the specified id */
  @SuppressWarnings("boxing")
  protected Class<?> getClass(byte id) {
    return CODE_TO_CLASS.get(id);
  }

  /** @return the id for the specified Class */
  @SuppressWarnings("boxing")
  protected byte getId(Class<?> clazz) {
    Byte b = CLASS_TO_CODE.get(clazz);
    if (b == null) {
      throw new NullPointerException("Nothing for : " + clazz);
    }
    return b;
  }
  
  @Override
  public String toString() {
    return this.instance.toString();
  }

  public void write(DataOutput out) throws IOException {
    // Write out the number of entries in the map
    out.writeInt(this.instance.size());

    // Then write out each key/value pair
    for (Map.Entry<byte [], V> e: instance.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      Byte id = getId(e.getValue().getClass());
      out.writeByte(id);
      Object value = e.getValue();
      if (value instanceof byte []) {
        Bytes.writeByteArray(out, (byte [])value);
      } else {
        ((Writable)value).write(out);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    // First clear the map.  Otherwise we will just accumulate
    // entries every time this method is called.
    this.instance.clear();
    
    // Read the number of entries in the map
    int entries = in.readInt();
    
    // Then read each key/value pair
    for (int i = 0; i < entries; i++) {
      byte [] key = Bytes.readByteArray(in);
      Class clazz = getClass(in.readByte());
      V value = null;
      if (clazz.equals(byte [].class)) {
        byte [] bytes = Bytes.readByteArray(in);
        value = (V)bytes;
      } else {
        Writable w = (Writable)ReflectionUtils.
          newInstance(clazz, getConf());
        w.readFields(in);
        value = (V)w;
      }
      this.instance.put(key, value);
    }
  }
}
