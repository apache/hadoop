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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable Map.
 * Like {@link org.apache.hadoop.io.MapWritable} but dumb. It will fail
 * if passed a Writable it has not already been told about. Its also been
 * primed with hbase Writables.
 */
public class HbaseMapWritable implements Map<Writable, Writable>, Writable,
    Configurable {
  private AtomicReference<Configuration> conf =
    new AtomicReference<Configuration>();
  
  // Static maps of code to class and vice versa.  Includes types used in hbase
  // only.
  static final Map<Byte, Class<? extends Writable>> CODE_TO_CLASS =
    new HashMap<Byte, Class<? extends Writable>>();
  static final Map<Class<? extends Writable>, Byte> CLASS_TO_CODE =
    new HashMap<Class<? extends Writable>, Byte>();

  static {
    byte code = 0;
    addToMap(HStoreKey.class, code++);
    addToMap(ImmutableBytesWritable.class, code++);
    addToMap(Text.class, code++);
  }

  @SuppressWarnings("boxing")
  private static void addToMap(final Class<? extends Writable> clazz,
      final byte code) {
    CLASS_TO_CODE.put(clazz, code);
    CODE_TO_CLASS.put(code, clazz);
  }
  
  private Map<Writable, Writable> instance;
  
  /** Default constructor. */
  public HbaseMapWritable() {
    super();
    this.instance = new HashMap<Writable, Writable>();
  }

  /** @return the conf */
  public Configuration getConf() {
    return conf.get();
  }

  /** @param conf the conf to set */
  public void setConf(Configuration conf) {
    this.conf.set(conf);
  }

  /** {@inheritDoc} */
  public void clear() {
    instance.clear();
  }

  /** {@inheritDoc} */
  public boolean containsKey(Object key) {
    return instance.containsKey(key);
  }

  /** {@inheritDoc} */
  public boolean containsValue(Object value) {
    return instance.containsValue(value);
  }

  /** {@inheritDoc} */
  public Set<Map.Entry<Writable, Writable>> entrySet() {
    return instance.entrySet();
  }

  /** {@inheritDoc} */
  public Writable get(Object key) {
    return instance.get(key);
  }
  
  /** {@inheritDoc} */
  public boolean isEmpty() {
    return instance.isEmpty();
  }

  /** {@inheritDoc} */
  public Set<Writable> keySet() {
    return instance.keySet();
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  public Writable put(Writable key, Writable value) {
    return instance.put(key, value);
  }

  /** {@inheritDoc} */
  public void putAll(Map<? extends Writable, ? extends Writable> t) {
    for (Map.Entry<? extends Writable, ? extends Writable> e: t.entrySet()) {
      instance.put(e.getKey(), e.getValue());
    }
  }

  /** {@inheritDoc} */
  public Writable remove(Object key) {
    return instance.remove(key);
  }

  /** {@inheritDoc} */
  public int size() {
    return instance.size();
  }

  /** {@inheritDoc} */
  public Collection<Writable> values() {
    return instance.values();
  }
  
  // Writable

  /** @return the Class class for the specified id */
  @SuppressWarnings({ "unchecked", "boxing" })
  protected Class<?> getClass(byte id) {
    return CODE_TO_CLASS.get(id);
  }

  /** @return the id for the specified Class */
  @SuppressWarnings({ "unchecked", "boxing" })
  protected byte getId(Class<?> clazz) {
    Byte b = CLASS_TO_CODE.get(clazz);
    if (b == null) {
      throw new NullPointerException("Nothing for : " + clazz);
    }
    return b;
  }

  public void write(DataOutput out) throws IOException {
    // Write out the number of entries in the map
    out.writeInt(instance.size());

    // Then write out each key/value pair
    for (Map.Entry<Writable, Writable> e: instance.entrySet()) {
      out.writeByte(getId(e.getKey().getClass()));
      e.getKey().write(out);
      out.writeByte(getId(e.getValue().getClass()));
      e.getValue().write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    // First clear the map.  Otherwise we will just accumulate
    // entries every time this method is called.
    this.instance.clear();
    
    // Read the number of entries in the map
    int entries = in.readInt();
    
    // Then read each key/value pair
    for (int i = 0; i < entries; i++) {
      Writable key = (Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      key.readFields(in);
      
      Writable value = (Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      value.readFields(in);
      instance.put(key, value);
    }
  }
}