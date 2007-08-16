/**
 * Copyright 2007 The Apache Software Foundation
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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HConstants;

/**
 * 
 */
public class MapWritable implements Writable, Map<WritableComparable, Writable> {
  private String keyClass = null;
  private String valueClass = null;
  private String mapClass = null;
  private Map<WritableComparable, Writable> instance = null;
  
  /**
   * Default constructor used by writable
   */
  public MapWritable() {}
  
  /**
   * @param keyClass the class of the keys
   * @param valueClass the class of the values
   * @param instance the Map to be wrapped in this Writable
   */
  @SuppressWarnings("unchecked")
  public MapWritable(Class keyClass, Class valueClass,
      Map<WritableComparable, Writable> instance) {
    
    this.keyClass = keyClass.getName();
    this.valueClass = valueClass.getName();
    this.instance = instance;
    this.mapClass = instance.getClass().getName();
  }
  
  private void checkInitialized() {
    if (keyClass == null || 
        valueClass == null || 
        mapClass == null || 
        instance == null) {
      
      throw new IllegalStateException("object has not been properly initialized");
    }
  }

  /** {@inheritDoc} */
  public void clear() {
    checkInitialized();
    instance.clear();
  }

  /** {@inheritDoc} */
  public boolean containsKey(Object key) {
    checkInitialized();
    return instance.containsKey(key);
  }

  /** {@inheritDoc} */
  public boolean containsValue(Object value) {
    checkInitialized();
    return instance.containsValue(value);
  }

  /** {@inheritDoc} */
  public Set<Map.Entry<WritableComparable, Writable>> entrySet() {
    checkInitialized();
    return instance.entrySet();
  }

  /** {@inheritDoc} */
  public Writable get(Object key) {
    checkInitialized();
    return instance.get(key);
  }
  
  /**
   * Returns the value to which this map maps the specified key
   * @param key
   * @return value associated with specified key
   */
  public Writable get(WritableComparable key) {
    checkInitialized();
    return instance.get(key);
  }

  /** {@inheritDoc} */
  public boolean isEmpty() {
    checkInitialized();
    return instance.isEmpty();
  }

  /** {@inheritDoc} */
  public Set<WritableComparable> keySet() {
    checkInitialized();
    return instance.keySet();
  }

  /** {@inheritDoc} */
  public Writable put(WritableComparable key, Writable value) {
    checkInitialized();
    return instance.put(key, value);
  }

  /** {@inheritDoc} */
  public void putAll(Map<? extends WritableComparable,? extends Writable> t) {
    checkInitialized();
    instance.putAll(t);
  }

  /** {@inheritDoc} */
  public Writable remove(Object key) {
    checkInitialized();
    return instance.remove(key);
  }

  /**
   * Removes the mapping for this key from this map if it is present
   * @param key
   * @return value corresponding to key
   */
  public Writable remove(WritableComparable key) {
    checkInitialized();
    return instance.remove(key);
  }

  /** {@inheritDoc} */
  public int size() {
    checkInitialized();
    return instance.size();
  }

  /** {@inheritDoc} */
  public Collection<Writable> values() {
    checkInitialized();
    return instance.values();
  }

  // Writable
  
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    checkInitialized();
    out.writeUTF(mapClass);
    out.writeUTF(keyClass);
    out.writeUTF(valueClass);
    out.writeInt(instance.size());
    
    for (Map.Entry<WritableComparable, Writable> e: instance.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    mapClass = in.readUTF();
    keyClass = in.readUTF();
    valueClass = in.readUTF();
    
    instance = (Map<WritableComparable, Writable>) objectFactory(mapClass);
    
    int entries = in.readInt();
    for (int i = 0; i < entries; i++) {
      WritableComparable key = (WritableComparable) objectFactory(keyClass);
      key.readFields(in);
      
      Writable value = (Writable) objectFactory(valueClass);
      value.readFields(in);
      
      instance.put(key, value);
    }
  }
  
  private Object objectFactory(String className) throws IOException {
    Object o = null;
    String exceptionMessage = null;
    try {
      o = Class.forName(className).newInstance();
      
    } catch (ClassNotFoundException e) {
      exceptionMessage = e.getMessage();
      
    } catch (InstantiationException e) {
      exceptionMessage = e.getMessage();
      
    } catch (IllegalAccessException e) {
      exceptionMessage = e.getMessage();
      
    } finally {
      if (exceptionMessage != null) {
        throw new IOException("error instantiating " + className + " because " +
            exceptionMessage);
      }
    }
    return o;
  }
  
  /**
   * A simple main program to test this class.
   * 
   * @param args not used
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void main(@SuppressWarnings("unused") String[] args)
  throws IOException {
    
    HStoreKey[] keys = {
        new HStoreKey(new Text("row1"), HConstants.COL_REGIONINFO),
        new HStoreKey(new Text("row2"), HConstants.COL_SERVER),
        new HStoreKey(new Text("row3"), HConstants.COL_STARTCODE)
    };
    
    ImmutableBytesWritable[] values = {
        new ImmutableBytesWritable("value1".getBytes()),
        new ImmutableBytesWritable("value2".getBytes()),
        new ImmutableBytesWritable("value3".getBytes())
    };

    @SuppressWarnings("unchecked")
    MapWritable inMap = new MapWritable(HStoreKey.class,
        ImmutableBytesWritable.class,
        (Map) new TreeMap<HStoreKey, ImmutableBytesWritable>());
    
    for (int i = 0; i < keys.length; i++) {
      inMap.put(keys[i], values[i]);
    }

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bytes);
    try {
      inMap.write(out);
      
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    
    MapWritable outMap = new MapWritable();
    DataInput in =
      new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    
    try {
      outMap.readFields(in);
      
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    
    if (outMap.size() != inMap.size()) {
      System.err.println("outMap.size()=" + outMap.size() + " != " +
          "inMap.size()=" + inMap.size());
    }
    
    for (Map.Entry<WritableComparable, Writable> e: inMap.entrySet()) {
      if (!outMap.containsKey(e.getKey())) {
        System.err.println("outMap does not contain key " + e.getKey().toString());
        continue;
      }
      if (((WritableComparable) outMap.get(e.getKey())).compareTo(
          e.getValue()) != 0) {
        System.err.println("output value for " + e.getKey().toString() + " != input value");
      }
    }
    System.out.println("it worked!");
  }
}
