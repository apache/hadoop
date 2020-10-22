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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Abstract base class for MapWritable and SortedMapWritable
 * 
 * Unlike org.apache.nutch.crawl.MapWritable, this class allows creation of
 * MapWritable&lt;Writable, MapWritable&gt; so the CLASS_TO_ID and ID_TO_CLASS
 * maps travel with the class instead of being static.
 * 
 * Class ids range from 1 to 127 so there can be at most 127 distinct classes
 * in any specific map instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractMapWritable implements Writable, Configurable {
  private AtomicReference<Configuration> conf;
  
  /* Class to id mappings */
  @VisibleForTesting
  Map<Class<?>, Byte> classToIdMap = new ConcurrentHashMap<Class<?>, Byte>();
  
  /* Id to Class mappings */
  @VisibleForTesting
  Map<Byte, Class<?>> idToClassMap = new ConcurrentHashMap<Byte, Class<?>>();
  
  /* The number of new classes (those not established by the constructor) */
  private volatile byte newClasses = 0;
  
  /** @return the number of known classes */
  byte getNewClasses() {
    return newClasses;
  }

  /**
   * Used to add "predefined" classes and by Writable to copy "new" classes.
   */
  private synchronized void addToMap(Class<?> clazz, byte id) {
    if (classToIdMap.containsKey(clazz)) {
      byte b = classToIdMap.get(clazz);
      if (b != id) {
        throw new IllegalArgumentException ("Class " + clazz.getName() +
          " already registered but maps to " + b + " and not " + id);
      }
    }
    if (idToClassMap.containsKey(id)) {
      Class<?> c = idToClassMap.get(id);
      if (!c.equals(clazz)) {
        throw new IllegalArgumentException("Id " + id + " exists but maps to " +
            c.getName() + " and not " + clazz.getName());
      }
    }
    classToIdMap.put(clazz, id);
    idToClassMap.put(id, clazz);
  }
  
  /** Add a Class to the maps if it is not already present. */ 
  protected synchronized void addToMap(Class<?> clazz) {
    if (classToIdMap.containsKey(clazz)) {
      return;
    }
    if (newClasses + 1 > Byte.MAX_VALUE) {
      throw new IndexOutOfBoundsException("adding an additional class would" +
      " exceed the maximum number allowed");
    }
    byte id = ++newClasses;
    addToMap(clazz, id);
  }

  /** @return the Class class for the specified id */
  protected Class<?> getClass(byte id) {
    return idToClassMap.get(id);
  }

  /** @return the id for the specified Class */
  protected byte getId(Class<?> clazz) {
    return classToIdMap.containsKey(clazz) ? classToIdMap.get(clazz) : -1;
  }

  /** Used by child copy constructors. */
  protected synchronized void copy(Writable other) {
    if (other != null) {
      try {
        DataOutputBuffer out = new DataOutputBuffer();
        other.write(out);
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        readFields(in);

      } catch (IOException e) {
        throw new IllegalArgumentException("map cannot be copied: " +
            e.getMessage());
      }

    } else {
      throw new IllegalArgumentException("source map cannot be null");
    }
  }

  /** constructor. */
  protected AbstractMapWritable() {
    this.conf = new AtomicReference<Configuration>();

    addToMap(ArrayWritable.class, (byte)-127);
    addToMap(BooleanWritable.class, (byte)-126);
    addToMap(BytesWritable.class, (byte)-125);
    addToMap(FloatWritable.class, (byte)-124);
    addToMap(IntWritable.class, (byte)-123);
    addToMap(LongWritable.class, (byte)-122);
    addToMap(MapWritable.class, (byte)-121);
    addToMap(MD5Hash.class, (byte)-120);
    addToMap(NullWritable.class, (byte)-119);
    addToMap(ObjectWritable.class, (byte)-118);
    addToMap(SortedMapWritable.class, (byte)-117);
    addToMap(Text.class, (byte)-116);
    addToMap(TwoDArrayWritable.class, (byte)-115);

    // UTF8 is deprecated so we don't support it

    addToMap(VIntWritable.class, (byte)-114);
    addToMap(VLongWritable.class, (byte)-113);
  }

  /** @return the conf */
  @Override
  public Configuration getConf() {
    return conf.get();
  }

  /** @param conf the conf to set */
  @Override
  public void setConf(Configuration conf) {
    this.conf.set(conf);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    
    // First write out the size of the class table and any classes that are
    // "unknown" classes
    
    out.writeByte(newClasses);

    for (byte i = 1; i <= newClasses; i++) {
      out.writeByte(i);
      out.writeUTF(getClass(i).getName());
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    
    // Get the number of "unknown" classes
    newClasses = in.readByte();

    // Use the classloader of the current thread to load classes instead of the
    // system-classloader so as to support both client-only and inside-a-MR-job
    // use-cases. The context-loader by default eventually falls back to the
    // system one, so there should be no cases where changing this is an issue.
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    // Then read in the class names and add them to our tables
    for (int i = 0; i < newClasses; i++) {
      byte id = in.readByte();
      String className = in.readUTF();
      try {
        addToMap(classLoader.loadClass(className), id);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }    
}
