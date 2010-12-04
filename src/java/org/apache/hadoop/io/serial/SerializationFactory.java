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

package org.apache.hadoop.io.serial;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SERIALIZATIONS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.lib.CompatibilitySerialization;
import org.apache.hadoop.io.serial.lib.WritableSerialization;
import org.apache.hadoop.io.serial.lib.avro.AvroSerialization;
import org.apache.hadoop.io.serial.lib.protobuf.ProtoBufSerialization;
import org.apache.hadoop.io.serial.lib.thrift.ThriftSerialization;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory that finds and creates Serializations.
 * 
 * There are two methods. The first finds a Serialization by its name (ie.
 * avro, writable, thrift, etc.). The second finds a TypedSerialization based
 * on the type that needs to be serialized.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SerializationFactory {
  private static final Log LOG = LogFactory.getLog(SerializationFactory.class);

  private final List<TypedSerialization<?>> typedSerializations =
    new ArrayList<TypedSerialization<?>>();
  private final Map<String, Serialization<?>> serializations = 
    new HashMap<String, Serialization<?>>();

  public SerializationFactory(Configuration conf) {
    Class<?>[] classes = 
      conf.getClasses(HADOOP_SERIALIZATIONS_KEY, 
                      new Class<?>[]{WritableSerialization.class,
                                     ProtoBufSerialization.class,
                                     ThriftSerialization.class,
                                     AvroSerialization.class,
                                     CompatibilitySerialization.class});
    for(Class<?> cls: classes) {
      if (Serialization.class.isAssignableFrom(cls)) {
        Serialization<?> serial = 
          (Serialization<?>) ReflectionUtils.newInstance(cls, conf);
        if (serial instanceof TypedSerialization<?>) {
          typedSerializations.add((TypedSerialization<?>) serial);
        }
        String name = serial.getName();
        if (serializations.containsKey(name)) {
          throw new IllegalArgumentException("Two serializations have the" + 
                                             " same name: " + name);
        }
        serializations.put(serial.getName(), serial);
        LOG.debug("Adding serialization " + serial.getName());
      } else {
        throw new IllegalArgumentException("Unknown serialization class " +
                                           cls.getName());
      }
    }
  }

  private static final Map<String, SerializationFactory> FACTORY_CACHE =
    new HashMap<String, SerializationFactory>();
  
  /**
   * Get the cached factory for the given configuration. Two configurations
   * that have the same io.configurations value will be considered identical
   * because we can't keep a reference to the Configuration without locking it
   * in memory.
   * @param conf the configuration
   * @return the factory for a given configuration
   */
  public static synchronized 
  SerializationFactory getInstance(Configuration conf) {
    String serializerNames = conf.get(HADOOP_SERIALIZATIONS_KEY, "*default*");
    String obsoleteSerializerNames = conf.get(IO_SERIALIZATIONS_KEY, "*default*");
    String key = serializerNames + " " + obsoleteSerializerNames;
    SerializationFactory result = FACTORY_CACHE.get(key);
    if (result == null) {
      result = new SerializationFactory(conf);
      FACTORY_CACHE.put(key, result);
    }
    return result;
  }

  /**
   * Look up a serialization by name and return a clone of it.
   * @param name
   * @return a newly cloned serialization of the right name
   */
  public Serialization<?> getSerialization(String name) {
    return serializations.get(name).clone();
  }
  
  /**
   * Find the first acceptable serialization for a given type.
   * @param cls the class that should be serialized
   * @return a serialization that should be used to serialize the class
   */
  @SuppressWarnings("unchecked")
  public <T> TypedSerialization<? super T> getSerializationByType(Class<T> cls){
    for (TypedSerialization<?> serial: typedSerializations) {
      if (serial.accept(cls)) {
        TypedSerialization<? super T> result = 
          (TypedSerialization<? super T>) serial.clone();
        result.setSpecificType(cls);
        return result;
      }
    }
    throw new IllegalArgumentException("Could not find a serialization to"+
                                       " accept " + cls.getName());
  }

}
