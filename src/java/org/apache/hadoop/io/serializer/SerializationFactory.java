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

package org.apache.hadoop.io.serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.avro.AvroGenericSerialization;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * <p>
 * A factory for {@link SerializationBase}s.
 * </p>
 */
public class SerializationFactory extends Configured {
  
  private static final Log LOG =
    LogFactory.getLog(SerializationFactory.class.getName());

  private List<SerializationBase<?>> serializations =
    new ArrayList<SerializationBase<?>>();
  private List<SerializationBase<?>> legacySerializations =
    new ArrayList<SerializationBase<?>>();
  
  /**
   * <p>
   * Serializations are found by reading the <code>io.serializations</code>
   * property from <code>conf</code>, which is a comma-delimited list of
   * classnames. 
   * </p>
   */
  public SerializationFactory(Configuration conf) {
    super(conf);
    for (String serializerName : conf.getStrings("io.serializations", 
      new String[]{WritableSerialization.class.getName(), 
        AvroSpecificSerialization.class.getName(), 
        AvroReflectSerialization.class.getName(),
        AvroGenericSerialization.class.getName()})) {
      add(conf, serializerName);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void add(Configuration conf, String serializationName) {
    try {
      Class<?> serializationClass = conf.getClassByName(serializationName);
      if (SerializationBase.class.isAssignableFrom(serializationClass)) {
	serializations.add((SerializationBase)
	    ReflectionUtils.newInstance(serializationClass, getConf()));	
      } else if (Serialization.class.isAssignableFrom(serializationClass)) {
	Serialization serialization = (Serialization)
	    ReflectionUtils.newInstance(serializationClass, getConf());
	legacySerializations.add(new LegacySerialization(serialization,
	    getConf()));	
      } else {
	LOG.warn("Serialization class " + serializationName + " is not an " +
			"instance of Serialization or BaseSerialization.");
      }
    } catch (ClassNotFoundException e) {
      LOG.warn("Serialization class not found: " +
          StringUtils.stringifyException(e));
    }
  }

  @Deprecated
  public <T> Serializer<T> getSerializer(Class<T> c) {
    return getSerialization(c).getSerializer(c);
  }

  @Deprecated
  public <T> Deserializer<T> getDeserializer(Class<T> c) {
    return getSerialization(c).getDeserializer(c);
  }

  @Deprecated
  public <T> Serialization<T> getSerialization(Class<T> c) {
    return getSerialization(SerializationBase.getMetadataFromClass(c));
  }
  
  public <T> SerializerBase<T> getSerializer(Map<String, String> metadata) {
    SerializationBase<T> serialization = getSerialization(metadata);
    return serialization.getSerializer(metadata);
  }
    
  public <T> DeserializerBase<T> getDeserializer(Map<String, String> metadata) {
    SerializationBase<T> serialization = getSerialization(metadata);
    return serialization.getDeserializer(metadata);
  }
    
  @SuppressWarnings("unchecked")
  public <T> SerializationBase<T> getSerialization(Map<String, String> metadata) {
    for (SerializationBase serialization : serializations) {
      if (serialization.accept(metadata)) {
        return (SerializationBase<T>) serialization;
      }
    }
    // Look in the legacy serializations last, since they ignore
    // non-class metadata
    for (SerializationBase serialization : legacySerializations) {
      if (serialization.accept(metadata)) {
        return (SerializationBase<T>) serialization;
      }
    }
    return null;
  }
}
