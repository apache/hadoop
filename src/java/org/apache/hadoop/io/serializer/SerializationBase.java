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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * Encapsulates a {@link SerializerBase}/{@link DeserializerBase} pair.
 * </p>
 * 
 * @param <T>
 */
public abstract class SerializationBase<T> extends Configured
  implements Serialization<T> {
    
  public static final String SERIALIZATION_KEY = "Serialization-Class";
  public static final String CLASS_KEY = "Serialized-Class";
  
  public static Map<String, String> getMetadataFromClass(Class<?> c) {
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(CLASS_KEY, c.getName());
    return metadata;
  }
  
  @Deprecated
  @Override
  public boolean accept(Class<?> c) {
    return accept(getMetadataFromClass(c));
  }

  @Deprecated
  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return getDeserializer(getMetadataFromClass(c));
  }

  @Deprecated
  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return getSerializer(getMetadataFromClass(c));
  }

  /**
   * Allows clients to test whether this {@link SerializationBase} supports the
   * given metadata.
   */
  public abstract boolean accept(Map<String, String> metadata);

  /**
   * @return a {@link SerializerBase} for the given metadata.
   */
  public abstract SerializerBase<T> getSerializer(Map<String, String> metadata);

  /**
   * @return a {@link DeserializerBase} for the given metadata.
   */
  public abstract DeserializerBase<T> getDeserializer(
      Map<String, String> metadata);
  
  protected Class<?> getClassFromMetadata(Map<String, String> metadata) {
    String classname = metadata.get(CLASS_KEY);
    if (classname == null) {
      return null;
    }
    try {
      return getConf().getClassByName(classname);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Provide a raw comparator for the specified serializable class.
   * Requires a serialization-specific metadata entry to name the class
   * to compare (e.g., "Serialized-Class" for JavaSerialization and
   * WritableSerialization).
   * @param metadata a set of string mappings providing serialization-specific
   * arguments that parameterize the data being serialized/compared.
   * @return a {@link RawComparator} for the given metadata.
   * @throws UnsupportedOperationException if it cannot instantiate a RawComparator
   * for this given metadata.
   */
  public abstract RawComparator<T> getRawComparator(Map<String,String> metadata);

  /**
   * Check that the SERIALIZATION_KEY, if set, matches the current class.
   * @param metadata the serialization metadata to check.
   * @return true if SERIALIZATION_KEY is unset, or if it matches the current class
   * (meaning that accept() should continue processing), or false if it is a mismatch,
   * meaning that accept() should return false.
   */
  protected boolean checkSerializationKey(Map<String, String> metadata) {
    String intendedSerializer = metadata.get(SERIALIZATION_KEY);
    return intendedSerializer == null ||
        getClass().getName().equals(intendedSerializer);
  }
}
