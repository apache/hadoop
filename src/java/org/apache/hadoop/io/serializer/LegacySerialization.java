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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * Wraps a legacy {@link Serialization} as a {@link SerializationBase}.
 * </p>
 * 
 * @param <T>
 */
@SuppressWarnings("deprecation")
class LegacySerialization<T> extends SerializationBase<T> {

  private Serialization<T> serialization;

  public LegacySerialization(Serialization<T> serialization,
      Configuration conf) {
    this.serialization = serialization;
    setConf(conf);
  }
  
  Serialization<T> getUnderlyingSerialization() {
    return serialization;
  }

  @Deprecated
  @Override
  public boolean accept(Class<?> c) {
    return serialization.accept(c);
  }

  @Deprecated
  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return serialization.getDeserializer(c);
  }

  @Deprecated
  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return serialization.getSerializer(c);
  }
  
  @Override
  public boolean accept(Map<String, String> metadata) {
    Class<?> c = getClassFromMetadata(metadata);
    return accept(c);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SerializerBase<T> getSerializer(Map<String, String> metadata) {
    Class<T> c = (Class<T>) getClassFromMetadata(metadata);
    return new LegacySerializer<T>(getSerializer(c));
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public DeserializerBase<T> getDeserializer(Map<String, String> metadata) {
    Class<T> c = (Class<T>) getClassFromMetadata(metadata);
    return new LegacyDeserializer<T>(getDeserializer(c));
  }

}
