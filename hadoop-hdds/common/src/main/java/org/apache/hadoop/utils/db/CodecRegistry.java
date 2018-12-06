/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.utils.db;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * Collection of available codecs.
 */
public class CodecRegistry {

  private Map<Class, Codec<?>> valueCodecs;

  public CodecRegistry() {
    valueCodecs = new HashMap<>();
    valueCodecs.put(String.class, new StringCodec());
  }

  /**
   * Convert raw value to strongly typed value/key with the help of a codec.
   *
   * @param rawData original byte array from the db.
   * @param format  Class of the return value
   * @param <T>     Type of the return value.
   * @return the object with the parsed field data
   */
  public <T> T asObject(byte[] rawData, Class<T> format) {
    if (rawData == null) {
      return null;
    }
    if (valueCodecs.containsKey(format)) {
      return (T) valueCodecs.get(format).fromPersistedFormat(rawData);
    } else {
      throw new IllegalStateException(
          "Codec is not registered for type: " + format);
    }
  }

  /**
   * Convert strongly typed object to raw data to store it in the kv store.
   *
   * @param object typed object.
   * @param <T>    Type of the typed object.
   * @return byte array to store it ini the kv store.
   */
  public <T> byte[] asRawData(T object) {
    Preconditions.checkNotNull(object,
        "Null value shouldn't be persisted in the database");
    Class<T> format = (Class<T>) object.getClass();
    if (valueCodecs.containsKey(format)) {
      Codec<T> codec = (Codec<T>) valueCodecs.get(format);
      return codec.toPersistedFormat(object);
    } else {
      throw new IllegalStateException(
          "Codec is not registered for type: " + format);
    }
  }

  /**
   * Addds codec to the internal collection.
   *
   * @param type  Type of the codec source/destination object.
   * @param codec The codec itself.
   * @param <T>   The type of the codec
   */
  public <T> void addCodec(Class<T> type, Codec<T> codec) {
    valueCodecs.put(type, codec);
  }

}
