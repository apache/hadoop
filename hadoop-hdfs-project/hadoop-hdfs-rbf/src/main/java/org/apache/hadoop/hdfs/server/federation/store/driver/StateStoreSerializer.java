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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Serializer to store and retrieve data in the State Store.
 */
public abstract class StateStoreSerializer {

  /** Singleton for the serializer instance. */
  private static StateStoreSerializer defaultSerializer;

  /**
   * Get the default serializer based.
   * @return Singleton serializer.
   */
  public static StateStoreSerializer getSerializer() {
    return getSerializer(null);
  }

  /**
   * Get a serializer based on the provided configuration.
   * @param conf Configuration. Default if null.
   * @return Singleton serializer.
   */
  public static StateStoreSerializer getSerializer(Configuration conf) {
    if (conf == null) {
      synchronized (StateStoreSerializer.class) {
        if (defaultSerializer == null) {
          conf = new Configuration();
          defaultSerializer = newSerializer(conf);
        }
      }
      return defaultSerializer;
    } else {
      return newSerializer(conf);
    }
  }

  private static StateStoreSerializer newSerializer(final Configuration conf) {
    Class<? extends StateStoreSerializer> serializerName = conf.getClass(
        RBFConfigKeys.FEDERATION_STORE_SERIALIZER_CLASS,
        RBFConfigKeys.FEDERATION_STORE_SERIALIZER_CLASS_DEFAULT,
        StateStoreSerializer.class);
    return ReflectionUtils.newInstance(serializerName, conf);
  }

  /**
   * Create a new record.
   *
   * @param clazz Class of the new record.
   * @param <T> Type of the record.
   * @return New record.
   */
  public static <T> T newRecord(Class<T> clazz) {
    return getSerializer(null).newRecordInstance(clazz);
  }

  /**
   * Create a new record.
   *
   * @param clazz Class of the new record.
   * @param <T> Type of the record.
   * @return New record.
   */
  public abstract <T> T newRecordInstance(Class<T> clazz);

  /**
   * Serialize a record into a byte array.
   * @param record Record to serialize.
   * @return Byte array with the serialized record.
   */
  public abstract byte[] serialize(BaseRecord record);

  /**
   * Serialize a record into a string.
   * @param record Record to serialize.
   * @return String with the serialized record.
   */
  public abstract String serializeString(BaseRecord record);

  /**
   * Deserialize a bytes array into a record.
   *
   * @param byteArray Byte array to deserialize.
   * @param clazz Class of the record.
   * @param <T> Type of the record.
   * @return New record.
   * @throws IOException If it cannot deserialize the record.
   */
  public abstract <T extends BaseRecord> T deserialize(
      byte[] byteArray, Class<T> clazz) throws IOException;

  /**
   * Deserialize a string into a record.
   *
   * @param data String with the data to deserialize.
   * @param clazz Class of the record.
   * @param <T> Type of the record.
   * @return New record.
   * @throws IOException If it cannot deserialize the record.
   */
  public abstract <T extends BaseRecord> T deserialize(
      String data, Class<T> clazz) throws IOException;
}