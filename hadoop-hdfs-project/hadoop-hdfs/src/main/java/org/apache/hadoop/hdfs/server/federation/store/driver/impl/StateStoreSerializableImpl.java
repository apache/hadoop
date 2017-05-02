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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

/**
 * State Store driver that stores a serialization of the records. The serializer
 * is pluggable.
 */
public abstract class StateStoreSerializableImpl extends StateStoreBaseImpl {

  /** Default serializer for this driver. */
  private StateStoreSerializer serializer;


  @Override
  public boolean init(final Configuration config, final String id,
      final Collection<Class<? extends BaseRecord>> records) {
    boolean ret = super.init(config, id, records);

    this.serializer = StateStoreSerializer.getSerializer(config);

    return ret;
  }

  /**
   * Serialize a record using the serializer.
   * @param record Record to serialize.
   * @return Byte array with the serialization of the record.
   */
  protected <T extends BaseRecord> byte[] serialize(T record) {
    return serializer.serialize(record);
  }

  /**
   * Serialize a record using the serializer.
   * @param record Record to serialize.
   * @return String with the serialization of the record.
   */
  protected <T extends BaseRecord> String serializeString(T record) {
    return serializer.serializeString(record);
  }

  /**
   * Creates a record from an input data string.
   * @param data Serialized text of the record.
   * @param clazz Record class.
   * @param includeDates If dateModified and dateCreated are serialized.
   * @return The created record.
   * @throws IOException
   */
  protected <T extends BaseRecord> T newRecord(
      String data, Class<T> clazz, boolean includeDates) throws IOException {
    return serializer.deserialize(data, clazz);
  }
}