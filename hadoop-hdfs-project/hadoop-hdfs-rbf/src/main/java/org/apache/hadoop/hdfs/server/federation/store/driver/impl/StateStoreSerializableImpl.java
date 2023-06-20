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
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

/**
 * State Store driver that stores a serialization of the records. The serializer
 * is pluggable.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class StateStoreSerializableImpl extends StateStoreBaseImpl {

  /** Mark for slashes in path names. */
  protected static final String SLASH_MARK = "0SLASH0";
  /** Mark for colon in path names. */
  protected static final String COLON_MARK = "_";

  /** Default serializer for this driver. */
  private StateStoreSerializer serializer;


  @Override
  public boolean init(final Configuration config, final String id,
      final Collection<Class<? extends BaseRecord>> records,
      final StateStoreMetrics metrics) {
    boolean ret = super.init(config, id, records, metrics);

    this.serializer = StateStoreSerializer.getSerializer(config);

    return ret;
  }

  /**
   * Serialize a record using the serializer.
   *
   * @param record Record to serialize.
   * @param <T> Type of the state store record.
   * @return Byte array with the serialization of the record.
   */
  protected <T extends BaseRecord> byte[] serialize(T record) {
    return serializer.serialize(record);
  }

  /**
   * Serialize a record using the serializer.
   *
   * @param record Record to serialize.
   * @param <T> Type of the state store record.
   * @return String with the serialization of the record.
   */
  protected <T extends BaseRecord> String serializeString(T record) {
    return serializer.serializeString(record);
  }

  /**
   * Creates a record from an input data string.
   *
   * @param data Serialized text of the record.
   * @param clazz Record class.
   * @param includeDates If dateModified and dateCreated are serialized.
   * @param <T> Type of the state store record.
   * @return The created record by deserializing the input text.
   * @throws IOException If the record deserialization fails.
   */
  protected <T extends BaseRecord> T newRecord(
      String data, Class<T> clazz, boolean includeDates) throws IOException {
    return serializer.deserialize(data, clazz);
  }

  /**
   * Get the primary key for a record. If we don't want to store in folders, we
   * need to remove / from the name.
   *
   * @param record Record to get the primary key for.
   * @return Primary key for the record.
   */
  protected static String getPrimaryKey(BaseRecord record) {
    String primaryKey = record.getPrimaryKey();
    primaryKey = primaryKey.replaceAll("/", SLASH_MARK);
    primaryKey = primaryKey.replaceAll(":", COLON_MARK);
    return primaryKey;
  }

  /**
   * Get the original primary key for the given state store record key. The returned
   * key is readable as it is the original key.
   *
   * @param stateStoreRecordKey The record primary key stored by the state store implementations.
   * @return The original primary key for the given record key.
   */
  protected static String getOriginalPrimaryKey(String stateStoreRecordKey) {
    Objects.requireNonNull(stateStoreRecordKey,
        "state store record key provided to getOriginalPrimaryKey should not be null");
    stateStoreRecordKey = stateStoreRecordKey.replaceAll(SLASH_MARK, "/");
    stateStoreRecordKey = stateStoreRecordKey.replaceAll(COLON_MARK, ":");
    return stateStoreRecordKey;
  }

}