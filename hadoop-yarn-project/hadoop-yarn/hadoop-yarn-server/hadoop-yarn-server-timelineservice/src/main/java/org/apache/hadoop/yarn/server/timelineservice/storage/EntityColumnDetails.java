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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Contains the Info Column Family details like Column names, types and byte
 * representations for
 * {@link org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity}
 * object that is stored in hbase Also has utility functions for storing each of
 * these to the backend
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
enum EntityColumnDetails {
  ID(EntityColumnFamily.INFO, "id"),
  TYPE(EntityColumnFamily.INFO, "type"),
  CREATED_TIME(EntityColumnFamily.INFO, "created_time"),
  MODIFIED_TIME(EntityColumnFamily.INFO, "modified_time"),
  FLOW_VERSION(EntityColumnFamily.INFO, "flow_version"),
  PREFIX_IS_RELATED_TO(EntityColumnFamily.INFO, "r"),
  PREFIX_RELATES_TO(EntityColumnFamily.INFO, "s"),
  PREFIX_EVENTS(EntityColumnFamily.INFO, "e");

  private final EntityColumnFamily columnFamily;
  private final String value;
  private final byte[] inBytes;

  private EntityColumnDetails(EntityColumnFamily columnFamily, 
      String value) {
    this.columnFamily = columnFamily;
    this.value = value;
    this.inBytes = Bytes.toBytes(this.value.toLowerCase());
  }

  public String getValue() {
    return value;
  }

  byte[] getInBytes() {
    return inBytes;
  }

  void store(byte[] rowKey, BufferedMutator entityTable, Object inputValue)
      throws IOException {
    TimelineWriterUtils.store(rowKey, entityTable,
        this.columnFamily.getInBytes(), null, this.getInBytes(), inputValue,
        null);
  }

  /**
   * stores events data with column prefix
   */
  void store(byte[] rowKey, BufferedMutator entityTable, byte[] idBytes,
      String key, Object inputValue) throws IOException {
    TimelineWriterUtils.store(rowKey, entityTable,
        this.columnFamily.getInBytes(),
        // column prefix
        TimelineWriterUtils.join(
            TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR_BYTES,
            this.getInBytes(), idBytes),
        // column qualifier
        Bytes.toBytes(key),
        inputValue, null);
  }

  /**
   * stores relation entities with a column prefix
   */
  void store(byte[] rowKey, BufferedMutator entityTable, String key,
      Set<String> inputValue) throws IOException {
    TimelineWriterUtils.store(rowKey, entityTable,
        this.columnFamily.getInBytes(),
        // column prefix
        this.getInBytes(),
        // column qualifier
        Bytes.toBytes(key),
        // value
        TimelineWriterUtils.getValueAsString(
            TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR, inputValue),
        // cell timestamp
        null);
  }

  // TODO add a method that accepts a byte array,
  // iterates over the enum and returns an enum from those bytes

}
