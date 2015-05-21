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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Contains the Column family names and byte representations for
 * {@link org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity}
 * object that is stored in hbase
 * Also has utility functions for storing each of these to the backend
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
enum EntityColumnFamily {
  INFO("i"),
  CONFIG("c"),
  METRICS("m");

  private final String value;
  private final byte[] inBytes;

  private EntityColumnFamily(String value) {
    this.value = value;
    this.inBytes = Bytes.toBytes(this.value.toLowerCase());
  }

  byte[] getInBytes() {
    return inBytes;
  }

  public String getValue() {
    return value;
  }

  /**
   * stores the key as column and value as hbase column value in the given
   * column family in the entity table
   *
   * @param rowKey
   * @param entityTable
   * @param inputValue
   * @throws IOException
   */
  public void store(byte[] rowKey, BufferedMutator entityTable, String key,
      String inputValue) throws IOException {
    if (key == null) {
      return;
    }
    TimelineWriterUtils.store(rowKey, entityTable, this.getInBytes(), null,
        Bytes.toBytes(key), inputValue, null);
  }

  /**
   * stores the values along with cell timestamp
   *
   * @param rowKey
   * @param entityTable
   * @param key
   * @param timestamp
   * @param inputValue
   * @throws IOException
   */
  public void store(byte[] rowKey, BufferedMutator entityTable, String key,
      Long timestamp, Number inputValue) throws IOException {
    if (key == null) {
      return;
    }
    TimelineWriterUtils.store(rowKey, entityTable, this.getInBytes(), null,
        Bytes.toBytes(key), inputValue, timestamp);
  }

  // TODO add a method that accepts a byte array,
  // iterates over the enum and returns an enum from those bytes
}
