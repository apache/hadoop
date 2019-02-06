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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents the entity table column families.
 */
public enum EntityColumnFamily implements ColumnFamily<EntityTable> {

  /**
   * Info column family houses known columns, specifically ones included in
   * columnfamily filters.
   */
  INFO("i"),

  /**
   * Configurations are in a separate column family for two reasons: a) the size
   * of the config values can be very large and b) we expect that config values
   * are often separately accessed from other metrics and info columns.
   */
  CONFIGS("c"),

  /**
   * Metrics have a separate column family, because they have a separate TTL.
   */
  METRICS("m");

  /**
   * Byte representation of this column family.
   */
  private final byte[] bytes;

  /**
   * @param value create a column family with this name. Must be lower case and
   *          without spaces.
   */
  EntityColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}
