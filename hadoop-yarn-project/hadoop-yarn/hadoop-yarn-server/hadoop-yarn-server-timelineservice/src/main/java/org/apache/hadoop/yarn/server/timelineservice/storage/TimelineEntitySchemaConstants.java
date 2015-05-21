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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * contains the constants used in the context of schema accesses for
 * {@link org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity}
 * information
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TimelineEntitySchemaConstants {

  /** entity prefix */
  public static final String ENTITY_PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX
      + ".entity";

  /** config param name that specifies the entity table name */
  public static final String ENTITY_TABLE_NAME = ENTITY_PREFIX
      + ".table.name";

  /**
   * config param name that specifies the TTL for metrics column family in
   * entity table
   */
  public static final String ENTITY_TABLE_METRICS_TTL = ENTITY_PREFIX
      + ".table.metrics.ttl";

  /** default value for entity table name */
  public static final String DEFAULT_ENTITY_TABLE_NAME = "timelineservice.entity";

  /** in bytes default value for entity table name */
  static final byte[] DEFAULT_ENTITY_TABLE_NAME_BYTES = Bytes
      .toBytes(DEFAULT_ENTITY_TABLE_NAME);

  /** separator in row key */
  public static final String ROW_KEY_SEPARATOR = "!";

  /** byte representation of the separator in row key */
  static final byte[] ROW_KEY_SEPARATOR_BYTES = Bytes
      .toBytes(ROW_KEY_SEPARATOR);

  public static final byte ZERO_BYTES = 0;

  /** default TTL is 30 days for metrics timeseries */
  public static final int ENTITY_TABLE_METRICS_TTL_DEFAULT = 2592000;

  /** default max number of versions */
  public static final int ENTITY_TABLE_METRICS_MAX_VERSIONS_DEFAULT = 1000;
}